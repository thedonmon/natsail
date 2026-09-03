import { createNatsailTelemetryReporter } from '@natsail/core'
import type {
  NatsailTelemetryAttributes,
  NatsailTelemetryClock,
  NatsailTelemetryReporter,
  NatsailTelemetrySink,
  SubscriptionLease,
} from '@natsail/core'

import {
  BrowserBrokerError,
  BrowserBrokerResumeRequiredError,
  byteArray,
  cloneIdentity,
  copyBuffer,
  copyCursor,
  envelope,
  parseBrowserBrokerMessage,
  parseCredentialSnapshot,
  parseIdentity,
  parseSource,
  parseStats,
  stringField,
  type BrowserBrokerBatchMessage,
  type BrowserBrokerCommand,
  type BrowserBrokerCredentialSnapshot,
  type BrowserBrokerCursor,
  type BrowserBrokerDelivery,
  type BrowserBrokerIdentity,
  type BrowserBrokerMessage,
  type BrowserBrokerSourceDescriptor,
  type BrowserBrokerStats,
} from './protocol.js'
import type { BrowserBrokerWorkerHost } from './worker.js'
import { nonNegativeInteger, positiveInteger } from './validation.js'

export type BrowserBrokerPortConnector = () => MessagePort | Promise<MessagePort>

const tabLocalBrokerHosts = new WeakMap<MessagePort, BrowserBrokerWorkerHost>()

export interface BrowserBrokerClientOptions {
  readonly identity: BrowserBrokerIdentity
  readonly credentials: () =>
    | BrowserBrokerCredentialSnapshot
    | Promise<BrowserBrokerCredentialSnapshot>
  /** Opens a MessagePort to the SharedWorker. */
  readonly connect: BrowserBrokerPortConnector
  /** Explicit tab-local connector used only when `connect` fails and strict mode is disabled. */
  readonly fallback?: BrowserBrokerPortConnector
  readonly strict?: boolean
  /** Heartbeat interval. Set to 0 for tests or externally managed heartbeats. Defaults to 10 seconds. */
  readonly heartbeatIntervalMs?: number
  /** Request timeout used to surface an unavailable/restarted worker. Defaults to 5 seconds. */
  readonly requestTimeoutMs?: number
  readonly telemetry?: NatsailTelemetrySink
  readonly telemetryAttributes?: NatsailTelemetryAttributes
  readonly telemetryClock?: NatsailTelemetryClock
}

export interface BrowserBrokerClient {
  readonly mode: 'shared-worker' | 'tab-local'
  connect(): Promise<void>
  createSource(descriptor: BrowserBrokerSourceDescriptor): BrowserBrokerSessionSource
  /** Publishes through the worker's application-authorized operation mapping. */
  publish(operation: string, data?: Uint8Array): Promise<void>
  /** Requests through the worker's application-authorized operation mapping. */
  request(operation: string, data?: Uint8Array): Promise<Uint8Array>
  refreshCredentials(): Promise<void>
  restart(subscriptionId: string): Promise<void>
  reconnect(): Promise<void>
  stats(): Promise<BrowserBrokerStats>
  close(): Promise<void>
}

export interface BrowserBrokerSubscriptionLease extends SubscriptionLease {
  readonly subscriptionId: string
  restart(): Promise<void>
}

export type BrowserBrokerSessionSource = (
  accept: (value: BrowserBrokerDelivery) => Promise<void>
) => BrowserBrokerSubscriptionLease

type Deferred<T> = {
  readonly promise: Promise<T>
  readonly resolve: (value: T | PromiseLike<T>) => void
  readonly reject: (reason?: unknown) => void
}

function deferred<T>(): Deferred<T> {
  let resolve!: Deferred<T>['resolve']
  let reject!: Deferred<T>['reject']
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, resolve, reject }
}

interface PendingClientRequest {
  readonly resolve: (value: unknown) => void
  readonly reject: (error: unknown) => void
  readonly timer: ReturnType<typeof setTimeout>
}

class ClientSourceLease implements BrowserBrokerSubscriptionLease {
  readonly ready: Promise<void>
  readonly closed: Promise<void>
  readonly subscriptionId: string
  readonly descriptor: BrowserBrokerSourceDescriptor
  lastCursor: BrowserBrokerCursor | undefined

  private readonly readyState = deferred<void>()
  private readonly closedState = deferred<void>()
  private acceptChain = Promise.resolve()
  private readySettled = false
  private closedSettled = false
  private terminal = false

  constructor(
    private readonly client: DefaultBrowserBrokerClient,
    descriptor: BrowserBrokerSourceDescriptor,
    private readonly accept: (value: BrowserBrokerDelivery) => Promise<void>
  ) {
    this.subscriptionId = `subscription-${client.nextSubscriptionNumber()}`
    this.descriptor = Object.freeze({ ...descriptor })
    this.ready = this.readyState.promise
    this.closed = this.closedState.promise
    void this.closed.catch(() => undefined)
    void this.attach()
  }

  close(): Promise<void> {
    return this.client.detachLease(this)
  }

  restart(): Promise<void> {
    return this.client.restart(this.subscriptionId)
  }

  attached(): void {
    if (!this.readySettled) {
      this.readySettled = true
      this.readyState.resolve()
    }
  }

  receive(batch: BrowserBrokerBatchMessage, generation: number): void {
    this.acceptChain = this.acceptChain
      .then(async () => {
        let cursor: BrowserBrokerCursor | undefined
        for (const item of batch.items) {
          if (this.terminal || !this.client.isCurrentGeneration(generation)) return
          cursor = copyCursor(item.cursor)
          await this.accept({
            data: new Uint8Array(item.data),
            ...(cursor === undefined ? {} : { cursor }),
          })
          if (this.terminal || !this.client.isCurrentGeneration(generation)) return
        }
        this.lastCursor = copyCursor(cursor)
        await this.client.ack(this, batch.batchId, cursor, generation)
      })
      .catch((error) => {
        // A replaced port rejects its pending acknowledgements. That failure
        // belongs to the old worker generation; the reattached lease remains live.
        if (this.client.isCurrentGeneration(generation)) this.client.failLease(this, error)
      })
  }

  fail(error: unknown): void {
    this.terminal = true
    if (!this.readySettled) {
      this.readySettled = true
      this.readyState.reject(error)
    }
    if (!this.closedSettled) {
      this.closedSettled = true
      this.closedState.reject(error)
    }
  }

  finish(): void {
    this.terminal = true
    if (!this.readySettled) {
      this.readySettled = true
      this.readyState.resolve()
    }
    if (!this.closedSettled) {
      this.closedSettled = true
      this.closedState.resolve()
    }
  }

  async reattach(): Promise<void> {
    if (this.terminal) return
    await this.client.attachLease(this, true)
  }

  private async attach(): Promise<void> {
    try {
      await this.client.attachLease(this)
      this.attached()
    } catch (error) {
      this.client.failLease(this, error)
    }
  }
}

class DefaultBrowserBrokerClient implements BrowserBrokerClient {
  mode: 'shared-worker' | 'tab-local' = 'shared-worker'
  private readonly telemetry: NatsailTelemetryReporter
  private readonly pending = new Map<number, PendingClientRequest>()
  private readonly leases = new Map<string, ClientSourceLease>()
  private readonly heartbeatIntervalMs: number
  private readonly requestTimeoutMs: number
  private port: MessagePort | undefined
  private localHost: BrowserBrokerWorkerHost | undefined
  private heartbeatTimer: ReturnType<typeof setInterval> | undefined
  private requestNumber = 1
  private subscriptionNumber = 1
  private connectPromise: Promise<void> | undefined
  private connectionGeneration = 0
  private closed = false

  constructor(private readonly options: BrowserBrokerClientOptions) {
    this.heartbeatIntervalMs = nonNegativeInteger(
      options.heartbeatIntervalMs,
      10_000,
      'heartbeatIntervalMs'
    )
    this.requestTimeoutMs = positiveInteger(options.requestTimeoutMs, 5_000, 'requestTimeoutMs')
    this.telemetry = createNatsailTelemetryReporter({
      ...(options.telemetry === undefined ? {} : { sink: options.telemetry }),
      ...(options.telemetryClock === undefined ? {} : { clock: options.telemetryClock }),
      ...(options.telemetryAttributes === undefined
        ? {}
        : { attributes: options.telemetryAttributes }),
    })
  }

  async connect(): Promise<void> {
    this.assertOpen()
    this.connectPromise ??= this.establish(false).finally(() => {
      this.connectPromise = undefined
    })
    return this.connectPromise
  }

  createSource(descriptor: BrowserBrokerSourceDescriptor): BrowserBrokerSessionSource {
    this.assertOpen()
    const parsed = Object.freeze(parseSource(descriptor))
    return (accept) => new ClientSourceLease(this, parsed, accept)
  }

  async refreshCredentials(): Promise<void> {
    this.assertOpen()
    const credentials = parseCredentialSnapshot(await this.options.credentials())
    const buffer = copyBuffer(credentials.bytes)
    await this.sendCommand(
      {
        ...envelope(),
        type: 'refresh-credentials',
        requestId: this.nextRequestId(),
        credentialRevision: credentials.revision,
        credentials: buffer,
      },
      [buffer]
    )
  }

  async publish(operation: string, data: Uint8Array = new Uint8Array(0)): Promise<void> {
    this.assertOpen()
    const buffer = copyBuffer(byteArray(data, 'data'))
    await this.sendCommand(
      {
        ...envelope(),
        type: 'publish',
        requestId: this.nextRequestId(),
        operation: stringField(operation, 'operation'),
        data: buffer,
      },
      [buffer]
    )
  }

  async request(operation: string, data: Uint8Array = new Uint8Array(0)): Promise<Uint8Array> {
    this.assertOpen()
    const buffer = copyBuffer(byteArray(data, 'data'))
    const result = await this.sendCommand(
      {
        ...envelope(),
        type: 'request',
        requestId: this.nextRequestId(),
        operation: stringField(operation, 'operation'),
        data: buffer,
      },
      [buffer]
    )
    if (Object.prototype.toString.call(result) !== '[object ArrayBuffer]') {
      throw new BrowserBrokerError('invalid-state', 'Brokered request returned invalid bytes')
    }
    return new Uint8Array(result as ArrayBuffer)
  }

  restart(subscriptionId: string): Promise<void> {
    this.assertOpen()
    return this.sendCommand({
      ...envelope(),
      type: 'restart',
      requestId: this.nextRequestId(),
      subscriptionId,
    }).then(() => undefined)
  }

  async reconnect(): Promise<void> {
    if (this.closed)
      throw new BrowserBrokerError('invalid-state', 'The browser broker client is closed')
    this.connectPromise ??= this.establish(true).finally(() => {
      this.connectPromise = undefined
    })
    await this.connectPromise
  }

  async stats(): Promise<BrowserBrokerStats> {
    this.assertOpen()
    return parseStats(
      await this.sendCommand({
        ...envelope(),
        type: 'stats',
        requestId: this.nextRequestId(),
      })
    )
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer)
    if (this.port) {
      await this.sendCommand({
        ...envelope(),
        type: 'close',
        requestId: this.nextRequestId(),
      }).catch(() => undefined)
      this.port.close()
      this.port = undefined
    }
    const localHost = this.localHost
    this.localHost = undefined
    await localHost?.close().catch(() => undefined)
    for (const lease of this.leases.values()) lease.finish()
    this.leases.clear()
    this.rejectPending(new BrowserBrokerError('unavailable', 'The browser broker client closed'))
  }

  nextSubscriptionNumber(): number {
    const value = this.subscriptionNumber
    this.subscriptionNumber += 1
    return value
  }

  async attachLease(lease: ClientSourceLease, reattach = false): Promise<void> {
    this.assertOpen()
    if (!this.port) await this.connect()
    this.leases.set(lease.subscriptionId, lease)
    try {
      await this.sendCommand({
        ...envelope(),
        type: 'attach',
        requestId: this.nextRequestId(),
        subscriptionId: lease.subscriptionId,
        source: lease.descriptor,
        reattach,
        ...(lease.lastCursor === undefined ? {} : { resumeAfter: copyCursor(lease.lastCursor) }),
      })
    } catch (error) {
      if (
        error instanceof BrowserBrokerError &&
        error.code !== 'unavailable' &&
        this.leases.get(lease.subscriptionId) === lease
      ) {
        this.leases.delete(lease.subscriptionId)
      }
      throw error
    }
  }

  async detachLease(lease: ClientSourceLease): Promise<void> {
    if (this.leases.get(lease.subscriptionId) !== lease) {
      lease.finish()
      return
    }
    this.leases.delete(lease.subscriptionId)
    if (this.port) {
      await this.sendCommand({
        ...envelope(),
        type: 'detach',
        requestId: this.nextRequestId(),
        subscriptionId: lease.subscriptionId,
      }).catch(() => undefined)
    }
    lease.finish()
  }

  ack(
    lease: ClientSourceLease,
    batchId: number,
    cursor: BrowserBrokerCursor | undefined,
    generation: number
  ): Promise<void> {
    if (!this.isCurrentGeneration(generation)) return Promise.resolve()
    return this.sendCommand({
      ...envelope(),
      type: 'ack',
      requestId: this.nextRequestId(),
      subscriptionId: lease.subscriptionId,
      batchId,
      ...(cursor === undefined ? {} : { cursor: copyCursor(cursor) }),
    }).then(() => undefined)
  }

  private async establish(reconnect: boolean): Promise<void> {
    this.assertOpen()
    const generation = this.connectionGeneration + 1
    this.connectionGeneration = generation
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer)
    const oldPort = this.port
    const oldLocalHost = this.localHost
    this.port = undefined
    this.localHost = undefined
    if (oldPort) {
      oldPort.onmessage = null
      oldPort.onmessageerror = null
      oldPort.close()
    }
    await oldLocalHost?.close().catch(() => undefined)
    this.rejectPending(new BrowserBrokerError('unavailable', 'The browser broker port changed'))

    let port: MessagePort
    try {
      port = await this.options.connect()
      this.mode = 'shared-worker'
    } catch (error) {
      if (this.options.strict || !this.options.fallback) {
        const unavailable = new BrowserBrokerError(
          'unavailable',
          `SharedWorker broker is unavailable: ${error instanceof Error ? error.message : String(error)}`
        )
        if (reconnect) {
          for (const lease of [...this.leases.values()]) this.failLease(lease, unavailable)
        }
        throw unavailable
      }
      try {
        port = await this.options.fallback()
      } catch (fallbackError) {
        const unavailable = new BrowserBrokerError(
          'unavailable',
          `The tab-local broker fallback is unavailable: ${fallbackError instanceof Error ? fallbackError.message : String(fallbackError)}`
        )
        if (reconnect) {
          for (const lease of [...this.leases.values()]) this.failLease(lease, unavailable)
        }
        throw unavailable
      }
      this.mode = 'tab-local'
      this.telemetryEvent('fallback')
    }
    const localHost = tabLocalBrokerHosts.get(port)
    try {
      this.assertOpen()
      this.port = port
      this.localHost = localHost
      port.onmessage = (event: MessageEvent<unknown>) => this.receive(event.data, generation)
      port.onmessageerror = () => {
        if (!this.isCurrentGeneration(generation)) return
        void this.reconnect().catch((error) => {
          for (const lease of [...this.leases.values()]) this.failLease(lease, error)
        })
      }
      port.start()

      const credentials = parseCredentialSnapshot(await this.options.credentials())
      const buffer = copyBuffer(credentials.bytes)
      await this.sendCommand(
        {
          ...envelope(),
          type: 'hello',
          requestId: this.nextRequestId(),
          identity: cloneIdentity(this.options.identity),
          credentialRevision: credentials.revision,
          credentials: buffer,
        },
        [buffer]
      )
      if (reconnect) {
        this.telemetryEvent('worker-reconnect')
        for (const lease of [...this.leases.values()]) {
          try {
            await lease.reattach()
          } catch (error) {
            if (error instanceof BrowserBrokerError && error.code === 'unavailable') throw error
            this.failLease(lease, error)
          }
        }
      }
      if (this.heartbeatIntervalMs > 0) {
        this.heartbeatTimer = setInterval(() => {
          if (!this.port) return
          void this.sendCommand({
            ...envelope(),
            type: 'heartbeat',
            requestId: this.nextRequestId(),
          }).catch(() => undefined)
        }, this.heartbeatIntervalMs)
      }
    } catch (error) {
      if (this.port === port) this.port = undefined
      if (this.localHost === localHost) this.localHost = undefined
      port.onmessage = null
      port.onmessageerror = null
      port.close()
      await localHost?.close().catch(() => undefined)
      if (reconnect) {
        for (const lease of [...this.leases.values()]) this.failLease(lease, error)
      }
      throw error
    }
  }

  private receive(value: unknown, generation: number): void {
    if (generation !== this.connectionGeneration) return
    let message: BrowserBrokerMessage
    try {
      message = parseBrowserBrokerMessage(value)
    } catch (error) {
      for (const lease of [...this.leases.values()]) this.failLease(lease, error)
      return
    }
    if (message.type === 'result') {
      const pending = this.pending.get(message.requestId)
      if (!pending) return
      clearTimeout(pending.timer)
      this.pending.delete(message.requestId)
      if (message.ok) {
        pending.resolve(message.result)
      } else {
        pending.reject(
          new BrowserBrokerError(
            message.error?.code ?? 'invalid-state',
            message.error?.message ?? 'Browser broker request failed'
          )
        )
      }
      return
    }
    if (this.closed) return
    const lease = this.leases.get(message.subscriptionId)
    if (!lease) return
    if (message.type === 'batch') {
      lease.receive(message, generation)
      return
    }
    if (message.state === 'live') {
      return
    } else if (message.state === 'resume-required') {
      this.telemetryEvent('tab-lagged')
      this.failLease(lease, new BrowserBrokerResumeRequiredError(message.cursor))
    } else if (message.state === 'error') {
      this.failLease(
        lease,
        new BrowserBrokerError('source-failed', 'The physical broker source failed')
      )
    } else if (message.state === 'closed') {
      void this.detachLease(lease).catch(() => lease.finish())
    }
  }

  failLease(lease: ClientSourceLease, error: unknown): void {
    lease.fail(error)
    void this.detachLease(lease).catch(() => undefined)
  }

  private sendCommand(
    command: BrowserBrokerCommand,
    transfers: Transferable[] = []
  ): Promise<unknown> {
    if (!this.port) {
      return Promise.reject(new BrowserBrokerError('unavailable', 'No browser broker port exists'))
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(command.requestId)
        const error = new BrowserBrokerError('unavailable', 'The browser broker request timed out')
        reject(error)
        if (command.type !== 'hello' && !this.closed) {
          void this.reconnect().catch((reconnectError) => {
            for (const lease of [...this.leases.values()]) {
              this.failLease(lease, reconnectError)
            }
          })
        }
      }, this.requestTimeoutMs)
      this.pending.set(command.requestId, { resolve, reject, timer })
      try {
        this.port!.postMessage(command, transfers)
      } catch (error) {
        clearTimeout(timer)
        this.pending.delete(command.requestId)
        const unavailable = new BrowserBrokerError(
          'unavailable',
          `The browser broker request could not be sent: ${error instanceof Error ? error.message : String(error)}`
        )
        reject(unavailable)
        if (command.type !== 'hello' && !this.closed) {
          void this.reconnect().catch((reconnectError) => {
            for (const lease of [...this.leases.values()]) {
              this.failLease(lease, reconnectError)
            }
          })
        }
      }
    })
  }

  private rejectPending(error: BrowserBrokerError): void {
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer)
      pending.reject(error)
    }
    this.pending.clear()
  }

  private nextRequestId(): number {
    const id = this.requestNumber
    this.requestNumber += 1
    return id
  }

  isCurrentGeneration(generation: number): boolean {
    return !this.closed && generation === this.connectionGeneration
  }

  private assertOpen(): void {
    if (this.closed) {
      throw new BrowserBrokerError('invalid-state', 'The browser broker client is closed')
    }
  }

  private telemetryEvent(action: string): void {
    if (!this.telemetry.enabled) return
    this.telemetry.record({
      type: 'counter',
      name: 'natsail.browser.broker.events',
      value: 1,
      at: this.telemetry.now(),
      attributes: { action, source: 'browser-broker' },
    })
  }
}

/** Creates and authenticates a protocol-v1 tab client. */
export async function createBrowserBrokerClient(
  options: BrowserBrokerClientOptions
): Promise<BrowserBrokerClient> {
  const client = new DefaultBrowserBrokerClient({
    ...options,
    identity: Object.freeze(parseIdentity(options.identity)),
  })
  await client.connect()
  return client
}

/** Standard SharedWorker connector. Construction stays in the tab, where credentials originate. */
export function createSharedWorkerConnector(
  url: string | URL,
  options: { readonly name?: string; readonly type?: WorkerType } = {}
): BrowserBrokerPortConnector {
  return () => {
    const Constructor = (globalThis as typeof globalThis & { SharedWorker?: typeof SharedWorker })
      .SharedWorker
    if (!Constructor) throw new BrowserBrokerError('unavailable', 'SharedWorker is unavailable')
    return new Constructor(url, {
      type: options.type ?? 'module',
      ...(options.name === undefined ? {} : { name: options.name }),
    }).port
  }
}

/**
 * Explicit non-strict fallback. The host and its runtime live only in the current
 * tab, but use the same validated protocol and SessionSource path.
 */
export function createTabLocalBrokerConnector(
  createHost: () => BrowserBrokerWorkerHost
): BrowserBrokerPortConnector {
  return async () => {
    const channel = new MessageChannel()
    const host = createHost()
    try {
      host.connect(channel.port1)
      tabLocalBrokerHosts.set(channel.port2, host)
      return channel.port2
    } catch (error) {
      channel.port1.close()
      channel.port2.close()
      await host.close().catch(() => undefined)
      throw error
    }
  }
}
