import type { Msg, NatsConnection, PublishOptions, Status, Subscription } from '@nats-io/nats-core'

export type MessageHandler<T> = (value: T, message: Msg) => void | Promise<void>

export interface CoreSubscriptionOptions<T> {
  subject: string
  queue?: string
  signal?: AbortSignal
  decode: (message: Msg) => T
}

export interface SubscriptionLease {
  /** Resolves after NATS registers the subscription. */
  readonly ready: Promise<void>
  /** Resolves when the subscription stops. Rejects after a processing error. */
  readonly closed: Promise<void>
  /** Stops delivery. Calls after the first call have no effect. */
  close(): Promise<void>
}

export interface RuntimeResource {
  readonly closed: Promise<void>
  close(): Promise<void>
}

export interface RuntimeResourceAllocation {
  jetStreamConsumers?: number
  bufferedMessages?: number
}

export interface NatsRuntimeAdapter {
  /** Registers an optional adapter resource with the runtime lifecycle. */
  manage<T extends RuntimeResource>(create: () => T, allocation?: RuntimeResourceAllocation): T
  /** Reports structured diagnostics from optional runtime adapters. */
  reportDiagnostic(diagnostic: NatsRuntimeDiagnostic): void
}

/** Adapter packages use this symbol to join the runtime lifecycle. */
export const NATS_RUNTIME_ADAPTER: unique symbol = Symbol('@natsail/core/runtime-adapter')

export type NatsRuntimeConnectionState =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'disconnected'
  | 'reconnecting'
  | 'closed'

export interface NatsRuntimeStatusEvent {
  readonly type: 'status'
  readonly state: NatsRuntimeConnectionState
  readonly at: number
  readonly server?: string
}

export type NatsRuntimeDiagnosticSource = 'connection' | 'jetstream' | 'runtime'
export type NatsRuntimeDiagnosticLevel = 'info' | 'warning' | 'error'

export interface NatsRuntimeDiagnosticEvent {
  readonly type: 'diagnostic'
  readonly source: NatsRuntimeDiagnosticSource
  readonly code: string
  readonly level: NatsRuntimeDiagnosticLevel
  readonly message: string
  readonly at: number
  readonly error?: Error
  readonly details?: Readonly<Record<string, unknown>>
}

export type NatsRuntimeDiagnostic = Omit<NatsRuntimeDiagnosticEvent, 'at' | 'type'>
export type NatsRuntimeEvent = NatsRuntimeStatusEvent | NatsRuntimeDiagnosticEvent

export type NatsRuntimeLimitCode = 'jetstream-consumers' | 'buffered-messages'

export class NatsRuntimeLimitError extends Error {
  readonly name = 'NatsRuntimeLimitError'

  constructor(
    readonly code: NatsRuntimeLimitCode,
    readonly limit: number,
    readonly used: number,
    readonly requested: number
  ) {
    super(
      `NATS runtime ${code} limit ${limit} cannot satisfy ${requested} with ${used} already reserved`
    )
  }
}

export interface NatsRuntime {
  readonly [NATS_RUNTIME_ADAPTER]: NatsRuntimeAdapter
  /** Multicasts current connection state and future structured diagnostics. */
  readonly events: AsyncIterable<NatsRuntimeEvent>
  /** Returns the connection shared by every runtime feature. */
  connection(): Promise<NatsConnection>
  /** Publishes through the shared connection. */
  publish(subject: string, data?: Uint8Array, options?: PublishOptions): Promise<void>
  /** Creates an ephemeral Core NATS subscription. */
  subscribe<T>(options: CoreSubscriptionOptions<T>, handler: MessageHandler<T>): SubscriptionLease
  /** Stops every subscription and drains the owned NATS connection. */
  close(): Promise<void>
}

export interface NatsRuntimeOptions {
  /** Creates the connection owned by this runtime. Called once per connection attempt. */
  connect: () => Promise<NatsConnection>
  /** Optional bounded retry policy for each initial connection series. */
  initialConnectRetry?: NatsRuntimeInitialConnectRetryOptions
  /** Optional connection-wide limits shared by every runtime adapter. */
  limits?: NatsRuntimeLimits
}

export interface NatsRuntimeInitialConnectRetryOptions {
  /** Total attempts in one connection series, including the first. Defaults to 1. */
  maxAttempts?: number
  /** Delay between attempts in milliseconds. Defaults to 1,000. */
  delayMs?: number
}

export interface NatsRuntimeLimits {
  /** Maximum active ordered JetStream consumers. Unbounded by default. */
  maxJetStreamConsumers?: number
  /** Maximum aggregate nats.js pull-buffer message capacity. Unbounded by default. */
  maxBufferedMessages?: number
}

type Deferred<T> = {
  promise: Promise<T>
  resolve: (value: T | PromiseLike<T>) => void
  reject: (reason?: unknown) => void
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

type RuntimeEventSubscriber = {
  readonly queue: NatsRuntimeEvent[]
  waiting: Deferred<IteratorResult<NatsRuntimeEvent, undefined>> | undefined
  closed: boolean
}

class RuntimeEventStream implements AsyncIterable<NatsRuntimeEvent> {
  [Symbol.asyncIterator](): AsyncIterator<NatsRuntimeEvent> {
    const subscriber: RuntimeEventSubscriber = {
      queue: [this.currentStatus],
      waiting: undefined,
      closed: this.closed,
    }
    if (!subscriber.closed) {
      this.subscribers.add(subscriber)
    }

    return {
      next: () => {
        const event = subscriber.queue.shift()
        if (event) {
          return Promise.resolve({ done: false, value: event })
        }
        if (subscriber.closed) {
          return Promise.resolve({ done: true, value: undefined })
        }

        const waiting = deferred<IteratorResult<NatsRuntimeEvent, undefined>>()
        subscriber.waiting = waiting
        return waiting.promise
      },
      return: async () => {
        this.remove(subscriber)
        return { done: true, value: undefined }
      },
    }
  }

  private currentStatus: NatsRuntimeStatusEvent = {
    type: 'status',
    state: 'idle',
    at: Date.now(),
  }
  private readonly subscribers = new Set<RuntimeEventSubscriber>()
  private closed = false

  setStatus(state: NatsRuntimeConnectionState, server?: string): void {
    if (
      this.currentStatus.state === state &&
      this.currentStatus.server === server &&
      !(state === 'closed' && !this.closed)
    ) {
      return
    }

    this.currentStatus = {
      type: 'status',
      state,
      at: Date.now(),
      ...(server === undefined ? {} : { server }),
    }
    this.emit(this.currentStatus)

    if (state === 'closed') {
      this.closed = true
      for (const subscriber of this.subscribers) {
        subscriber.closed = true
      }
      this.subscribers.clear()
    }
  }

  diagnostic(event: NatsRuntimeDiagnostic): void {
    this.emit({ type: 'diagnostic', at: Date.now(), ...event })
  }

  private emit(event: NatsRuntimeEvent): void {
    if (this.closed) return

    for (const subscriber of this.subscribers) {
      if (subscriber.waiting) {
        subscriber.waiting.resolve({ done: false, value: event })
        subscriber.waiting = undefined
      } else {
        subscriber.queue.push(event)
      }
    }
  }

  private remove(subscriber: RuntimeEventSubscriber): void {
    subscriber.closed = true
    this.subscribers.delete(subscriber)
    subscriber.waiting?.resolve({ done: true, value: undefined })
    subscriber.waiting = undefined
  }
}

class CoreSubscription<T> implements SubscriptionLease {
  readonly ready: Promise<void>
  readonly closed: Promise<void>

  private readonly readyState = deferred<void>()
  private readonly closedState = deferred<void>()
  private subscription?: Subscription
  private closeRequested = false
  private readySettled = false

  constructor(
    connection: Promise<NatsConnection>,
    private readonly options: CoreSubscriptionOptions<T>,
    private readonly handler: MessageHandler<T>
  ) {
    this.ready = this.readyState.promise
    this.closed = this.closedState.promise

    // Callers can observe the original promise. This handler only prevents an
    // ignored processing error from becoming an unhandled rejection.
    void this.closed.catch(() => undefined)
    void this.start(connection)
  }

  async close(): Promise<void> {
    if (!this.closeRequested) {
      this.closeRequested = true
      this.subscription?.unsubscribe()
    }

    return this.closed
  }

  private async start(connectionPromise: Promise<NatsConnection>): Promise<void> {
    let abort: (() => void) | undefined

    try {
      const connection = await connectionPromise

      if (this.closeRequested || this.options.signal?.aborted) {
        this.resolveReady()
        this.closedState.resolve()
        return
      }

      this.subscription = this.options.queue
        ? connection.subscribe(this.options.subject, { queue: this.options.queue })
        : connection.subscribe(this.options.subject)

      abort = () => {
        void this.close().catch(() => undefined)
      }
      this.options.signal?.addEventListener('abort', abort, { once: true })
      this.resolveReady()

      for await (const message of this.subscription) {
        await this.handler(this.options.decode(message), message)
      }

      const subscriptionError = await this.subscription.closed
      if (subscriptionError) {
        throw subscriptionError
      }

      this.closedState.resolve()
    } catch (error) {
      if (!this.readySettled) {
        this.readySettled = true
        this.readyState.reject(error)
      }
      this.closedState.reject(error)
    } finally {
      if (abort) {
        this.options.signal?.removeEventListener('abort', abort)
      }
    }
  }

  private resolveReady(): void {
    if (!this.readySettled) {
      this.readySettled = true
      this.readyState.resolve()
    }
  }
}

class DefaultNatsRuntime implements NatsRuntime {
  readonly [NATS_RUNTIME_ADAPTER]: NatsRuntimeAdapter = {
    manage: <T extends RuntimeResource>(
      create: () => T,
      allocation?: RuntimeResourceAllocation
    ): T => this.manage(create, allocation),
    reportDiagnostic: (diagnostic) => this.eventStream.diagnostic(diagnostic),
  }
  readonly events: AsyncIterable<NatsRuntimeEvent>

  private readonly eventStream = new RuntimeEventStream()
  private connectionPromise: Promise<NatsConnection> | undefined
  private readonly resources = new Set<RuntimeResource>()
  private usedJetStreamConsumers = 0
  private usedBufferedMessages = 0
  private closePromise?: Promise<void>
  private closeRequested = false
  private readonly retryAbortController = new AbortController()

  constructor(private readonly options: NatsRuntimeOptions) {
    this.events = this.eventStream
  }

  connection(): Promise<NatsConnection> {
    if (this.closeRequested) {
      return Promise.reject(new Error('The NATS runtime is closed'))
    }

    if (!this.connectionPromise) {
      const connectionPromise = this.connectWithRetry()
      this.connectionPromise = connectionPromise
      void connectionPromise.catch(() => {
        if (this.connectionPromise === connectionPromise) {
          this.connectionPromise = undefined
        }
      })
    }
    return this.connectionPromise
  }

  async publish(
    subject: string,
    data: Uint8Array = new Uint8Array(0),
    options?: PublishOptions
  ): Promise<void> {
    const connection = await this.connection()
    connection.publish(subject, data, options)
  }

  subscribe<T>(options: CoreSubscriptionOptions<T>, handler: MessageHandler<T>): SubscriptionLease {
    if (this.closeRequested) {
      throw new Error('The NATS runtime is closed')
    }

    return this.manage(() => new CoreSubscription(this.connection(), options, handler))
  }

  close(): Promise<void> {
    this.closeRequested = true
    this.retryAbortController.abort()
    this.closePromise ??= this.closeRuntime()
    return this.closePromise
  }

  private async closeRuntime(): Promise<void> {
    const resources = [...this.resources]
    await Promise.allSettled(resources.map((resource) => resource.close()))

    if (this.connectionPromise) {
      const connection = await this.connectionPromise.catch(() => undefined)
      if (connection && !connection.isClosed()) {
        await connection.drain()
      }
    }

    this.eventStream.setStatus('closed')
  }

  private async connectWithRetry(): Promise<NatsConnection> {
    const maxAttempts = this.options.initialConnectRetry?.maxAttempts ?? 1
    const delayMs = this.options.initialConnectRetry?.delayMs ?? 1_000

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      if (this.closeRequested) {
        throw new Error('The NATS runtime is closed')
      }

      this.eventStream.setStatus('connecting')
      try {
        const connection = await this.options.connect()
        if (this.closeRequested) {
          if (!connection.isClosed()) {
            await connection.drain()
          }
          throw new Error('The NATS runtime is closed')
        }

        this.eventStream.setStatus('connected', connection.getServer())
        void this.observeConnection(connection)
        return connection
      } catch (error) {
        if (this.closeRequested) {
          throw error
        }

        if (attempt === maxAttempts) {
          this.eventStream.diagnostic({
            source: 'connection',
            code: 'connection-failed',
            level: 'error',
            message: `The NATS connection failed after ${maxAttempts} attempt${maxAttempts === 1 ? '' : 's'}`,
            ...(error instanceof Error ? { error } : {}),
            details: { attempt, maxAttempts },
          })
          this.eventStream.setStatus('disconnected')
          throw error
        }

        this.eventStream.diagnostic({
          source: 'connection',
          code: 'connection-retry-scheduled',
          level: 'warning',
          message: 'The NATS connection attempt failed; another attempt is scheduled',
          ...(error instanceof Error ? { error } : {}),
          details: { attempt, nextAttempt: attempt + 1, maxAttempts, delayMs },
        })
        this.eventStream.setStatus('disconnected')
        await this.waitForRetry(delayMs)
      }
    }

    throw new Error('The NATS connection retry series ended unexpectedly')
  }

  private waitForRetry(delayMs: number): Promise<void> {
    if (this.closeRequested || this.retryAbortController.signal.aborted) {
      return Promise.reject(new Error('The NATS runtime is closed'))
    }
    if (delayMs === 0) {
      return Promise.resolve()
    }

    return new Promise((resolve, reject) => {
      const onAbort = () => {
        clearTimeout(timer)
        reject(new Error('The NATS runtime is closed'))
      }
      const timer = setTimeout(() => {
        this.retryAbortController.signal.removeEventListener('abort', onAbort)
        resolve()
      }, delayMs)
      this.retryAbortController.signal.addEventListener('abort', onAbort, { once: true })
    })
  }

  private async observeConnection(connection: NatsConnection): Promise<void> {
    try {
      for await (const status of connection.status()) {
        this.reportConnectionStatus(status)
      }
    } catch (error) {
      this.eventStream.diagnostic({
        source: 'connection',
        code: 'status-stream-failed',
        level: 'error',
        message: 'The NATS connection status stream failed',
        ...(error instanceof Error ? { error } : {}),
      })
    }
  }

  private reportConnectionStatus(status: Status): void {
    switch (status.type) {
      case 'disconnect':
        this.eventStream.setStatus('disconnected', status.server)
        break
      case 'reconnecting':
        this.eventStream.setStatus('reconnecting')
        break
      case 'reconnect':
        this.eventStream.setStatus('connected', status.server)
        break
      case 'close':
        this.eventStream.setStatus('closed')
        break
      case 'forceReconnect':
        this.eventStream.diagnostic({
          source: 'connection',
          code: 'forced-reconnect',
          level: 'info',
          message: 'The NATS connection was asked to reconnect',
        })
        break
      case 'error':
        this.eventStream.diagnostic({
          source: 'connection',
          code: 'connection-error',
          level: 'error',
          message: status.error.message,
          error: status.error,
        })
        break
      case 'staleConnection':
        this.eventStream.diagnostic({
          source: 'connection',
          code: 'stale-connection',
          level: 'warning',
          message: 'The NATS connection became stale',
        })
        break
      case 'slowConsumer':
        this.eventStream.diagnostic({
          source: 'connection',
          code: 'slow-consumer',
          level: 'warning',
          message: 'A Core NATS subscription is consuming too slowly',
          details: { pending: status.pending },
        })
        break
      case 'ldm':
        this.eventStream.diagnostic({
          source: 'connection',
          code: 'lame-duck-mode',
          level: 'warning',
          message: 'The NATS server entered lame duck mode',
          details: { server: status.server },
        })
        break
      case 'update':
        this.eventStream.diagnostic({
          source: 'connection',
          code: 'cluster-update',
          level: 'info',
          message: 'The NATS cluster server pool changed',
          details: { added: status.added ?? [], deleted: status.deleted ?? [] },
        })
        break
      case 'ping':
        this.eventStream.diagnostic({
          source: 'connection',
          code: 'client-ping',
          level: status.pendingPings > 0 ? 'warning' : 'info',
          message: 'The NATS client reported pending pings',
          details: { pendingPings: status.pendingPings },
        })
        break
    }
  }

  private manage<T extends RuntimeResource>(
    create: () => T,
    requestedAllocation: RuntimeResourceAllocation = {}
  ): T {
    if (this.closeRequested) {
      throw new Error('The NATS runtime is closed')
    }

    const allocation = this.reserve(requestedAllocation)
    let resource: T
    try {
      resource = create()
    } catch (error) {
      this.release(allocation)
      throw error
    }

    this.resources.add(resource)
    void resource.closed
      .finally(() => {
        this.resources.delete(resource)
        this.release(allocation)
      })
      .catch(() => undefined)
    return resource
  }

  private reserve(allocation: RuntimeResourceAllocation): Required<RuntimeResourceAllocation> {
    const jetStreamConsumers = allocation.jetStreamConsumers ?? 0
    const bufferedMessages = allocation.bufferedMessages ?? 0
    this.validateAllocation('jetStreamConsumers', jetStreamConsumers)
    this.validateAllocation('bufferedMessages', bufferedMessages)

    this.assertWithinLimit(
      'jetstream-consumers',
      this.options.limits?.maxJetStreamConsumers,
      this.usedJetStreamConsumers,
      jetStreamConsumers
    )
    this.assertWithinLimit(
      'buffered-messages',
      this.options.limits?.maxBufferedMessages,
      this.usedBufferedMessages,
      bufferedMessages
    )

    this.usedJetStreamConsumers += jetStreamConsumers
    this.usedBufferedMessages += bufferedMessages
    return { jetStreamConsumers, bufferedMessages }
  }

  private release(allocation: Required<RuntimeResourceAllocation>): void {
    this.usedJetStreamConsumers -= allocation.jetStreamConsumers
    this.usedBufferedMessages -= allocation.bufferedMessages
  }

  private assertWithinLimit(
    code: NatsRuntimeLimitCode,
    limit: number | undefined,
    used: number,
    requested: number
  ): void {
    if (limit === undefined || used + requested <= limit) return

    const error = new NatsRuntimeLimitError(code, limit, used, requested)
    this.eventStream.diagnostic({
      source: 'runtime',
      code: 'resource-limit-exceeded',
      level: 'warning',
      message: error.message,
      error,
      details: { resource: code, limit, used, requested },
    })
    throw error
  }

  private validateAllocation(name: string, value: number): void {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new RangeError(`NATS runtime allocation ${name} must be a non-negative integer`)
    }
  }
}

export function createNatsRuntime(options: NatsRuntimeOptions): NatsRuntime {
  for (const [name, value] of Object.entries(options.limits ?? {})) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new RangeError(`NATS runtime limit ${name} must be a non-negative integer`)
    }
  }

  const { maxAttempts, delayMs } = options.initialConnectRetry ?? {}
  if (maxAttempts !== undefined && (!Number.isSafeInteger(maxAttempts) || maxAttempts <= 0)) {
    throw new RangeError('NATS runtime initialConnectRetry maxAttempts must be a positive integer')
  }
  if (delayMs !== undefined && (!Number.isSafeInteger(delayMs) || delayMs < 0)) {
    throw new RangeError('NATS runtime initialConnectRetry delayMs must be a non-negative integer')
  }

  return new DefaultNatsRuntime(options)
}
