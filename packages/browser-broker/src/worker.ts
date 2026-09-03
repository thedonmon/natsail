import { createNatsailTelemetryReporter } from '@natsail/core'
import type {
  NatsailTelemetryAttributes,
  NatsailTelemetryClock,
  NatsailTelemetryEvent,
  NatsailTelemetryReporter,
  NatsailTelemetrySink,
} from '@natsail/core'
import { defineSession } from '@natsail/session'
import type { SessionHandle, SessionRegistry } from '@natsail/session'

import { MutableCredentials } from './credentials.js'
import {
  BrowserBrokerError,
  NATS_BROWSER_BROKER_PROTOCOL_VERSION,
  cloneIdentity,
  copyBuffer,
  copyBytes,
  copyCursor,
  cursorsEqual,
  envelope,
  identityKey,
  parseBrowserBrokerCommand,
  parseCursor,
  sourceKey,
  type BrowserBrokerBatchMessage,
  type BrowserBrokerCommand,
  type BrowserBrokerCursor,
  type BrowserBrokerDelivery,
  type BrowserBrokerIdentity,
  type BrowserBrokerMessage,
  type BrowserBrokerOperationContext,
  type BrowserBrokerPublishHandler,
  type BrowserBrokerRequestHandler,
  type BrowserBrokerResult,
  type BrowserBrokerSourceDescriptor,
  type BrowserBrokerSourceFactory,
  type BrowserBrokerStateMessage,
  type BrowserBrokerStats,
} from './protocol.js'

import { nonNegativeInteger, positiveInteger } from './validation.js'

interface LogEntry {
  readonly data: Uint8Array
  readonly cursor?: BrowserBrokerCursor
}

interface InflightBatch {
  readonly id: number
  readonly entries: readonly LogEntry[]
  readonly items: number
  readonly bytes: number
  readonly cursor?: BrowserBrokerCursor
}

interface HostSubscription {
  readonly id: string
  readonly client: HostClient
  readonly source: PhysicalSource
  pending: LogEntry[]
  pendingBytes: number
  inflight: InflightBatch | undefined
  nextBatchId: number
  lastCursor: BrowserBrokerCursor | undefined
  resumeRequired: boolean
}

interface HostClient {
  readonly port: MessagePort
  readonly subscriptions: Map<string, HostSubscription>
  identity?: BrowserBrokerIdentity
  credentialKey?: string
  lastSeenAt: number
  queuedCommands: number
  readonly activeCommands: Set<Promise<void>>
  closing: boolean
  closed: boolean
  publishPending: Promise<void>
  pending: Promise<void>
}

interface PhysicalSource {
  readonly key: string
  readonly identityKey: string
  readonly identity: BrowserBrokerIdentity
  readonly descriptor: BrowserBrokerSourceDescriptor
  readonly handle: SessionHandle<BrowserBrokerDelivery>
  readonly subscriptions: Set<HostSubscription>
  readonly unsubscribe: () => void
  log: LogEntry[]
  logBytes: number
  logTruncated: boolean
  lastValueRevision: number
  idleTimer: ReturnType<typeof setTimeout> | undefined
  closed: boolean
}

export interface BrowserBrokerWorkerOptions {
  readonly sessions: SessionRegistry
  readonly createSource: BrowserBrokerSourceFactory
  /** Optional application-authorized publish mapping for tab operation keys. */
  readonly publish?: BrowserBrokerPublishHandler
  /** Optional application-authorized request mapping for tab operation keys. */
  readonly request?: BrowserBrokerRequestHandler
  /**
   * Releases and resets worker-owned runtime resources after the final physical
   * source closes, and when the broker host closes. A later operation waits for
   * this hook before asking the application to lazily recreate those resources.
   */
  readonly closeIdleResources?: () => void | Promise<void>
  /** Per-tab outstanding item limit, including the one in-flight batch. Defaults to 256. */
  readonly maxTabQueueItems?: number
  /** Per-tab outstanding encoded-byte limit. Defaults to 1 MiB. */
  readonly maxTabQueueBytes?: number
  /** Shared retained source-log item limit. Defaults to 1,024. */
  readonly maxRetainedItems?: number
  /** Shared retained source-log byte limit. Defaults to 4 MiB. */
  readonly maxRetainedBytes?: number
  /** Maximum items transferred in one per-tab batch. Defaults to 64. */
  readonly maxBatchItems?: number
  /** Maximum bytes transferred in one per-tab batch. Defaults to 256 KiB. */
  readonly maxBatchBytes?: number
  /** Delay after the final detach before releasing the physical source. Defaults to 0. */
  readonly idleTeardownMs?: number
  /** Closes tabs that stop heartbeating after this duration. Defaults to 30 seconds. */
  readonly clientTimeoutMs?: number
  /** Host heartbeat sweep interval. Set to 0 for an externally driven sweep. Defaults to 10 seconds. */
  readonly sweepIntervalMs?: number
  readonly telemetry?: NatsailTelemetrySink
  readonly telemetryAttributes?: NatsailTelemetryAttributes
  readonly telemetryClock?: NatsailTelemetryClock
}

export interface BrowserBrokerWorkerHost {
  connect(port: MessagePort): void
  /** Reports lifecycle of the caller-owned upstream runtime connection. */
  reportConnection(state: 'opened' | 'closed' | 'reconnected'): void
  inspect(): BrowserBrokerStats
  sweepIdleClients(at?: number): Promise<void>
  close(): Promise<void>
}

class DefaultBrowserBrokerWorkerHost implements BrowserBrokerWorkerHost {
  private readonly clients = new Map<MessagePort, HostClient>()
  private readonly sources = new Map<string, PhysicalSource>()
  private readonly credentialStores = new Map<string, MutableCredentials>()
  private readonly telemetry: NatsailTelemetryReporter
  private readonly maxTabQueueItems: number
  private readonly maxTabQueueBytes: number
  private readonly maxRetainedItems: number
  private readonly maxRetainedBytes: number
  private readonly maxBatchItems: number
  private readonly maxBatchBytes: number
  private readonly idleTeardownMs: number
  private readonly clientTimeoutMs: number
  private readonly sweepTimer?: ReturnType<typeof setInterval>
  private activeConnections = 0
  private activeOperations = 0
  private closed = false
  private resourcesActive = false
  private idleResourceClose: Promise<void> | undefined

  constructor(private readonly options: BrowserBrokerWorkerOptions) {
    this.maxTabQueueItems = positiveInteger(options.maxTabQueueItems, 256, 'maxTabQueueItems')
    this.maxTabQueueBytes = positiveInteger(
      options.maxTabQueueBytes,
      1024 * 1024,
      'maxTabQueueBytes'
    )
    this.maxRetainedItems = positiveInteger(options.maxRetainedItems, 1024, 'maxRetainedItems')
    this.maxRetainedBytes = positiveInteger(
      options.maxRetainedBytes,
      4 * 1024 * 1024,
      'maxRetainedBytes'
    )
    this.maxBatchItems = positiveInteger(options.maxBatchItems, 64, 'maxBatchItems')
    this.maxBatchBytes = positiveInteger(options.maxBatchBytes, 256 * 1024, 'maxBatchBytes')
    this.idleTeardownMs = nonNegativeInteger(options.idleTeardownMs, 0, 'idleTeardownMs')
    this.clientTimeoutMs = positiveInteger(options.clientTimeoutMs, 30_000, 'clientTimeoutMs')
    const sweepIntervalMs = nonNegativeInteger(options.sweepIntervalMs, 10_000, 'sweepIntervalMs')
    this.telemetry = createNatsailTelemetryReporter({
      ...(options.telemetry === undefined ? {} : { sink: options.telemetry }),
      ...(options.telemetryClock === undefined ? {} : { clock: options.telemetryClock }),
      ...(options.telemetryAttributes === undefined
        ? {}
        : { attributes: options.telemetryAttributes }),
    })
    if (sweepIntervalMs > 0) {
      this.sweepTimer = setInterval(() => {
        void this.sweepIdleClients()
      }, sweepIntervalMs)
    }
  }

  connect(port: MessagePort): void {
    if (this.closed) throw new BrowserBrokerError('invalid-state', 'The browser broker is closed')
    if (this.clients.has(port)) {
      throw new BrowserBrokerError('invalid-state', 'The MessagePort is already connected')
    }
    const client: HostClient = {
      port,
      subscriptions: new Map(),
      lastSeenAt: this.now(),
      queuedCommands: 0,
      activeCommands: new Set(),
      closing: false,
      closed: false,
      publishPending: Promise.resolve(),
      pending: Promise.resolve(),
    }
    this.clients.set(port, client)
    port.onmessage = (event: MessageEvent<unknown>) => {
      if (this.closed || client.closed || client.closing) return
      client.lastSeenAt = this.now()
      client.queuedCommands += 1
      const execute = () => this.receive(client, event.data)
      const type =
        typeof event.data === 'object' && event.data !== null
          ? (event.data as Record<string, unknown>).type
          : undefined
      if (type === 'close') client.closing = true
      if (type === 'publish' || type === 'request' || type === 'heartbeat') {
        const predecessor = type === 'publish' ? client.publishPending : Promise.resolve()
        const active = predecessor.then(execute).finally(() => {
          client.queuedCommands -= 1
        })
        if (type === 'publish') client.publishPending = active.catch(() => undefined)
        client.activeCommands.add(active)
        void active.finally(() => client.activeCommands.delete(active)).catch(() => undefined)
        return
      }
      client.pending = client.pending
        .then(execute)
        .finally(() => {
          client.queuedCommands -= 1
        })
        .catch(() => undefined)
    }
    port.onmessageerror = () => {
      void this.enqueueClientClose(client)
    }
    port.start()
  }

  reportConnection(state: 'opened' | 'closed' | 'reconnected'): void {
    if (state === 'opened') this.activeConnections += 1
    if (state === 'closed') this.activeConnections = Math.max(0, this.activeConnections - 1)
    this.telemetryEvent(`physical-connection-${state}`)
    this.telemetryGauge('natsail.browser.broker.connections.active', this.activeConnections)
  }

  inspect(): BrowserBrokerStats {
    let subscriptionCount = 0
    let queuedItems = 0
    let queuedBytes = 0
    for (const source of this.sources.values()) {
      subscriptionCount += source.subscriptions.size
      for (const subscription of source.subscriptions) {
        queuedItems += subscription.pending.length + (subscription.inflight?.items ?? 0)
        queuedBytes += subscription.pendingBytes + (subscription.inflight?.bytes ?? 0)
      }
    }
    return {
      tabCount: [...this.clients.values()].filter((client) => client.identity !== undefined).length,
      activeConnectionCount: this.activeConnections,
      physicalSourceCount: this.sources.size,
      subscriptionCount,
      queuedItems,
      queuedBytes,
    }
  }

  async sweepIdleClients(at = this.now()): Promise<void> {
    await Promise.all(
      [...this.clients.values()]
        .filter(
          (client) => client.queuedCommands === 0 && at - client.lastSeenAt >= this.clientTimeoutMs
        )
        .map((client) => this.enqueueClientClose(client))
    )
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    if (this.sweepTimer) clearInterval(this.sweepTimer)
    try {
      await Promise.all(
        [...this.clients.values()].flatMap((client) => [client.pending, ...client.activeCommands])
      )
      await Promise.all([...this.clients.values()].map((client) => this.closeClient(client)))
      await Promise.all([...this.sources.values()].map((source) => this.teardownSource(source)))
      await this.closeIdleResources()
    } finally {
      for (const store of this.credentialStores.values()) store.dispose()
      this.credentialStores.clear()
    }
  }

  private async receive(client: HostClient, value: unknown): Promise<void> {
    if (this.closed || client.closed) return
    let command: BrowserBrokerCommand
    try {
      command = parseBrowserBrokerCommand(value)
    } catch (error) {
      const candidate = typeof value === 'object' && value !== null ? value : {}
      const rawId = (candidate as Record<string, unknown>).requestId
      if (Number.isSafeInteger(rawId) && (rawId as number) > 0) {
        this.postFailure(client, rawId as number, error)
      }
      return
    }

    try {
      const handled = await this.handleCommand(client, command)
      this.postResult(client, command.requestId, handled.result, handled.transfers)
    } catch (error) {
      this.postFailure(client, command.requestId, error)
    }
  }

  private async handleCommand(
    client: HostClient,
    command: BrowserBrokerCommand
  ): Promise<{ readonly result?: unknown; readonly transfers?: Transferable[] }> {
    if (command.type === 'hello') {
      await this.hello(client, command)
      return { result: { protocolVersion: NATS_BROWSER_BROKER_PROTOCOL_VERSION } }
    }
    if (command.type === 'close') {
      await Promise.all([...client.activeCommands])
      await this.closeClient(client)
      return {}
    }
    if (!client.identity || !client.credentialKey) {
      throw new BrowserBrokerError('not-connected', 'hello must be the first command on a port')
    }

    switch (command.type) {
      case 'attach':
        await this.attach(client, command)
        return {}
      case 'detach':
        await this.detach(client, command.subscriptionId)
        return {}
      case 'ack':
        this.ack(client, command)
        return {}
      case 'refresh-credentials': {
        const incoming = new Uint8Array(command.credentials)
        let listenerFailure: unknown
        try {
          listenerFailure = this.credentialStores
            .get(client.credentialKey)!
            .update(command.credentialRevision, incoming)
        } finally {
          incoming.fill(0)
        }
        if (listenerFailure !== undefined) {
          await this.invalidateIdentity(client, client.credentialKey)
          throw new BrowserBrokerError(
            'source-failed',
            'A source rejected the refreshed credentials; the broker identity was closed'
          )
        }
        return {}
      }
      case 'publish': {
        if (!this.options.publish) {
          throw new BrowserBrokerError('unavailable', 'Brokered publish is not configured')
        }
        await this.withResources(async () => {
          await this.options.publish!(
            this.operationContext(client, command.operation, command.data)
          )
          this.telemetryEvent('publish')
        })
        return {}
      }
      case 'request': {
        if (!this.options.request) {
          throw new BrowserBrokerError('unavailable', 'Brokered request is not configured')
        }
        const response = await this.withResources(() =>
          this.options.request!(this.operationContext(client, command.operation, command.data))
        )
        if (!(response instanceof Uint8Array)) {
          throw new BrowserBrokerError('source-failed', 'Brokered request must return Uint8Array')
        }
        const result = copyBuffer(response)
        this.telemetryEvent('request')
        return { result, transfers: [result] }
      }
      case 'restart':
        await this.restart(client, command.subscriptionId)
        return {}
      case 'stats':
        return { result: this.inspect() }
      case 'heartbeat':
        return {}
      default:
        throw new BrowserBrokerError('invalid-command', 'Unexpected hello command')
    }
  }

  private async hello(
    client: HostClient,
    command: Extract<BrowserBrokerCommand, { type: 'hello' }>
  ): Promise<void> {
    if (client.identity) {
      if (identityKey(client.identity) !== identityKey(command.identity)) {
        throw new BrowserBrokerError(
          'identity-mismatch',
          'Tenant and authentication context are immutable on a broker port'
        )
      }
      throw new BrowserBrokerError('invalid-state', 'hello was already accepted on this port')
    }
    const identity = cloneIdentity(command.identity)
    const key = identityKey(identity)
    const incoming = new Uint8Array(command.credentials)
    let store = this.credentialStores.get(key)
    let listenerFailure: unknown
    try {
      if (!store) {
        store = new MutableCredentials(command.credentialRevision, incoming)
        this.credentialStores.set(key, store)
      } else {
        listenerFailure = store.accept(command.credentialRevision, incoming)
      }
    } finally {
      incoming.fill(0)
    }
    if (listenerFailure !== undefined) {
      await this.invalidateIdentity(client, key)
      throw new BrowserBrokerError(
        'source-failed',
        'A source rejected the refreshed credentials; the broker identity was closed'
      )
    }
    client.identity = identity
    client.credentialKey = key
    this.telemetryEvent('tab-connected')
    this.telemetryGauge('natsail.browser.broker.tabs.active', this.inspect().tabCount)
  }

  private async attach(
    client: HostClient,
    command: Extract<BrowserBrokerCommand, { type: 'attach' }>
  ): Promise<void> {
    await this.awaitIdleResources()
    if (client.subscriptions.has(command.subscriptionId)) {
      throw new BrowserBrokerError(
        'subscription-exists',
        `Subscription ${command.subscriptionId} already exists`
      )
    }
    const identity = client.identity!
    const key = sourceKey(identity, command.source)
    let source = this.sources.get(key)
    let created = false
    if (source && source.descriptor.contract !== command.source.contract) {
      throw new BrowserBrokerError(
        'contract-mismatch',
        `Source ${command.source.key} is active with a different contract`
      )
    }
    if (!source) {
      try {
        source = await this.openSource(identity, command.source, command.resumeAfter)
        created = true
      } catch (error) {
        await this.closeIdleResources().catch(() => {
          this.telemetryEvent('idle-resource-close-failed')
        })
        throw error
      }
    }
    if (source.idleTimer) {
      clearTimeout(source.idleTimer)
      source.idleTimer = undefined
    }

    const subscription: HostSubscription = {
      id: command.subscriptionId,
      client,
      source,
      pending: [],
      pendingBytes: 0,
      inflight: undefined,
      nextBatchId: 1,
      lastCursor: copyCursor(command.resumeAfter),
      resumeRequired: false,
    }
    client.subscriptions.set(subscription.id, subscription)
    source.subscriptions.add(subscription)
    this.postState(subscription, 'connecting')
    if (!created && command.reattach) this.enqueueRetained(subscription, command.resumeAfter)
    this.telemetryEvent('subscription-attached')

    try {
      const snapshot = source.handle.getSnapshot()
      if (!created && snapshot.phase === 'error') await this.restartSource(source)
      await source.handle.ready
      if (!subscription.resumeRequired) {
        this.postState(subscription, 'live', undefined, subscription.lastCursor)
        this.flush(subscription)
      }
      this.recordQueueTelemetry()
    } catch (error) {
      if (client.subscriptions.get(subscription.id) === subscription) {
        client.subscriptions.delete(subscription.id)
        source.subscriptions.delete(subscription)
        if (source.subscriptions.size === 0) await this.scheduleTeardown(source)
      }
      throw error
    }
  }

  private async openSource(
    identity: BrowserBrokerIdentity,
    descriptor: BrowserBrokerSourceDescriptor,
    resumeAfter?: BrowserBrokerCursor
  ): Promise<PhysicalSource> {
    this.resourcesActive = true
    const key = sourceKey(identity, descriptor)
    const credentials = this.credentialStores.get(identityKey(identity))
    if (!credentials) throw new BrowserBrokerError('not-connected', 'Credentials are unavailable')
    const source = this.options.createSource({
      identity: cloneIdentity(identity),
      descriptor: Object.freeze({ ...descriptor }),
      credentials,
      ...(resumeAfter === undefined ? {} : { resumeAfter: copyCursor(resumeAfter) }),
    })
    const handle = this.options.sessions.acquire(
      defineSession({ key, contract: descriptor.contract, source })
    )
    let unsubscribe: () => void = () => undefined
    const physical: PhysicalSource = {
      key,
      identityKey: identityKey(identity),
      identity: cloneIdentity(identity),
      descriptor: Object.freeze({ ...descriptor }),
      handle,
      subscriptions: new Set(),
      unsubscribe: () => unsubscribe(),
      log: [],
      logBytes: 0,
      logTruncated: false,
      lastValueRevision: 0,
      idleTimer: undefined,
      closed: false,
    }
    try {
      unsubscribe = handle.subscribe(() => this.syncSource(physical))
      this.sources.set(key, physical)
      this.syncSource(physical)
      this.telemetryEvent('physical-source-opened')
      this.telemetryGauge('natsail.browser.broker.sources.active', this.sources.size)
      return physical
    } catch (error) {
      physical.closed = true
      unsubscribe()
      if (this.sources.get(key) === physical) this.sources.delete(key)
      await handle.release().catch(() => {
        this.telemetryEvent('physical-source-release-failed')
      })
      throw error
    }
  }

  private syncSource(source: PhysicalSource): void {
    if (!source || source.closed) return
    const snapshot = source.handle.getSnapshot()
    if (snapshot.valueRevision > source.lastValueRevision && snapshot.value !== undefined) {
      source.lastValueRevision = snapshot.valueRevision
      this.appendDelivery(source, snapshot.value)
    }
    if (snapshot.phase === 'error') {
      for (const subscription of source.subscriptions) {
        this.postState(subscription, 'error', 'source-error', subscription.lastCursor)
      }
    } else if (snapshot.phase === 'closed') {
      for (const subscription of source.subscriptions) {
        this.postState(subscription, 'closed', undefined, subscription.lastCursor)
      }
    }
  }

  private appendDelivery(source: PhysicalSource, delivery: BrowserBrokerDelivery): void {
    if (!(delivery.data instanceof Uint8Array)) {
      throw new BrowserBrokerError('source-failed', 'Source delivery data must be Uint8Array')
    }
    if (delivery.cursor !== undefined) parseCursor(delivery.cursor)
    const entry: LogEntry = {
      data: copyBytes(delivery.data),
      ...(delivery.cursor === undefined ? {} : { cursor: copyCursor(delivery.cursor) }),
    }
    source.log.push(entry)
    source.logBytes += entry.data.byteLength
    while (source.log.length > this.maxRetainedItems || source.logBytes > this.maxRetainedBytes) {
      const removed = source.log.shift()
      if (!removed) break
      source.logBytes -= removed.data.byteLength
      source.logTruncated = true
    }
    for (const subscription of source.subscriptions) this.enqueue(subscription, entry)
    this.recordQueueTelemetry()
  }

  private enqueueRetained(
    subscription: HostSubscription,
    resumeAfter: BrowserBrokerCursor | undefined
  ): void {
    if (resumeAfter === undefined) {
      if (subscription.source.logTruncated) {
        this.requireResume(subscription)
        return
      }
      for (const entry of subscription.source.log) this.enqueue(subscription, entry)
      return
    }
    const index = subscription.source.log.findIndex((entry) =>
      cursorsEqual(entry.cursor, resumeAfter)
    )
    if (index < 0) {
      this.requireResume(subscription)
      return
    }
    for (const entry of subscription.source.log.slice(index + 1)) this.enqueue(subscription, entry)
  }

  private enqueue(subscription: HostSubscription, entry: LogEntry): void {
    if (subscription.resumeRequired) return
    const queued = this.clientQueueUsage(subscription.client)
    if (
      entry.data.byteLength > this.maxBatchBytes ||
      queued.items + 1 > this.maxTabQueueItems ||
      queued.bytes + entry.data.byteLength > this.maxTabQueueBytes
    ) {
      this.requireResume(subscription)
      return
    }
    subscription.pending.push(entry)
    subscription.pendingBytes += entry.data.byteLength
    this.flush(subscription)
  }

  private clientQueueUsage(client: HostClient): { readonly items: number; readonly bytes: number } {
    let items = 0
    let bytes = 0
    for (const subscription of client.subscriptions.values()) {
      items += subscription.pending.length + (subscription.inflight?.items ?? 0)
      bytes += subscription.pendingBytes + (subscription.inflight?.bytes ?? 0)
    }
    return { items, bytes }
  }

  private requireResume(subscription: HostSubscription): void {
    subscription.resumeRequired = true
    subscription.pending = []
    subscription.pendingBytes = 0
    subscription.inflight = undefined
    this.postState(subscription, 'resume-required', 'lagged', subscription.lastCursor)
    this.telemetryEvent('tab-lagged')
    this.recordQueueTelemetry()
  }

  private flush(subscription: HostSubscription): void {
    if (
      subscription.resumeRequired ||
      subscription.inflight !== undefined ||
      subscription.pending.length === 0
    ) {
      return
    }
    const entries: LogEntry[] = []
    let bytes = 0
    while (entries.length < this.maxBatchItems && subscription.pending.length > 0) {
      const next = subscription.pending[0]!
      if (entries.length > 0 && bytes + next.data.byteLength > this.maxBatchBytes) break
      subscription.pending.shift()
      subscription.pendingBytes -= next.data.byteLength
      entries.push(next)
      bytes += next.data.byteLength
    }
    const cursor = copyCursor(entries.at(-1)?.cursor)
    const inflight: InflightBatch = {
      id: subscription.nextBatchId,
      entries,
      items: entries.length,
      bytes,
      ...(cursor === undefined ? {} : { cursor }),
    }
    subscription.nextBatchId += 1
    subscription.inflight = inflight

    const items = entries.map((entry) => {
      const data = copyBuffer(entry.data)
      return {
        data,
        ...(entry.cursor === undefined ? {} : { cursor: copyCursor(entry.cursor) }),
      }
    })
    const message: BrowserBrokerBatchMessage = {
      ...envelope(),
      type: 'batch',
      subscriptionId: subscription.id,
      batchId: inflight.id,
      items,
    }
    this.post(
      subscription.client,
      message,
      items.map((item) => item.data)
    )
    this.recordQueueTelemetry()
  }

  private ack(client: HostClient, command: Extract<BrowserBrokerCommand, { type: 'ack' }>): void {
    const subscription = this.subscription(client, command.subscriptionId)
    const inflight = subscription.inflight
    if (!inflight || inflight.id !== command.batchId) {
      throw new BrowserBrokerError('invalid-state', 'The acknowledgement batch is not in flight')
    }
    if (!cursorsEqual(inflight.cursor, command.cursor)) {
      throw new BrowserBrokerError('invalid-state', 'The acknowledgement cursor does not match')
    }
    subscription.lastCursor = copyCursor(command.cursor)
    subscription.inflight = undefined
    this.flush(subscription)
    this.recordQueueTelemetry()
  }

  private async restart(client: HostClient, subscriptionId: string): Promise<void> {
    const subscription = this.subscription(client, subscriptionId)
    await this.restartSource(subscription.source)
  }

  private async restartSource(source: PhysicalSource): Promise<void> {
    this.telemetryEvent('physical-source-reconnect')
    await source.handle.restart()
  }

  private subscription(client: HostClient, subscriptionId: string): HostSubscription {
    const subscription = client.subscriptions.get(subscriptionId)
    if (!subscription) {
      throw new BrowserBrokerError(
        'subscription-missing',
        `Subscription ${subscriptionId} does not exist`
      )
    }
    return subscription
  }

  private async detach(client: HostClient, subscriptionId: string): Promise<void> {
    const subscription = this.subscription(client, subscriptionId)
    client.subscriptions.delete(subscriptionId)
    subscription.source.subscriptions.delete(subscription)
    this.telemetryEvent('subscription-detached')
    this.recordQueueTelemetry()
    if (subscription.source.subscriptions.size === 0) {
      await this.scheduleTeardown(subscription.source)
    }
  }

  private async scheduleTeardown(source: PhysicalSource): Promise<void> {
    if (source.closed || source.idleTimer) return
    if (this.idleTeardownMs === 0) {
      await this.teardownSource(source)
      return
    }
    source.idleTimer = setTimeout(() => {
      source.idleTimer = undefined
      if (source.subscriptions.size === 0) void this.teardownSource(source)
    }, this.idleTeardownMs)
  }

  private async teardownSource(source: PhysicalSource): Promise<void> {
    if (source.closed) return
    source.closed = true
    if (source.idleTimer) clearTimeout(source.idleTimer)
    source.unsubscribe()
    if (this.sources.get(source.key) === source) this.sources.delete(source.key)
    await source.handle.release().catch(() => undefined)
    source.log = []
    source.logBytes = 0
    this.telemetryEvent('physical-source-closed')
    this.telemetryGauge('natsail.browser.broker.sources.active', this.sources.size)
    this.cleanupCredentials(source.identityKey)
    if (this.sources.size === 0) await this.closeIdleResources()
  }

  private operationContext(
    client: HostClient,
    operation: string,
    data: ArrayBuffer
  ): BrowserBrokerOperationContext {
    return {
      identity: cloneIdentity(client.identity!),
      operation,
      data: new Uint8Array(data),
      credentials: this.credentialStores.get(client.credentialKey!)!,
    }
  }

  private async awaitIdleResources(): Promise<void> {
    await this.idleResourceClose?.catch(() => undefined)
  }

  private async withResources<T>(operation: () => T | Promise<T>): Promise<T> {
    await this.awaitIdleResources()
    this.resourcesActive = true
    this.activeOperations += 1
    try {
      return await operation()
    } finally {
      this.activeOperations -= 1
      if (this.activeOperations === 0 && this.sources.size === 0) {
        await this.closeIdleResources().catch(() => {
          this.telemetryEvent('idle-resource-close-failed')
        })
      }
    }
  }

  private closeIdleResources(): Promise<void> {
    if (
      !this.resourcesActive ||
      !this.options.closeIdleResources ||
      this.sources.size > 0 ||
      this.activeOperations > 0
    ) {
      return Promise.resolve()
    }
    if (this.idleResourceClose) return this.idleResourceClose
    const closing = Promise.resolve().then(() => this.options.closeIdleResources!())
    this.idleResourceClose = closing
    void closing.then(
      () => {
        this.resourcesActive = false
        if (this.idleResourceClose === closing) this.idleResourceClose = undefined
      },
      () => {
        if (this.idleResourceClose === closing) this.idleResourceClose = undefined
      }
    )
    return closing
  }

  private async closeClient(client: HostClient): Promise<void> {
    if (client.closed) return
    client.closing = true
    client.closed = true
    for (const subscription of [...client.subscriptions.values()]) {
      await this.detach(client, subscription.id).catch(() => undefined)
    }
    this.clients.delete(client.port)
    client.port.onmessage = null
    client.port.onmessageerror = null
    if (client.identity) {
      this.telemetryEvent('tab-disconnected')
      this.telemetryGauge('natsail.browser.broker.tabs.active', this.inspect().tabCount)
    }
    if (client.credentialKey) this.cleanupCredentials(client.credentialKey)
  }

  private async invalidateIdentity(current: HostClient, key: string): Promise<void> {
    const invalidate = async (client: HostClient) => {
      if (client.credentialKey !== key) return
      const subscriptions = [...client.subscriptions.values()]
      for (const subscription of subscriptions) {
        this.postState(subscription, 'error', 'source-error', subscription.lastCursor)
        await this.detach(client, subscription.id).catch(() => undefined)
      }
      delete client.identity
      delete client.credentialKey
      this.telemetryEvent('identity-closed')
    }
    const invalidations = [...this.clients.values()]
      .filter((client) => client.credentialKey === key)
      .map((client) => {
        if (client === current) return invalidate(client)
        const queued = client.pending.then(() => invalidate(client))
        client.pending = queued.catch(() => undefined)
        return queued
      })
    await Promise.all(invalidations)
    this.cleanupCredentials(key)
    this.telemetryGauge('natsail.browser.broker.tabs.active', this.inspect().tabCount)
  }

  private enqueueClientClose(client: HostClient): Promise<void> {
    if (client.closed) return Promise.resolve()
    client.closing = true
    const close = client.pending
      .then(() => Promise.all([...client.activeCommands]))
      .then(() => this.closeClient(client))
    client.pending = close.catch(() => undefined)
    return close
  }

  private cleanupCredentials(key: string): void {
    const usedByClient = [...this.clients.values()].some((client) => client.credentialKey === key)
    const usedBySource = [...this.sources.values()].some((source) => source.identityKey === key)
    if (usedByClient || usedBySource) return
    this.credentialStores.get(key)?.dispose()
    this.credentialStores.delete(key)
  }

  private postResult(
    client: HostClient,
    id: number,
    result?: unknown,
    transfers: Transferable[] = []
  ): void {
    if (client.closed && result !== undefined) return
    const message: BrowserBrokerResult = {
      ...envelope(),
      type: 'result',
      requestId: id,
      ok: true,
      ...(result === undefined ? {} : { result }),
    }
    this.post(client, message, transfers)
  }

  private postFailure(client: HostClient, id: number, error: unknown): void {
    const failure =
      error instanceof BrowserBrokerError
        ? error
        : new BrowserBrokerError(
            'source-failed',
            error instanceof Error ? error.message : String(error)
          )
    const message: BrowserBrokerResult = {
      ...envelope(),
      type: 'result',
      requestId: id,
      ok: false,
      error: { code: failure.code, message: failure.message },
    }
    this.post(client, message)
  }

  private postState(
    subscription: HostSubscription,
    state: BrowserBrokerStateMessage['state'],
    reason?: BrowserBrokerStateMessage['reason'],
    cursor?: BrowserBrokerCursor
  ): void {
    this.post(subscription.client, {
      ...envelope(),
      type: 'state',
      subscriptionId: subscription.id,
      state,
      ...(reason === undefined ? {} : { reason }),
      ...(cursor === undefined ? {} : { cursor: copyCursor(cursor) }),
    } satisfies BrowserBrokerStateMessage)
  }

  private telemetryEvent(action: string): void {
    if (!this.telemetry.enabled) return
    this.recordTelemetry({
      type: 'counter',
      name: 'natsail.browser.broker.events',
      value: 1,
      at: this.telemetry.now(),
      attributes: { action, source: 'browser-broker' },
    })
  }

  private telemetryGauge(
    name: Extract<NatsailTelemetryEvent, { type: 'gauge' }>['name'],
    value: number
  ) {
    if (!this.telemetry.enabled) return
    this.recordTelemetry({
      type: 'gauge',
      name,
      value,
      at: this.telemetry.now(),
      attributes: { source: 'browser-broker' },
    })
  }

  private recordQueueTelemetry(): void {
    const inspection = this.inspect()
    this.telemetryGauge('natsail.browser.broker.queue.depth', inspection.queuedItems)
    this.telemetryGauge('natsail.browser.broker.queue.bytes', inspection.queuedBytes)
  }

  private recordTelemetry(event: NatsailTelemetryEvent): void {
    if (this.telemetry.enabled) this.telemetry.record(event)
  }

  private post(
    client: HostClient,
    message: BrowserBrokerMessage,
    transfers: Transferable[] = []
  ): void {
    try {
      client.port.postMessage(message, transfers)
    } catch {
      void this.closeClient(client)
    }
  }

  private now(): number {
    return this.telemetry.now()
  }
}

/** Creates the protocol-v1 host that belongs in a SharedWorker global scope. */
export function createBrowserBrokerWorker(
  options: BrowserBrokerWorkerOptions
): BrowserBrokerWorkerHost {
  return new DefaultBrowserBrokerWorkerHost(options)
}
