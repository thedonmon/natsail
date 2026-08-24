import type {
  Msg,
  MsgHdrs,
  NatsConnection,
  Payload,
  PublishOptions,
  Status,
  Subscription,
} from '@nats-io/nats-core'

export type { NatsConnection } from '@nats-io/nats-core'

export type MessageHandler<T> = (value: T, message: Msg) => void | Promise<void>

/** Encodes and decodes one application payload without exposing text or byte plumbing. */
export interface NatsPayloadCodec<T> {
  encode(value: T): Uint8Array
  decode(data: Uint8Array): T
}

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

const bytesCodec: NatsPayloadCodec<Uint8Array> = Object.freeze({
  encode: (value: Uint8Array) => value,
  decode: (data: Uint8Array) => data,
})

const textCodec: NatsPayloadCodec<string> = Object.freeze({
  encode: (value: string) => textEncoder.encode(value),
  decode: (data: Uint8Array) => textDecoder.decode(data),
})

function createJsonCodec<T>(): NatsPayloadCodec<T> {
  return Object.freeze({
    encode(value: T) {
      const serialized = JSON.stringify(value)
      if (serialized === undefined) {
        throw new TypeError('The NATS JSON codec cannot encode this value')
      }
      return textCodec.encode(serialized)
    },
    decode(data: Uint8Array) {
      return JSON.parse(textCodec.decode(data)) as T
    },
  })
}

/** Built-in payload codecs. Supply any `NatsPayloadCodec` when an application needs another format. */
export const natsCodecs = Object.freeze({
  bytes: bytesCodec,
  text: textCodec,
  json: createJsonCodec,
})

type PayloadDecoding<T, Message, Decoded = T> =
  | { codec: NatsPayloadCodec<T>; decode?: never }
  | { codec?: never; decode: (message: Message) => Decoded }

export interface CoreSubscriptionBaseOptions {
  subject: string
  queue?: string
  signal?: AbortSignal
}

export type CoreSubscriptionOptions<T> = CoreSubscriptionBaseOptions & PayloadDecoding<T, Msg>

export interface CoreRequestBaseOptions {
  subject: string
  data?: Payload
  /** Maximum time to wait for one response. Defaults to 5 seconds. */
  timeoutMs?: number
  headers?: MsgHdrs
  signal?: AbortSignal
}

export type CoreRequestOptions<T> = CoreRequestBaseOptions & PayloadDecoding<T, Msg, T | Promise<T>>

export class NatsRuntimeRequestAbortedError extends Error {
  readonly name = 'NatsRuntimeRequestAbortedError'

  constructor() {
    super('The NATS request was aborted')
  }
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
  bufferedBytes?: number
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

export type NatsRuntimeLimitCode = 'jetstream-consumers' | 'buffered-messages' | 'buffered-bytes'

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
  /** Forces the current connection to authenticate again, or replaces it after a permanent close. */
  reconnect(options?: NatsRuntimeReconnectOptions): Promise<NatsConnection>
  /** Publishes through the shared connection. */
  publish(subject: string, data?: Payload, options?: PublishOptions): Promise<void>
  /** Sends one request through the shared connection without replaying an ambiguous attempt. */
  request<T>(options: CoreRequestOptions<T>): Promise<T>
  /** Creates an ephemeral Core NATS subscription. */
  subscribe<T>(options: CoreSubscriptionOptions<T>, handler: MessageHandler<T>): SubscriptionLease
  /** Returns a point-in-time view of owned connection and resource state. */
  inspect(): NatsRuntimeInspection
  /** Stops every subscription and drains the owned NATS connection. */
  close(): Promise<void>
}

export interface NatsRuntimeReconnectOptions {
  /** Application-defined reason included in runtime diagnostics. */
  reason?: string
}

export interface NatsRuntimeInspection {
  readonly connection: NatsRuntimeStatusEvent
  readonly connectionGeneration: number
  readonly activeResources: number
  readonly usedJetStreamConsumers: number
  readonly usedBufferedMessages: number
  readonly usedBufferedBytes: number
  readonly limits: Readonly<NatsRuntimeLimits>
}

export interface NatsRuntimeOptions {
  /** Creates the connection owned by this runtime. Called once per connection attempt. */
  connect: () => Promise<NatsConnection>
  /** Optional bounded retry policy for each initial connection series. */
  initialConnectRetry?: NatsRuntimeInitialConnectRetryOptions
  /** Controls whether a permanently closed owned connection is replaced immediately. */
  connectionRecovery?: NatsRuntimeConnectionRecoveryOptions
  /** Optional connection-wide limits shared by every runtime adapter. */
  limits?: NatsRuntimeLimits
}

export interface NatsRuntimeConnectionRecoveryOptions {
  /** `restart` reconnects immediately; `wait` defers replacement until the next caller. */
  onPermanentClose?: 'restart' | 'wait'
}

export interface NatsRuntimeRetryContext {
  readonly attempt: number
  readonly maxAttempts: number
  readonly error: unknown
}

export interface NatsRuntimeInitialConnectRetryOptions {
  /** Total attempts in one connection series, including the first. Defaults to 1. */
  maxAttempts?: number
  /** Delay between attempts in milliseconds. Defaults to 1,000. */
  delayMs?: number | ((context: NatsRuntimeRetryContext) => number)
  /** Stops the current connection series early when it returns false. */
  shouldRetry?: (context: NatsRuntimeRetryContext) => boolean
}

export interface NatsRuntimeLimits {
  /** Maximum active ordered JetStream consumers. Unbounded by default. */
  maxJetStreamConsumers?: number
  /** Maximum aggregate nats.js pull-buffer message capacity. Unbounded by default. */
  maxBufferedMessages?: number
  /** Maximum aggregate nats.js pull-buffer byte capacity. Unbounded by default. */
  maxBufferedBytes?: number
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

type PayloadDecoder<T, Message> = {
  codec?: NatsPayloadCodec<T>
  decode?: (message: Message) => T | Promise<T>
}

function validatePayloadDecoder<T, Message>(
  options: PayloadDecoder<T, Message>,
  operation: string
): void {
  const hasCodec = options.codec !== undefined
  const hasDecoder = options.decode !== undefined
  if (hasCodec === hasDecoder) {
    throw new TypeError(`${operation} requires exactly one codec or decode function`)
  }
}

function decodePayload<T, Message extends { data: Uint8Array }>(
  options: PayloadDecoder<T, Message>,
  message: Message
): T | Promise<T> {
  return options.codec ? options.codec.decode(message.data) : options.decode!(message)
}

function rejectWhenAborted<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) {
    return Promise.reject(new NatsRuntimeRequestAbortedError())
  }

  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(new NatsRuntimeRequestAbortedError())
    signal.addEventListener('abort', onAbort, { once: true })
    void operation.then(resolve, reject).finally(() => {
      signal.removeEventListener('abort', onAbort)
    })
  })
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

  status(): NatsRuntimeStatusEvent {
    return { ...this.currentStatus }
  }

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
        await this.handler(await decodePayload(this.options, message), message)
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
  private activeConnection: NatsConnection | undefined
  private connectionGeneration = 0
  private readonly resources = new Set<RuntimeResource>()
  private usedJetStreamConsumers = 0
  private usedBufferedMessages = 0
  private usedBufferedBytes = 0
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

    if (this.activeConnection?.isClosed()) {
      this.activeConnection = undefined
      this.connectionPromise = undefined
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

  async reconnect(options: NatsRuntimeReconnectOptions = {}): Promise<NatsConnection> {
    if (this.closeRequested) {
      throw new Error('The NATS runtime is closed')
    }

    let connection = await this.connection()
    this.eventStream.diagnostic({
      source: 'connection',
      code: 'reconnect-requested',
      level: 'info',
      message: 'The NATS runtime was asked to reconnect',
      details: {
        generation: this.connectionGeneration,
        ...(options.reason === undefined ? {} : { reason: options.reason }),
      },
    })

    if (connection.isClosed()) {
      this.clearConnection(connection)
      connection = await this.connection()
      return connection
    }

    try {
      await connection.reconnect()
    } catch (error) {
      if (!connection.isClosed()) throw error
    }

    if (connection.isClosed()) {
      this.clearConnection(connection)
      return this.connection()
    }
    return connection
  }

  async publish(
    subject: string,
    data: Payload = new Uint8Array(0),
    options?: PublishOptions
  ): Promise<void> {
    const connection = await this.connection()
    connection.publish(subject, data, options)
  }

  async request<T>(options: CoreRequestOptions<T>): Promise<T> {
    if (options.subject.length === 0) {
      throw new TypeError('NATS request subject must not be empty')
    }
    if (
      options.timeoutMs !== undefined &&
      (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs <= 0)
    ) {
      throw new RangeError('NATS request timeoutMs must be a positive integer')
    }
    if (options.signal?.aborted) {
      throw new NatsRuntimeRequestAbortedError()
    }
    validatePayloadDecoder(options, 'NATS request')
    const operation = (async () => {
      const connection = await this.connection()
      if (options.signal?.aborted) {
        throw new NatsRuntimeRequestAbortedError()
      }
      const response = await connection.request(
        options.subject,
        options.data ?? new Uint8Array(0),
        {
          timeout: options.timeoutMs ?? 5_000,
          ...(options.headers === undefined ? {} : { headers: options.headers }),
        }
      )
      return decodePayload(options, response)
    })()

    const runtimeBound = rejectWhenAborted(operation, this.retryAbortController.signal)
    return options.signal ? rejectWhenAborted(runtimeBound, options.signal) : runtimeBound
  }

  subscribe<T>(options: CoreSubscriptionOptions<T>, handler: MessageHandler<T>): SubscriptionLease {
    if (this.closeRequested) {
      throw new Error('The NATS runtime is closed')
    }
    validatePayloadDecoder(options, 'NATS subscription')

    return this.manage(() => new CoreSubscription(this.connection(), options, handler))
  }

  inspect(): NatsRuntimeInspection {
    return {
      connection: this.eventStream.status(),
      connectionGeneration: this.connectionGeneration,
      activeResources: this.resources.size,
      usedJetStreamConsumers: this.usedJetStreamConsumers,
      usedBufferedMessages: this.usedBufferedMessages,
      usedBufferedBytes: this.usedBufferedBytes,
      limits: { ...this.options.limits },
    }
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

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      if (this.closeRequested) {
        throw new Error('The NATS runtime is closed')
      }

      this.eventStream.setStatus('connecting')
      try {
        const connection = await this.options.connect()
        if (connection.isClosed()) {
          throw new Error('The NATS connection factory returned a closed connection')
        }
        if (this.closeRequested) {
          if (!connection.isClosed()) {
            await connection.drain()
          }
          throw new Error('The NATS runtime is closed')
        }

        this.activeConnection = connection
        this.connectionGeneration += 1
        this.eventStream.setStatus('connected', connection.getServer())
        void this.observeConnection(connection, this.connectionGeneration)
        void this.observePermanentClose(connection)
        return connection
      } catch (error) {
        if (this.closeRequested) {
          throw error
        }

        const context: NatsRuntimeRetryContext = { attempt, maxAttempts, error }
        const retryAllowed =
          attempt < maxAttempts &&
          (this.options.initialConnectRetry?.shouldRetry?.(context) ?? true)

        if (!retryAllowed) {
          this.eventStream.diagnostic({
            source: 'connection',
            code: 'connection-failed',
            level: 'error',
            message: `The NATS connection failed after ${attempt} attempt${attempt === 1 ? '' : 's'}`,
            ...(error instanceof Error ? { error } : {}),
            details: { attempt, maxAttempts, retryAllowed },
          })
          this.eventStream.setStatus('disconnected')
          throw error
        }

        const delayMs = this.retryDelay(context)

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

  private retryDelay(context: NatsRuntimeRetryContext): number {
    const configured = this.options.initialConnectRetry?.delayMs ?? 1_000
    const delayMs = typeof configured === 'function' ? configured(context) : configured
    if (!Number.isSafeInteger(delayMs) || delayMs < 0) {
      throw new RangeError('NATS runtime retry delay must be a non-negative integer')
    }
    return delayMs
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

  private async observeConnection(connection: NatsConnection, generation: number): Promise<void> {
    try {
      for await (const status of connection.status()) {
        if (generation !== this.connectionGeneration || connection !== this.activeConnection) {
          return
        }
        this.reportConnectionStatus(connection, status)
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

  private reportConnectionStatus(connection: NatsConnection, status: Status): void {
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
        this.handlePermanentClose(connection)
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

  private async observePermanentClose(connection: NatsConnection): Promise<void> {
    const error = await connection.closed()
    this.handlePermanentClose(connection, error)
  }

  private handlePermanentClose(connection: NatsConnection, error?: void | Error): void {
    if (connection !== this.activeConnection) return

    this.clearConnection(connection)
    if (this.closeRequested) return

    this.eventStream.diagnostic({
      source: 'connection',
      code: 'connection-closed',
      level: 'warning',
      message: 'The owned NATS connection closed permanently',
      ...(error instanceof Error ? { error } : {}),
      details: { generation: this.connectionGeneration },
    })
    this.eventStream.setStatus('disconnected')

    if ((this.options.connectionRecovery?.onPermanentClose ?? 'restart') === 'restart') {
      void this.connection().catch(() => undefined)
    }
  }

  private clearConnection(connection: NatsConnection): void {
    if (connection !== this.activeConnection) return
    this.activeConnection = undefined
    this.connectionPromise = undefined
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
    const bufferedBytes = allocation.bufferedBytes ?? 0
    this.validateAllocation('jetStreamConsumers', jetStreamConsumers)
    this.validateAllocation('bufferedMessages', bufferedMessages)
    this.validateAllocation('bufferedBytes', bufferedBytes)

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
    this.assertWithinLimit(
      'buffered-bytes',
      this.options.limits?.maxBufferedBytes,
      this.usedBufferedBytes,
      bufferedBytes
    )

    this.usedJetStreamConsumers += jetStreamConsumers
    this.usedBufferedMessages += bufferedMessages
    this.usedBufferedBytes += bufferedBytes
    return { jetStreamConsumers, bufferedMessages, bufferedBytes }
  }

  private release(allocation: Required<RuntimeResourceAllocation>): void {
    this.usedJetStreamConsumers -= allocation.jetStreamConsumers
    this.usedBufferedMessages -= allocation.bufferedMessages
    this.usedBufferedBytes -= allocation.bufferedBytes
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
  if (
    delayMs !== undefined &&
    typeof delayMs !== 'function' &&
    (!Number.isSafeInteger(delayMs) || delayMs < 0)
  ) {
    throw new RangeError('NATS runtime initialConnectRetry delayMs must be a non-negative integer')
  }

  const recovery = options.connectionRecovery?.onPermanentClose
  if (recovery !== undefined && recovery !== 'restart' && recovery !== 'wait') {
    throw new TypeError('NATS runtime onPermanentClose must be restart or wait')
  }

  return new DefaultNatsRuntime(options)
}
