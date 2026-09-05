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

export interface NatsHandlerContext {
  /** Aborted on explicit cancellation or expiry of the runtime shutdown grace period. */
  readonly signal: AbortSignal
}

export type MessageHandler<T> = (
  value: T,
  message: Msg,
  context: NatsHandlerContext
) => void | Promise<void>

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
  /** Cancels unfinished work when the runtime's shutdown grace period expires. */
  abort?(reason: Error): void
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
  /** Shared, failure-isolated measurement path for runtime adapter packages. */
  readonly telemetry: NatsailTelemetryReporter
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

export type NatsailTelemetryOutcome = 'success' | 'failure'

/**
 * Deliberately small, low-cardinality dimensions emitted by NATSail itself.
 * Application identifiers, subjects, payloads, credentials, session keys,
 * stream names, and consumer names are never included.
 */
export type NatsailTelemetryAttributeValue = string | number | boolean
export type NatsailTelemetryAttributes = Readonly<Record<string, NatsailTelemetryAttributeValue>>

export type NatsailTelemetryCounterName =
  | 'natsail.batch.flushes'
  | 'natsail.browser.broker.events'
  | 'natsail.buffer.signals'
  | 'natsail.connection.attempts'
  | 'natsail.connection.transitions'
  | 'natsail.jetstream.acknowledgements'
  | 'natsail.jetstream.deliveries'
  | 'natsail.jetstream.recoveries'
  | 'natsail.runtime.resource.operations'
  | 'natsail.session.lifecycle'
  | 'natsail.session.references'
  | 'natsail.work.yields'

export type NatsailTelemetryGaugeName =
  | 'natsail.browser.broker.connections.active'
  | 'natsail.browser.broker.queue.bytes'
  | 'natsail.browser.broker.queue.depth'
  | 'natsail.browser.broker.sources.active'
  | 'natsail.browser.broker.tabs.active'
  | 'natsail.jetstream.replay.remaining'
  | 'natsail.runtime.capacity.limit'
  | 'natsail.runtime.capacity.used'
  | 'natsail.runtime.resources.active'
  | 'natsail.session.active'
  | 'natsail.session.references.active'

export type NatsailTelemetryDurationName =
  | 'natsail.checkpoint.operation.duration'
  | 'natsail.connection.attempt.duration'
  | 'natsail.connection.recovery.duration'
  | 'natsail.core.publish.duration'
  | 'natsail.core.request.duration'
  | 'natsail.jetstream.handler.duration'
  | 'natsail.jetstream.replay.duration'

interface NatsailTelemetryEventBase {
  /** Monotonic clock value supplied by the runtime host. */
  readonly at: number
  readonly attributes?: NatsailTelemetryAttributes
}

export type NatsailTelemetryEvent =
  | (NatsailTelemetryEventBase & {
      readonly type: 'counter'
      readonly name: NatsailTelemetryCounterName
      readonly value: number
    })
  | (NatsailTelemetryEventBase & {
      readonly type: 'gauge'
      readonly name: NatsailTelemetryGaugeName
      readonly value: number
    })
  | (NatsailTelemetryEventBase & {
      readonly type: 'duration'
      readonly name: NatsailTelemetryDurationName
      readonly durationMs: number
    })

/** Synchronous, dependency-free destination for NATSail measurements. */
export interface NatsailTelemetrySink {
  /**
   * Called inline with the observed operation. Implementations should enqueue
   * measurements and avoid blocking I/O; NATSail cannot preempt a blocking sink.
   */
  record(event: NatsailTelemetryEvent): void
}

/** Injectable monotonic clock used by telemetry duration measurements. */
export interface NatsailTelemetryClock {
  now(): number
}

/** Adapter-facing reporter. It is always safe to call, even when the sink throws. */
export interface NatsailTelemetryReporter {
  readonly enabled: boolean
  now(): number
  record(event: NatsailTelemetryEvent): void
}

export interface NatsailTelemetryReporterOptions {
  readonly sink?: NatsailTelemetrySink
  readonly clock?: NatsailTelemetryClock
  /** Low-cardinality primitive attributes added to every event. */
  readonly attributes?: NatsailTelemetryAttributes
}

const defaultTelemetryClock: NatsailTelemetryClock = {
  now: () => globalThis.performance?.now() ?? Date.now(),
}

/** Creates the failure-isolated reporter shared by Core, sessions, and adapter packages. */
export function createNatsailTelemetryReporter(
  options: NatsailTelemetryReporterOptions = {}
): NatsailTelemetryReporter {
  const clock = options.clock ?? defaultTelemetryClock
  const now = () => {
    try {
      const value = clock.now()
      return Number.isFinite(value) ? value : 0
    } catch {
      return 0
    }
  }
  return Object.freeze({
    enabled: options.sink !== undefined,
    now,
    record: (event: NatsailTelemetryEvent) => {
      try {
        options.sink?.record({
          ...event,
          ...(options.attributes === undefined && event.attributes === undefined
            ? {}
            : { attributes: { ...options.attributes, ...event.attributes } }),
        })
      } catch {
        // Telemetry is observational. A sink can never change runtime behavior.
      }
    },
  })
}

/** A cancellable task scheduled against a host-provided clock. */
export interface NatsailScheduledTask {
  cancel(): void
}

/**
 * The small scheduling surface shared by batching and cooperative reducer work.
 * Tests and non-browser hosts can replace it without patching global timers.
 */
export interface NatsailScheduler {
  /** Monotonic milliseconds. */
  now(): number
  schedule(task: () => void, delayMs: number): NatsailScheduledTask
  /** Gives other work a turn before reducer processing continues. */
  yield(): Promise<void>
}

export const natsailDefaultScheduler: NatsailScheduler = Object.freeze({
  now: () => globalThis.performance?.now() ?? Date.now(),
  schedule: (task: () => void, delayMs: number) => {
    const timer = setTimeout(task, delayMs)
    return { cancel: () => clearTimeout(timer) }
  },
  yield: () => new Promise<void>((resolve) => setTimeout(resolve, 0)),
})

/** At least one bound must be supplied. Byte bounds require `sizeOf`. */
export interface NatsailBatchPolicy<T> {
  readonly maxItems?: number
  readonly maxBytes?: number
  readonly maxWaitMs?: number
  readonly sizeOf?: (value: T) => number
}

/** Cooperative time slice for serial reducer work. */
export interface NatsailWorkBudget {
  readonly yieldAfterMs: number
  readonly scheduler: NatsailScheduler
}

export type NatsailBatchFlushReason = 'count' | 'bytes' | 'time' | 'completion' | 'manual'

export class NatsailBatchCancelledError extends Error {
  readonly name = 'NatsailBatchCancelledError'

  constructor() {
    super('The NATSail batch was cancelled before its pending values were applied')
  }
}

export class NatsailBatchItemTooLargeError extends Error {
  readonly name = 'NatsailBatchItemTooLargeError'

  constructor(
    readonly size: number,
    readonly limit: number
  ) {
    super(`NATSail batch item size ${size} exceeds the configured byte limit ${limit}`)
  }
}

function positiveSafeInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new TypeError(`NATSail ${label} must be a positive safe integer`)
  }
}

/** Validates and freezes a batch policy without evaluating application values. */
export function defineNatsailBatchPolicy<T>(
  policy: NatsailBatchPolicy<T>
): Readonly<NatsailBatchPolicy<T>> {
  positiveSafeInteger(policy.maxItems, 'batch maxItems')
  positiveSafeInteger(policy.maxBytes, 'batch maxBytes')
  if (
    policy.maxWaitMs !== undefined &&
    (!Number.isFinite(policy.maxWaitMs) || policy.maxWaitMs <= 0)
  ) {
    throw new TypeError('NATSail batch maxWaitMs must be a positive finite number')
  }
  if (
    policy.maxItems === undefined &&
    policy.maxBytes === undefined &&
    policy.maxWaitMs === undefined
  ) {
    throw new TypeError('NATSail batch policy must define at least one bound')
  }
  if (policy.maxBytes !== undefined && policy.sizeOf === undefined) {
    throw new TypeError('NATSail batch maxBytes requires sizeOf')
  }
  return Object.freeze({ ...policy })
}

/** Validates and freezes one cooperative work budget. */
export function defineNatsailWorkBudget(budget: NatsailWorkBudget): Readonly<NatsailWorkBudget> {
  if (!Number.isFinite(budget.yieldAfterMs) || budget.yieldAfterMs <= 0) {
    throw new TypeError('NATSail work yieldAfterMs must be a positive finite number')
  }
  if (
    typeof budget.scheduler?.now !== 'function' ||
    typeof budget.scheduler.schedule !== 'function' ||
    typeof budget.scheduler.yield !== 'function'
  ) {
    throw new TypeError('NATSail work scheduler must implement now, schedule, and yield')
  }
  return Object.freeze({ ...budget })
}

export interface NatsailBatcher<T> {
  /** Resolves only after the batch containing this value was applied. */
  add(value: T): Promise<void>
  /** Applies a pending partial batch during normal source completion. */
  complete(): Promise<void>
  /** Applies a pending partial batch explicitly. */
  flush(): Promise<void>
  /** Current application barrier, or undefined when intake may continue. */
  backpressure(): Promise<void> | undefined
  /** Number of values waiting for the next application. */
  pendingItems(): number
  /** Prevents later batch applications until a downstream commit settles. */
  barrier(commit: Promise<void>): void
  /** Settles after currently applying work and registered barriers. */
  settled(): Promise<void>
  /** Drops a pending partial batch. An in-flight application is not interrupted. */
  cancel(): void
}

export interface NatsailBatcherOptions {
  readonly scheduler?: NatsailScheduler
  readonly telemetry?: NatsailTelemetryReporter
  /** Stable low-cardinality adapter name, such as `session` or `effect`. */
  readonly source?: string
}

type PendingBatchValue<T> = {
  readonly value: T
  readonly size: number
  readonly resolve: () => void
  readonly reject: (error: unknown) => void
}

/**
 * Creates a bounded asynchronous batch coordinator. Applications are serialized;
 * a failed application rejects only that batch and later batches may continue.
 */
export function createNatsailBatcher<T>(
  policyInput: NatsailBatchPolicy<T>,
  apply: (values: readonly T[], reason: NatsailBatchFlushReason) => void | Promise<void>,
  options: NatsailBatcherOptions = {}
): NatsailBatcher<T> {
  const policy = defineNatsailBatchPolicy(policyInput)
  const scheduler = options.scheduler ?? natsailDefaultScheduler
  let pending: PendingBatchValue<T>[] = []
  let pendingBytes = 0
  let timer: NatsailScheduledTask | undefined
  let applications = Promise.resolve()
  let queuedApplications = 0
  let barrierFailure: unknown
  let cancelled = false
  let completed = false

  const recordFlush = (reason: NatsailBatchFlushReason) => {
    if (!options.telemetry?.enabled) return
    options.telemetry.record({
      type: 'counter',
      name: 'natsail.batch.flushes',
      value: 1,
      at: options.telemetry.now(),
      attributes: { reason, source: options.source ?? 'core' },
    })
  }
  const clearTimer = () => {
    timer?.cancel()
    timer = undefined
  }
  const drain = (reason: NatsailBatchFlushReason): Promise<void> => {
    if (pending.length === 0 || cancelled) return applications
    clearTimer()
    const batch = pending
    pending = []
    pendingBytes = 0
    queuedApplications += 1
    const operation = applications.then(async () => {
      try {
        if (barrierFailure !== undefined) throw barrierFailure
        await apply(
          batch.map((entry) => entry.value),
          reason
        )
        recordFlush(reason)
        for (const entry of batch) entry.resolve()
      } catch (error) {
        for (const entry of batch) entry.reject(error)
      } finally {
        queuedApplications -= 1
      }
    })
    applications = operation.catch(() => undefined)
    return operation
  }
  const armTimer = () => {
    if (timer !== undefined || policy.maxWaitMs === undefined) return
    timer = scheduler.schedule(() => {
      timer = undefined
      void drain('time')
    }, policy.maxWaitMs)
  }

  return {
    add: (value) => {
      if (cancelled || completed) {
        return Promise.reject(new NatsailBatchCancelledError())
      }
      let size = 0
      if (policy.maxBytes !== undefined) {
        try {
          size = policy.sizeOf!(value)
        } catch (error) {
          return Promise.reject(error)
        }
        if (!Number.isFinite(size) || size < 0) {
          return Promise.reject(
            new TypeError('NATSail batch sizeOf must return a finite non-negative number')
          )
        }
        if (size > policy.maxBytes) {
          return Promise.reject(new NatsailBatchItemTooLargeError(size, policy.maxBytes))
        }
      }
      if (
        policy.maxBytes !== undefined &&
        pending.length > 0 &&
        pendingBytes + size > policy.maxBytes
      ) {
        void drain('bytes')
      }
      const promise = new Promise<void>((resolve, reject) => {
        pending.push({ value, size, resolve, reject })
      })
      pendingBytes += size
      const countReached = policy.maxItems !== undefined && pending.length >= policy.maxItems
      const bytesReached = policy.maxBytes !== undefined && pendingBytes >= policy.maxBytes
      if (countReached || bytesReached) void drain(countReached ? 'count' : 'bytes')
      else armTimer()
      return promise
    },
    complete: async () => {
      if (cancelled || completed) return applications
      completed = true
      await drain('completion')
    },
    flush: () => drain('manual'),
    backpressure: () => (queuedApplications > 0 ? applications : undefined),
    pendingItems: () => pending.length,
    barrier: (commit) => {
      applications = applications
        .then(() => commit)
        .catch((error) => {
          barrierFailure ??= error
        })
    },
    settled: () => applications,
    cancel: () => {
      if (cancelled) return
      cancelled = true
      clearTimer()
      const error = new NatsailBatchCancelledError()
      for (const entry of pending) entry.reject(error)
      pending = []
      pendingBytes = 0
    },
  }
}

export interface NatsailWorkController {
  /** Yields when the current slice consumed the configured budget. */
  checkpoint(): Promise<void>
  reset(): void
}

/** Creates a serial-loop work controller using only the injected scheduler. */
export function createNatsailWorkController(
  budgetInput: NatsailWorkBudget,
  telemetry?: NatsailTelemetryReporter,
  source = 'core'
): NatsailWorkController {
  const budget = defineNatsailWorkBudget(budgetInput)
  let startedAt = budget.scheduler.now()
  return {
    checkpoint: async () => {
      if (budget.scheduler.now() - startedAt < budget.yieldAfterMs) return
      await budget.scheduler.yield()
      startedAt = budget.scheduler.now()
      if (telemetry?.enabled) {
        telemetry.record({
          type: 'counter',
          name: 'natsail.work.yields',
          value: 1,
          at: telemetry.now(),
          attributes: { source },
        })
      }
    },
    reset: () => {
      startedAt = budget.scheduler.now()
    },
  }
}

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
  /** Shutdown grace period, including connection drain. Defaults to 30,000 ms. */
  shutdownTimeoutMs?: number
  /** Per-iterator event capacity. Oldest events are dropped with an overflow diagnostic. Defaults to 256. */
  maxBufferedEvents?: number
  /** Optional bounded retry policy for each initial connection series. */
  initialConnectRetry?: NatsRuntimeInitialConnectRetryOptions
  /** Controls whether a permanently closed owned connection is replaced immediately. */
  connectionRecovery?: NatsRuntimeConnectionRecoveryOptions
  /** Optional connection-wide limits shared by every runtime adapter. */
  limits?: NatsRuntimeLimits
  /** Optional synchronous measurement sink. Sink failures are ignored. */
  telemetry?: NatsailTelemetrySink
  /**
   * Low-cardinality primitive attributes added to every measurement. NATSail's
   * reserved per-event keys take precedence over colliding caller keys.
   */
  telemetryAttributes?: NatsailTelemetryAttributes
  /** Monotonic telemetry clock override, primarily for deterministic hosts and tests. */
  telemetryClock?: NatsailTelemetryClock
}

export class NatsRuntimeShutdownTimeoutError extends Error {
  readonly name = 'NatsRuntimeShutdownTimeoutError'

  constructor(readonly timeoutMs: number) {
    super(`NATS runtime shutdown exceeded ${timeoutMs} ms; unfinished work was abandoned`)
  }
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
  dropped: number
}

class RuntimeEventStream implements AsyncIterable<NatsRuntimeEvent> {
  constructor(private readonly capacity = 256) {}

  [Symbol.asyncIterator](): AsyncIterator<NatsRuntimeEvent> {
    const subscriber: RuntimeEventSubscriber = {
      queue: [this.currentStatus],
      waiting: undefined,
      closed: this.closed,
      dropped: 0,
    }
    if (!subscriber.closed) {
      this.subscribers.add(subscriber)
    }

    return {
      next: () => {
        if (subscriber.dropped > 0) {
          const dropped = subscriber.dropped
          subscriber.dropped = 0
          return Promise.resolve({
            done: false,
            value: {
              type: 'diagnostic' as const,
              source: 'runtime' as const,
              code: 'event-buffer-overflow',
              level: 'warning' as const,
              message:
                'A slow runtime event subscriber lost older events; inspect() returns current state',
              at: Date.now(),
              details: { dropped, capacity: this.capacity },
            },
          })
        }
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
        if (subscriber.queue.length === this.capacity) {
          subscriber.queue.shift()
          subscriber.dropped = Math.min(Number.MAX_SAFE_INTEGER, subscriber.dropped + 1)
        }
        subscriber.queue.push(event)
      }
    }
  }

  private remove(subscriber: RuntimeEventSubscriber): void {
    subscriber.closed = true
    subscriber.queue.length = 0
    subscriber.dropped = 0
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
  private readonly cancellation = new AbortController()

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

  abort(reason: Error): void {
    this.cancellation.abort(reason)
    void this.close().catch(() => undefined)
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
        this.cancellation.abort(this.options.signal?.reason)
        void this.close().catch(() => undefined)
      }
      this.options.signal?.addEventListener('abort', abort, { once: true })
      this.resolveReady()

      for await (const message of this.subscription) {
        if (this.closeRequested) break
        const value = await decodePayload(this.options, message)
        this.cancellation.signal.throwIfAborted()
        if (this.closeRequested) break
        await this.handler(value, message, { signal: this.cancellation.signal })
        this.cancellation.signal.throwIfAborted()
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
      this.subscription?.unsubscribe()
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
  readonly [NATS_RUNTIME_ADAPTER]: NatsRuntimeAdapter
  readonly events: AsyncIterable<NatsRuntimeEvent>

  private readonly eventStream: RuntimeEventStream
  private readonly telemetry: NatsailTelemetryReporter
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
  private disconnectedAt: number | undefined

  constructor(private readonly options: NatsRuntimeOptions) {
    this.eventStream = new RuntimeEventStream(options.maxBufferedEvents)
    this.telemetry = createNatsailTelemetryReporter({
      ...(options.telemetry === undefined ? {} : { sink: options.telemetry }),
      ...(options.telemetryClock === undefined ? {} : { clock: options.telemetryClock }),
      ...(options.telemetryAttributes === undefined
        ? {}
        : { attributes: options.telemetryAttributes }),
    })
    this[NATS_RUNTIME_ADAPTER] = {
      manage: <T extends RuntimeResource>(
        create: () => T,
        allocation?: RuntimeResourceAllocation
      ): T => this.manage(create, allocation),
      reportDiagnostic: (diagnostic) => this.eventStream.diagnostic(diagnostic),
      telemetry: this.telemetry,
    }
    this.events = this.eventStream
    this.recordConfiguredLimits()
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

    const reconnectCycle = this.observeReconnectCycle()
    try {
      await connection.reconnect()
      await reconnectCycle.completed
    } catch (error) {
      if (!connection.isClosed()) throw error
    } finally {
      await reconnectCycle.cancel()
    }

    if (connection.isClosed()) {
      this.clearConnection(connection)
      return this.connection()
    }
    return connection
  }

  private observeReconnectCycle(): { completed: Promise<void>; cancel(): Promise<void> } {
    const iterator = this.eventStream[Symbol.asyncIterator]()
    const completed = (async () => {
      let disconnected = false
      while (true) {
        const next = await iterator.next()
        if (next.done) throw new Error('The NATS runtime closed during reconnect')
        if (next.value.type !== 'status') continue
        if (next.value.state === 'disconnected' || next.value.state === 'reconnecting') {
          disconnected = true
        } else if (disconnected && next.value.state === 'connected') {
          return
        } else if (next.value.state === 'closed') {
          throw new Error('The NATS runtime closed during reconnect')
        }
      }
    })()
    void completed.catch(() => undefined)
    return {
      completed,
      cancel: async () => {
        await iterator.return?.()
      },
    }
  }

  async publish(
    subject: string,
    data: Payload = new Uint8Array(0),
    options?: PublishOptions
  ): Promise<void> {
    const startedAt = this.telemetry.enabled ? this.telemetry.now() : 0
    try {
      const connection = await this.connection()
      connection.publish(subject, data, options)
      this.recordDuration('natsail.core.publish.duration', startedAt, {
        outcome: 'success',
        source: 'core',
      })
    } catch (error) {
      this.recordDuration('natsail.core.publish.duration', startedAt, {
        outcome: 'failure',
        source: 'core',
      })
      throw error
    }
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
    const startedAt = this.telemetry.enabled ? this.telemetry.now() : 0
    try {
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
      const result = await (options.signal
        ? rejectWhenAborted(runtimeBound, options.signal)
        : runtimeBound)
      this.recordDuration('natsail.core.request.duration', startedAt, {
        outcome: 'success',
        source: 'core',
      })
      return result
    } catch (error) {
      this.recordDuration('natsail.core.request.duration', startedAt, {
        outcome: 'failure',
        source: 'core',
      })
      throw error
    }
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
    const timeoutMs = this.options.shutdownTimeoutMs ?? 30_000
    let timer: ReturnType<typeof setTimeout> | undefined
    const graceful = async () => {
      const results = await Promise.allSettled(
        resources.map((resource) => Promise.resolve().then(() => resource.close()))
      )
      if (this.connectionPromise) {
        const connection = await this.connectionPromise.catch(() => undefined)
        if (connection && !connection.isClosed()) await connection.drain()
      }
      const errors = results
        .filter((result) => result.status === 'rejected')
        .map((result) => result.reason)
      if (errors.length) throw new AggregateError(errors, 'NATS runtime resource cleanup failed')
    }
    const expired = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        const error = new NatsRuntimeShutdownTimeoutError(timeoutMs)
        for (const resource of resources) {
          try {
            resource.abort?.(error)
          } catch {
            /* Continue closing the other resources. */
          }
        }
        this.forceCloseConnection()
        reject(error)
      }, timeoutMs)
    })
    try {
      await Promise.race([graceful(), expired])
    } catch (error) {
      this.forceCloseConnection()
      throw error
    } finally {
      clearTimeout(timer)
      this.eventStream.setStatus('closed')
    }
  }

  private forceCloseConnection(): void {
    const connection = this.activeConnection
    if (connection && !connection.isClosed()) {
      try {
        void connection.close().catch(() => undefined)
      } catch {
        /* Preserve the shutdown failure. */
      }
    }
  }

  private async connectWithRetry(): Promise<NatsConnection> {
    const maxAttempts = this.options.initialConnectRetry?.maxAttempts ?? 1

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      if (this.closeRequested) {
        throw new Error('The NATS runtime is closed')
      }

      const startedAt = this.telemetry.enabled ? this.telemetry.now() : 0
      this.recordCounter('natsail.connection.attempts', 1, { source: 'runtime' })
      this.eventStream.setStatus('connecting')
      try {
        const connection = await this.options.connect()
        if (connection.isClosed()) {
          throw new Error('The NATS connection factory returned a closed connection')
        }
        if (this.closeRequested) {
          if (!connection.isClosed()) {
            await connection.close()
          }
          throw new Error('The NATS runtime is closed')
        }

        this.activeConnection = connection
        this.connectionGeneration += 1
        this.recordDuration('natsail.connection.attempt.duration', startedAt, {
          outcome: 'success',
          source: 'runtime',
        })
        this.eventStream.setStatus('connected', connection.getServer())
        this.markRecovered()
        void this.observeConnection(connection, this.connectionGeneration)
        void this.observePermanentClose(connection)
        return connection
      } catch (error) {
        this.recordDuration('natsail.connection.attempt.duration', startedAt, {
          outcome: 'failure',
          source: 'runtime',
        })
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
          this.markDisconnected()
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
        this.markDisconnected()
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
        this.markDisconnected()
        break
      case 'reconnecting':
        this.eventStream.setStatus('reconnecting')
        break
      case 'reconnect':
        this.eventStream.setStatus('connected', status.server)
        this.markRecovered()
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
        this.recordCounter('natsail.buffer.signals', 1, {
          source: 'core',
          signal: 'slow-consumer',
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
    this.markDisconnected()

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
    this.recordResourceTelemetry('allocated')
    void resource.closed
      .finally(() => {
        this.resources.delete(resource)
        this.release(allocation)
        this.recordResourceTelemetry('released')
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
    this.recordCounter('natsail.runtime.resource.operations', 1, {
      action: 'rejected',
      resource: code,
      source: 'runtime',
    })
    throw error
  }

  private markDisconnected(): void {
    if (!this.telemetry.enabled || this.disconnectedAt !== undefined) return
    this.disconnectedAt = this.telemetry.now()
    this.recordCounter('natsail.connection.transitions', 1, {
      source: 'runtime',
      state: 'disconnected',
    })
  }

  private markRecovered(): void {
    if (this.disconnectedAt === undefined) return
    const startedAt = this.disconnectedAt
    this.disconnectedAt = undefined
    this.recordCounter('natsail.connection.transitions', 1, {
      source: 'runtime',
      state: 'recovered',
    })
    this.recordDuration('natsail.connection.recovery.duration', startedAt, {
      outcome: 'success',
      source: 'runtime',
    })
  }

  private recordResourceTelemetry(action: 'allocated' | 'released'): void {
    this.recordCounter('natsail.runtime.resource.operations', 1, {
      action,
      resource: 'managed',
      source: 'runtime',
    })
    this.recordGauge('natsail.runtime.resources.active', this.resources.size, {
      resource: 'managed',
      source: 'runtime',
    })
    this.recordGauge('natsail.runtime.capacity.used', this.usedJetStreamConsumers, {
      resource: 'jetstream-consumers',
      source: 'runtime',
    })
    this.recordGauge('natsail.runtime.capacity.used', this.usedBufferedMessages, {
      resource: 'buffered-messages',
      source: 'runtime',
    })
    this.recordGauge('natsail.runtime.capacity.used', this.usedBufferedBytes, {
      resource: 'buffered-bytes',
      source: 'runtime',
    })
  }

  private recordConfiguredLimits(): void {
    const limits = [
      ['jetstream-consumers', this.options.limits?.maxJetStreamConsumers],
      ['buffered-messages', this.options.limits?.maxBufferedMessages],
      ['buffered-bytes', this.options.limits?.maxBufferedBytes],
    ] as const
    for (const [resource, value] of limits) {
      if (value === undefined) continue
      this.recordGauge('natsail.runtime.capacity.limit', value, {
        resource,
        source: 'runtime',
      })
    }
  }

  private recordCounter(
    name: NatsailTelemetryCounterName,
    value: number,
    attributes?: NatsailTelemetryAttributes
  ): void {
    if (!this.telemetry.enabled) return
    this.telemetry.record({
      type: 'counter',
      name,
      value,
      at: this.telemetry.now(),
      ...(attributes === undefined ? {} : { attributes }),
    })
  }

  private recordGauge(
    name: NatsailTelemetryGaugeName,
    value: number,
    attributes?: NatsailTelemetryAttributes
  ): void {
    if (!this.telemetry.enabled) return
    this.telemetry.record({
      type: 'gauge',
      name,
      value,
      at: this.telemetry.now(),
      ...(attributes === undefined ? {} : { attributes }),
    })
  }

  private recordDuration(
    name: NatsailTelemetryDurationName,
    startedAt: number,
    attributes?: NatsailTelemetryAttributes
  ): void {
    if (!this.telemetry.enabled) return
    const at = this.telemetry.now()
    this.telemetry.record({
      type: 'duration',
      name,
      durationMs: Math.max(0, at - startedAt),
      at,
      ...(attributes === undefined ? {} : { attributes }),
    })
  }

  private validateAllocation(name: string, value: number): void {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new RangeError(`NATS runtime allocation ${name} must be a non-negative integer`)
    }
  }
}

export function createNatsRuntime(options: NatsRuntimeOptions): NatsRuntime {
  positiveSafeInteger(options.maxBufferedEvents, 'maxBufferedEvents')
  if (
    options.shutdownTimeoutMs !== undefined &&
    (!Number.isSafeInteger(options.shutdownTimeoutMs) ||
      options.shutdownTimeoutMs < 0 ||
      options.shutdownTimeoutMs > 2_147_483_647)
  ) {
    throw new RangeError(
      'NATS runtime shutdownTimeoutMs must be an integer between 0 and 2147483647'
    )
  }
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
