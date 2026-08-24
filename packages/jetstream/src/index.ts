import {
  AckPolicy,
  DeliverPolicy,
  jetstream,
  jetstreamManager,
  ReplayPolicy,
  type ConsumerConfig,
  type Consumer,
  type ConsumerMessages,
  type ConsumerNotification,
  type JsMsg,
  type OrderedConsumerOptions,
} from '@nats-io/jetstream'

import type { CheckpointStore, StreamCheckpoint } from '@natsail/checkpoints'
import {
  NATS_RUNTIME_ADAPTER,
  type NatsPayloadCodec,
  type NatsRuntime,
  type NatsRuntimeDiagnostic,
  type SubscriptionLease,
} from '@natsail/core'
import type { SessionSource } from '@natsail/session'

export type JetStreamStart = 'all' | 'new' | { after: number }

export interface StreamCursor {
  stream: string
  epoch?: string
  sequence: number
}

export interface JetStreamDelivery<T> {
  value: T
  subject: string
  cursor: StreamCursor
  /** True when this sequence was already application-committed. */
  duplicate: boolean
  redelivered: boolean
}

export type JetStreamDuplicateDeliveryPolicy = 'deliver' | 'drop' | 'error'

export interface JetStreamSubscriptionBaseOptions {
  stream: string
  filter: string | readonly string[]
  start: JetStreamStart
  signal?: AbortSignal
  /** Server cleanup delay for an abandoned ordered consumer. Defaults to 5 minutes. */
  inactiveThresholdMs?: number
  /** Action for a sequence at or behind the committed cursor. Defaults to `drop`. */
  duplicateDeliveryPolicy?: JetStreamDuplicateDeliveryPolicy
  /** Resumes from and advances a durable application checkpoint. */
  resume?: JetStreamResumeOptions
}

export type JetStreamBufferOptions =
  | {
      /** Maximum messages held by the nats.js pull loop. Defaults to 32. */
      maxBufferedMessages?: number
      maxBufferedBytes?: never
    }
  | {
      maxBufferedMessages?: never
      /** Maximum bytes held by the nats.js pull loop. Replaces the message-count limit. */
      maxBufferedBytes: number
    }

type JetStreamDecoding<T> =
  | { codec: NatsPayloadCodec<T>; decode?: never }
  | { codec?: never; decode: (message: JsMsg) => T }

export type JetStreamSubscriptionOptions<T> = JetStreamSubscriptionBaseOptions &
  JetStreamBufferOptions &
  JetStreamDecoding<T>

export interface JetStreamResumeOptions {
  key: string
  store: CheckpointStore
  /** Decoder or domain version appended to the normalized filter checkpoint identity. */
  scope?: string
  /** Action when stream retention removed unprocessed sequences. Defaults to `error`. */
  retentionGapPolicy?: 'error' | 'continue'
}

export type JetStreamResumeErrorCode =
  | 'checkpoint-stream-mismatch'
  | 'checkpoint-epoch-mismatch'
  | 'checkpoint-scope-mismatch'
  | 'retention-gap'

export interface JetStreamResumeErrorDetails {
  checkpointSequence?: number
  firstAvailableSequence?: number
}

export class JetStreamResumeError extends Error {
  readonly name = 'JetStreamResumeError'
  readonly checkpointSequence?: number
  readonly firstAvailableSequence?: number

  constructor(
    readonly code: JetStreamResumeErrorCode,
    message: string,
    details: JetStreamResumeErrorDetails = {}
  ) {
    super(message)
    if (details.checkpointSequence !== undefined) {
      this.checkpointSequence = details.checkpointSequence
    }
    if (details.firstAvailableSequence !== undefined) {
      this.firstAvailableSequence = details.firstAvailableSequence
    }
  }
}

export class JetStreamDuplicateError extends Error {
  readonly name = 'JetStreamDuplicateError'

  constructor(
    readonly sequence: number,
    readonly committedSequence: number
  ) {
    super(
      `JetStream sequence ${sequence} is not newer than committed sequence ${committedSequence}`
    )
  }
}

export type JetStreamHandler<T> = (delivery: JetStreamDelivery<T>) => void | Promise<void>

export type JetStreamProcessorConsumer =
  | { mode: 'bind'; name: string }
  | { mode: 'ensure'; name: string }
  | { mode: 'owned'; name: string }

export interface JetStreamProcessingDelivery<T> {
  value: T
  subject: string
  cursor: StreamCursor
  redelivered: boolean
  deliveryAttempt: number
}

export interface JetStreamProcessorBaseOptions {
  stream: string
  consumer: JetStreamProcessorConsumer
  filter: string | readonly string[]
  start: JetStreamStart
  signal?: AbortSignal
  /** Server wait before an unacknowledged delivery becomes eligible for redelivery. */
  ackWaitMs?: number
  maxDeliver?: number
  maxAckPending?: number
  replayPolicy?: ReplayPolicy
}

export type JetStreamProcessorOptions<T> = JetStreamProcessorBaseOptions &
  JetStreamBufferOptions &
  JetStreamDecoding<T>
export type JetStreamProcessorHandler<T> = (
  delivery: JetStreamProcessingDelivery<T>
) => void | Promise<void>

export type JetStreamProcessorConfigurationErrorCode =
  | 'ack-policy'
  | 'push-consumer'
  | 'filter-mismatch'
  | 'start-mismatch'
  | 'ack-wait-mismatch'
  | 'max-deliver-mismatch'
  | 'max-ack-pending-mismatch'
  | 'replay-policy-mismatch'

export class JetStreamProcessorConfigurationError extends Error {
  readonly name = 'JetStreamProcessorConfigurationError'

  constructor(
    readonly code: JetStreamProcessorConfigurationErrorCode,
    message: string
  ) {
    super(message)
  }
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

function validateJetStreamDecoder<T>(
  options: { codec?: NatsPayloadCodec<T>; decode?: (message: JsMsg) => T },
  operation: string
): void {
  const hasCodec = options.codec !== undefined
  const hasDecoder = options.decode !== undefined
  if (hasCodec === hasDecoder) {
    throw new TypeError(`${operation} requires exactly one codec or decode function`)
  }
}

function decodeJetStreamPayload<T>(
  options: { codec?: NatsPayloadCodec<T>; decode?: (message: JsMsg) => T },
  message: JsMsg
): T {
  return options.codec ? options.codec.decode(message.data) : options.decode!(message)
}

function orderedOptions<T>(
  options: JetStreamSubscriptionOptions<T>,
  start: JetStreamStart = options.start
): Partial<OrderedConsumerOptions> {
  const base: Partial<OrderedConsumerOptions> = {
    filter_subjects: typeof options.filter === 'string' ? options.filter : [...options.filter],
    inactive_threshold: options.inactiveThresholdMs ?? 5 * 60_000,
  }

  if (start === 'all') {
    return { ...base, deliver_policy: DeliverPolicy.All }
  }

  if (start === 'new') {
    return { ...base, deliver_policy: DeliverPolicy.New }
  }

  return {
    ...base,
    deliver_policy: DeliverPolicy.StartSequence,
    opt_start_seq: start.after + 1,
  }
}

function checkpointScope<T>(options: JetStreamSubscriptionOptions<T>): string {
  const filters = (typeof options.filter === 'string' ? [options.filter] : [...options.filter])
    .slice()
    .sort()
  const filterScope = JSON.stringify(filters)
  return options.resume?.scope ? `${filterScope}:${options.resume.scope}` : filterScope
}

function consumerDiagnostic<T>(
  notification: ConsumerNotification,
  options: JetStreamSubscriptionOptions<T>
): NatsRuntimeDiagnostic | undefined {
  const context = {
    stream: options.stream,
    filter: typeof options.filter === 'string' ? options.filter : [...options.filter],
  }

  switch (notification.type) {
    case 'heartbeats_missed':
      return {
        source: 'jetstream',
        code: 'heartbeats-missed',
        level: 'warning',
        message: 'The ordered consumer missed heartbeats',
        details: { ...context, count: notification.count },
      }
    case 'consumer_not_found':
      return {
        source: 'jetstream',
        code: 'consumer-not-found',
        level: 'warning',
        message: 'The ordered consumer was not found',
        details: {
          ...context,
          name: notification.name,
          count: notification.count,
        },
      }
    case 'stream_not_found':
      return {
        source: 'jetstream',
        code: 'stream-not-found',
        level: 'error',
        message: 'The JetStream stream was not found',
        details: {
          ...context,
          name: notification.name,
          consumerCreateFails: notification.consumerCreateFails ?? 0,
        },
      }
    case 'consumer_deleted':
      return {
        source: 'jetstream',
        code: 'consumer-deleted',
        level: 'warning',
        message: notification.description,
        details: { ...context, statusCode: notification.code },
      }
    case 'ordered_consumer_recreated':
      return {
        source: 'jetstream',
        code: 'ordered-consumer-recreated',
        level: 'info',
        message: 'The ordered consumer was recreated',
        details: { ...context, name: notification.name },
      }
    case 'exceeded_limits':
      return {
        source: 'jetstream',
        code: 'consumer-limits-exceeded',
        level: 'warning',
        message: notification.description,
        details: { ...context, statusCode: notification.code },
      }
    case 'no_responders':
      return {
        source: 'jetstream',
        code: 'no-responders',
        level: 'warning',
        message: 'JetStream did not respond to a consumer request',
        details: { ...context, statusCode: notification.code },
      }
    case 'discard':
      return {
        source: 'jetstream',
        code: 'consumer-discard',
        level: 'warning',
        message: 'The server could not satisfy the complete pull request',
        details: {
          ...context,
          messagesLeft: notification.messagesLeft,
          bytesLeft: notification.bytesLeft,
        },
      }
    case 'reset':
      return {
        source: 'jetstream',
        code: 'consumer-reset',
        level: 'info',
        message: 'The ordered consumer was reset',
        details: { ...context, name: notification.name },
      }
    default:
      return undefined
  }
}

class JetStreamSubscription<T> implements SubscriptionLease {
  readonly ready: Promise<void>
  readonly closed: Promise<void>

  private readonly readyState = deferred<void>()
  private readonly closedState = deferred<void>()
  private consumer?: Consumer
  private messages?: ConsumerMessages
  private closeRequested = false
  private readySettled = false

  constructor(
    runtime: NatsRuntime,
    private readonly options: JetStreamSubscriptionOptions<T>,
    private readonly handler: JetStreamHandler<T>
  ) {
    this.ready = this.readyState.promise
    this.closed = this.closedState.promise
    void this.closed.catch(() => undefined)
    void this.start(runtime)
  }

  async close(): Promise<void> {
    if (!this.closeRequested) {
      this.closeRequested = true
      if (this.messages) {
        await this.messages.close()
      }
    }

    return this.closed
  }

  private async start(runtime: NatsRuntime): Promise<void> {
    let abort: (() => void) | undefined

    try {
      const connection = await runtime.connection()
      const client = jetstream(connection)
      let checkpoint: StreamCheckpoint | undefined
      let streamEpoch: string | undefined
      let streamScope: string | undefined
      if (this.options.resume) {
        const manager = await jetstreamManager(connection)
        const [storedCheckpoint, streamInfo] = await Promise.all([
          this.options.resume.store.load(this.options.resume.key),
          manager.streams.info(this.options.stream),
        ])
        checkpoint = storedCheckpoint
        streamEpoch = streamInfo.created
        streamScope = checkpointScope(this.options)

        if (checkpoint && checkpoint.stream !== this.options.stream) {
          throw new JetStreamResumeError(
            'checkpoint-stream-mismatch',
            `Checkpoint stream ${checkpoint.stream} does not match ${this.options.stream}`,
            { checkpointSequence: checkpoint.sequence }
          )
        }
        if (checkpoint && checkpoint.epoch !== streamEpoch) {
          throw new JetStreamResumeError(
            'checkpoint-epoch-mismatch',
            `Checkpoint epoch does not match stream ${this.options.stream}`,
            { checkpointSequence: checkpoint.sequence }
          )
        }
        if (checkpoint?.scope !== undefined && checkpoint.scope !== streamScope) {
          throw new JetStreamResumeError(
            'checkpoint-scope-mismatch',
            `Checkpoint scope does not match stream ${this.options.stream}`,
            { checkpointSequence: checkpoint.sequence }
          )
        }
        if (
          checkpoint &&
          checkpoint.sequence + 1 < streamInfo.state.first_seq &&
          this.options.resume.retentionGapPolicy !== 'continue'
        ) {
          throw new JetStreamResumeError(
            'retention-gap',
            `Checkpoint ${checkpoint.sequence} is older than the first available stream sequence ${streamInfo.state.first_seq}`,
            {
              checkpointSequence: checkpoint.sequence,
              firstAvailableSequence: streamInfo.state.first_seq,
            }
          )
        }
      }

      const start = checkpoint ? { after: checkpoint.sequence } : this.options.start
      let committedSequence =
        checkpoint?.sequence ?? (typeof start === 'object' ? start.after : undefined)
      this.consumer = await client.consumers.get(
        this.options.stream,
        orderedOptions(this.options, start)
      )

      if (this.closeRequested || this.options.signal?.aborted) {
        this.resolveReady()
        this.closedState.resolve()
        return
      }

      const consumeOptions =
        this.options.maxBufferedBytes === undefined
          ? { max_messages: this.options.maxBufferedMessages ?? 32 }
          : { max_bytes: this.options.maxBufferedBytes }
      this.messages = await this.consumer.consume(consumeOptions)
      void this.observeDiagnostics(runtime, this.messages)
      abort = () => {
        void this.close().catch(() => undefined)
      }
      this.options.signal?.addEventListener('abort', abort, { once: true })
      this.resolveReady()

      for await (const message of this.messages) {
        const sequence = message.info.streamSequence
        const duplicate = committedSequence !== undefined && sequence <= committedSequence
        if (duplicate && committedSequence !== undefined) {
          const policy = this.options.duplicateDeliveryPolicy ?? 'drop'
          if (policy === 'drop') {
            continue
          }
          if (policy === 'error') {
            throw new JetStreamDuplicateError(sequence, committedSequence)
          }
        }

        const cursor: StreamCursor = {
          stream: message.info.stream,
          ...(streamEpoch === undefined ? {} : { epoch: streamEpoch }),
          sequence,
        }
        await this.handler({
          value: decodeJetStreamPayload(this.options, message),
          subject: message.subject,
          cursor,
          duplicate,
          redelivered: message.redelivered,
        })

        if (!duplicate) {
          committedSequence = cursor.sequence
        }
        if (!duplicate && this.options.resume && streamEpoch) {
          checkpoint = {
            stream: this.options.stream,
            epoch: streamEpoch,
            sequence: cursor.sequence,
            ...(streamScope === undefined ? {} : { scope: streamScope }),
          }
          await this.options.resume.store.save(this.options.resume.key, checkpoint)
        }
      }

      const messageError = await this.messages.closed()
      if (messageError) {
        throw messageError
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

      if (this.messages) {
        await this.messages.close().catch(() => undefined)
      }
      if (this.consumer) {
        await this.consumer.delete().catch(() => undefined)
      }
    }
  }

  private resolveReady(): void {
    if (!this.readySettled) {
      this.readySettled = true
      this.readyState.resolve()
    }
  }

  private async observeDiagnostics(
    runtime: NatsRuntime,
    messages: ConsumerMessages
  ): Promise<void> {
    try {
      for await (const notification of messages.status()) {
        const diagnostic = consumerDiagnostic(notification, this.options)
        if (diagnostic) {
          runtime[NATS_RUNTIME_ADAPTER].reportDiagnostic(diagnostic)
        }
      }
    } catch (error) {
      runtime[NATS_RUNTIME_ADAPTER].reportDiagnostic({
        source: 'jetstream',
        code: 'consumer-status-stream-failed',
        level: 'error',
        message: 'The ordered-consumer status stream failed',
        ...(error instanceof Error ? { error } : {}),
        details: { stream: this.options.stream },
      })
    }
  }
}

function millisecondsToNanos(value: number): number {
  const nanos = value * 1_000_000
  if (!Number.isSafeInteger(nanos)) {
    throw new RangeError('JetStream millisecond duration is too large')
  }
  return nanos
}

function durableConsumerConfig<T>(options: JetStreamProcessorOptions<T>): ConsumerConfig {
  const filters = typeof options.filter === 'string' ? [options.filter] : [...options.filter]
  const start = options.start
  const config: ConsumerConfig = {
    ...(options.consumer.mode === 'owned'
      ? { name: options.consumer.name }
      : { durable_name: options.consumer.name }),
    ack_policy: AckPolicy.Explicit,
    deliver_policy:
      start === 'all'
        ? DeliverPolicy.All
        : start === 'new'
          ? DeliverPolicy.New
          : DeliverPolicy.StartSequence,
    replay_policy: options.replayPolicy ?? ReplayPolicy.Instant,
    ...(typeof start === 'object' ? { opt_start_seq: start.after + 1 } : {}),
    ...(filters.length === 1 ? { filter_subject: filters[0]! } : { filter_subjects: filters }),
    ...(options.ackWaitMs === undefined
      ? {}
      : { ack_wait: millisecondsToNanos(options.ackWaitMs) }),
    ...(options.maxDeliver === undefined ? {} : { max_deliver: options.maxDeliver }),
    ...(options.maxAckPending === undefined ? {} : { max_ack_pending: options.maxAckPending }),
  }
  return config
}

class JetStreamProcessor<T> implements SubscriptionLease {
  readonly ready: Promise<void>
  readonly closed: Promise<void>

  private readonly readyState = deferred<void>()
  private readonly closedState = deferred<void>()
  private consumer?: Consumer
  private messages?: ConsumerMessages
  private closeRequested = false
  private readySettled = false

  constructor(
    runtime: NatsRuntime,
    private readonly options: JetStreamProcessorOptions<T>,
    private readonly handler: JetStreamProcessorHandler<T>
  ) {
    this.ready = this.readyState.promise
    this.closed = this.closedState.promise
    void this.closed.catch(() => undefined)
    void this.start(runtime)
  }

  async close(): Promise<void> {
    if (!this.closeRequested) {
      this.closeRequested = true
      if (this.messages) {
        await this.messages.close()
      }
    }
    return this.closed
  }

  private async start(runtime: NatsRuntime): Promise<void> {
    let abort: (() => void) | undefined
    let failure: unknown
    try {
      const connection = await runtime.connection()
      const client = jetstream(connection)
      if (this.options.consumer.mode !== 'bind') {
        const manager = await jetstreamManager(connection)
        try {
          await manager.consumers.add(this.options.stream, durableConsumerConfig(this.options))
        } catch (error) {
          if (this.options.consumer.mode === 'ensure') {
            try {
              const existing = await manager.consumers.info(
                this.options.stream,
                this.options.consumer.name
              )
              validateProcessorConsumer(existing.config, this.options)
            } catch (contractError) {
              if (contractError instanceof JetStreamProcessorConfigurationError) {
                throw contractError
              }
            }
          }
          throw error
        }
      }
      this.consumer = await client.consumers.get(this.options.stream, this.options.consumer.name)
      const consumerInfo = await this.consumer.info()
      validateProcessorConsumer(consumerInfo.config, this.options)

      if (this.closeRequested || this.options.signal?.aborted) {
        this.resolveReady()
        return
      }

      const consumeOptions =
        this.options.maxBufferedBytes === undefined
          ? { max_messages: this.options.maxBufferedMessages ?? 32 }
          : { max_bytes: this.options.maxBufferedBytes }
      this.messages = await this.consumer.consume(consumeOptions)
      abort = () => {
        void this.close().catch(() => undefined)
      }
      this.options.signal?.addEventListener('abort', abort, { once: true })
      this.resolveReady()

      for await (const message of this.messages) {
        await this.handler({
          value: decodeJetStreamPayload(this.options, message),
          subject: message.subject,
          cursor: {
            stream: message.info.stream,
            sequence: message.info.streamSequence,
          },
          redelivered: message.redelivered,
          deliveryAttempt: message.info.deliveryCount,
        })
        message.ack()
      }

      const messageError = await this.messages.closed()
      if (messageError) throw messageError
    } catch (error) {
      failure = error
      if (!this.readySettled) {
        this.readySettled = true
        this.readyState.reject(error)
      }
    } finally {
      if (abort) this.options.signal?.removeEventListener('abort', abort)
      if (this.messages) await this.messages.close().catch(() => undefined)
      if (this.options.consumer.mode === 'owned' && this.consumer) {
        await this.consumer.delete().catch(() => undefined)
      }
      if (failure === undefined) {
        this.closedState.resolve()
      } else {
        this.closedState.reject(failure)
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

function validateProcessorConsumer<T>(
  config: ConsumerConfig,
  options: JetStreamProcessorOptions<T>
): void {
  const name = options.consumer.name
  if (config.ack_policy !== AckPolicy.Explicit) {
    throw new JetStreamProcessorConfigurationError(
      'ack-policy',
      `JetStream consumer ${name} must use AckPolicy.Explicit`
    )
  }
  if (config.deliver_subject) {
    throw new JetStreamProcessorConfigurationError(
      'push-consumer',
      `JetStream consumer ${name} must be a pull consumer`
    )
  }

  const actualFilters = [
    ...(config.filter_subject ? [config.filter_subject] : []),
    ...(config.filter_subjects ?? []),
  ].sort()
  const expectedFilters = (
    typeof options.filter === 'string' ? [options.filter] : [...options.filter]
  ).sort()
  if (
    actualFilters.length !== expectedFilters.length ||
    actualFilters.some((filter, index) => filter !== expectedFilters[index])
  ) {
    throw new JetStreamProcessorConfigurationError(
      'filter-mismatch',
      `JetStream consumer ${name} filters do not match the requested processor filters`
    )
  }

  const expectedDeliverPolicy =
    options.start === 'all'
      ? DeliverPolicy.All
      : options.start === 'new'
        ? DeliverPolicy.New
        : DeliverPolicy.StartSequence
  if (
    config.deliver_policy !== expectedDeliverPolicy ||
    (typeof options.start === 'object' && config.opt_start_seq !== options.start.after + 1)
  ) {
    throw new JetStreamProcessorConfigurationError(
      'start-mismatch',
      `JetStream consumer ${name} start position does not match the requested processor start`
    )
  }

  validateOptionalConsumerField(
    name,
    'ack-wait-mismatch',
    'ack wait',
    options.ackWaitMs === undefined ? undefined : millisecondsToNanos(options.ackWaitMs),
    config.ack_wait
  )
  validateOptionalConsumerField(
    name,
    'max-deliver-mismatch',
    'maximum deliveries',
    options.maxDeliver,
    config.max_deliver
  )
  validateOptionalConsumerField(
    name,
    'max-ack-pending-mismatch',
    'maximum pending acknowledgements',
    options.maxAckPending,
    config.max_ack_pending
  )
  validateOptionalConsumerField(
    name,
    'replay-policy-mismatch',
    'replay policy',
    options.replayPolicy,
    config.replay_policy
  )
}

function validateOptionalConsumerField(
  consumerName: string,
  code: JetStreamProcessorConfigurationErrorCode,
  label: string,
  expected: number | string | undefined,
  actual: number | string | undefined
): void {
  if (expected !== undefined && actual !== expected) {
    throw new JetStreamProcessorConfigurationError(
      code,
      `JetStream consumer ${consumerName} ${label} does not match the requested processor value`
    )
  }
}

/**
 * Opens one ordered JetStream consumer for replay and live delivery.
 *
 * Ordered consumers use AckPolicy.None. Persist a delivered cursor only after
 * application processing succeeds, then reopen with `{ after: sequence }`.
 */
export function consumeJetStream<T>(
  runtime: NatsRuntime,
  options: JetStreamSubscriptionOptions<T>,
  handler: JetStreamHandler<T>
): SubscriptionLease {
  validateJetStreamDecoder(options, 'JetStream subscription')
  validateStart(options.start)
  validateBufferOptions(options)
  if (options.resume?.scope !== undefined && options.resume.scope.length === 0) {
    throw new TypeError('JetStream resume scope must not be empty')
  }

  if (
    options.duplicateDeliveryPolicy !== undefined &&
    !(['deliver', 'drop', 'error'] as const).includes(options.duplicateDeliveryPolicy)
  ) {
    throw new TypeError('JetStream duplicateDeliveryPolicy must be deliver, drop, or error')
  }

  const maxBufferedMessages =
    options.maxBufferedMessages ?? (options.maxBufferedBytes === undefined ? 32 : 0)
  const maxBufferedBytes = options.maxBufferedBytes ?? 0
  return runtime[NATS_RUNTIME_ADAPTER].manage(
    () => new JetStreamSubscription(runtime, options, handler),
    {
      jetStreamConsumers: 1,
      bufferedMessages: maxBufferedMessages,
      bufferedBytes: maxBufferedBytes,
    }
  )
}

/** Processes one named pull consumer and acknowledges each message after its handler succeeds. */
export function processJetStream<T>(
  runtime: NatsRuntime,
  options: JetStreamProcessorOptions<T>,
  handler: JetStreamProcessorHandler<T>
): SubscriptionLease {
  validateProcessorOptions(options)
  const maxBufferedMessages =
    options.maxBufferedMessages ?? (options.maxBufferedBytes === undefined ? 32 : 0)
  const maxBufferedBytes = options.maxBufferedBytes ?? 0
  return runtime[NATS_RUNTIME_ADAPTER].manage(
    () => new JetStreamProcessor(runtime, options, handler),
    {
      jetStreamConsumers: 1,
      bufferedMessages: maxBufferedMessages,
      bufferedBytes: maxBufferedBytes,
    }
  )
}

function validateProcessorOptions<T>(options: JetStreamProcessorOptions<T>): void {
  validateJetStreamDecoder(options, 'JetStream processor')
  if (options.stream.trim().length === 0) {
    throw new TypeError('JetStream processor stream must not be empty')
  }
  if (options.consumer.name.trim().length === 0) {
    throw new TypeError('JetStream processor consumer name must not be empty')
  }
  const filters = typeof options.filter === 'string' ? [options.filter] : options.filter
  if (filters.length === 0 || filters.some((filter) => filter.trim().length === 0)) {
    throw new TypeError('JetStream processor filter must contain at least one subject')
  }
  validateStart(options.start)
  validateBufferOptions(options)
  if (
    options.ackWaitMs !== undefined &&
    (!Number.isSafeInteger(options.ackWaitMs) || options.ackWaitMs <= 0)
  ) {
    throw new RangeError('JetStream processor ackWaitMs must be a positive integer')
  }
  if (options.ackWaitMs !== undefined) millisecondsToNanos(options.ackWaitMs)
  if (
    options.maxDeliver !== undefined &&
    options.maxDeliver !== -1 &&
    (!Number.isSafeInteger(options.maxDeliver) || options.maxDeliver <= 0)
  ) {
    throw new RangeError('JetStream processor maxDeliver must be -1 or a positive integer')
  }
  if (
    options.maxAckPending !== undefined &&
    (!Number.isSafeInteger(options.maxAckPending) || options.maxAckPending <= 0)
  ) {
    throw new RangeError('JetStream processor maxAckPending must be a positive integer')
  }
  if (
    options.replayPolicy !== undefined &&
    options.replayPolicy !== ReplayPolicy.Instant &&
    options.replayPolicy !== ReplayPolicy.Original
  ) {
    throw new TypeError('JetStream processor replayPolicy must be Instant or Original')
  }
}

function validateStart(start: JetStreamStart): void {
  if (typeof start === 'object' && (!Number.isSafeInteger(start.after) || start.after < 0)) {
    throw new RangeError('JetStream start.after must be a non-negative integer')
  }
}

function validateBufferOptions(options: JetStreamBufferOptions): void {
  if (
    options.maxBufferedMessages !== undefined &&
    (!Number.isSafeInteger(options.maxBufferedMessages) || options.maxBufferedMessages < 1)
  ) {
    throw new RangeError('JetStream maxBufferedMessages must be a positive integer')
  }
  if (
    options.maxBufferedBytes !== undefined &&
    (!Number.isSafeInteger(options.maxBufferedBytes) || options.maxBufferedBytes < 1)
  ) {
    throw new RangeError('JetStream maxBufferedBytes must be a positive integer')
  }
  if (options.maxBufferedMessages !== undefined && options.maxBufferedBytes !== undefined) {
    throw new TypeError('JetStream maxBufferedMessages and maxBufferedBytes are mutually exclusive')
  }
}

/** Adapts one checkpointed JetStream consumer into a registry-shared session source. */
export function createJetStreamSessionSource<T>(
  runtime: NatsRuntime,
  options: JetStreamSubscriptionOptions<T>
): SessionSource<JetStreamDelivery<T>> {
  return (accept) => consumeJetStream(runtime, options, accept)
}
