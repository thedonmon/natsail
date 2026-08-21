import {
  DeliverPolicy,
  jetstream,
  jetstreamManager,
  type Consumer,
  type ConsumerMessages,
  type ConsumerNotification,
  type JsMsg,
  type OrderedConsumerOptions,
} from '@nats-io/jetstream'

import type { CheckpointStore, StreamCheckpoint } from '@natsail/checkpoints'
import {
  NATS_RUNTIME_ADAPTER,
  type NatsRuntime,
  type NatsRuntimeDiagnostic,
  type SubscriptionLease,
} from '@natsail/core'

export type JetStreamStart = 'all' | 'new' | { after: number }

export interface StreamCursor {
  stream: string
  epoch?: string
  sequence: number
}

export interface JetStreamDelivery<T> {
  value: T
  cursor: StreamCursor
  /** True when this sequence was already application-committed. */
  duplicate: boolean
  redelivered: boolean
}

export type JetStreamDuplicateDeliveryPolicy = 'deliver' | 'drop' | 'error'

export interface JetStreamSubscriptionOptions<T> {
  stream: string
  filter: string | readonly string[]
  start: JetStreamStart
  decode: (message: JsMsg) => T
  signal?: AbortSignal
  /** Maximum messages held by the nats.js pull loop. Defaults to 32. */
  maxBufferedMessages?: number
  /** Server cleanup delay for an abandoned ordered consumer. Defaults to 5 minutes. */
  inactiveThresholdMs?: number
  /** Action for a sequence at or behind the committed cursor. Defaults to `drop`. */
  duplicateDeliveryPolicy?: JetStreamDuplicateDeliveryPolicy
  /** Resumes from and advances a durable application checkpoint. */
  resume?: JetStreamResumeOptions
}

export interface JetStreamResumeOptions {
  key: string
  store: CheckpointStore
  /** Action when stream retention removed unprocessed sequences. Defaults to `error`. */
  retentionGapPolicy?: 'error' | 'continue'
}

export type JetStreamResumeErrorCode =
  | 'checkpoint-stream-mismatch'
  | 'checkpoint-epoch-mismatch'
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
      if (this.options.resume) {
        const manager = await jetstreamManager(connection)
        const [storedCheckpoint, streamInfo] = await Promise.all([
          this.options.resume.store.load(this.options.resume.key),
          manager.streams.info(this.options.stream),
        ])
        checkpoint = storedCheckpoint
        streamEpoch = streamInfo.created

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

      this.messages = await this.consumer.consume({
        max_messages: this.options.maxBufferedMessages ?? 32,
      })
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
          value: this.options.decode(message),
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
  if (
    typeof options.start === 'object' &&
    (!Number.isSafeInteger(options.start.after) || options.start.after < 0)
  ) {
    throw new RangeError('JetStream start.after must be a non-negative integer')
  }

  if (
    options.maxBufferedMessages !== undefined &&
    (!Number.isSafeInteger(options.maxBufferedMessages) || options.maxBufferedMessages < 1)
  ) {
    throw new RangeError('JetStream maxBufferedMessages must be a positive integer')
  }

  if (
    options.duplicateDeliveryPolicy !== undefined &&
    !(['deliver', 'drop', 'error'] as const).includes(options.duplicateDeliveryPolicy)
  ) {
    throw new TypeError('JetStream duplicateDeliveryPolicy must be deliver, drop, or error')
  }

  const maxBufferedMessages = options.maxBufferedMessages ?? 32
  return runtime[NATS_RUNTIME_ADAPTER].manage(
    () => new JetStreamSubscription(runtime, options, handler),
    { jetStreamConsumers: 1, bufferedMessages: maxBufferedMessages }
  )
}
