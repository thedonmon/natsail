import {
  Cause,
  Clock,
  Context,
  Data,
  Duration,
  Effect,
  Exit,
  Layer,
  Option,
  Queue,
  Stream,
} from 'effect'
import type { Pull } from 'effect'

import type {
  CoreRequestOptions,
  CoreSubscriptionOptions,
  NatsConnection,
  NatsRuntime,
  NatsRuntimeEvent,
  NatsRuntimeReconnectOptions,
  NatsRuntimeStatusEvent,
} from '@natsail/core'
import {
  createJetStreamSessionSource,
  processJetStream,
  type JetStreamCatchUp,
  type JetStreamDelivery,
  type JetStreamLease,
  type JetStreamProcessingDelivery,
  type JetStreamProcessorOptions,
  type ReducingJetStreamSessionOptions,
  type JetStreamSessionSourceOptions,
  type JetStreamStateSnapshot,
  type StreamCursor,
} from '@natsail/jetstream'
import type {
  SessionDefinition,
  SessionRegistry,
  SessionRegistryEvent,
  SessionSnapshot,
} from '@natsail/session'

export type NatsailOperation =
  | 'connection'
  | 'publish'
  | 'reconnect'
  | 'request'
  | 'restart-session'
  | 'runtime-events'
  | 'session-events'

export class NatsailOperationError extends Data.TaggedError('NatsailOperationError')<{
  readonly operation: NatsailOperation
  readonly message: string
  readonly cause: unknown
}> {}

export type NatsailSessionErrorStage = 'acquire' | 'source'

export class NatsailSessionError extends Data.TaggedError('NatsailSessionError')<{
  readonly key: string
  readonly stage: NatsailSessionErrorStage
  readonly message: string
  readonly cause: unknown
}> {}

export class NatsailStreamBufferOverflowError extends Data.TaggedError(
  'NatsailStreamBufferOverflowError'
)<{
  readonly stream: string
  readonly capacity: number
  readonly message: string
}> {}

export type NatsailSessionStreamError = NatsailSessionError | NatsailStreamBufferOverflowError

export type NatsailStreamOverflowStrategy = 'error' | 'dropping' | 'sliding'

export type NatsailSubjectStreamOverflowStrategy = NatsailStreamOverflowStrategy | 'suspend'

export type NatsailSubjectErrorStage = 'subscribe' | 'ready' | 'source'

export class NatsailSubjectError extends Data.TaggedError('NatsailSubjectError')<{
  readonly subject: string
  readonly queue?: string
  readonly stage: NatsailSubjectErrorStage
  readonly message: string
  readonly cause: unknown
}> {}

export type NatsailSubjectStreamError = NatsailSubjectError | NatsailStreamBufferOverflowError

export interface NatsailSubjectStreamOptions {
  /** Maximum decoded messages waiting for the Effect consumer. Defaults to 32. */
  readonly bufferSize?: number
  /**
   * `suspend` applies local backpressure and is the default. `error` fails
   * instead of losing messages. `dropping` and `sliding` are explicit
   * best-effort policies for ephemeral subjects.
   */
  readonly overflowStrategy?: NatsailSubjectStreamOverflowStrategy
}

export type NatsailJetStreamErrorStage = 'subscribe' | 'ready' | 'catch-up' | 'source' | 'processor'

export class NatsailJetStreamError extends Data.TaggedError('NatsailJetStreamError')<{
  readonly stream: string
  readonly filter: string | readonly string[]
  readonly stage: NatsailJetStreamErrorStage
  readonly message: string
  readonly cause: unknown
}> {}

export type NatsailJetStreamStreamError = NatsailJetStreamError | NatsailStreamBufferOverflowError

export interface NatsailJetStreamStreamOptions {
  /** Maximum deliveries and control events waiting for Effect. Defaults to 32. */
  readonly bufferSize?: number
  /** Reliable streams suspend by default. `error` terminates before a delivery is lost. */
  readonly overflowStrategy?: 'suspend' | 'error'
}

export type NatsailJetStreamEvent<T> =
  | {
      readonly type: 'delivery'
      readonly delivery: JetStreamDelivery<T>
    }
  | {
      readonly type: 'caught-up'
      readonly catchUp: JetStreamCatchUp
    }

export interface NatsailJetStreamMaterializer<Value, State, E = never, R = never> {
  readonly initial: () => State
  /** Called serially with one non-empty bounded batch. */
  readonly reduceBatch: (
    state: State,
    deliveries: readonly JetStreamDelivery<Value>[]
  ) => Effect.Effect<State, E, R>
}

/** A fresh materializer must rebuild state and therefore cannot resume only an event cursor. */
export type NatsailJetStreamMaterializeSourceOptions<T> = ReducingJetStreamSessionOptions<T>

export interface NatsailJetStreamMaterializeOptions extends NatsailJetStreamStreamOptions {
  /** Maximum deliveries reduced in one application call. Defaults to 256. */
  readonly batchSize?: number
  /** Maximum wait before a partial live batch is reduced. Defaults to 16ms. */
  readonly batchWithin?: Duration.Input
}

export interface NatsailJetStreamMaterializedState<State> {
  readonly phase: 'replaying' | 'live'
  readonly data: State
  readonly cursor?: StreamCursor
  readonly replay: {
    readonly delivered: number
  }
}

export interface NatsailJetStreamStateOptions extends NatsailSessionStreamOptions {
  /**
   * Maximum time that subsequent cumulative live states are coalesced. Replay,
   * reconnect, and the first hydrated live state remain immediate. Defaults to
   * 16ms; use zero to observe every reduced state.
   */
  readonly liveBatchWithin?: Duration.Input
}

export interface NatsailSessionStreamOptions {
  /** Maximum queued snapshots. Defaults to 32. Use `unbounded` only deliberately. */
  readonly bufferSize?: number | 'unbounded'
  /** Defaults to a typed `error` instead of silently losing state. */
  readonly overflowStrategy?: NatsailStreamOverflowStrategy
}

export interface NatsailResource {
  readonly runtime: NatsRuntime
  readonly sessions: SessionRegistry
  /** Replaces the default registry-then-runtime shutdown sequence. */
  close?(): Promise<void>
}

export interface NatsailService {
  /** Escape hatch for NATSail or nats.js operations not wrapped by this adapter. */
  readonly runtime: NatsRuntime
  /** Escape hatch for inspection and advanced shared-session operations. */
  readonly sessions: SessionRegistry
  readonly runtimeEvents: Stream.Stream<NatsRuntimeEvent, NatsailOperationError>
  readonly runtimeStatus: Stream.Stream<NatsRuntimeStatusEvent, NatsailOperationError>
  readonly sessionEvents: Stream.Stream<SessionRegistryEvent, NatsailOperationError>
  connection(): Effect.Effect<NatsConnection, NatsailOperationError>
  reconnect(
    options?: NatsRuntimeReconnectOptions
  ): Effect.Effect<NatsConnection, NatsailOperationError>
  publish(
    subject: string,
    data?: Parameters<NatsRuntime['publish']>[1],
    options?: Parameters<NatsRuntime['publish']>[2]
  ): Effect.Effect<void, NatsailOperationError>
  request<T>(options: CoreRequestOptions<T>): Effect.Effect<T, NatsailOperationError>
  /** Creates a cold, scoped Stream over one ephemeral Core NATS subscription. */
  subscribe<T>(
    options: CoreSubscriptionOptions<T>,
    streamOptions?: NatsailSubjectStreamOptions
  ): Stream.Stream<T, NatsailSubjectStreamError>
  /** Ordered replay plus live delivery with an explicit caught-up event. */
  jetStreamEvents<T>(
    options: JetStreamSessionSourceOptions<T>,
    streamOptions?: NatsailJetStreamStreamOptions
  ): Stream.Stream<NatsailJetStreamEvent<T>, NatsailJetStreamStreamError>
  /** Ordered JetStream deliveries without the caught-up control event. */
  jetStreamDeliveries<T>(
    options: JetStreamSessionSourceOptions<T>,
    streamOptions?: NatsailJetStreamStreamOptions
  ): Stream.Stream<JetStreamDelivery<T>, NatsailJetStreamStreamError>
  /** Rebuilds replay silently, emits once at catch-up, then microbatches live state. */
  materializeJetStream<Value, State, E, R>(
    options: NatsailJetStreamMaterializeSourceOptions<Value>,
    materializer: NatsailJetStreamMaterializer<Value, State, E, R>,
    streamOptions?: NatsailJetStreamMaterializeOptions
  ): Stream.Stream<NatsailJetStreamMaterializedState<State>, NatsailJetStreamStreamError | E, R>
  /** Observes one registry-shared reducing JetStream definition. */
  jetStreamStates<State>(
    definition: SessionDefinition<JetStreamStateSnapshot<State>>,
    options?: NatsailJetStreamStateOptions
  ): Stream.Stream<JetStreamStateSnapshot<State>, NatsailSessionStreamError>
  /** Runs an explicit-ack processor and acknowledges only after the Effect succeeds. */
  runJetStreamProcessor<T, E, R>(
    options: JetStreamProcessorOptions<T>,
    handler: (delivery: JetStreamProcessingDelivery<T>) => Effect.Effect<void, E, R>
  ): Effect.Effect<void, NatsailJetStreamError | E, R>
  restartSession(key: string): Effect.Effect<void, NatsailOperationError>
  sessionSnapshots<T>(
    definition: SessionDefinition<T>,
    options?: NatsailSessionStreamOptions
  ): Stream.Stream<SessionSnapshot<T>, NatsailSessionStreamError>
  sessionValues<T>(
    definition: SessionDefinition<T>,
    options?: NatsailSessionStreamOptions
  ): Stream.Stream<T, NatsailSessionStreamError>
}

/** Context service supplied by a NATSail Effect Layer. */
export class Natsail extends Context.Service<Natsail, NatsailService>()(
  '@natsail/effect/Natsail'
) {}

type PushOptions =
  | { readonly bufferSize: 'unbounded' }
  | { readonly bufferSize: number; readonly strategy: 'dropping' | 'sliding' }

interface ResolvedStreamOptions {
  readonly bufferSize: number | 'unbounded'
  readonly overflowStrategy: NatsailStreamOverflowStrategy
  readonly callbackOptions: PushOptions
}

interface ResolvedSubjectStreamOptions {
  readonly bufferSize: number
  readonly overflowStrategy: NatsailSubjectStreamOverflowStrategy
  readonly callbackStrategy: 'dropping' | 'sliding' | 'suspend'
}

interface ResolvedJetStreamStreamOptions {
  readonly bufferSize: number
  readonly overflowStrategy: 'suspend' | 'error'
  readonly callbackStrategy: 'suspend' | 'dropping'
}

interface JetStreamMaterializerAccumulator<State> {
  readonly phase: 'replaying' | 'live'
  readonly data: State
  readonly cursor?: StreamCursor
  readonly replayDelivered: number
}

class JetStreamEffectFailure<E> {
  constructor(readonly error: E) {}
}

class JetStreamEffectCause {
  constructor(readonly cause: Cause.Cause<never>) {}
}

const DEFAULT_STREAM_BUFFER_SIZE = 32

function describeCause(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause)
}

function operationError(operation: NatsailOperation, cause: unknown): NatsailOperationError {
  return new NatsailOperationError({
    operation,
    cause,
    message: `NATSail ${operation} failed: ${describeCause(cause)}`,
  })
}

function sessionError(
  key: string,
  stage: NatsailSessionErrorStage,
  cause: unknown
): NatsailSessionError {
  return new NatsailSessionError({
    key,
    stage,
    cause,
    message: `NATSail session ${key} ${stage} failed: ${describeCause(cause)}`,
  })
}

function tryRuntimePromise<A>(
  operation: NatsailOperation,
  run: (signal: AbortSignal) => PromiseLike<A>
): Effect.Effect<A, NatsailOperationError> {
  return Effect.tryPromise({
    try: run,
    catch: (cause) => operationError(operation, cause),
  })
}

function resolveStreamOptions(options: NatsailSessionStreamOptions = {}): ResolvedStreamOptions {
  const bufferSize = options.bufferSize ?? DEFAULT_STREAM_BUFFER_SIZE
  const overflowStrategy = options.overflowStrategy ?? 'error'

  if (bufferSize !== 'unbounded' && (!Number.isSafeInteger(bufferSize) || bufferSize <= 0)) {
    throw new TypeError('NATSail Effect stream bufferSize must be a positive safe integer')
  }

  return {
    bufferSize,
    overflowStrategy,
    callbackOptions:
      bufferSize === 'unbounded'
        ? { bufferSize: 'unbounded' }
        : {
            bufferSize,
            strategy: overflowStrategy === 'sliding' ? 'sliding' : 'dropping',
          },
  }
}

function resolveSubjectStreamOptions(
  options: NatsailSubjectStreamOptions = {}
): ResolvedSubjectStreamOptions {
  const bufferSize = options.bufferSize ?? DEFAULT_STREAM_BUFFER_SIZE
  const overflowStrategy = options.overflowStrategy ?? 'suspend'

  if (!Number.isSafeInteger(bufferSize) || bufferSize <= 0) {
    throw new TypeError('NATSail Effect subject bufferSize must be a positive safe integer')
  }

  return {
    bufferSize,
    overflowStrategy,
    callbackStrategy: overflowStrategy === 'error' ? 'dropping' : overflowStrategy,
  }
}

function resolveJetStreamStreamOptions(
  options: NatsailJetStreamStreamOptions = {}
): ResolvedJetStreamStreamOptions {
  const bufferSize = options.bufferSize ?? DEFAULT_STREAM_BUFFER_SIZE
  const overflowStrategy = options.overflowStrategy ?? 'suspend'

  if (!Number.isSafeInteger(bufferSize) || bufferSize <= 0) {
    throw new TypeError('NATSail Effect JetStream bufferSize must be a positive safe integer')
  }

  return {
    bufferSize,
    overflowStrategy,
    callbackStrategy: overflowStrategy === 'error' ? 'dropping' : 'suspend',
  }
}

function subjectError(
  options: CoreSubscriptionOptions<unknown>,
  stage: NatsailSubjectErrorStage,
  cause: unknown
): NatsailSubjectError {
  return new NatsailSubjectError({
    subject: options.subject,
    ...(options.queue === undefined ? {} : { queue: options.queue }),
    stage,
    cause,
    message: `NATSail subject ${options.subject} ${stage} failed: ${describeCause(cause)}`,
  })
}

function jetStreamError(
  options: Pick<JetStreamSessionSourceOptions<unknown>, 'stream' | 'filter'>,
  stage: NatsailJetStreamErrorStage,
  cause: unknown
): NatsailJetStreamError {
  return new NatsailJetStreamError({
    stream: options.stream,
    filter: options.filter,
    stage,
    cause,
    message: `NATSail JetStream ${options.stream} ${stage} failed: ${describeCause(cause)}`,
  })
}

function createSubjectStream<T>(
  runtime: NatsRuntime,
  options: CoreSubscriptionOptions<T>,
  streamOptions?: NatsailSubjectStreamOptions
): Stream.Stream<T, NatsailSubjectStreamError> {
  const resolved = resolveSubjectStreamOptions(streamOptions)
  const streamName = `subject:${options.subject}`

  return Stream.callback<T, NatsailSubjectStreamError>(
    (queue) =>
      Effect.gen(function* () {
        const effectSignal = yield* Effect.abortSignal
        const signal = options.signal
          ? AbortSignal.any([options.signal, effectSignal])
          : effectSignal
        const subscriptionOptions = { ...options, signal }
        const lease = yield* Effect.acquireRelease(
          Effect.try({
            try: () =>
              runtime.subscribe(subscriptionOptions, async (value) => {
                const accepted = await Effect.runPromise(Queue.offer(queue, value))

                if (
                  !accepted &&
                  resolved.overflowStrategy === 'error' &&
                  queue.state._tag === 'Open'
                ) {
                  const overflow = new NatsailStreamBufferOverflowError({
                    stream: streamName,
                    capacity: resolved.bufferSize,
                    message: `NATSail subject ${options.subject} exceeded its ${resolved.bufferSize}-message Effect buffer`,
                  })
                  await Effect.runPromise(Queue.fail(queue, overflow))
                  throw overflow
                }
              }),
            catch: (cause) => subjectError(options, 'subscribe', cause),
          }),
          (active) => Effect.promise(() => active.close().catch(() => undefined))
        )

        yield* Effect.tryPromise({
          try: () => lease.ready,
          catch: (cause) => subjectError(options, 'ready', cause),
        })
        yield* Effect.tryPromise({
          try: () => lease.closed,
          catch: (cause) =>
            cause instanceof NatsailStreamBufferOverflowError
              ? cause
              : subjectError(options, 'source', cause),
        })
      }).pipe(
        Effect.matchEffect({
          onFailure: (error) => Queue.fail(queue, error),
          onSuccess: () => Queue.end(queue),
        })
      ),
    {
      bufferSize: resolved.bufferSize,
      strategy: resolved.callbackStrategy,
    }
  )
}

function createJetStreamEventStream<T>(
  runtime: NatsRuntime,
  options: JetStreamSessionSourceOptions<T>,
  streamOptions?: NatsailJetStreamStreamOptions
): Stream.Stream<NatsailJetStreamEvent<T>, NatsailJetStreamStreamError> {
  const resolved = resolveJetStreamStreamOptions(streamOptions)
  const streamName = `jetstream:${options.stream}`

  return Stream.callback<NatsailJetStreamEvent<T>, NatsailJetStreamStreamError>(
    (queue) =>
      Effect.gen(function* () {
        const effectSignal = yield* Effect.abortSignal
        const signal = options.signal
          ? AbortSignal.any([options.signal, effectSignal])
          : effectSignal
        let resolveCatchUpMarker!: () => void
        let rejectCatchUpMarker!: (cause: unknown) => void
        const catchUpMarkerEnqueued = new Promise<void>((resolve, reject) => {
          resolveCatchUpMarker = resolve
          rejectCatchUpMarker = reject
        })
        void catchUpMarkerEnqueued.catch(() => undefined)
        const offerEvent = async (event: NatsailJetStreamEvent<T>): Promise<void> => {
          const accepted = await Effect.runPromise(Queue.offer(queue, event))

          if (!accepted && resolved.overflowStrategy === 'error' && queue.state._tag === 'Open') {
            const overflow = new NatsailStreamBufferOverflowError({
              stream: streamName,
              capacity: resolved.bufferSize,
              message: `NATSail JetStream ${options.stream} exceeded its ${resolved.bufferSize}-event Effect buffer`,
            })
            await Effect.runPromise(Queue.fail(queue, overflow))
            throw overflow
          }
        }
        const lease: JetStreamLease<T> = yield* Effect.acquireRelease(
          Effect.try({
            try: () => {
              const source = createJetStreamSessionSource(runtime, { ...options, signal })
              return source(async (delivery) => {
                if (delivery.replay === 'live') await catchUpMarkerEnqueued
                await offerEvent({ type: 'delivery', delivery })
              }) as JetStreamLease<T>
            },
            catch: (cause) => jetStreamError(options, 'subscribe', cause),
          }),
          (active) => Effect.promise(() => active.close().catch(() => undefined))
        )

        yield* Effect.tryPromise({
          try: () => lease.ready,
          catch: (cause) => jetStreamError(options, 'ready', cause),
        })
        const catchUpEvent = lease.caughtUp.then(async (catchUp) => {
          await offerEvent({ type: 'caught-up', catchUp })
          return catchUp
        })
        void catchUpEvent.then(resolveCatchUpMarker, rejectCatchUpMarker)
        yield* Effect.tryPromise({
          try: () => catchUpEvent,
          catch: (cause) =>
            cause instanceof NatsailStreamBufferOverflowError
              ? cause
              : jetStreamError(options, 'catch-up', cause),
        })
        yield* Effect.tryPromise({
          try: () => lease.closed,
          catch: (cause) =>
            cause instanceof NatsailStreamBufferOverflowError
              ? cause
              : jetStreamError(options, 'source', cause),
        })
      }).pipe(
        Effect.matchEffect({
          onFailure: (error) => Queue.fail(queue, error),
          onSuccess: () => Queue.end(queue),
        })
      ),
    {
      bufferSize: resolved.bufferSize,
      strategy: resolved.callbackStrategy,
    }
  )
}

function createJetStreamDeliveryStream<T>(
  runtime: NatsRuntime,
  options: JetStreamSessionSourceOptions<T>,
  streamOptions?: NatsailJetStreamStreamOptions
): Stream.Stream<JetStreamDelivery<T>, NatsailJetStreamStreamError> {
  return createJetStreamEventStream(runtime, options, streamOptions).pipe(
    Stream.filter(
      (event): event is Extract<NatsailJetStreamEvent<T>, { readonly type: 'delivery' }> =>
        event.type === 'delivery'
    ),
    Stream.map((event) => event.delivery)
  )
}

function createJetStreamMaterializedStream<Value, State, E, R>(
  runtime: NatsRuntime,
  options: NatsailJetStreamMaterializeSourceOptions<Value>,
  materializer: NatsailJetStreamMaterializer<Value, State, E, R>,
  streamOptions: NatsailJetStreamMaterializeOptions = {}
): Stream.Stream<NatsailJetStreamMaterializedState<State>, NatsailJetStreamStreamError | E, R> {
  const batchSize = streamOptions.batchSize ?? 256
  const batchWithin = streamOptions.batchWithin ?? '16 millis'

  if (options.resume) {
    throw new TypeError(
      'A JetStream materializer cannot resume an event cursor without restoring matching materialized state'
    )
  }
  if (!Number.isSafeInteger(batchSize) || batchSize <= 0) {
    throw new TypeError('NATSail Effect JetStream batchSize must be a positive safe integer')
  }

  return Stream.suspend(() => {
    const initial = materializer.initial()
    const initialAccumulator: JetStreamMaterializerAccumulator<State> = {
      phase: 'replaying',
      data: initial,
      replayDelivered: 0,
    }
    const initialSnapshot: NatsailJetStreamMaterializedState<State> = {
      phase: 'replaying',
      data: initial,
      replay: { delivered: 0 },
    }
    const updates = createJetStreamEventStream(runtime, options, streamOptions).pipe(
      Stream.groupedWithin(batchSize, batchWithin),
      Stream.mapAccumEffect(
        () => initialAccumulator,
        (previous, events) =>
          Effect.gen(function* () {
            let data = previous.data
            let phase = previous.phase
            let cursor = previous.cursor
            let replayDelivered = previous.replayDelivered
            let pending: Array<JetStreamDelivery<Value>> = []
            const snapshots: Array<NatsailJetStreamMaterializedState<State>> = []

            const snapshot = (): NatsailJetStreamMaterializedState<State> => ({
              phase: 'live',
              data,
              ...(cursor === undefined ? {} : { cursor }),
              replay: { delivered: replayDelivered },
            })

            const reducePending = (): Effect.Effect<void, E, R> => {
              if (pending.length === 0) return Effect.void

              const deliveries = pending
              pending = []
              return materializer.reduceBatch(data, deliveries).pipe(
                Effect.tap((nextData) =>
                  Effect.sync(() => {
                    data = nextData
                    cursor = deliveries[deliveries.length - 1]?.cursor ?? cursor
                    if (phase === 'replaying') replayDelivered += deliveries.length
                  })
                ),
                Effect.asVoid
              )
            }

            for (const event of events) {
              if (event.type === 'delivery') {
                pending.push(event.delivery)
                continue
              }

              yield* reducePending()
              phase = 'live'
              replayDelivered = event.catchUp.delivered
              cursor = event.catchUp.cursor ?? cursor
              snapshots.push(snapshot())
            }
            const hasDeliveriesAfterLastSnapshot = pending.length > 0
            yield* reducePending()
            if (phase === 'live' && hasDeliveriesAfterLastSnapshot) snapshots.push(snapshot())

            const next: JetStreamMaterializerAccumulator<State> = {
              phase,
              data,
              replayDelivered,
              ...(cursor === undefined ? {} : { cursor }),
            }

            return [next, snapshots] as const
          })
      )
    )

    return Stream.concat(Stream.succeed(initialSnapshot), updates)
  })
}

function runJetStreamProcessorEffect<T, E, R>(
  runtime: NatsRuntime,
  options: JetStreamProcessorOptions<T>,
  handler: (delivery: JetStreamProcessingDelivery<T>) => Effect.Effect<void, E, R>
): Effect.Effect<void, NatsailJetStreamError | E, R> {
  return Effect.scoped(
    Effect.gen(function* () {
      const context = yield* Effect.context<R>()
      const effectSignal = yield* Effect.abortSignal
      const signal = options.signal ? AbortSignal.any([options.signal, effectSignal]) : effectSignal
      const processorOptions = { ...options, signal }
      const lease = yield* Effect.acquireRelease(
        Effect.try({
          try: () =>
            processJetStream(runtime, processorOptions, async (delivery) => {
              const exit = await Effect.runPromiseExitWith(context)(handler(delivery), { signal })
              if (Exit.isFailure(exit)) {
                const expected = Cause.findErrorOption(exit.cause)
                if (Option.isSome(expected)) throw new JetStreamEffectFailure(expected.value)
                throw new JetStreamEffectCause(exit.cause as Cause.Cause<never>)
              }
            }),
          catch: (cause) => jetStreamError(options, 'processor', cause),
        }),
        (active) => Effect.promise(() => active.close().catch(() => undefined))
      )

      yield* Effect.tryPromise({
        try: () => lease.ready,
        catch: (cause) => jetStreamError(options, 'processor', cause),
      })
      yield* Effect.tryPromise({
        try: () => lease.closed,
        catch: (cause) => cause,
      }).pipe(
        Effect.catch((cause) => {
          if (cause instanceof JetStreamEffectCause) return Effect.failCause(cause.cause)
          const error: E | NatsailJetStreamError =
            cause instanceof JetStreamEffectFailure
              ? (cause.error as E)
              : jetStreamError(options, 'processor', cause)
          return Effect.fail(error)
        })
      )
    })
  )
}

function createSessionSnapshotStream<T>(
  sessions: SessionRegistry,
  definition: SessionDefinition<T>,
  options?: NatsailSessionStreamOptions
): Stream.Stream<SessionSnapshot<T>, NatsailSessionStreamError> {
  const resolved = resolveStreamOptions(options)

  return Stream.callback<SessionSnapshot<T>, NatsailSessionStreamError>(
    (queue) =>
      Effect.gen(function* () {
        const handle = yield* Effect.acquireRelease(
          Effect.try({
            try: () => sessions.acquire(definition),
            catch: (cause) => sessionError(definition.key, 'acquire', cause),
          }),
          (active) => Effect.promise(() => active.release())
        )
        let terminal = false

        const emitSnapshot = () => {
          if (terminal) return
          const snapshot = handle.getSnapshot()
          const accepted = Queue.offerUnsafe(queue, snapshot)

          if (
            !accepted &&
            resolved.bufferSize !== 'unbounded' &&
            resolved.overflowStrategy === 'error'
          ) {
            terminal = true
            Queue.failCauseUnsafe(
              queue,
              Cause.fail(
                new NatsailStreamBufferOverflowError({
                  stream: `session:${definition.key}`,
                  capacity: resolved.bufferSize,
                  message: `NATSail session ${definition.key} exceeded its ${resolved.bufferSize}-snapshot Effect buffer`,
                })
              )
            )
            return
          }

          if (snapshot.phase === 'error') {
            terminal = true
            Queue.failCauseUnsafe(
              queue,
              Cause.fail(
                sessionError(
                  definition.key,
                  'source',
                  snapshot.error ?? new Error('The session source failed')
                )
              )
            )
          } else if (snapshot.phase === 'closed') {
            terminal = true
            Queue.endUnsafe(queue)
          }
        }

        yield* Effect.acquireRelease(
          Effect.sync(() => handle.subscribe(emitSnapshot)),
          (unsubscribe) => Effect.sync(unsubscribe)
        )
        emitSnapshot()
      }),
    resolved.callbackOptions.bufferSize === 'unbounded'
      ? undefined
      : {
          bufferSize: resolved.callbackOptions.bufferSize,
          strategy: resolved.callbackOptions.strategy,
        }
  )
}

function createSessionValueStream<T>(
  sessions: SessionRegistry,
  definition: SessionDefinition<T>,
  options?: NatsailSessionStreamOptions
): Stream.Stream<T, NatsailSessionStreamError> {
  return createSessionSnapshotStream(sessions, definition, options).pipe(
    Stream.mapAccum(
      () => -1,
      (valueRevision, snapshot): readonly [number, ReadonlyArray<T>] => {
        if (
          snapshot.valueRevision !== valueRevision &&
          Object.prototype.hasOwnProperty.call(snapshot, 'value')
        ) {
          return [snapshot.valueRevision, [snapshot.value as T]]
        }
        return [valueRevision, []]
      }
    )
  )
}

function resolveLiveBatchMs(input: Duration.Input): number {
  try {
    if (typeof input === 'number' && !Number.isFinite(input)) throw new TypeError()
    const duration = Duration.fromInputUnsafe(input)
    const milliseconds = Duration.toMillis(duration)
    if (!Duration.isFinite(duration) || !Number.isFinite(milliseconds) || milliseconds < 0) {
      throw new TypeError()
    }
    return milliseconds
  } catch {
    throw new TypeError('NATSail Effect liveBatchWithin must be a finite non-negative duration')
  }
}

function coalesceJetStreamStates<State, E, R>(
  source: Stream.Stream<JetStreamStateSnapshot<State>, E, R>,
  liveBatchMs: number
): Stream.Stream<JetStreamStateSnapshot<State>, E, R> {
  if (liveBatchMs === 0) return source

  return Stream.transformPull(source, (pull) =>
    Effect.sync(() => {
      type Snapshot = JetStreamStateSnapshot<State>

      let buffered: Snapshot[] = []
      let bufferedIndex = 0
      let seenLive = false
      let pendingLive: Snapshot | undefined
      let flushAt: number | undefined
      let terminal: Cause.Cause<E | Cause.Done<void>> | undefined

      const flushPending = (output: Snapshot[]): void => {
        if (pendingLive === undefined) return
        output.push(pendingLive)
        pendingLive = undefined
        flushAt = undefined
      }

      const next: Pull.Pull<readonly [Snapshot, ...Snapshot[]], E, void, R> =
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const output: Snapshot[] = []

            while (true) {
              while (bufferedIndex < buffered.length) {
                const value = buffered[bufferedIndex++]!
                if (value.phase !== 'live') {
                  flushPending(output)
                  seenLive = false
                  output.push(value)
                  continue
                }

                if (!seenLive) {
                  seenLive = true
                  output.push(value)
                  continue
                }

                pendingLive = value
                flushAt ??= (yield* Clock.currentTimeMillis) + liveBatchMs
              }
              buffered = []
              bufferedIndex = 0

              if (output.length > 0) {
                return output as [Snapshot, ...Snapshot[]]
              }

              if (terminal !== undefined) {
                if (pendingLive !== undefined) {
                  flushPending(output)
                  return output as [Snapshot, ...Snapshot[]]
                }
                return yield* Effect.failCause(terminal)
              }

              if (pendingLive === undefined) {
                const result = yield* restore(Effect.exit(pull))
                if (Exit.isFailure(result)) terminal = result.cause
                else buffered.push(...result.value)
                continue
              }

              const now = yield* Clock.currentTimeMillis
              const remaining = Math.max(0, (flushAt ?? now) - now)
              if (remaining === 0) {
                flushPending(output)
                return output as [Snapshot, ...Snapshot[]]
              }

              const result = yield* restore(
                Effect.raceFirst(
                  Effect.exit(pull).pipe(Effect.map((exit) => ({ _tag: 'Pulled' as const, exit }))),
                  Effect.sleep(remaining).pipe(Effect.as({ _tag: 'Elapsed' as const }))
                )
              )

              if (result._tag === 'Elapsed') {
                flushPending(output)
                return output as [Snapshot, ...Snapshot[]]
              }
              if (Exit.isFailure(result.exit)) terminal = result.exit.cause
              else buffered.push(...result.exit.value)
            }
          })
        )

      return next
    })
  )
}

function createJetStreamStateStream<State>(
  sessions: SessionRegistry,
  definition: SessionDefinition<JetStreamStateSnapshot<State>>,
  options: NatsailJetStreamStateOptions = {}
): Stream.Stream<JetStreamStateSnapshot<State>, NatsailSessionStreamError> {
  const { liveBatchWithin = '16 millis', ...sessionOptions } = options
  const liveBatchMs = resolveLiveBatchMs(liveBatchWithin)
  return coalesceJetStreamStates(
    createSessionValueStream(sessions, definition, sessionOptions),
    liveBatchMs
  )
}

/** Creates a service over application-owned runtime and registry objects. */
export function makeNatsail(resource: NatsailResource): NatsailService {
  const { runtime, sessions } = resource
  const runtimeEvents = Stream.fromAsyncIterable(runtime.events, (cause) =>
    operationError('runtime-events', cause)
  )

  return {
    runtime,
    sessions,
    runtimeEvents,
    runtimeStatus: runtimeEvents.pipe(
      Stream.filter((event): event is NatsRuntimeStatusEvent => event.type === 'status'),
      Stream.changesWith(
        (previous, next) => previous.state === next.state && previous.server === next.server
      )
    ),
    sessionEvents: Stream.fromAsyncIterable(sessions.events, (cause) =>
      operationError('session-events', cause)
    ),
    connection: () => tryRuntimePromise('connection', () => runtime.connection()),
    reconnect: (options) => tryRuntimePromise('reconnect', () => runtime.reconnect(options)),
    publish: (subject, data, options) =>
      tryRuntimePromise('publish', () => runtime.publish(subject, data, options)),
    request: <T>(options: CoreRequestOptions<T>) =>
      tryRuntimePromise('request', (effectSignal) =>
        runtime.request({
          ...options,
          signal: options.signal ? AbortSignal.any([options.signal, effectSignal]) : effectSignal,
        })
      ),
    subscribe: (options, streamOptions) => createSubjectStream(runtime, options, streamOptions),
    jetStreamEvents: (options, streamOptions) =>
      createJetStreamEventStream(runtime, options, streamOptions),
    jetStreamDeliveries: (options, streamOptions) =>
      createJetStreamDeliveryStream(runtime, options, streamOptions),
    materializeJetStream: (options, materializer, streamOptions) =>
      createJetStreamMaterializedStream(runtime, options, materializer, streamOptions),
    jetStreamStates: (definition, options) =>
      createJetStreamStateStream(sessions, definition, options),
    runJetStreamProcessor: (options, handler) =>
      runJetStreamProcessorEffect(runtime, options, handler),
    restartSession: (key) => tryRuntimePromise('restart-session', () => sessions.restart(key)),
    sessionSnapshots: (definition, options) =>
      createSessionSnapshotStream(sessions, definition, options),
    sessionValues: <T>(definition: SessionDefinition<T>, options?: NatsailSessionStreamOptions) =>
      createSessionValueStream(sessions, definition, options),
  }
}

/**
 * Creates a cold Core NATS subject Stream that reads its runtime from the
 * `Natsail` service. Each consumer owns one scoped subscription.
 */
export function subscribe<T>(
  options: CoreSubscriptionOptions<T>,
  streamOptions?: NatsailSubjectStreamOptions
): Stream.Stream<T, NatsailSubjectStreamError, Natsail> {
  return Stream.unwrap(Natsail.useSync((service) => service.subscribe(options, streamOptions)))
}

/**
 * Creates a cold ordered JetStream Stream with an explicit replay-complete
 * event. NATSail remains the single owner of recovery and checkpoints.
 */
export function jetStreamEvents<T>(
  options: JetStreamSessionSourceOptions<T>,
  streamOptions?: NatsailJetStreamStreamOptions
): Stream.Stream<NatsailJetStreamEvent<T>, NatsailJetStreamStreamError, Natsail> {
  return Stream.unwrap(
    Natsail.useSync((service) => service.jetStreamEvents(options, streamOptions))
  )
}

/** Creates a cold ordered JetStream Stream containing delivery values only. */
export function jetStreamDeliveries<T>(
  options: JetStreamSessionSourceOptions<T>,
  streamOptions?: NatsailJetStreamStreamOptions
): Stream.Stream<JetStreamDelivery<T>, NatsailJetStreamStreamError, Natsail> {
  return Stream.unwrap(
    Natsail.useSync((service) => service.jetStreamDeliveries(options, streamOptions))
  )
}

/**
 * Rebuilds historical state without rendering every intermediate state, emits
 * one atomic live snapshot at catch-up, and then emits microbatched updates.
 */
export function materializeJetStream<Value, State, E, R>(
  options: NatsailJetStreamMaterializeSourceOptions<Value>,
  materializer: NatsailJetStreamMaterializer<Value, State, E, R>,
  streamOptions?: NatsailJetStreamMaterializeOptions
): Stream.Stream<
  NatsailJetStreamMaterializedState<State>,
  NatsailJetStreamStreamError | E,
  Natsail | R
> {
  return Stream.unwrap(
    Natsail.useSync((service) => service.materializeJetStream(options, materializer, streamOptions))
  )
}

/**
 * Observes cumulative state from one registry-shared reducing JetStream
 * definition. Every source update is retained by the registry; only downstream
 * live presentation notifications are coalesced.
 */
export function jetStreamStates<State>(
  definition: SessionDefinition<JetStreamStateSnapshot<State>>,
  options?: NatsailJetStreamStateOptions
): Stream.Stream<JetStreamStateSnapshot<State>, NatsailSessionStreamError, Natsail> {
  return Stream.unwrap(Natsail.useSync((service) => service.jetStreamStates(definition, options)))
}

/**
 * Runs a named explicit-ack JetStream processor. The message is acknowledged
 * only after the supplied Effect completes successfully.
 */
export function runJetStreamProcessor<T, E, R>(
  options: JetStreamProcessorOptions<T>,
  handler: (delivery: JetStreamProcessingDelivery<T>) => Effect.Effect<void, E, R>
): Effect.Effect<void, NatsailJetStreamError | E, Natsail | R> {
  return Natsail.use((service) => service.runJetStreamProcessor(options, handler))
}

/** Supplies an existing resource without taking ownership of it. */
export function makeNatsailLayer(resource: NatsailResource): Layer.Layer<Natsail> {
  return Layer.succeed(Natsail, makeNatsail(resource))
}

async function closeResource(resource: NatsailResource): Promise<void> {
  if (resource.close) {
    await resource.close()
    return
  }

  const failures: unknown[] = []
  try {
    await resource.sessions.close()
  } catch (error) {
    failures.push(error)
  }
  try {
    await resource.runtime.close()
  } catch (error) {
    failures.push(error)
  }

  if (failures.length === 1) throw failures[0]
  if (failures.length > 1) {
    throw new AggregateError(failures, 'NATSail registry and runtime shutdown both failed')
  }
}

/**
 * Acquires one Layer-owned resource and closes it when the Layer scope exits.
 * The default finalizer closes the registry first and always attempts to close
 * the runtime, even if registry shutdown fails.
 */
export function makeNatsailScopedLayer<E, R>(
  acquire: Effect.Effect<NatsailResource, E, R>
): Layer.Layer<Natsail, E, R> {
  return Layer.effect(
    Natsail,
    Effect.acquireRelease(acquire, (resource) =>
      Effect.promise(() => closeResource(resource))
    ).pipe(Effect.map(makeNatsail))
  )
}
