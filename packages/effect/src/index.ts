import { Cause, Context, Data, Effect, Layer, Queue, Stream } from 'effect'

import type {
  CoreRequestOptions,
  CoreSubscriptionOptions,
  NatsConnection,
  NatsRuntime,
  NatsRuntimeEvent,
  NatsRuntimeReconnectOptions,
  NatsRuntimeStatusEvent,
} from '@natsail/core'
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
    restartSession: (key) => tryRuntimePromise('restart-session', () => sessions.restart(key)),
    sessionSnapshots: (definition, options) =>
      createSessionSnapshotStream(sessions, definition, options),
    sessionValues: <T>(definition: SessionDefinition<T>, options?: NatsailSessionStreamOptions) =>
      createSessionSnapshotStream(sessions, definition, options).pipe(
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
      ),
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
