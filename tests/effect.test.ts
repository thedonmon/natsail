import { it } from '@effect/vitest'
import { Cause, Effect, Exit, Fiber, Schema, Stream, Tracer } from 'effect'
import { TestClock } from 'effect/testing'
import { describe, expect, vi } from 'vitest'

import type {
  CoreRequestOptions,
  CoreSubscriptionOptions,
  MessageHandler,
  NatsRuntime,
  NatsailTelemetryEvent,
  RuntimeResource,
  SubscriptionLease,
} from '@natsail/core'
import { createNatsailTelemetryReporter, NATS_RUNTIME_ADAPTER } from '@natsail/core'
import {
  jetStreamStates as jetStreamStatesEffect,
  makeNatsail,
  Natsail,
  type NatsailService,
  NatsailOperationError,
  NatsailSessionError,
  NatsailStreamBufferOverflowError,
  NatsailSubjectError,
  subscribe as subscribeEffect,
} from '@natsail/effect'
import { natsSchemaCodec } from '@natsail/effect/schema'
import type { JetStreamStateSnapshot } from '@natsail/jetstream'
import {
  createSessionRegistry,
  defineSession,
  type SessionRegistry,
  type SessionSource,
} from '@natsail/session'

import { controllableEvents, controllableSource, emptyEvents, runtimeStub } from './fixtures/fakes'

const invalidOptionsDefinition = defineSession({
  key: 'conversation:effect-invalid-options',
  contract: 'conversation:v1',
  source: controllableSource<JetStreamStateSnapshot<number>>().source,
})

/** `drainOnClose` mirrors CoreSubscription.close(), which awaits in-flight handlers. */
function controllableSubscription<T>(
  telemetryEvents?: NatsailTelemetryEvent[],
  drainOnClose = false
): {
  readonly runtime: NatsRuntime
  readonly subscribe: ReturnType<typeof vi.fn>
  readonly close: ReturnType<typeof vi.fn>
  deliver(value: T): Promise<void>
  fail(error: unknown): void
} {
  let handler!: MessageHandler<T>
  let closeSubscription!: () => void
  let failSubscription!: (error: unknown) => void
  const closed = new Promise<void>((resolve, reject) => {
    closeSubscription = resolve
    failSubscription = reject
  })
  const inFlight = new Set<Promise<unknown>>()
  const close = vi.fn(async () => {
    if (drainOnClose) await Promise.allSettled(inFlight)
    closeSubscription()
  })
  const lease: SubscriptionLease = {
    ready: Promise.resolve(),
    closed,
    close,
  }
  const subscribe = vi.fn((_options: CoreSubscriptionOptions<T>, next: MessageHandler<T>) => {
    handler = next
    return lease
  })

  return {
    runtime: runtimeStub({
      subscribe: subscribe as NatsRuntime['subscribe'],
      ...(telemetryEvents === undefined
        ? {}
        : {
            [NATS_RUNTIME_ADAPTER]: {
              manage: <Resource extends RuntimeResource>(create: () => Resource) => create(),
              reportDiagnostic: () => undefined,
              telemetry: createNatsailTelemetryReporter({
                sink: { record: (event) => telemetryEvents.push(event) },
                clock: { now: () => 0 },
              }),
            },
          }),
    }),
    subscribe,
    close,
    deliver: (value) => {
      const delivered = Promise.resolve(
        handler(value, {} as never, { signal: new AbortController().signal })
      )
      inFlight.add(delivered)
      const settled = () => inFlight.delete(delivered)
      delivered.then(settled, settled)
      return delivered
    },
    fail: failSubscription,
  }
}

describe('Effect adapter', () => {
  it('maps runtime Promise failures into operation-tagged Effect failures', async () => {
    const cause = new Error('permission denied')
    const runtime = runtimeStub({
      publish: vi.fn(async () => Promise.reject(cause)),
    })
    const service = makeNatsail({ runtime, sessions: createSessionRegistry() })

    const error = await Effect.runPromise(Effect.flip(service.publish('events.denied')))

    expect(error).toBeInstanceOf(NatsailOperationError)
    expect(error).toMatchObject({
      _tag: 'NatsailOperationError',
      operation: 'publish',
      cause,
    })
  })

  it('aborts an in-flight NATS request when its Effect fiber is interrupted', async () => {
    let requestSignal: AbortSignal | undefined
    const runtime = runtimeStub({
      request: <T>(options: CoreRequestOptions<T>) =>
        new Promise<T>((_resolve, reject) => {
          requestSignal = options.signal
          options.signal?.addEventListener('abort', () => reject(new Error('request aborted')), {
            once: true,
          })
        }),
    })
    const service = makeNatsail({ runtime, sessions: createSessionRegistry() })
    const fiber = Effect.runFork(
      service.request({
        subject: 'request.interrupt',
        decode: () => 'response',
      })
    )

    await vi.waitFor(() => expect(requestSignal).toBeDefined())
    await Effect.runPromise(Fiber.interrupt(fiber))

    expect(requestSignal?.aborted).toBe(true)
  })

  it('creates a cold scoped Core subject Stream with wildcard and queue options', async () => {
    const controlled = controllableSubscription<string>()
    const resource = {
      runtime: controlled.runtime,
      sessions: createSessionRegistry(),
    }
    const stream = subscribeEffect(
      {
        subject: 'events.*',
        queue: 'effect-workers',
        decode: () => '',
      },
      { bufferSize: 4 }
    )

    expect(controlled.subscribe).not.toHaveBeenCalled()

    const fiber = Effect.runFork(
      stream.pipe(Stream.take(1), Stream.runCollect, Effect.provide(Natsail.layer(resource)))
    )
    await vi.waitFor(() => expect(controlled.subscribe).toHaveBeenCalledOnce())
    await controlled.deliver('hello')

    expect(await Effect.runPromise(Fiber.join(fiber))).toEqual(['hello'])
    expect(controlled.subscribe).toHaveBeenCalledWith(
      expect.objectContaining({ subject: 'events.*', queue: 'effect-workers' }),
      expect.any(Function)
    )
    expect(controlled.close).toHaveBeenCalledOnce()
  })

  it('suspends the Core subscription handler when the bounded buffer is full', async () => {
    const controlled = controllableSubscription<number>()
    const service = makeNatsail({
      runtime: controlled.runtime,
      sessions: createSessionRegistry(),
    })
    let releaseFirst!: () => void
    const firstMayFinish = new Promise<void>((resolve) => {
      releaseFirst = resolve
    })
    const received: number[] = []
    const fiber = Effect.runFork(
      service
        .subscribe(
          { subject: 'numbers', decode: () => 0 },
          { bufferSize: 1, overflowStrategy: 'suspend' }
        )
        .pipe(
          Stream.take(3),
          Stream.runForEach((value) =>
            Effect.promise(async () => {
              received.push(value)
              if (value === 1) await firstMayFinish
            })
          )
        )
    )

    await vi.waitFor(() => expect(controlled.subscribe).toHaveBeenCalledOnce())
    await controlled.deliver(1)
    await vi.waitFor(() => expect(received).toEqual([1]))
    await controlled.deliver(2)

    let thirdAccepted = false
    const third = controlled.deliver(3).then(() => {
      thirdAccepted = true
    })
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(thirdAccepted).toBe(false)

    releaseFirst()
    await third
    await Effect.runPromise(Fiber.join(fiber))

    expect(received).toEqual([1, 2, 3])
    expect(controlled.close).toHaveBeenCalledOnce()
  })

  it('closes the subscription when the consumer is interrupted while a delivery waits for buffer space', async () => {
    const controlled = controllableSubscription<number>(undefined, true)
    const service = makeNatsail({
      runtime: controlled.runtime,
      sessions: createSessionRegistry(),
    })
    let markStarted!: () => void
    const started = new Promise<void>((resolve) => {
      markStarted = resolve
    })
    const fiber = Effect.runFork(
      service.subscribe({ subject: 'numbers', decode: () => 0 }, { bufferSize: 1 }).pipe(
        Stream.runForEach(() =>
          Effect.promise(() => {
            markStarted()
            return new Promise<void>(() => undefined)
          })
        )
      )
    )

    await vi.waitFor(() => expect(controlled.subscribe).toHaveBeenCalledOnce())
    void controlled.deliver(1)
    await started
    void controlled.deliver(2)
    const parked = controlled.deliver(3)
    await new Promise((resolve) => setTimeout(resolve, 10))

    await Effect.runPromise(Fiber.interrupt(fiber))

    expect(controlled.close).toHaveBeenCalledOnce()
    await parked
  })

  it('composes subject delivery with Effect v4 chunk processing', async () => {
    const controlled = controllableSubscription<number>()
    const service = makeNatsail({
      runtime: controlled.runtime,
      sessions: createSessionRegistry(),
    })
    const batches: number[][] = []
    const fiber = Effect.runFork(
      service
        .subscribe(
          { subject: 'numbers.batch', decode: () => 0 },
          { bufferSize: 2, overflowStrategy: 'suspend' }
        )
        .pipe(
          Stream.take(5),
          Stream.rechunk(2),
          Stream.runForEachArray((batch) =>
            Effect.sync(() => {
              batches.push([...batch])
            })
          )
        )
    )

    await vi.waitFor(() => expect(controlled.subscribe).toHaveBeenCalledOnce())
    for (const value of [1, 2, 3, 4, 5]) {
      await controlled.deliver(value)
    }
    await Effect.runPromise(Fiber.join(fiber))

    expect(batches).toEqual([[1, 2], [3, 4], [5]])
    expect(controlled.close).toHaveBeenCalledOnce()
  })

  it('can fail a Core subject Stream instead of silently dropping a message', async () => {
    const telemetryEvents: NatsailTelemetryEvent[] = []
    const controlled = controllableSubscription<number>(telemetryEvents)
    const service = makeNatsail({
      runtime: controlled.runtime,
      sessions: createSessionRegistry(),
    })
    let releaseFirst!: () => void
    let markFirstStarted!: () => void
    const firstMayFinish = new Promise<void>((resolve) => {
      releaseFirst = resolve
    })
    const firstStarted = new Promise<void>((resolve) => {
      markFirstStarted = resolve
    })
    const fiber = Effect.runFork(
      service
        .subscribe(
          { subject: 'numbers', decode: () => 0 },
          { bufferSize: 1, overflowStrategy: 'error' }
        )
        .pipe(
          Stream.runForEach((value) =>
            value === 1
              ? Effect.promise(() => {
                  markFirstStarted()
                  return firstMayFinish
                })
              : Effect.void
          )
        )
    )

    await vi.waitFor(() => expect(controlled.subscribe).toHaveBeenCalledOnce())
    await controlled.deliver(1)
    await firstStarted
    await controlled.deliver(2)
    await expect(controlled.deliver(3)).rejects.toBeInstanceOf(NatsailStreamBufferOverflowError)
    releaseFirst()

    const error = await Effect.runPromise(Fiber.join(fiber).pipe(Effect.flip))
    expect(error).toMatchObject({
      _tag: 'NatsailStreamBufferOverflowError',
      stream: 'subject:numbers',
      capacity: 1,
    })
    expect(controlled.close).toHaveBeenCalledOnce()
    expect(telemetryEvents).toContainEqual(
      expect.objectContaining({
        type: 'counter',
        name: 'natsail.buffer.signals',
        attributes: { signal: 'overflow', source: 'effect' },
      })
    )
  })

  it('maps Core subscription and decoding failures to the subject and source stage', async () => {
    const controlled = controllableSubscription<string>()
    const service = makeNatsail({
      runtime: controlled.runtime,
      sessions: createSessionRegistry(),
    })
    const cause = new SyntaxError('invalid message payload')
    const fiber = Effect.runFork(
      service.subscribe({ subject: 'events.decode', decode: () => '' }).pipe(Stream.runDrain)
    )

    await vi.waitFor(() => expect(controlled.subscribe).toHaveBeenCalledOnce())
    controlled.fail(cause)

    const error = await Effect.runPromise(Fiber.join(fiber).pipe(Effect.flip))
    expect(error).toBeInstanceOf(NatsailSubjectError)
    expect(error).toMatchObject({
      _tag: 'NatsailSubjectError',
      subject: 'events.decode',
      stage: 'source',
      cause,
    })
    expect(controlled.close).toHaveBeenCalledOnce()
  })

  it.effect('shares reduced JetStream state and coalesces cumulative live updates', () =>
    Effect.gen(function* () {
      const controlled = controllableSource<JetStreamStateSnapshot<number>>()
      const sessions = createSessionRegistry()
      const definition = defineSession({
        key: 'conversation:effect-jetstream-state',
        contract: 'conversation:v1',
        source: controlled.source,
      })
      const resource = { runtime: runtimeStub(), sessions }
      const service = makeNatsail(resource)
      const first = yield* Effect.forkChild(
        service
          .jetStreamStates(definition, { bufferSize: 256, liveBatchWithin: '20 millis' })
          .pipe(Stream.take(3), Stream.runCollect)
      )
      const second = yield* Effect.forkChild(
        jetStreamStatesEffect(definition, {
          bufferSize: 256,
          liveBatchWithin: '20 millis',
        }).pipe(Stream.take(2), Stream.runCollect, Effect.provide(Natsail.layer(resource)))
      )

      yield* Effect.promise(() =>
        vi.waitFor(() => expect(sessions.inspect().sessions[0]?.references).toBe(2))
      )
      yield* Effect.promise(async () => {
        await controlled.deliver({
          phase: 'replaying',
          data: 0,
          restarts: 0,
          replay: { delivered: 0, remaining: 3 },
        })
        await controlled.deliver({
          phase: 'live',
          data: 3,
          restarts: 0,
          replay: { delivered: 3, remaining: 0 },
        })
        for (let data = 4; data <= 220; data += 1) {
          await controlled.deliver({
            phase: 'live',
            data,
            restarts: 0,
            replay: { delivered: 3, remaining: 0 },
          })
        }
      })
      // The coalescing window only closes when the test clock says so.
      while (first.pollUnsafe() === undefined) {
        yield* Effect.promise(() => new Promise((resolve) => setImmediate(resolve)))
        yield* TestClock.adjust('20 millis')
      }

      const firstStates = yield* Fiber.join(first)
      const secondStates = yield* Fiber.join(second)

      expect(firstStates.map(({ phase, data }) => ({ phase, data }))).toEqual([
        { phase: 'replaying', data: 0 },
        { phase: 'live', data: 3 },
        { phase: 'live', data: 220 },
      ])
      expect(secondStates.map(({ phase, data }) => ({ phase, data }))).toEqual([
        { phase: 'replaying', data: 0 },
        { phase: 'live', data: 3 },
      ])
      expect(controlled.starts).toHaveBeenCalledOnce()
      expect(controlled.close).toHaveBeenCalledOnce()
      expect(sessions.inspect().activeSessions).toBe(0)
    })
  )

  it('flushes pending live state before an immediate reconnect boundary', async () => {
    const controlled = controllableSource<JetStreamStateSnapshot<number>>()
    const sessions = createSessionRegistry()
    const definition = defineSession({
      key: 'conversation:effect-jetstream-reconnect',
      contract: 'conversation:v1',
      source: controlled.source,
    })
    const service = makeNatsail({ runtime: runtimeStub(), sessions })
    const fiber = Effect.runFork(
      service
        .jetStreamStates(definition, { liveBatchWithin: '1 minute' })
        .pipe(Stream.take(4), Stream.runCollect)
    )

    await vi.waitFor(() => expect(sessions.inspect().activeSessions).toBe(1))
    await controlled.deliver({
      phase: 'live',
      data: 1,
      restarts: 0,
      replay: { delivered: 1, remaining: 0 },
    })
    await controlled.deliver({
      phase: 'live',
      data: 2,
      restarts: 0,
      replay: { delivered: 1, remaining: 0 },
    })
    await controlled.deliver({
      phase: 'reconnecting',
      data: 2,
      restarts: 1,
      replay: { delivered: 1 },
    })
    await controlled.deliver({
      phase: 'live',
      data: 3,
      restarts: 1,
      replay: { delivered: 1, remaining: 0 },
    })

    expect(
      (await Effect.runPromise(Fiber.join(fiber))).map(({ phase, data }) => ({ phase, data }))
    ).toEqual([
      { phase: 'live', data: 1 },
      { phase: 'live', data: 2 },
      { phase: 'reconnecting', data: 2 },
      { phase: 'live', data: 3 },
    ])
    expect(controlled.close).toHaveBeenCalledOnce()
  })

  it('flushes the latest cumulative live state when the shared session completes', async () => {
    const controlled = controllableSource<JetStreamStateSnapshot<number>>()
    const sessions = createSessionRegistry()
    const definition = defineSession({
      key: 'conversation:effect-jetstream-complete',
      contract: 'conversation:v1',
      source: controlled.source,
    })
    const service = makeNatsail({ runtime: runtimeStub(), sessions })
    const fiber = Effect.runFork(
      service.jetStreamStates(definition, { liveBatchWithin: '1 minute' }).pipe(Stream.runCollect)
    )

    await vi.waitFor(() => expect(sessions.inspect().activeSessions).toBe(1))
    await controlled.deliver({
      phase: 'live',
      data: 1,
      restarts: 0,
      replay: { delivered: 1, remaining: 0 },
    })
    await controlled.deliver({
      phase: 'live',
      data: 2,
      restarts: 0,
      replay: { delivered: 1, remaining: 0 },
    })
    controlled.finish()

    expect(
      (await Effect.runPromise(Fiber.join(fiber))).map(({ phase, data }) => ({ phase, data }))
    ).toEqual([
      { phase: 'live', data: 1 },
      { phase: 'live', data: 2 },
    ])
    expect(controlled.close).toHaveBeenCalledOnce()
  })

  it.each([
    [
      'subject bufferSize',
      (service: NatsailService) =>
        service.subscribe({ subject: 'events.invalid', decode: () => 0 }, { bufferSize: 0 }),
      'bufferSize',
    ],
    [
      'session bufferSize',
      (service: NatsailService) =>
        service.sessionSnapshots(invalidOptionsDefinition, { bufferSize: -1 }),
      'bufferSize',
    ],
    [
      'negative liveBatchWithin',
      (service: NatsailService) =>
        service.jetStreamStates(invalidOptionsDefinition, { liveBatchWithin: -1 }),
      'liveBatchWithin',
    ],
    [
      'NaN liveBatchWithin',
      (service: NatsailService) =>
        service.jetStreamStates(invalidOptionsDefinition, { liveBatchWithin: Number.NaN }),
      'liveBatchWithin',
    ],
  ] as const)('defers invalid %s to a defect when the Stream runs', async (_name, create, message) => {
    const service = makeNatsail({ runtime: runtimeStub(), sessions: createSessionRegistry() })
    let stream!: Stream.Stream<unknown, unknown>

    expect(() => {
      stream = create(service)
    }).not.toThrow()
    const exit = await Effect.runPromiseExit(Stream.runDrain(stream))

    if (!Exit.isFailure(exit)) throw new Error('Expected the invalid Stream to fail')
    expect(Cause.hasDies(exit.cause)).toBe(true)
    expect(Cause.pretty(exit.cause)).toContain(message)
  })

  it('fails a session Stream with the typed acquire error when the registry is closed', async () => {
    const sessions = createSessionRegistry()
    await sessions.close()
    const service = makeNatsail({ runtime: runtimeStub(), sessions })

    const error = await Effect.runPromise(
      service.sessionValues(invalidOptionsDefinition).pipe(Stream.runCollect, Effect.flip)
    )

    expect(error).toBeInstanceOf(NatsailSessionError)
    expect(error).toMatchObject({ key: invalidOptionsDefinition.key, stage: 'acquire' })
  })

  it('dies instead of hanging when stream setup throws a defect', async () => {
    const service = makeNatsail({ runtime: runtimeStub(), sessions: createSessionRegistry() })
    const stream = service.subscribe({
      subject: 'events.defect',
      decode: () => 0,
      signal: 'not a signal' as never,
    })

    const exit = await Effect.runPromiseExit(Stream.runDrain(stream))

    if (!Exit.isFailure(exit)) throw new Error('Expected the Stream to fail')
    expect(Cause.hasDies(exit.cause)).toBe(true)
  })

  it('fails a value Stream with a source-tagged session error', async () => {
    const controlled = controllableSource<string>()
    const sessions = createSessionRegistry()
    const definition = defineSession({
      key: 'conversation:effect-failure',
      contract: 'conversation:v1',
      source: controlled.source,
    })
    const service = makeNatsail({ runtime: runtimeStub(), sessions })
    const cause = new Error('consumer stopped')

    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(
          service.sessionValues(definition).pipe(Stream.runDrain)
        )
        yield* Effect.promise(() =>
          vi.waitFor(() => expect(sessions.inspect().activeSessions).toBe(1))
        )
        controlled.fail(cause)
        return yield* Fiber.join(fiber).pipe(Effect.flip)
      })
    )

    expect(error).toBeInstanceOf(NatsailSessionError)
    expect(error).toMatchObject({
      _tag: 'NatsailSessionError',
      key: definition.key,
      stage: 'source',
      cause,
    })
    expect(sessions.inspect().activeSessions).toBe(0)
  })

  it('fails instead of silently dropping snapshots when the bounded buffer fills', async () => {
    let closeSession!: () => void
    const closed = new Promise<void>((resolve) => {
      closeSession = resolve
    })
    const source: SessionSource<number> = (accept) => {
      queueMicrotask(() => {
        void accept(1)
        void accept(2)
        void accept(3)
      })
      return {
        ready: Promise.resolve(),
        closed,
        close: async () => closeSession(),
      }
    }
    const sessions = createSessionRegistry()
    const definition = defineSession({
      key: 'conversation:effect-overflow',
      contract: 'conversation:v1',
      source,
    })
    const service = makeNatsail({ runtime: runtimeStub(), sessions })

    const error = await Effect.runPromise(
      service.sessionSnapshots(definition, { bufferSize: 1 }).pipe(
        Stream.runForEach(() => Effect.sleep('25 millis')),
        Effect.flip
      )
    )

    expect(error).toBeInstanceOf(NatsailStreamBufferOverflowError)
    expect(error).toMatchObject({
      _tag: 'NatsailStreamBufferOverflowError',
      stream: `session:${definition.key}`,
      capacity: 1,
    })
    expect(sessions.inspect().activeSessions).toBe(0)
  })

  it('cancels runtime event iterators when a Stream finishes early', async () => {
    const events = controllableEvents()
    const service = makeNatsail({
      runtime: runtimeStub({ events: events.iterable }),
      sessions: createSessionRegistry(),
    })

    const event = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(
          service.runtimeEvents.pipe(Stream.take(1), Stream.runCollect)
        )
        yield* Effect.promise(() => vi.waitFor(() => expect(events.activeIterators()).toBe(1)))
        events.push({ type: 'status', state: 'connected', at: 1 })
        return yield* Fiber.join(fiber)
      })
    )

    expect(event).toEqual([{ type: 'status', state: 'connected', at: 1 }])
    await vi.waitFor(() => expect(events.activeIterators()).toBe(0))
  })

  it('scopes shutdown over registry and runtime and still attempts both closes', async () => {
    const registryFailure = new Error('registry close failed')
    const sessions = {
      events: emptyEvents(),
      close: vi.fn(async () => {
        throw registryFailure
      }),
    } as unknown as SessionRegistry
    const runtime = runtimeStub()
    const layer = Natsail.layerScoped(Effect.succeed({ runtime, sessions }))

    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const service = yield* Natsail
        expect(service.runtime).toBe(runtime)
      }).pipe(Effect.provide(layer))
    )

    expect(exit._tag).toBe('Failure')
    expect(sessions.close).toHaveBeenCalledOnce()
    expect(runtime.close).toHaveBeenCalledOnce()
  })

  it('traces publish and request with OpenTelemetry messaging attributes', async () => {
    const spans: Tracer.NativeSpan[] = []
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options)
        spans.push(span)
        return span
      },
    })
    const service = makeNatsail({
      runtime: runtimeStub({ request: vi.fn(async () => 'pong' as never) }),
      sessions: createSessionRegistry(),
    })

    await Effect.runPromise(
      Effect.all([
        service.publish('events.audit'),
        service.request({ subject: 'rpc.ping', decode: () => 'pong' }),
      ]).pipe(Effect.provideService(Tracer.Tracer, tracer))
    )

    expect(
      spans.map((span) => ({
        name: span.name,
        kind: span.kind,
        attributes: Object.fromEntries(span.attributes),
      }))
    ).toEqual([
      {
        name: 'Natsail.publish',
        kind: 'producer',
        attributes: {
          'messaging.system': 'nats',
          'messaging.destination.name': 'events.audit',
          'messaging.operation.type': 'send',
          'messaging.operation.name': 'publish',
        },
      },
      {
        name: 'Natsail.request',
        kind: 'client',
        attributes: {
          'messaging.system': 'nats',
          'messaging.destination.name': 'rpc.ping',
          'messaging.operation.type': 'send',
          'messaging.operation.name': 'send',
        },
      },
    ])
  })
})

describe('natsSchemaCodec', () => {
  const codec = natsSchemaCodec(Schema.Struct({ id: Schema.BigInt, at: Schema.Date }))

  it('round-trips transformed values over a JSON wire format', () => {
    const value = { id: 7n, at: new Date('2026-10-02T10:00:00.000Z') }

    const bytes = codec.encode(value)

    expect(new TextDecoder().decode(bytes)).toBe('{"id":"7","at":"2026-10-02T10:00:00.000Z"}')
    expect(codec.decode(bytes)).toEqual(value)
  })

  it('throws on a payload that violates the schema', () => {
    expect(() => codec.decode(new TextEncoder().encode('{"id":"seven","at":"x"}'))).toThrow()
  })
})

