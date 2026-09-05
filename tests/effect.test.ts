import { Effect, Fiber, Stream } from 'effect'
import { describe, expect, it, vi } from 'vitest'

import type {
  CoreRequestOptions,
  CoreSubscriptionOptions,
  MessageHandler,
  NatsRuntime,
  NatsRuntimeEvent,
  NatsailTelemetryEvent,
  RuntimeResource,
  SubscriptionLease,
} from '@natsail/core'
import { createNatsailTelemetryReporter, NATS_RUNTIME_ADAPTER } from '@natsail/core'
import {
  jetStreamStates as jetStreamStatesEffect,
  makeNatsail,
  makeNatsailLayer,
  makeNatsailScopedLayer,
  Natsail,
  NatsailOperationError,
  NatsailSessionError,
  NatsailStreamBufferOverflowError,
  NatsailSubjectError,
  subscribe as subscribeEffect,
} from '@natsail/effect'
import type { JetStreamStateSnapshot } from '@natsail/jetstream'
import {
  createSessionRegistry,
  defineSession,
  type SessionRegistry,
  type SessionSource,
} from '@natsail/session'

function emptyEvents<T>(): AsyncIterable<T> {
  return {
    async *[Symbol.asyncIterator]() {},
  }
}

function runtimeStub(overrides: Partial<NatsRuntime> = {}): NatsRuntime {
  return {
    [NATS_RUNTIME_ADAPTER]: {
      manage: <T extends RuntimeResource>(create: () => T) => create(),
      reportDiagnostic: () => undefined,
      telemetry: createNatsailTelemetryReporter(),
    },
    events: emptyEvents<NatsRuntimeEvent>(),
    connection: vi.fn(async () => ({}) as never),
    reconnect: vi.fn(async () => ({}) as never),
    publish: vi.fn(async () => undefined),
    request: vi.fn(async () => undefined as never),
    subscribe: vi.fn(() => {
      throw new Error('Not implemented by this test runtime')
    }),
    inspect: vi.fn(() => ({}) as never),
    close: vi.fn(async () => undefined),
    ...overrides,
  } as unknown as NatsRuntime
}

function controllableSource<T>(): {
  readonly source: SessionSource<T>
  readonly starts: ReturnType<typeof vi.fn<SessionSource<T>>>
  readonly closeLease: ReturnType<typeof vi.fn>
  deliver(value: T): Promise<void>
  finish(): void
  fail(error: unknown): void
} {
  let accept!: (value: T) => Promise<void>
  let closeSession!: () => void
  let failSession!: (error: unknown) => void
  const closed = new Promise<void>((resolve, reject) => {
    closeSession = resolve
    failSession = reject
  })
  const closeLease = vi.fn(async () => closeSession())
  const lease: SubscriptionLease = {
    ready: Promise.resolve(),
    closed,
    close: closeLease,
  }
  const starts = vi.fn<SessionSource<T>>((next) => {
    accept = next
    return lease
  })

  return {
    source: starts,
    starts,
    closeLease,
    deliver: (value) => accept(value),
    finish: closeSession,
    fail: failSession,
  }
}

function controllableSubscription<T>(telemetryEvents?: NatsailTelemetryEvent[]): {
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
  const close = vi.fn(async () => closeSubscription())
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
    deliver: async (value) => handler(value, {} as never, { signal: new AbortController().signal }),
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
      stream.pipe(Stream.take(1), Stream.runCollect, Effect.provide(makeNatsailLayer(resource)))
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

  it('shares one registry source across concurrent Effect stream consumers', async () => {
    const controlled = controllableSource<string>()
    const sessions = createSessionRegistry()
    const definition = defineSession({
      key: 'conversation:effect-shared',
      contract: 'conversation:v1',
      source: controlled.source,
    })
    const service = makeNatsail({ runtime: runtimeStub(), sessions })

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const first = yield* Effect.forkChild(
          service.sessionValues(definition).pipe(Stream.take(1), Stream.runCollect)
        )
        const second = yield* Effect.forkChild(
          service.sessionValues(definition).pipe(Stream.take(1), Stream.runCollect)
        )

        yield* Effect.promise(() =>
          vi.waitFor(() => expect(sessions.inspect().sessions[0]?.references).toBe(2))
        )
        yield* Effect.promise(() => controlled.deliver('hello'))

        return yield* Effect.all([Fiber.join(first), Fiber.join(second)], {
          concurrency: 'unbounded',
        })
      })
    )

    expect(result).toEqual([['hello'], ['hello']])
    expect(controlled.starts).toHaveBeenCalledOnce()
    expect(sessions.inspect().activeSessions).toBe(0)
  })

  it('shares reduced JetStream state and coalesces cumulative live updates', async () => {
    const controlled = controllableSource<JetStreamStateSnapshot<number>>()
    const sessions = createSessionRegistry()
    const definition = defineSession({
      key: 'conversation:effect-jetstream-state',
      contract: 'conversation:v1',
      source: controlled.source,
    })
    const resource = { runtime: runtimeStub(), sessions }
    const service = makeNatsail(resource)
    const first = Effect.runFork(
      service
        .jetStreamStates(definition, { bufferSize: 256, liveBatchWithin: '20 millis' })
        .pipe(Stream.take(3), Stream.runCollect)
    )
    const second = Effect.runFork(
      jetStreamStatesEffect(definition, {
        bufferSize: 256,
        liveBatchWithin: '20 millis',
      }).pipe(Stream.take(2), Stream.runCollect, Effect.provide(makeNatsailLayer(resource)))
    )

    await vi.waitFor(() => expect(sessions.inspect().sessions[0]?.references).toBe(2))
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

    const [firstStates, secondStates] = await Promise.all([
      Effect.runPromise(Fiber.join(first)),
      Effect.runPromise(Fiber.join(second)),
    ])

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
    expect(controlled.closeLease).toHaveBeenCalledOnce()
    expect(sessions.inspect().activeSessions).toBe(0)
  })

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
    expect(controlled.closeLease).toHaveBeenCalledOnce()
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
    expect(controlled.closeLease).toHaveBeenCalledOnce()
  })

  it('rejects invalid shared JetStream live batch windows', () => {
    const controlled = controllableSource<JetStreamStateSnapshot<number>>()
    const service = makeNatsail({ runtime: runtimeStub(), sessions: createSessionRegistry() })
    const definition = defineSession({
      key: 'conversation:effect-jetstream-invalid-window',
      contract: 'conversation:v1',
      source: controlled.source,
    })

    expect(() => service.jetStreamStates(definition, { liveBatchWithin: -1 })).toThrow(
      'liveBatchWithin'
    )
    expect(() => service.jetStreamStates(definition, { liveBatchWithin: Number.NaN })).toThrow(
      'liveBatchWithin'
    )
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

  it('scopes shutdown in registry-then-runtime order and still attempts both closes', async () => {
    const order: string[] = []
    const registryFailure = new Error('registry close failed')
    const sessions = {
      events: emptyEvents(),
      close: vi.fn(async () => {
        order.push('sessions')
        throw registryFailure
      }),
    } as unknown as SessionRegistry
    const runtime = runtimeStub({
      close: vi.fn(async () => {
        order.push('runtime')
      }),
    })
    const layer = makeNatsailScopedLayer(Effect.succeed({ runtime, sessions }))

    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const service = yield* Natsail
        expect(service.runtime).toBe(runtime)
      }).pipe(Effect.provide(layer))
    )

    expect(exit._tag).toBe('Failure')
    expect(order).toEqual(['sessions', 'runtime'])
  })
})

function controllableEvents(): {
  iterable: AsyncIterable<NatsRuntimeEvent>
  push(event: NatsRuntimeEvent): void
  activeIterators(): number
} {
  const subscribers = new Set<{
    queue: NatsRuntimeEvent[]
    resume?: () => void
    closed: boolean
  }>()

  return {
    iterable: {
      [Symbol.asyncIterator]() {
        const subscriber: {
          queue: NatsRuntimeEvent[]
          resume?: () => void
          closed: boolean
        } = { queue: [], closed: false }
        subscribers.add(subscriber)

        return {
          async next(): Promise<IteratorResult<NatsRuntimeEvent>> {
            while (subscriber.queue.length === 0 && !subscriber.closed) {
              await new Promise<void>((resolve) => {
                subscriber.resume = resolve
              })
            }
            if (subscriber.closed) return { done: true, value: undefined }
            return { done: false, value: subscriber.queue.shift()! }
          },
          async return(): Promise<IteratorResult<NatsRuntimeEvent>> {
            subscriber.closed = true
            subscribers.delete(subscriber)
            subscriber.resume?.()
            return { done: true, value: undefined }
          },
        }
      },
    },
    push(event) {
      for (const subscriber of subscribers) {
        subscriber.queue.push(event)
        subscriber.resume?.()
        delete subscriber.resume
      }
    },
    activeIterators: () => subscribers.size,
  }
}
