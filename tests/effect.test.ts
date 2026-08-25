import { Chunk, Effect, Fiber, Stream } from 'effect'
import { describe, expect, it, vi } from 'vitest'

import type { NatsRuntime, NatsRuntimeEvent, SubscriptionLease } from '@natsail/core'
import {
  makeNatsail,
  makeNatsailScopedLayer,
  Natsail,
  NatsailOperationError,
  NatsailSessionError,
  NatsailStreamBufferOverflowError,
} from '@natsail/effect'
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
  deliver(value: T): Promise<void>
  fail(error: unknown): void
} {
  let accept!: (value: T) => Promise<void>
  let closeSession!: () => void
  let failSession!: (error: unknown) => void
  const closed = new Promise<void>((resolve, reject) => {
    closeSession = resolve
    failSession = reject
  })
  const lease: SubscriptionLease = {
    ready: Promise.resolve(),
    closed,
    close: async () => closeSession(),
  }
  const starts = vi.fn<SessionSource<T>>((next) => {
    accept = next
    return lease
  })

  return {
    source: starts,
    starts,
    deliver: (value) => accept(value),
    fail: failSession,
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
      request: vi.fn(
        (options) =>
          new Promise((_resolve, reject) => {
            requestSignal = options.signal
            options.signal?.addEventListener('abort', () => reject(new Error('request aborted')), {
              once: true,
            })
          })
      ),
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
        const first = yield* Effect.fork(
          service.sessionValues(definition).pipe(Stream.take(1), Stream.runCollect)
        )
        const second = yield* Effect.fork(
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

    expect(result.map(Chunk.toReadonlyArray)).toEqual([['hello'], ['hello']])
    expect(controlled.starts).toHaveBeenCalledOnce()
    expect(sessions.inspect().activeSessions).toBe(0)
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
        const fiber = yield* Effect.fork(service.sessionValues(definition).pipe(Stream.runDrain))
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
        const fiber = yield* Effect.fork(
          service.runtimeEvents.pipe(Stream.take(1), Stream.runCollect)
        )
        yield* Effect.promise(() => vi.waitFor(() => expect(events.activeIterators()).toBe(1)))
        events.push({ type: 'status', state: 'connected', at: 1 })
        return yield* Fiber.join(fiber)
      })
    )

    expect(Chunk.toReadonlyArray(event)).toEqual([{ type: 'status', state: 'connected', at: 1 }])
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
        const subscriber = { queue: [] as NatsRuntimeEvent[], closed: false }
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
        subscriber.resume = undefined
      }
    },
    activeIterators: () => subscribers.size,
  }
}
