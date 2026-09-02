import { Effect, Fiber, Stream } from 'effect'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { NatsRuntime, NatsRuntimeEvent, SubscriptionLease } from '@natsail/core'
import { makeNatsail, NatsailJetStreamError, type NatsailJetStreamEvent } from '@natsail/effect'
import type {
  JetStreamCatchUp,
  JetStreamDelivery,
  JetStreamLease,
  JetStreamProcessingDelivery,
  JetStreamProcessorHandler,
  JetStreamProcessorOptions,
  JetStreamSessionSourceOptions,
} from '@natsail/jetstream'
import { createSessionRegistry } from '@natsail/session'

const jetStreamMocks = vi.hoisted(() => ({
  createJetStreamSessionSource: vi.fn(),
  processJetStream: vi.fn(),
}))

vi.mock('@natsail/jetstream', () => jetStreamMocks)

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (cause: unknown) => void
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, resolve, reject }
}

function emptyEvents<T>(): AsyncIterable<T> {
  return {
    async *[Symbol.asyncIterator]() {},
  }
}

function runtimeStub(): NatsRuntime {
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
  } as unknown as NatsRuntime
}

function delivery(
  value: number,
  sequence: number,
  replay: 'initial' | 'live'
): JetStreamDelivery<number> {
  return {
    value,
    subject: 'events.orders',
    cursor: { stream: 'ORDERS', sequence },
    duplicate: false,
    redelivered: false,
    consumerPending: 0,
    replay,
  }
}

function processingDelivery(value: number, sequence: number): JetStreamProcessingDelivery<number> {
  return {
    value,
    subject: 'jobs.orders',
    cursor: { stream: 'ORDERS', sequence },
    redelivered: false,
    deliveryAttempt: 1,
  }
}

function controlledJetStreamSource<T>() {
  let accept!: (value: JetStreamDelivery<T>) => Promise<void>
  const caughtUp = deferred<JetStreamCatchUp>()
  const closed = deferred<void>()
  const close = vi.fn(async () => closed.resolve())
  const lease: JetStreamLease<T> = {
    ready: Promise.resolve(),
    closed: closed.promise,
    caughtUp: caughtUp.promise,
    close,
    inspect: () => ({
      phase: 'replaying',
      initialPending: 0,
      initialDelivered: 0,
      remaining: 0,
      restarts: 0,
    }),
    subscribe: () => () => undefined,
  }
  const source = vi.fn((next: (value: JetStreamDelivery<T>) => Promise<void>) => {
    accept = next
    return lease
  })
  jetStreamMocks.createJetStreamSessionSource.mockReturnValue(source)

  return {
    source,
    close,
    deliver: (value: JetStreamDelivery<T>) => accept(value),
    catchUp: (value: JetStreamCatchUp) => caughtUp.resolve(value),
    fail: (cause: unknown) => closed.reject(cause),
  }
}

const sourceOptions: JetStreamSessionSourceOptions<number> = {
  stream: 'ORDERS',
  filter: 'events.orders',
  start: 'all',
  maxBufferedMessages: 4,
  decode: () => 0,
  recovery: { delayMs: 10 },
}

describe('Effect JetStream adapter', () => {
  beforeEach(() => {
    jetStreamMocks.createJetStreamSessionSource.mockReset()
    jetStreamMocks.processJetStream.mockReset()
  })

  it('delivers ordered replay followed by one explicit caught-up event', async () => {
    const controlled = controlledJetStreamSource<number>()
    const service = makeNatsail({
      runtime: runtimeStub(),
      sessions: createSessionRegistry(),
    })
    const fiber = Effect.runFork(
      service.jetStreamEvents(sourceOptions).pipe(Stream.take(3), Stream.runCollect)
    )

    await vi.waitFor(() => expect(controlled.source).toHaveBeenCalledOnce())
    await controlled.deliver(delivery(1, 1, 'initial'))
    await controlled.deliver(delivery(2, 2, 'initial'))
    controlled.catchUp({ cursor: { stream: 'ORDERS', sequence: 2 }, delivered: 2 })

    const events = await Effect.runPromise(Fiber.join(fiber))
    expect(events.map((event: NatsailJetStreamEvent<number>) => event.type)).toEqual([
      'delivery',
      'delivery',
      'caught-up',
    ])
    expect(controlled.close).toHaveBeenCalledOnce()
    expect(jetStreamMocks.createJetStreamSessionSource).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        stream: 'ORDERS',
        recovery: { delayMs: 10 },
        signal: expect.any(AbortSignal),
      })
    )
  })

  it('applies reliable backpressure before the JetStream accept Promise resolves', async () => {
    const controlled = controlledJetStreamSource<number>()
    const service = makeNatsail({
      runtime: runtimeStub(),
      sessions: createSessionRegistry(),
    })
    let releaseFirst!: () => void
    const firstMayFinish = new Promise<void>((resolve) => {
      releaseFirst = resolve
    })
    const received: number[] = []
    const fiber = Effect.runFork(
      service
        .jetStreamDeliveries(sourceOptions, { bufferSize: 1, overflowStrategy: 'suspend' })
        .pipe(
          Stream.take(3),
          Stream.runForEach((event) =>
            Effect.promise(async () => {
              received.push(event.value)
              if (event.value === 1) await firstMayFinish
            })
          )
        )
    )

    await vi.waitFor(() => expect(controlled.source).toHaveBeenCalledOnce())
    await controlled.deliver(delivery(1, 1, 'initial'))
    await vi.waitFor(() => expect(received).toEqual([1]))
    await controlled.deliver(delivery(2, 2, 'initial'))

    let thirdAccepted = false
    const third = controlled.deliver(delivery(3, 3, 'initial')).then(() => {
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

  it('materializes replay atomically and microbatches live state', async () => {
    const controlled = controlledJetStreamSource<number>()
    const service = makeNatsail({
      runtime: runtimeStub(),
      sessions: createSessionRegistry(),
    })
    const reduced: number[][] = []
    const fiber = Effect.runFork(
      service
        .materializeJetStream(
          sourceOptions,
          {
            initial: () => 0,
            reduceBatch: (state, deliveries) =>
              Effect.sync(() => {
                reduced.push(deliveries.map((event) => event.value))
                return state + deliveries.reduce((sum, event) => sum + event.value, 0)
              }),
          },
          { batchSize: 10, batchWithin: '20 millis' }
        )
        .pipe(Stream.take(3), Stream.runCollect)
    )

    await vi.waitFor(() => expect(controlled.source).toHaveBeenCalledOnce())
    await controlled.deliver(delivery(1, 1, 'initial'))
    await controlled.deliver(delivery(2, 2, 'initial'))
    controlled.catchUp({ cursor: { stream: 'ORDERS', sequence: 2 }, delivered: 2 })
    await controlled.deliver(delivery(4, 3, 'live'))

    expect(await Effect.runPromise(Fiber.join(fiber))).toEqual([
      { phase: 'replaying', data: 0, replay: { delivered: 0 } },
      {
        phase: 'live',
        data: 3,
        cursor: { stream: 'ORDERS', sequence: 2 },
        replay: { delivered: 2 },
      },
      {
        phase: 'live',
        data: 7,
        cursor: { stream: 'ORDERS', sequence: 3 },
        replay: { delivered: 2 },
      },
    ])
    expect(reduced).toEqual([[1, 2], [4]])
    expect(controlled.close).toHaveBeenCalledOnce()
  })

  it('does not complete a processor handler until its Effect succeeds', async () => {
    let processorHandler!: JetStreamProcessorHandler<number>
    const closed = deferred<void>()
    const close = vi.fn(async () => closed.resolve())
    const lease: SubscriptionLease = {
      ready: Promise.resolve(),
      closed: closed.promise,
      close,
    }
    jetStreamMocks.processJetStream.mockImplementation(
      (
        _runtime: NatsRuntime,
        _options: JetStreamProcessorOptions<number>,
        handler: JetStreamProcessorHandler<number>
      ) => {
        processorHandler = handler
        return lease
      }
    )
    let releaseHandler!: () => void
    const handlerMayFinish = new Promise<void>((resolve) => {
      releaseHandler = resolve
    })
    const service = makeNatsail({
      runtime: runtimeStub(),
      sessions: createSessionRegistry(),
    })
    const processor = Effect.runFork(
      service.runJetStreamProcessor(
        {
          stream: 'ORDERS',
          consumer: { mode: 'ensure', name: 'effect-orders' },
          filter: 'jobs.orders',
          start: 'all',
          decode: () => 0,
        },
        () => Effect.promise(() => handlerMayFinish)
      )
    )

    await vi.waitFor(() => expect(jetStreamMocks.processJetStream).toHaveBeenCalledOnce())
    let accepted = false
    const processing = processorHandler(processingDelivery(42, 1)).then(() => {
      accepted = true
    })
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(accepted).toBe(false)

    releaseHandler()
    await processing
    expect(accepted).toBe(true)
    closed.resolve()
    await Effect.runPromise(Fiber.join(processor))
    expect(close).toHaveBeenCalledOnce()
  })

  it('preserves a processor Effect typed failure', async () => {
    let processorHandler!: JetStreamProcessorHandler<number>
    const closed = deferred<void>()
    const close = vi.fn(async () => closed.resolve())
    const lease: SubscriptionLease = {
      ready: Promise.resolve(),
      closed: closed.promise,
      close,
    }
    jetStreamMocks.processJetStream.mockImplementation(
      (
        _runtime: NatsRuntime,
        _options: JetStreamProcessorOptions<number>,
        handler: JetStreamProcessorHandler<number>
      ) => {
        processorHandler = handler
        return lease
      }
    )
    const service = makeNatsail({
      runtime: runtimeStub(),
      sessions: createSessionRegistry(),
    })
    const applicationError = { _tag: 'RejectedOrder' as const, orderId: 42 }
    const processor = Effect.runFork(
      service.runJetStreamProcessor(
        {
          stream: 'ORDERS',
          consumer: { mode: 'ensure', name: 'effect-orders' },
          filter: 'jobs.orders',
          start: 'all',
          decode: () => 0,
        },
        () => Effect.fail(applicationError)
      )
    )

    await vi.waitFor(() => expect(jetStreamMocks.processJetStream).toHaveBeenCalledOnce())
    const rejected = processorHandler(processingDelivery(42, 1))
    await expect(rejected).rejects.toBeDefined()
    try {
      await rejected
    } catch (cause) {
      closed.reject(cause)
    }

    expect(await Effect.runPromise(Fiber.join(processor).pipe(Effect.flip))).toBe(applicationError)
    expect(close).toHaveBeenCalledOnce()
  })

  it('maps processor infrastructure failures without hiding the stage', async () => {
    const cause = new Error('consumer configuration rejected')
    jetStreamMocks.processJetStream.mockImplementation(() => {
      throw cause
    })
    const service = makeNatsail({
      runtime: runtimeStub(),
      sessions: createSessionRegistry(),
    })

    const error = await Effect.runPromise(
      service
        .runJetStreamProcessor(
          {
            stream: 'ORDERS',
            consumer: { mode: 'bind', name: 'orders' },
            filter: 'jobs.orders',
            start: 'all',
            decode: () => 0,
          },
          () => Effect.void
        )
        .pipe(Effect.flip)
    )

    expect(error).toBeInstanceOf(NatsailJetStreamError)
    expect(error).toMatchObject({ stream: 'ORDERS', stage: 'processor', cause })
  })
})
