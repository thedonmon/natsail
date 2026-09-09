import { afterEach, expect, it, vi } from 'vitest'
import type { NatsConnection } from '@nats-io/nats-core'
import { errors } from '@nats-io/nats-core'
import {
  createNatsRuntime,
  natsCodecs,
  type NatsRuntimeEvent,
  type NatsailTelemetryEvent,
} from '@natsail/core'
import { deferred, transport } from './fixtures/lifecycle'

afterEach(() => vi.useRealTimers())

it.each([
  [new errors.AuthorizationError('CONNECT synthetic-secret'), 'authentication'],
  [new errors.UserAuthenticationExpiredError('synthetic-secret'), 'authentication'],
  [new errors.TimeoutError(), 'timeout'],
  [new errors.ConnectionError('synthetic-secret'), 'connection'],
  [new errors.ProtocolError('synthetic-secret'), 'connection'],
  [Object.assign(new Error('synthetic-secret'), { name: 'synthetic-secret' }), 'unknown'],
  [
    Object.defineProperty(new Error(), 'name', {
      get: () => {
        throw new Error('synthetic-secret')
      },
    }),
    'unknown',
  ],
] as const)('reports a sanitized failure category (%#)', async (error, category) => {
  const events: NatsRuntimeEvent[] = []
  const measurements: NatsailTelemetryEvent[] = []
  const runtime = createNatsRuntime({
    connect: async () => {
      throw error
    },
    telemetry: { record: (event) => measurements.push(event) },
  })
  const watching = (async () => {
    for await (const event of runtime.events) events.push(event)
  })()
  const result = await runtime.connection().catch((failure: unknown) => failure)
  await runtime.close()
  await watching
  expect(result).toBe(error)
  const failed = events.find(
    (event) => event.type === 'diagnostic' && event.code === 'connection-attempt-failed'
  )
  expect(failed).toMatchObject({ details: { failureCategory: category } })
  expect(events).toContainEqual(
    expect.objectContaining({
      code: 'connection-failed',
      details: expect.objectContaining({ failureCategory: category }),
    })
  )
  expect(measurements).toContainEqual(
    expect.objectContaining({
      name: 'natsail.connection.transitions',
      attributes: expect.objectContaining({
        state: 'connection-attempt-failed',
        failureCategory: category,
      }),
    })
  )
  expect(JSON.stringify([events, measurements])).not.toContain('synthetic-secret')
})

it.each(['clock', 'attempt-counter', 'attempt-started'] as const)(
  'does not invoke a factory if %s telemetry disposes the runtime',
  async (point) => {
    vi.useFakeTimers()
    const factory = deferred<NatsConnection>()
    const network = transport()
    const connect = vi.fn(() => factory.promise)
    let armed = false
    let closing: Promise<unknown> | undefined
    const stop = () => {
      if (armed) closing ??= runtime.close().catch((error: unknown) => error)
    }
    const runtime = createNatsRuntime({
      connect,
      shutdownTimeoutMs: 10,
      telemetryClock: {
        now: () => {
          if (point === 'clock') stop()
          return 0
        },
      },
      telemetry: {
        record: (event) => {
          if (point === 'attempt-counter' && event.name === 'natsail.connection.attempts') stop()
          if (
            point === 'attempt-started' &&
            event.attributes?.state === 'connection-attempt-started'
          )
            stop()
        },
      },
    })
    armed = true
    const connecting = runtime.connection().catch((error: unknown) => error)
    await vi.advanceTimersByTimeAsync(10)
    try {
      expect(await closing).toBeUndefined()
      expect(connect).not.toHaveBeenCalled()
    } finally {
      factory.resolve(network.connection)
      await connecting
    }
  }
)

it.each(['connection-attempt-succeeded', 'attempt-duration'] as const)(
  'drains an adopted connection without publishing connected after %s disposes it',
  async (point) => {
    const network = transport()
    const events: NatsRuntimeEvent[] = []
    let closing: Promise<void> | undefined
    const runtime = createNatsRuntime({
      connect: async () => network.connection,
      telemetry: {
        record: (event) => {
          if (
            event.attributes?.state === point ||
            (point === 'attempt-duration' &&
              event.name === 'natsail.connection.attempt.duration' &&
              event.attributes?.outcome === 'success')
          ) {
            closing ??= runtime.close()
          }
        },
      },
    })
    const watching = (async () => {
      for await (const event of runtime.events) events.push(event)
    })()
    await runtime.connection().catch(() => undefined)
    await closing
    await watching
    const disposal = events.findIndex(
      (event) => event.type === 'diagnostic' && event.code === 'disposal-requested'
    )
    expect(disposal).toBeGreaterThan(-1)
    expect(
      events.slice(disposal).some((event) => event.type === 'status' && event.state === 'connected')
    ).toBe(false)
    expect(network.drain).toHaveBeenCalledOnce()
    expect(network.close).not.toHaveBeenCalled()
    expect(network.connection.isClosed()).toBe(true)
  }
)

it('shares one pending factory even when startup synchronously asks for the connection again', async () => {
  const factory = deferred<NatsConnection>()
  const network = transport()
  let entered = false
  let nested: Promise<NatsConnection> | undefined
  const connect = vi.fn(() => {
    if (!entered) {
      entered = true
      nested = runtime.connection()
    }
    return factory.promise
  })
  const runtime = createNatsRuntime({ connect })
  const initial = runtime.connection()
  factory.resolve(network.connection)
  await initial
  await nested
  await runtime.close()
  expect(connect).toHaveBeenCalledOnce()
  expect(nested).toBe(initial)
  expect(runtime.inspect().connectionGeneration).toBe(1)
})

it('does not leak an unhandled ready rejection when disposal cancels an unopened lease', async () => {
  vi.useFakeTimers()
  const factory = deferred<NatsConnection>()
  const runtime = createNatsRuntime({ connect: () => factory.promise, shutdownTimeoutMs: 10 })
  runtime.subscribe({ subject: 'work', codec: natsCodecs.text }, async () => undefined)
  const closing = runtime.close().catch((error: unknown) => error)
  await vi.advanceTimersByTimeAsync(10)
  expect(await closing).toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError' })
  expect(runtime.inspect().activeResources).toBe(0)
  factory.reject(new Error('factory stopped'))
  await vi.advanceTimersByTimeAsync(0)
})

it('preserves legacy factories with optional arguments and keeps adopted factory signals untouched', async () => {
  const first = transport()
  const legacy = vi.fn(async (_server?: string) => first.connection)
  const oldRuntime = createNatsRuntime({ connect: legacy })
  await oldRuntime.connection()
  expect(legacy).toHaveBeenCalledWith()
  await oldRuntime.close()
  const second = transport()
  let signal: AbortSignal | undefined
  const runtime = createNatsRuntime({
    connect: {
      create: async (context) => {
        signal = context.signal
        return second.connection
      },
    },
  })
  await runtime.connection()
  await runtime.close()
  expect(signal?.aborted).toBe(false)
  expect(second.drain).toHaveBeenCalledOnce()
  expect(second.close).not.toHaveBeenCalled()
})

it.each(['resolve', 'reject', 'cleanup-reject'] as const)(
  'observes a late factory %s after the shutdown deadline',
  async (mode) => {
    vi.useFakeTimers()
    const factory = deferred<NatsConnection>()
    const runtime = createNatsRuntime({ connect: () => factory.promise, shutdownTimeoutMs: 10 })
    const events: NatsRuntimeEvent[] = []
    const watching = (async () => {
      for await (const event of runtime.events) events.push(event)
    })()
    const connecting = runtime.connection().catch((error: unknown) => error)
    const closing = runtime.close().catch((error: unknown) => error)
    expect(runtime.close()).toBe(runtime.close())
    await vi.advanceTimersByTimeAsync(10)
    expect(await closing).toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError' })
    const network = transport()
    if (mode === 'cleanup-reject') network.close.mockRejectedValueOnce(new Error('cleanup failed'))
    if (mode === 'reject') factory.reject(new Error('factory failed'))
    else factory.resolve(network.connection)
    expect(await connecting).toBeInstanceOf(Error)
    await watching
    expect(network.close).toHaveBeenCalledTimes(mode === 'reject' ? 0 : 1)
    expect(events.some((event) => event.type === 'status' && event.state === 'connected')).toBe(
      false
    )
    expect(runtime.inspect()).toMatchObject({
      connectionGeneration: 0,
      activeResources: 0,
      connection: { state: 'closed' },
    })
    // Vitest's unhandled-rejection detection remains enabled for all discarded promises.
    await vi.advanceTimersByTimeAsync(0)
  }
)

it.each(['resolve', 'reject'] as const)(
  'finishes shutdown when a pending factory cooperates before the deadline (%s)',
  async (mode) => {
    vi.useFakeTimers()
    const factory = deferred<NatsConnection>()
    const runtime = createNatsRuntime({ connect: () => factory.promise, shutdownTimeoutMs: 10 })
    const connecting = runtime.connection().catch((error: unknown) => error)
    const closing = runtime.close()
    const network = transport()
    if (mode === 'reject') factory.reject(new Error('cancelled'))
    else factory.resolve(network.connection)
    await expect(closing).resolves.toBeUndefined()
    expect(await connecting).toBeInstanceOf(Error)
    expect(network.close).toHaveBeenCalledTimes(mode === 'resolve' ? 1 : 0)
    expect(runtime.inspect().connectionGeneration).toBe(0)
    expect(vi.getTimerCount()).toBe(0)
  }
)

it('requests forced connection cleanup only once even if that cleanup remains pending', async () => {
  vi.useFakeTimers()
  const network = transport()
  const drain = deferred<void>()
  const closed = deferred<void>()
  network.drain.mockReturnValueOnce(drain.promise)
  network.close.mockReturnValueOnce(closed.promise)
  const runtime = createNatsRuntime({
    connect: async () => network.connection,
    shutdownTimeoutMs: 10,
  })
  await runtime.connection()
  const closing = runtime.close().catch((error: unknown) => error)
  await vi.advanceTimersByTimeAsync(10)
  expect(await closing).toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError' })
  try {
    expect(network.close).toHaveBeenCalledOnce()
  } finally {
    closed.resolve()
    drain.resolve()
    await vi.advanceTimersByTimeAsync(0)
  }
})

it('rejects shutdown if a late connection cannot be cleaned up before the deadline', async () => {
  const factory = deferred<NatsConnection>()
  const network = transport()
  const error = new Error('transport cleanup failed')
  network.close.mockRejectedValueOnce(error)
  const runtime = createNatsRuntime({ connect: () => factory.promise })
  const events: NatsRuntimeEvent[] = []
  const watching = (async () => {
    for await (const event of runtime.events) events.push(event)
  })()
  const connecting = runtime.connection().catch((failure: unknown) => failure)
  const closing = runtime.close()
  factory.resolve(network.connection)
  await expect(closing).rejects.toBe(error)
  expect(await connecting).toBe(error)
  await watching
  expect(
    events.filter((event) => event.type === 'diagnostic').map((event) => event.code)
  ).toContain('disposal-failed')
  expect(
    events.filter((event) => event.type === 'diagnostic').map((event) => event.code)
  ).not.toContain('disposal-completed')
  expect(network.close).toHaveBeenCalledOnce()
})

it('correlates attempt start, failure, retry, success and graceful disposal without factory error payloads', async () => {
  const network = transport()
  const connect = vi
    .fn<() => Promise<NatsConnection>>()
    .mockRejectedValueOnce(new errors.AuthorizationError('CONNECT secret-credential'))
    .mockResolvedValueOnce(network.connection)
  const runtime = createNatsRuntime({
    connect,
    initialConnectRetry: { maxAttempts: 2, delayMs: 0 },
  })
  const events: NatsRuntimeEvent[] = []
  const watching = (async () => {
    for await (const event of runtime.events) events.push(event)
  })()
  await runtime.connection()
  await runtime.close()
  await watching
  const diagnostics = events.filter((event) => event.type === 'diagnostic')
  expect(diagnostics.map((event) => event.code)).toEqual([
    'connection-attempt-started',
    'connection-attempt-failed',
    'connection-retry-scheduled',
    'connection-attempt-started',
    'connection-attempt-succeeded',
    'disposal-requested',
    'disposal-completed',
  ])
  expect(diagnostics[0]?.details).toMatchObject({ attempt: 1, attemptId: 1, generation: 0 })
  expect(diagnostics[1]?.details).toMatchObject({ failureCategory: 'authentication' })
  expect(diagnostics[2]?.details).toMatchObject({ failureCategory: 'authentication' })
  expect(diagnostics[4]?.details).toMatchObject({ attempt: 2, attemptId: 2, generation: 1 })
  expect(diagnostics.every((event) => event.error === undefined)).toBe(true)
  expect(JSON.stringify(events)).not.toContain('secret-credential')
})

it('reports disposal timeout and a late discard without reopening the terminal event stream', async () => {
  vi.useFakeTimers()
  const factory = deferred<NatsConnection>()
  const measurements: NatsailTelemetryEvent[] = []
  const events: NatsRuntimeEvent[] = []
  const runtime = createNatsRuntime({
    connect: () => factory.promise,
    shutdownTimeoutMs: 10,
    telemetry: { record: (event) => measurements.push(event) },
  })
  const watching = (async () => {
    for await (const event of runtime.events) events.push(event)
  })()
  const connecting = runtime.connection().catch((error: unknown) => error)
  const closing = runtime.close().catch((error: unknown) => error)
  await vi.advanceTimersByTimeAsync(10)
  await closing
  await watching
  const network = transport()
  factory.resolve(network.connection)
  await connecting
  expect(events.filter((event) => event.type === 'diagnostic').map((event) => event.code)).toEqual([
    'connection-attempt-started',
    'disposal-requested',
    'disposal-timed-out',
  ])
  expect(measurements).toContainEqual(
    expect.objectContaining({
      name: 'natsail.connection.transitions',
      attributes: expect.objectContaining({ state: 'late-connection-discarded' }),
    })
  )
  expect(events.at(-1)).toMatchObject({ type: 'status', state: 'closed' })
  expect(measurements).toContainEqual(
    expect.objectContaining({
      name: 'natsail.connection.transitions',
      attributes: expect.objectContaining({
        state: 'connection-attempt-failed',
        failureCategory: 'cancelled',
      }),
    })
  )
  expect(runtime.inspect().connectionGeneration).toBe(0)
  expect(network.close).toHaveBeenCalledOnce()
})
