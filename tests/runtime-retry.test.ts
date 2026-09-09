import { describe, expect, it, vi } from 'vitest'

import type { NatsConnection, Status } from '@nats-io/nats-core'
import { errors } from '@nats-io/nats-core'
import { createNatsRuntime, type NatsRuntimeEvent } from '@natsail/core'

describe('runtime initial connection retry', () => {
  it.each([0, -1, 1.5, Number.POSITIVE_INFINITY])(
    'rejects invalid event capacity %s',
    (maxBufferedEvents) => {
      expect(() =>
        createNatsRuntime({ connect: async () => fakeConnection(), maxBufferedEvents })
      ).toThrow('maxBufferedEvents')
    }
  )
  it('bounds a stalled event subscriber and reports the gap before retained events', async () => {
    const controlled = controllableConnection('nats://test')
    const runtime = createNatsRuntime({
      connect: async () => controlled.connection,
      maxBufferedEvents: 2,
    })
    await runtime.connection()
    const stalled = runtime.events[Symbol.asyncIterator]()
    for (let attempt = 0; attempt < 5; attempt += 1) await runtime.reconnect()
    await runtime.close()
    const retained = []
    for (;;) {
      const result = await stalled.next()
      if (result.done) break
      retained.push(result.value)
    }
    expect(retained).toHaveLength(3)
    expect(retained[0]).toMatchObject({
      type: 'diagnostic',
      code: 'event-buffer-overflow',
      details: { capacity: 2 },
    })
    expect(retained.at(-1)).toMatchObject({ type: 'status', state: 'closed' })
  })
  it('retries a bounded number of times and shares the successful connection', async () => {
    vi.useFakeTimers()
    try {
      const connection = fakeConnection()
      const connect = vi
        .fn<() => Promise<NatsConnection>>()
        .mockRejectedValueOnce(new Error('first'))
        .mockRejectedValueOnce(new Error('second'))
        .mockResolvedValue(connection)
      const runtime = createNatsRuntime({
        connect,
        initialConnectRetry: { maxAttempts: 3, delayMs: 10 },
      })

      const first = runtime.connection()
      const second = runtime.connection()
      await vi.runAllTimersAsync()

      await expect(first).resolves.toBe(connection)
      await expect(second).resolves.toBe(connection)
      expect(connect).toHaveBeenCalledTimes(3)
      await runtime.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('allows a later connection series after the retry budget is exhausted', async () => {
    vi.useFakeTimers()
    try {
      const connect = vi.fn<() => Promise<NatsConnection>>().mockRejectedValue(new Error('offline'))
      const runtime = createNatsRuntime({
        connect,
        initialConnectRetry: { maxAttempts: 2, delayMs: 10 },
      })

      const first = runtime.connection()
      await vi.runAllTimersAsync()
      await expect(first).rejects.toThrow('offline')

      const second = runtime.connection()
      await vi.runAllTimersAsync()
      await expect(second).rejects.toThrow('offline')
      expect(connect).toHaveBeenCalledTimes(4)
      await expect(runtime.close()).resolves.toBeUndefined()
    } finally {
      vi.useRealTimers()
    }
  })

  it('cancels a pending retry when the runtime closes', async () => {
    vi.useFakeTimers()
    try {
      const connect = vi.fn<() => Promise<NatsConnection>>().mockRejectedValue(new Error('offline'))
      const runtime = createNatsRuntime({
        connect,
        initialConnectRetry: { maxAttempts: 5, delayMs: 60_000 },
      })

      const connection = runtime.connection()
      await vi.advanceTimersByTimeAsync(0)
      expect(connect).toHaveBeenCalledTimes(1)

      await expect(runtime.close()).resolves.toBeUndefined()
      await expect(connection).rejects.toBeInstanceOf(Error)
      expect(connect).toHaveBeenCalledTimes(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it('validates the retry policy', () => {
    expect(() =>
      createNatsRuntime({
        connect: async () => fakeConnection(),
        initialConnectRetry: { maxAttempts: 0, delayMs: 10 },
      })
    ).toThrow('maxAttempts')
    expect(() =>
      createNatsRuntime({
        connect: async () => fakeConnection(),
        initialConnectRetry: { maxAttempts: 2, delayMs: -1 },
      })
    ).toThrow('delayMs')
  })

  it('supports error-aware retry decisions and computed delays', async () => {
    vi.useFakeTimers()
    try {
      const connection = fakeConnection()
      const connect = vi
        .fn<() => Promise<NatsConnection>>()
        .mockRejectedValueOnce(new Error('offline'))
        .mockResolvedValue(connection)
      const delayMs = vi.fn(() => 25)
      const shouldRetry = vi.fn(() => true)
      const runtime = createNatsRuntime({
        connect,
        initialConnectRetry: { maxAttempts: 2, delayMs, shouldRetry },
      })

      const pending = runtime.connection()
      await vi.runAllTimersAsync()

      await expect(pending).resolves.toBe(connection)
      expect(shouldRetry).toHaveBeenCalledWith(
        expect.objectContaining({ attempt: 1, maxAttempts: 2, error: expect.any(Error) })
      )
      expect(delayMs).toHaveBeenCalledWith(
        expect.objectContaining({ attempt: 1, maxAttempts: 2, error: expect.any(Error) })
      )
      await runtime.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('replaces a permanently closed connection without closing the runtime event stream', async () => {
    const first = controllableConnection('nats://first')
    const second = controllableConnection('nats://second')
    const connect = vi
      .fn<() => Promise<NatsConnection>>()
      .mockResolvedValueOnce(first.connection)
      .mockResolvedValueOnce(second.connection)
    const runtime = createNatsRuntime({ connect })
    const iterator = runtime.events[Symbol.asyncIterator]()

    await expect(runtime.connection()).resolves.toBe(first.connection)
    expect(runtime.inspect()).toEqual(
      expect.objectContaining({ connectionGeneration: 1, activeResources: 0 })
    )

    first.closePermanently()
    await expect.poll(() => connect).toHaveBeenCalledTimes(2)
    await expect(runtime.connection()).resolves.toBe(second.connection)
    expect(runtime.inspect()).toEqual(
      expect.objectContaining({
        connection: expect.objectContaining({ state: 'connected', server: 'nats://second' }),
        connectionGeneration: 2,
      })
    )

    await runtime.close()
    await expect(iterator.next()).resolves.toEqual(
      expect.objectContaining({ done: false, value: expect.objectContaining({ state: 'idle' }) })
    )
  })

  it('can wait for a caller before replacing a permanently closed connection', async () => {
    const first = controllableConnection('nats://first')
    const second = controllableConnection('nats://second')
    const connect = vi
      .fn<() => Promise<NatsConnection>>()
      .mockResolvedValueOnce(first.connection)
      .mockResolvedValueOnce(second.connection)
    const runtime = createNatsRuntime({
      connect,
      connectionRecovery: { onPermanentClose: 'wait' },
    })

    await runtime.connection()
    first.closePermanently()
    await expect.poll(() => runtime.inspect().connection.state).toBe('disconnected')
    expect(connect).toHaveBeenCalledOnce()

    await expect(runtime.connection()).resolves.toBe(second.connection)
    expect(connect).toHaveBeenCalledTimes(2)
    await runtime.close()
  })

  it('forces a live reconnect so rotating authenticators can run again', async () => {
    const controlled = controllableConnection('nats://test')
    const runtime = createNatsRuntime({ connect: async () => controlled.connection })

    await runtime.connection()
    await expect(runtime.reconnect({ reason: 'credentials-changed' })).resolves.toBe(
      controlled.connection
    )

    expect(controlled.reconnect).toHaveBeenCalledTimes(1)
    expect(runtime.inspect().connectionGeneration).toBe(1)
    await runtime.close()
  })

  it('does not resolve reconnect before the runtime observes the new live connection', async () => {
    const controlled = controllableConnection('nats://test')
    controlled.reconnect.mockImplementationOnce(async () => undefined)
    const runtime = createNatsRuntime({ connect: async () => controlled.connection })
    await runtime.connection()

    let settled = false
    const reconnecting = runtime.reconnect().then(() => {
      settled = true
    })
    await expect.poll(() => controlled.reconnect.mock.calls.length).toBe(1)
    expect(settled).toBe(false)

    controlled.emitStatus({ type: 'disconnect', server: 'nats://test' })
    controlled.emitStatus({ type: 'reconnect', server: 'nats://test' })
    await reconnecting
    expect(settled).toBe(true)

    await runtime.close()
  })

  it('replaces a connection that closes during a forced reconnect', async () => {
    const first = controllableConnection('nats://first')
    const second = controllableConnection('nats://second')
    first.reconnect.mockImplementationOnce(async () => {
      first.closePermanently(new Error('authentication expired'))
      throw new Error('connection closed')
    })
    const connect = vi
      .fn<() => Promise<NatsConnection>>()
      .mockResolvedValueOnce(first.connection)
      .mockResolvedValueOnce(second.connection)
    const runtime = createNatsRuntime({ connect })

    await runtime.connection()
    await expect(runtime.reconnect({ reason: 'token-rotated' })).resolves.toBe(second.connection)
    expect(runtime.inspect().connectionGeneration).toBe(2)

    await runtime.close()
  })

  it('coalesces concurrent explicit reconnects, including during startup', async () => {
    const controlled = controllableConnection('nats://test')
    let resolveFactory!: (connection: NatsConnection) => void
    const connect = vi.fn(
      () =>
        new Promise<NatsConnection>((resolve) => {
          resolveFactory = resolve
        })
    )
    const runtime = createNatsRuntime({ connect })
    const events: NatsRuntimeEvent[] = []
    const watching = (async () => {
      for await (const event of runtime.events) events.push(event)
    })()
    const initial = runtime.connection()
    const first = runtime.reconnect()
    const second = runtime.reconnect()
    expect(connect).toHaveBeenCalledOnce()
    resolveFactory(controlled.connection)
    await expect(initial).resolves.toBe(controlled.connection)
    await expect(first).resolves.toBe(controlled.connection)
    await expect(second).resolves.toBe(controlled.connection)
    expect(controlled.reconnect).toHaveBeenCalledOnce()
    await runtime.close()
    await watching
    expect(
      events
        .filter((event) => event.type === 'diagnostic')
        .map((event) => event.code)
        .filter((code) => code.startsWith('reconnect-'))
    ).toEqual(['reconnect-requested', 'reconnect-completed'])
  })

  it('settles a stalled native reconnect when shutdown starts', async () => {
    vi.useFakeTimers()
    const controlled = controllableConnection('nats://test')
    let finishReconnect!: () => void
    controlled.reconnect.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finishReconnect = resolve
        })
    )
    const runtime = createNatsRuntime({ connect: async () => controlled.connection })
    await runtime.connection()
    let failure: unknown
    const reconnecting = runtime.reconnect().catch((error: unknown) => {
      failure = error
    })
    await vi.advanceTimersByTimeAsync(0)
    await runtime.close()
    await vi.advanceTimersByTimeAsync(0)
    try {
      expect(failure).toBeInstanceOf(Error)
      expect(String(failure)).toContain('closed')
    } finally {
      finishReconnect()
      await reconnecting
      vi.useRealTimers()
    }
  })

  it('replaces a permanently closed connection even when its native reconnect never settles', async () => {
    vi.useFakeTimers()
    const first = controllableConnection('nats://first')
    const second = controllableConnection('nats://second')
    let finish!: () => void
    first.reconnect.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finish = resolve
        })
    )
    const connect = vi
      .fn<() => Promise<NatsConnection>>()
      .mockResolvedValueOnce(first.connection)
      .mockResolvedValueOnce(second.connection)
    const runtime = createNatsRuntime({ connect })
    await runtime.connection()
    let result: NatsConnection | undefined
    const reconnecting = runtime.reconnect().then((connection) => {
      result = connection
    })
    await vi.advanceTimersByTimeAsync(0)
    first.closePermanently()
    await vi.advanceTimersByTimeAsync(0)
    try {
      expect(result).toBe(second.connection)
      expect(connect).toHaveBeenCalledTimes(2)
    } finally {
      finish()
      await reconnecting
      await runtime.close()
      vi.useRealTimers()
    }
  })

  it('does not publish a queued connected status once disposal has started', async () => {
    vi.useFakeTimers()
    const controlled = controllableConnection('nats://test')
    let finishDrain!: () => void
    vi.spyOn(controlled.connection, 'drain').mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finishDrain = resolve
        })
    )
    const runtime = createNatsRuntime({ connect: async () => controlled.connection })
    await runtime.connection()
    controlled.emitStatus({ type: 'disconnect', server: 'nats://test' })
    await vi.advanceTimersByTimeAsync(0)
    const closing = runtime.close()
    await vi.advanceTimersByTimeAsync(0)
    controlled.emitStatus({ type: 'reconnect', server: 'nats://test' })
    await vi.advanceTimersByTimeAsync(0)
    try {
      expect(runtime.inspect().connection.state).toBe('disconnected')
    } finally {
      controlled.closePermanently()
      finishDrain()
      await closing
      vi.useRealTimers()
    }
  })

  it('rejects startup reconnect promptly on disposal and never forces the late connection', async () => {
    vi.useFakeTimers()
    const controlled = controllableConnection('nats://test')
    let resolveFactory!: (connection: NatsConnection) => void
    const connect = vi.fn(
      () =>
        new Promise<NatsConnection>((resolve) => {
          resolveFactory = resolve
        })
    )
    const runtime = createNatsRuntime({ connect, shutdownTimeoutMs: 10 })
    const initial = runtime.connection().catch((error: unknown) => error)
    const reconnecting = runtime.reconnect().catch((error: unknown) => error)
    await vi.advanceTimersByTimeAsync(0)
    const closing = runtime.close().catch((error: unknown) => error)
    expect(await reconnecting).toBeInstanceOf(Error)
    await vi.advanceTimersByTimeAsync(10)
    expect(await closing).toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError' })
    resolveFactory(controlled.connection)
    await initial
    expect(controlled.reconnect).not.toHaveBeenCalled()
    expect(connect).toHaveBeenCalledOnce()
    expect(runtime.inspect().connectionGeneration).toBe(0)
    await expect(runtime.reconnect()).rejects.toThrow('closed')
    vi.useRealTimers()
  })

  it('does not start a factory when reconnect is immediately followed by disposal', async () => {
    const connect = vi.fn(async () => fakeConnection())
    const runtime = createNatsRuntime({ connect })
    const reconnecting = runtime.reconnect().catch((error: unknown) => error)
    await runtime.close()
    expect(await reconnecting).toBeInstanceOf(Error)
    expect(connect).not.toHaveBeenCalled()
  })

  it('shares native reconnect failure and permits a later explicit retry', async () => {
    const controlled = controllableConnection('nats://test')
    const error = new errors.TimeoutError()
    controlled.reconnect.mockRejectedValueOnce(error)
    const runtime = createNatsRuntime({ connect: async () => controlled.connection })
    const events: NatsRuntimeEvent[] = []
    const watching = (async () => {
      for await (const event of runtime.events) events.push(event)
    })()
    await runtime.connection()
    const first = runtime.reconnect()
    const second = runtime.reconnect()
    expect(first).toBe(second)
    await expect(first).rejects.toBe(error)
    await expect(second).rejects.toBe(error)
    await expect(runtime.reconnect()).resolves.toBe(controlled.connection)
    expect(controlled.reconnect).toHaveBeenCalledTimes(2)
    await runtime.close()
    await watching
    expect(
      events
        .filter((event) => event.type === 'diagnostic')
        .map((event) => event.code)
        .filter((code) => code.startsWith('reconnect-'))
    ).toEqual([
      'reconnect-requested',
      'reconnect-failed',
      'reconnect-requested',
      'reconnect-completed',
    ])
    expect(events).toContainEqual(
      expect.objectContaining({
        code: 'reconnect-failed',
        details: expect.objectContaining({ failureCategory: 'timeout' }),
      })
    )
  })
})

function fakeConnection(): NatsConnection {
  let closed = false
  let resolveClosed!: () => void
  const closedPromise = new Promise<void>((resolve) => {
    resolveClosed = resolve
  })

  return {
    getServer: () => 'nats://test',
    status: () => ({
      async *[Symbol.asyncIterator]() {},
    }),
    isClosed: () => closed,
    closed: () => closedPromise,
    drain: async () => {
      closed = true
      resolveClosed()
    },
  } as unknown as NatsConnection
}

function controllableConnection(server: string): {
  connection: NatsConnection
  reconnect: ReturnType<typeof vi.fn<() => Promise<void>>>
  emitStatus(status: Status): void
  closePermanently(error?: Error): void
} {
  let closed = false
  let waiting: ((result: IteratorResult<Status>) => void) | undefined
  const queued: Status[] = []
  let resolveClosed!: (error?: void | Error) => void
  const closedPromise = new Promise<void | Error>((resolve) => {
    resolveClosed = resolve
  })

  const emit = (status: Status) => {
    if (waiting) {
      const resolve = waiting
      waiting = undefined
      resolve({ done: false, value: status })
    } else {
      queued.push(status)
    }
  }
  const reconnect = vi.fn<() => Promise<void>>(async () => {
    emit({ type: 'disconnect', server })
    emit({ type: 'reconnect', server })
  })

  const connection = {
    getServer: () => server,
    status: () => ({
      [Symbol.asyncIterator]() {
        return {
          next: () => {
            const status = queued.shift()
            if (status) return Promise.resolve({ done: false as const, value: status })
            if (closed) return Promise.resolve({ done: true as const, value: undefined })
            return new Promise<IteratorResult<Status>>((resolve) => {
              waiting = resolve
            })
          },
        }
      },
    }),
    isClosed: () => closed,
    closed: () => closedPromise,
    reconnect,
    close: async () => {
      closed = true
      resolveClosed()
      waiting?.({ done: true, value: undefined })
      waiting = undefined
    },
    drain: async () => {
      closed = true
      resolveClosed()
      waiting?.({ done: true, value: undefined })
      waiting = undefined
    },
  } as unknown as NatsConnection

  return {
    connection,
    reconnect,
    emitStatus: emit,
    closePermanently: (error?: Error) => {
      closed = true
      resolveClosed(error)
      emit({ type: 'close' })
    },
  }
}
