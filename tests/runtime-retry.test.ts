import { describe, expect, it, vi } from 'vitest'

import type { NatsConnection, Status } from '@nats-io/nats-core'
import { createNatsRuntime } from '@natsail/core'

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
