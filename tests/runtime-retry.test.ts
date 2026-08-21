import { describe, expect, it, vi } from 'vitest'

import type { NatsConnection } from '@nats-io/nats-core'
import { createNatsRuntime } from '@natsail/core'

describe('runtime initial connection retry', () => {
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
})

function fakeConnection(): NatsConnection {
  let closed = false

  return {
    getServer: () => 'nats://test',
    status: () => ({
      async *[Symbol.asyncIterator]() {},
    }),
    isClosed: () => closed,
    drain: async () => {
      closed = true
    },
  } as NatsConnection
}
