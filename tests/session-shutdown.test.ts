import { afterEach, expect, it, vi } from 'vitest'
import { createNatsRuntime, natsCodecs, type SubscriptionLease } from '@natsail/core'
import {
  closeNatsResources,
  createCoreSessionSource,
  createSessionRegistry,
} from '@natsail/session'
import type { NatsConnection } from '@nats-io/nats-core'
import { deferred, transport } from './fixtures/lifecycle'

afterEach(() => vi.useRealTimers())

it.each(['runtime', 'sessions'] as const)(
  'starts both cleanups even when %s close throws synchronously',
  async (throwing) => {
    const failure = new Error('cleanup failed')
    const pending = deferred<void>()
    const close = vi.fn(() => {
      throw failure
    })
    const otherClose = vi.fn(() => pending.promise)
    const runtime = { close: throwing === 'runtime' ? close : otherClose }
    const sessions = { close: throwing === 'sessions' ? close : otherClose }
    await expect(closeNatsResources({ runtime, sessions })).rejects.toBe(failure)
    expect(close).toHaveBeenCalledOnce()
    expect(otherClose).toHaveBeenCalledOnce()
    pending.reject(new Error('later cleanup failure'))
    await Promise.resolve()
  }
)

it('preserves the last accepted value while a real Core session drains', async () => {
  const network = transport(['first', 'buffered'])
  const runtime = createNatsRuntime({ connect: async () => network.connection })
  const sessions = createSessionRegistry()
  const decoding = deferred<void>()
  const release = deferred<void>()
  const handle = sessions.acquire(
    'work',
    createCoreSessionSource(runtime, {
      subject: 'work',
      decode: async (message) => {
        decoding.resolve()
        await release.promise
        return natsCodecs.text.decode(message.data)
      },
    })
  )
  await decoding.promise
  const closing = closeNatsResources({ sessions, runtime })
  release.resolve()
  await closing
  expect(handle.getSnapshot()).toMatchObject({
    phase: 'closed',
    value: 'buffered',
    valueRevision: 2,
  })
  expect(runtime.inspect().activeResources).toBe(0)
  expect(network.drain).toHaveBeenCalledOnce()
  expect(network.close).not.toHaveBeenCalled()
})

it('bounds coordinated cleanup even when a real session waits for a pending factory', async () => {
  vi.useFakeTimers()
  const factory = deferred<NatsConnection>()
  const network = transport()
  const runtime = createNatsRuntime({ connect: () => factory.promise, shutdownTimeoutMs: 10 })
  const sessions = createSessionRegistry()
  sessions.acquire(
    'work',
    createCoreSessionSource(runtime, { subject: 'work', codec: natsCodecs.text })
  )
  const closing = closeNatsResources({ runtime, sessions }).catch((error: unknown) => error)
  try {
    await vi.advanceTimersByTimeAsync(10)
    expect(await closing).toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError' })
    expect(runtime.inspect()).toMatchObject({ activeResources: 0, connection: { state: 'closed' } })
    expect(sessions.inspect()).toMatchObject({ closed: true, activeSessions: 0 })
  } finally {
    factory.resolve(network.connection)
    await vi.advanceTimersByTimeAsync(0)
    vi.useRealTimers()
  }
})

it('does not accept buffered values on explicit cancellation during decoding', async () => {
  const network = transport(['first', 'buffered'])
  const runtime = createNatsRuntime({ connect: async () => network.connection })
  const sessions = createSessionRegistry()
  const decoding = deferred<void>()
  const release = deferred<void>()
  const cancellation = new AbortController()
  let lease!: SubscriptionLease
  const handle = sessions.acquire('work', (accept) => {
    lease = runtime.subscribe(
      {
        subject: 'work',
        signal: cancellation.signal,
        decode: async (message) => {
          decoding.resolve()
          await release.promise
          return natsCodecs.text.decode(message.data)
        },
      },
      accept
    )
    return lease
  })
  await decoding.promise
  const error = new Error('explicit cancellation')
  cancellation.abort(error)
  release.resolve()
  await expect(lease.closed).rejects.toBe(error)
  await sessions.close()
  await runtime.close()
  expect(handle.getSnapshot()).toMatchObject({ phase: 'closed', valueRevision: 0 })
  expect(runtime.inspect().activeResources).toBe(0)
  expect(network.flush).not.toHaveBeenCalled()
})
