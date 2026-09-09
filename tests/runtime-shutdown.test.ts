import { afterEach, describe, expect, it, vi } from 'vitest'
import type { NatsConnection } from '@nats-io/nats-core'
import { createNatsRuntime, natsCodecs } from '@natsail/core'
import { deferred, transport } from './fixtures/lifecycle'

afterEach(() => vi.useRealTimers())

describe('bounded runtime shutdown', () => {
  it('signals a shared pending factory at disposal without pretending to abort its transport', async () => {
    vi.useFakeTimers()
    const network = transport()
    const connecting = deferred<NatsConnection>()
    let signal: AbortSignal | undefined
    const connect = vi.fn((context?: { signal: AbortSignal }) => {
      signal = context?.signal
      return connecting.promise
    })
    const runtime = createNatsRuntime({ connect: { create: connect }, shutdownTimeoutMs: 10 })
    const first = runtime.connection()
    const second = runtime.connection()
    void first.catch(() => undefined)
    expect(first).toBe(second)
    expect(connect).toHaveBeenCalledOnce()
    expect(signal?.aborted).toBe(false)
    const closing = runtime.close().catch((error: unknown) => error)
    expect(signal?.aborted).toBe(true)
    expect(network.close).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(10)
    expect(await closing).toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError' })
    connecting.resolve(network.connection)
    await expect(first).rejects.toThrow('closed')
    expect(network.close).toHaveBeenCalledOnce()
    expect(runtime.inspect().connectionGeneration).toBe(0)
  })

  it('closes a connection factory result that arrives after the deadline', async () => {
    vi.useFakeTimers()
    const network = transport()
    const connecting = deferred<NatsConnection>()
    const runtime = createNatsRuntime({ connect: () => connecting.promise, shutdownTimeoutMs: 10 })
    const connection = runtime.connection().catch((error: unknown) => error)
    const closing = runtime.close().catch((error: unknown) => error)
    await vi.advanceTimersByTimeAsync(10)
    await expect(closing).resolves.toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError' })
    connecting.resolve(network.connection)
    await connection
    expect(network.close).toHaveBeenCalledOnce()
    expect(network.drain).not.toHaveBeenCalled()
    expect(runtime.inspect().connection.state).toBe('closed')
  })

  it('finishes buffered and in-flight messages before draining the connection', async () => {
    const network = transport(['first', 'buffered'])
    network.flush.mockImplementationOnce(async () => network.deliver('in-flight'))
    const runtime = createNatsRuntime({ connect: async () => network.connection })
    const started = deferred<void>()
    const release = deferred<void>()
    const processed: string[] = []
    const lease = runtime.subscribe({ subject: 'work', codec: natsCodecs.text }, async (value) => {
      processed.push(value)
      if (value === 'first') {
        started.resolve()
        await release.promise
      }
    })
    await started.promise
    const closing = runtime.close()
    expect(network.close).not.toHaveBeenCalled()
    release.resolve()
    await expect(closing).resolves.toBeUndefined()
    expect(processed).toEqual(['first', 'buffered', 'in-flight'])
    expect(network.drain).toHaveBeenCalledOnce()
    expect(network.close).not.toHaveBeenCalled()
    await expect(lease.closed).resolves.toBeUndefined()
  })

  it.each([-1, 0.5, Number.POSITIVE_INFINITY, 2_147_483_648])(
    'rejects unsafe shutdown timeout %s',
    (shutdownTimeoutMs) => {
      expect(() =>
        createNatsRuntime({ connect: async () => transport().connection, shutdownTimeoutMs })
      ).toThrow('shutdownTimeoutMs')
    }
  )
  it('signals cooperative handlers only after allowing the grace period', async () => {
    vi.useFakeTimers()
    const network = transport()
    const runtime = createNatsRuntime({
      connect: async () => network.connection,
      shutdownTimeoutMs: 50,
    })
    const started = deferred<void>()
    let signal: AbortSignal | undefined
    const lease = runtime.subscribe(
      { subject: 'work', codec: natsCodecs.text },
      async (_value, _message, context) => {
        signal = context?.signal
        started.resolve()
        if (signal)
          await new Promise<void>((resolve) =>
            signal!.addEventListener('abort', () => resolve(), { once: true })
          )
      }
    )
    await started.promise
    const closing = runtime.close().catch((error: unknown) => error)
    await vi.advanceTimersByTimeAsync(49)
    expect(signal?.aborted).toBe(false)
    await vi.advanceTimersByTimeAsync(1)
    expect(signal?.aborted).toBe(true)
    await expect(closing).resolves.toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError' })
    await lease.closed.catch(() => undefined)
  })
  it('reports unfinished work at the deadline instead of waiting forever', async () => {
    vi.useFakeTimers()
    const network = transport(['first', 'buffered'])
    const runtime = createNatsRuntime({
      connect: async () => network.connection,
      shutdownTimeoutMs: 50,
    })
    const started = deferred<void>()
    const release = deferred<void>()
    const processed: string[] = []
    const lease = runtime.subscribe({ subject: 'work', codec: natsCodecs.text }, async (value) => {
      processed.push(value)
      started.resolve()
      await release.promise
    })
    await started.promise
    let failure: unknown
    let settled = false
    const closing = runtime
      .close()
      .catch((error: unknown) => {
        failure = error
      })
      .finally(() => {
        settled = true
      })
    await vi.advanceTimersByTimeAsync(50)
    try {
      expect(settled).toBe(true)
      expect(failure).toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError', timeoutMs: 50 })
      expect(runtime.inspect().connection.state).toBe('closed')
      expect(network.close).toHaveBeenCalledOnce()
      expect(network.drain).not.toHaveBeenCalled()
    } finally {
      release.resolve()
      await network.close()
      await lease.closed.catch(() => undefined)
      await closing
    }
    expect(processed).toEqual(['first'])
  })

  it('force-closes when the subscription drain round trip never finishes', async () => {
    vi.useFakeTimers()
    const network = transport()
    network.flush.mockImplementation(() => new Promise(() => undefined))
    const runtime = createNatsRuntime({
      connect: async () => network.connection,
      shutdownTimeoutMs: 50,
    })
    const lease = runtime.subscribe({ subject: 'work', codec: natsCodecs.text }, () => undefined)
    await lease.ready
    const closing = runtime.close().catch((error: unknown) => error)
    await vi.advanceTimersByTimeAsync(50)
    await expect(closing).resolves.toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError' })
    expect(network.close).toHaveBeenCalledOnce()
    expect(network.drain).not.toHaveBeenCalled()
    await lease.closed.catch(() => undefined)
  })

  it('bounds a stalled connection drain after handlers finish', async () => {
    vi.useFakeTimers()
    const network = transport()
    network.drain.mockImplementation(() => new Promise(() => undefined))
    const runtime = createNatsRuntime({
      connect: async () => network.connection,
      shutdownTimeoutMs: 50,
    })
    const lease = runtime.subscribe({ subject: 'work', codec: natsCodecs.text }, () => undefined)
    await lease.ready
    const closing = runtime.close().catch((error: unknown) => error)
    await vi.advanceTimersByTimeAsync(0)
    await expect(lease.closed).resolves.toBeUndefined()
    expect(network.drain).toHaveBeenCalledOnce()
    expect(network.close).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(50)
    await expect(closing).resolves.toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError' })
    expect(network.close).toHaveBeenCalledOnce()
  })

  it('drains a lease without closing the shared connection', async () => {
    const network = transport(['first', 'buffered'])
    const runtime = createNatsRuntime({ connect: async () => network.connection })
    const started = deferred<void>()
    const release = deferred<void>()
    const processed: string[] = []
    const lease = runtime.subscribe({ subject: 'work', codec: natsCodecs.text }, async (value) => {
      processed.push(value)
      if (value === 'first') {
        started.resolve()
        await release.promise
      }
    })
    await started.promise
    const closing = lease.close()
    release.resolve()
    await closing
    expect(processed).toEqual(['first', 'buffered'])
    expect(network.connection.isClosed()).toBe(false)
    expect(network.drain).not.toHaveBeenCalled()
    expect(network.close).not.toHaveBeenCalled()
    await runtime.close()
  })

  it('cancels buffered delivery immediately on explicit abort', async () => {
    const network = transport(['first', 'buffered'])
    const runtime = createNatsRuntime({ connect: async () => network.connection })
    const cancellation = new AbortController()
    const reason = new Error('cancelled by caller')
    const started = deferred<void>()
    const release = deferred<void>()
    const processed: string[] = []
    const lease = runtime.subscribe(
      { subject: 'work', codec: natsCodecs.text, signal: cancellation.signal },
      async (value) => {
        processed.push(value)
        started.resolve()
        await release.promise
      }
    )
    await started.promise
    cancellation.abort(reason)
    release.resolve()
    await expect(lease.closed).rejects.toBe(reason)
    expect(processed).toEqual(['first'])
    expect(network.flush).not.toHaveBeenCalled()
    await runtime.close()
  })
})
