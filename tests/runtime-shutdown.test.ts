import { afterEach, describe, expect, it, vi } from 'vitest'
import type { Msg, NatsConnection } from '@nats-io/nats-core'
import { SubscriptionImpl, Subscriptions, type ProtocolHandler } from '@nats-io/nats-core/internal'
import { createNatsRuntime, natsCodecs } from '@natsail/core'

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

function transport(values: readonly string[] = ['work']) {
  const disconnected = deferred<void>()
  let stopped = false
  const subscriptions = new Subscriptions()
  const flush = vi.fn<() => Promise<void>>(async () => undefined)
  const protocol = {
    options: {},
    isClosed: () => stopped,
    subscriptions,
    unsub: vi.fn(),
    unsubscribe: (subscription: SubscriptionImpl) => subscriptions.cancel(subscription),
    flush,
  } as unknown as ProtocolHandler
  const subscription = subscriptions.add(new SubscriptionImpl(protocol, 'work'))
  const deliver = (value: string) => {
    if (subscriptions.get(subscription.sid)) {
      subscription.callback(null, { subject: 'work', data: natsCodecs.text.encode(value) } as Msg)
    }
  }
  const disconnect = () => {
    stopped = true
    subscriptions.close()
    disconnected.resolve()
  }
  const close = vi.fn(async () => disconnect())
  const drain = vi.fn(async () => {
    await Promise.all(subscriptions.all().map((active) => active.drain()))
    disconnect()
  })
  const connection = {
    subscribe: () => {
      values.forEach(deliver)
      return subscription
    },
    getServer: () => 'mock:4222',
    status: async function* () {},
    isClosed: () => stopped,
    closed: () => disconnected.promise,
    close,
    drain,
  } as unknown as NatsConnection
  return { connection, close, drain, flush, deliver }
}

afterEach(() => vi.useRealTimers())

describe('bounded runtime shutdown', () => {
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
