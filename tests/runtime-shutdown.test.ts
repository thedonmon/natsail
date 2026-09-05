import { afterEach, describe, expect, it, vi } from 'vitest'
import type { Msg, NatsConnection, Subscription } from '@nats-io/nats-core'
import { createNatsRuntime, natsCodecs } from '@natsail/core'

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

function transport() {
  const disconnected = deferred<void>()
  let stopped = false
  const subscription = {
    async *[Symbol.asyncIterator]() {
      yield { data: natsCodecs.text.encode('work') } as Msg
      await disconnected.promise
    },
    unsubscribe: () => undefined,
    closed: Promise.resolve(undefined),
  } as unknown as Subscription
  const close = vi.fn(async () => {
    stopped = true
    disconnected.resolve()
  })
  const connection = {
    subscribe: () => subscription,
    getServer: () => 'mock:4222',
    status: async function* () {},
    isClosed: () => stopped,
    closed: () => disconnected.promise,
    close,
    drain: close,
  } as unknown as NatsConnection
  return { connection, close }
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
    expect(runtime.inspect().connection.state).toBe('closed')
  })

  it('still drains successfully when a handler finishes within the grace period', async () => {
    const network = transport()
    const runtime = createNatsRuntime({ connect: async () => network.connection })
    const lease = runtime.subscribe({ subject: 'work', codec: natsCodecs.text }, () => undefined)
    await lease.ready
    await network.close()
    await expect(runtime.close()).resolves.toBeUndefined()
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
    const network = transport()
    const runtime = createNatsRuntime({
      connect: async () => network.connection,
      shutdownTimeoutMs: 50,
    })
    const started = deferred<void>()
    const release = deferred<void>()
    const lease = runtime.subscribe({ subject: 'work', codec: natsCodecs.text }, async () => {
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
    } finally {
      release.resolve()
      await network.close()
      await lease.closed.catch(() => undefined)
      await closing
    }
  })
})
