import type { NatsConnection, Status } from '@nats-io/nats-core'
import { describe, expect, it, vi } from 'vitest'

import { createNatsRuntime, natsCodecs, type NatsRuntimeEvent } from '@natsail/core'
import { deferred, transport } from './fixtures/lifecycle'

function runtimeWithStatuses(statuses: readonly Status[], failure?: Error) {
  const closed = deferred<void>()
  const connection = {
    closed: () => closed.promise,
    drain: vi.fn(async () => closed.resolve()),
    getServer: () => 'mock:4222',
    isClosed: () => false,
    status: async function* () {
      yield* statuses
      if (failure) throw failure
      await closed.promise
    },
  } as unknown as NatsConnection
  const runtime = createNatsRuntime({ connect: async () => connection })
  const events: NatsRuntimeEvent[] = []
  void (async () => {
    for await (const event of runtime.events) events.push(event)
  })()
  return { runtime, events }
}

const connectionError = new Error('authorization violation')

describe('connection status diagnostics', () => {
  it.each([
    [
      { type: 'error', error: connectionError },
      { code: 'connection-error', level: 'error', message: 'authorization violation' },
    ],
    [{ type: 'staleConnection' }, { code: 'stale-connection', level: 'warning' }],
    [
      { type: 'ldm', server: 'nats://a:4222' },
      { code: 'lame-duck-mode', level: 'warning', details: { server: 'nats://a:4222' } },
    ],
    [
      { type: 'update', added: ['nats://b:4222'], deleted: [] },
      { code: 'cluster-update', level: 'info', details: { added: ['nats://b:4222'], deleted: [] } },
    ],
    [
      { type: 'update' },
      { code: 'cluster-update', level: 'info', details: { added: [], deleted: [] } },
    ],
    [
      { type: 'ping', pendingPings: 0 },
      { code: 'client-ping', level: 'info', details: { pendingPings: 0 } },
    ],
    [
      { type: 'ping', pendingPings: 2 },
      { code: 'client-ping', level: 'warning', details: { pendingPings: 2 } },
    ],
  ] as const)('maps %j to a structured diagnostic', async (status, expected) => {
    const { runtime, events } = runtimeWithStatuses([status as unknown as Status])
    await runtime.connection()

    await vi.waitFor(() =>
      expect(events).toContainEqual(
        expect.objectContaining({ type: 'diagnostic', source: 'connection', ...expected })
      )
    )
    await runtime.close()
  })

  it('reports a failed status stream as an error diagnostic', async () => {
    const failure = new Error('status stream broke')
    const { runtime, events } = runtimeWithStatuses([], failure)
    await runtime.connection()

    await vi.waitFor(() =>
      expect(events).toContainEqual(
        expect.objectContaining({
          type: 'diagnostic',
          code: 'status-stream-failed',
          level: 'error',
          error: failure,
        })
      )
    )
    await runtime.close()
  })
})

describe('Core subscription failures', () => {
  it('rejects closed and frees the resource when the handler throws', async () => {
    const runtime = createNatsRuntime({ connect: async () => transport(['payload']).connection })

    const lease = runtime.subscribe({ subject: 'work', codec: natsCodecs.text }, () => {
      throw new Error('handler failed')
    })

    await expect(lease.closed).rejects.toThrow('handler failed')
    await vi.waitFor(() => expect(runtime.inspect().activeResources).toBe(0))
    await runtime.close()
  })

  it('rejects closed without calling the handler when the decoder rejects', async () => {
    const runtime = createNatsRuntime({ connect: async () => transport(['payload']).connection })
    const handler = vi.fn()

    const lease = runtime.subscribe(
      {
        subject: 'work',
        decode: (): string => {
          throw new Error('decode failed')
        },
      },
      handler
    )

    await expect(lease.closed).rejects.toThrow('decode failed')
    expect(handler).not.toHaveBeenCalled()
    await vi.waitFor(() => expect(runtime.inspect().activeResources).toBe(0))
    await runtime.close()
  })

  it('rejects ready and closed when the connection cannot start', async () => {
    const failure = new Error('connect failed')
    const runtime = createNatsRuntime({ connect: async () => Promise.reject(failure) })

    const lease = runtime.subscribe({ subject: 'work', decode: () => 'value' }, () => undefined)

    await expect(lease.ready).rejects.toBe(failure)
    await expect(lease.closed).rejects.toBe(failure)
    await runtime.close().catch(() => undefined)
  })

  it.each([
    ['joins the queue group', 'workers', ['work', { queue: 'workers' }]],
    ['subscribes without a queue group', undefined, ['work']],
  ] as const)('%s', async (_name, queue, expectedArguments) => {
    const network = transport([])
    const subscribe = vi.spyOn(network.connection, 'subscribe')
    const runtime = createNatsRuntime({ connect: async () => network.connection })

    const lease = runtime.subscribe(
      { subject: 'work', decode: () => 'value', ...(queue ? { queue } : {}) },
      () => undefined
    )
    await lease.ready

    expect(subscribe.mock.calls[0]).toEqual(expectedArguments)
    await runtime.close()
  })
})
