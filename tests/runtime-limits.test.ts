import { describe, expect, it, vi } from 'vitest'

import {
  createNatsRuntime,
  NATS_RUNTIME_ADAPTER,
  NatsRuntimeLimitError,
  type NatsRuntimeEvent,
  type RuntimeResource,
  type RuntimeResourceAllocation,
} from '@natsail/core'
import { deferred, transport } from './fixtures/lifecycle'

function arrange(limits: { [name: string]: number }) {
  const runtime = createNatsRuntime({
    connect: async () => transport().connection,
    limits,
  })
  const events: NatsRuntimeEvent[] = []
  void (async () => {
    for await (const event of runtime.events) events.push(event)
  })()
  const manage = <T extends RuntimeResource>(
    create: () => T,
    allocation?: RuntimeResourceAllocation
  ) => runtime[NATS_RUNTIME_ADAPTER].manage(create, allocation)
  return { runtime, events, manage }
}

function resource() {
  const closed = deferred<void>()
  const value: RuntimeResource = { closed: closed.promise, close: vi.fn(async () => closed.resolve()) }
  return value
}

describe('runtime capacity limits', () => {
  it.each([
    ['maxJetStreamConsumers', 'jetStreamConsumers', 'jetstream-consumers', 'usedJetStreamConsumers'],
    ['maxBufferedMessages', 'bufferedMessages', 'buffered-messages', 'usedBufferedMessages'],
    ['maxBufferedBytes', 'bufferedBytes', 'buffered-bytes', 'usedBufferedBytes'],
  ] as const)(
    'rejects over-limit %s, reports it, and admits again after release',
    async (limitName, allocationName, code, usedName) => {
      const { runtime, events, manage } = arrange({ [limitName]: 3 })
      const first = manage(resource, { [allocationName]: 2 })
      const create = vi.fn(resource)

      let rejection: unknown
      try {
        manage(create, { [allocationName]: 2 })
      } catch (error) {
        rejection = error
      }

      expect(rejection).toBeInstanceOf(NatsRuntimeLimitError)
      expect(rejection).toMatchObject({ code, limit: 3, used: 2, requested: 2 })
      expect(create).not.toHaveBeenCalled()
      expect(runtime.inspect()).toMatchObject({ activeResources: 1, [usedName]: 2 })
      await vi.waitFor(() =>
        expect(events).toContainEqual(
          expect.objectContaining({
            type: 'diagnostic',
            code: 'resource-limit-exceeded',
            level: 'warning',
            details: { resource: code, limit: 3, used: 2, requested: 2 },
          })
        )
      )

      await first.close()
      await vi.waitFor(() => expect(runtime.inspect()).toMatchObject({ [usedName]: 0 }))
      expect(() => manage(resource, { [allocationName]: 3 })).not.toThrow()
      await runtime.close()
    }
  )

  it('releases the reservation when resource creation throws', async () => {
    const { runtime, manage } = arrange({ maxBufferedMessages: 1 })
    const failure = new Error('create failed')

    expect(() =>
      manage(
        () => {
          throw failure
        },
        { bufferedMessages: 1 }
      )
    ).toThrow(failure)

    expect(runtime.inspect()).toMatchObject({ activeResources: 0, usedBufferedMessages: 0 })
    expect(() => manage(resource, { bufferedMessages: 1 })).not.toThrow()
    await runtime.close()
  })

  it.each([-1, 1.5, Number.NaN])('rejects invalid allocation %s without reserving', async (value) => {
    const { runtime, manage } = arrange({ maxBufferedBytes: 10 })
    const create = vi.fn(resource)

    expect(() => manage(create, { bufferedBytes: value })).toThrow(RangeError)

    expect(create).not.toHaveBeenCalled()
    expect(runtime.inspect().usedBufferedBytes).toBe(0)
    await runtime.close()
  })

  it.each([-1, 0.5])('rejects invalid limit %s at construction', (limit) => {
    expect(() =>
      createNatsRuntime({
        connect: async () => transport().connection,
        limits: { maxBufferedBytes: limit },
      })
    ).toThrow('maxBufferedBytes')
  })
})
