import type { Consumer, ConsumerMessages, JsMsg } from '@nats-io/jetstream'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { natsCodecs } from '@natsail/core'
import {
  createReducingJetStreamSessionSource,
  defineReducingJetStreamSession,
  JetStreamCatchUpCancelledError,
} from '@natsail/jetstream'
import { createSessionRegistry, SessionContractMismatchError } from '@natsail/session'

import { fakeConnectionRuntime, ManualScheduler } from './fixtures/fakes'

const mocks = vi.hoisted(() => ({
  getConsumer: vi.fn(),
  checkpointLoad: vi.fn(),
  checkpointSave: vi.fn(),
}))

vi.mock('@natsail/checkpoints', async (importOriginal) => {
  const original = await importOriginal<typeof import('@natsail/checkpoints')>()
  return {
    ...original,
    createMemoryCheckpointStore: () => ({
      load: mocks.checkpointLoad,
      save: mocks.checkpointSave,
      clear: vi.fn(async () => undefined),
    }),
  }
})

vi.mock('@nats-io/jetstream', async (importOriginal) => {
  const original = await importOriginal<typeof import('@nats-io/jetstream')>()
  return {
    ...original,
    jetstream: () => ({ consumers: { get: mocks.getConsumer } }),
    jetstreamManager: () => ({
      streams: { info: async () => ({ created: 'epoch', state: { first_seq: 1 } }) },
    }),
  }
})

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((done) => (resolve = done))
  return { promise, resolve }
}

function controlledMessages() {
  const queued: JsMsg[] = []
  let wake = deferred()
  let closed = false
  let closedError: Error | undefined
  let pulled = 0
  const messages = {
    async *[Symbol.asyncIterator]() {
      while (!closed) {
        if (queued.length === 0) await wake.promise
        if (closed) break
        const message = queued.shift()
        if (message) {
          pulled += 1
          yield message
        }
      }
    },
    close: vi.fn(async () => {
      closed = true
      wake.resolve()
    }),
    closed: vi.fn(async () => closedError),
    status: async function* () {},
  } as unknown as ConsumerMessages
  return {
    messages,
    push(sequence: number) {
      queued.push({
        data: natsCodecs.text.encode(String(sequence)),
        subject: 'events.one',
        info: { stream: 'EVENTS', streamSequence: sequence, pending: 0 },
        redelivered: false,
      } as JsMsg)
      wake.resolve()
      wake = deferred()
    },
    fail(error: Error) {
      closedError = error
      closed = true
      wake.resolve()
    },
    pulled: () => pulled,
  }
}

async function turn(): Promise<void> {
  for (let index = 0; index < 64; index += 1) await Promise.resolve()
}

describe('reducing JetStream batch barriers', () => {
  beforeEach(() => {
    mocks.getConsumer.mockReset()
    mocks.checkpointLoad.mockReset().mockResolvedValue(undefined)
    mocks.checkpointSave.mockReset().mockResolvedValue(undefined)
  })

  it('bounds intake to one applying batch and commits cursors in batch order', async () => {
    const controlled = controlledMessages()
    mocks.getConsumer.mockResolvedValue({
      consume: async () => controlled.messages,
      info: async () => ({ num_pending: 0 }),
      delete: async () => true,
    } as unknown as Consumer)
    const active = fakeConnectionRuntime()
    const firstApplication = deferred()
    const snapshots: number[][] = []
    const source = createReducingJetStreamSessionSource(
      active,
      {
        stream: 'EVENTS',
        filter: 'events.>',
        start: 'all',
        codec: natsCodecs.text,
        batchPolicy: { maxItems: 2 },
      },
      {
        scope: 'numbers:v1',
        initial: () => [] as number[],
        reduce: (state, delivery) => [...state, Number(delivery.value)],
      }
    )
    const lease = source(async (snapshot) => {
      if (snapshot.data.length === 2) await firstApplication.promise
      snapshots.push([...snapshot.data])
    })
    await lease.ready

    controlled.push(1)
    controlled.push(2)
    controlled.push(3)
    controlled.push(4)
    await turn()
    expect(controlled.pulled()).toBe(2)
    expect(lease.inspect().cursor).toBeUndefined()

    firstApplication.resolve()
    await turn()
    expect(controlled.pulled()).toBe(4)
    expect(lease.inspect().cursor?.sequence).toBe(4)
    expect(snapshots.at(-1)).toEqual([1, 2, 3, 4])

    await lease.close()
    await active.close()
  })

  it('continues byte-only intake after preflushing the prior bounded batch', async () => {
    const controlled = controlledMessages()
    mocks.getConsumer.mockResolvedValue({
      consume: async () => controlled.messages,
      info: async () => ({ num_pending: 0 }),
      delete: async () => true,
    } as unknown as Consumer)
    const active = fakeConnectionRuntime()
    const snapshots: number[][] = []
    const source = createReducingJetStreamSessionSource(
      active,
      {
        stream: 'EVENTS',
        filter: 'events.>',
        start: 'all',
        codec: natsCodecs.text,
        batchPolicy: { maxBytes: 3, sizeOf: () => 2 },
        liveBatchMs: 0,
      },
      {
        scope: 'numbers:v1',
        initial: () => [] as number[],
        reduce: (state, delivery) => [...state, Number(delivery.value)],
      }
    )
    const lease = source(async (snapshot) => {
      snapshots.push([...snapshot.data])
    })
    await lease.ready
    controlled.push(1)
    controlled.push(2)
    controlled.push(3)
    await turn()

    expect(controlled.pulled()).toBe(3)
    expect(lease.inspect().cursor?.sequence).toBe(2)
    expect(snapshots.at(-1)).toEqual([1, 2])
    await lease.close()
    await active.close()
  })

  it('lets an in-flight full batch finish but discards a partial batch on close', async () => {
    const controlled = controlledMessages()
    mocks.getConsumer.mockResolvedValue({
      consume: async () => controlled.messages,
      info: async () => ({ num_pending: 0 }),
      delete: async () => true,
    } as unknown as Consumer)
    const active = fakeConnectionRuntime()
    const scheduler = new ManualScheduler()
    const firstApplication = deferred()
    const snapshots: number[][] = []
    const source = createReducingJetStreamSessionSource(
      active,
      {
        stream: 'EVENTS',
        filter: 'events.>',
        start: 'all',
        codec: natsCodecs.text,
        batchPolicy: { maxItems: 2, maxWaitMs: 5 },
        scheduler,
      },
      {
        scope: 'numbers:v1',
        initial: () => [] as number[],
        reduce: (state, delivery) => [...state, Number(delivery.value)],
      }
    )
    const lease = source(async (snapshot) => {
      if (snapshot.data.length === 1) await firstApplication.promise
      snapshots.push([...snapshot.data])
    })
    await lease.ready
    controlled.push(1)
    await turn()
    scheduler.advance(5)
    await turn()
    controlled.push(2)
    await turn()
    expect(controlled.pulled()).toBe(2)
    const closing = lease.close()
    firstApplication.resolve()
    await closing

    expect(snapshots.at(-1)).toEqual([1])
    expect(lease.inspect().cursor?.sequence).toBe(1)
    await active.close()
  })

  it('does not resolve close while a timed partial batch is already applying', async () => {
    const controlled = controlledMessages()
    mocks.getConsumer.mockResolvedValue({
      consume: async () => controlled.messages,
      info: async () => ({ num_pending: 0 }),
      delete: async () => true,
    } as unknown as Consumer)
    const active = fakeConnectionRuntime()
    const scheduler = new ManualScheduler()
    const application = deferred()
    const snapshots: number[][] = []
    const source = createReducingJetStreamSessionSource(
      active,
      {
        stream: 'EVENTS',
        filter: 'events.>',
        start: 'all',
        codec: natsCodecs.text,
        batchPolicy: { maxItems: 10, maxWaitMs: 5 },
        scheduler,
      },
      {
        scope: 'numbers:v1',
        initial: () => [] as number[],
        reduce: (state, delivery) => [...state, Number(delivery.value)],
      }
    )
    const lease = source(async (snapshot) => {
      if (snapshot.data.length === 1) await application.promise
      snapshots.push([...snapshot.data])
    })
    await lease.ready
    controlled.push(1)
    await turn()
    scheduler.advance(5)
    await turn()
    let closed = false
    const closing = lease.close().then(() => (closed = true))
    await turn()
    expect(closed).toBe(false)

    application.resolve()
    await closing
    expect(snapshots.at(-1)).toEqual([1])
    expect(lease.inspect().cursor?.sequence).toBe(1)
    await active.close()
  })

  it('does not publish or advance a cursor past a failed reducer batch', async () => {
    const controlled = controlledMessages()
    controlled.push(1)
    controlled.push(2)
    mocks.getConsumer.mockResolvedValue({
      consume: async () => controlled.messages,
      info: async () => ({ num_pending: 2 }),
      delete: async () => true,
    } as unknown as Consumer)
    const active = fakeConnectionRuntime()
    const snapshots: number[][] = []
    const source = createReducingJetStreamSessionSource(
      active,
      {
        stream: 'EVENTS',
        filter: 'events.>',
        start: 'all',
        codec: natsCodecs.text,
        batchPolicy: { maxItems: 2 },
      },
      {
        scope: 'numbers:v1',
        initial: () => [] as number[],
        reduce: (state, delivery) => {
          if (delivery.value === '2') throw new Error('cannot reduce two')
          return [...state, Number(delivery.value)]
        },
      }
    )
    const lease = source(async (snapshot) => {
      snapshots.push([...snapshot.data])
    })

    await expect(lease.ready).rejects.toThrow('cannot reduce two')
    await expect(lease.closed).rejects.toThrow('cannot reduce two')
    expect(lease.inspect().cursor).toBeUndefined()
    expect(snapshots).toEqual([[]])
    await active.close()
  })

  it('does not admit another batch after a downstream checkpoint commit fails', async () => {
    const controlled = controlledMessages()
    mocks.getConsumer.mockResolvedValue({
      consume: async () => controlled.messages,
      info: async () => ({ num_pending: 0 }),
      delete: async () => true,
    } as unknown as Consumer)
    mocks.checkpointSave.mockRejectedValueOnce(new Error('checkpoint unavailable'))
    const active = fakeConnectionRuntime()
    const reduced: number[] = []
    const snapshots: number[][] = []
    const source = createReducingJetStreamSessionSource(
      active,
      {
        stream: 'EVENTS',
        filter: 'events.>',
        start: 'all',
        codec: natsCodecs.text,
        batchPolicy: { maxItems: 1 },
      },
      {
        scope: 'numbers:v1',
        initial: () => [] as number[],
        reduce: (state, delivery) => {
          reduced.push(Number(delivery.value))
          return [...state, Number(delivery.value)]
        },
      }
    )
    const lease = source(async (snapshot) => {
      snapshots.push([...snapshot.data])
    })
    await lease.ready
    controlled.push(1)
    controlled.push(2)

    await expect(lease.closed).rejects.toThrow('checkpoint unavailable')
    expect(reduced).toEqual([1])
    expect(snapshots.at(-1)).toEqual([1])
    expect(lease.inspect().cursor).toBeUndefined()
    expect(mocks.checkpointSave).toHaveBeenCalledOnce()
    await active.close()
  })

  it('creates a fresh batcher when an infrastructure failure is recovered', async () => {
    const first = controlledMessages()
    const second = controlledMessages()
    mocks.getConsumer
      .mockResolvedValueOnce({
        consume: async () => first.messages,
        info: async () => ({ num_pending: 0 }),
        delete: async () => true,
      } as unknown as Consumer)
      .mockResolvedValueOnce({
        consume: async () => second.messages,
        info: async () => ({ num_pending: 0 }),
        delete: async () => true,
      } as unknown as Consumer)
    const active = fakeConnectionRuntime()
    const snapshots: number[][] = []
    const source = createReducingJetStreamSessionSource(
      active,
      {
        stream: 'EVENTS',
        filter: 'events.>',
        start: 'all',
        codec: natsCodecs.text,
        recovery: { maxAttempts: 2, delayMs: 0 },
        batchPolicy: { maxItems: 1 },
      },
      {
        scope: 'numbers:v1',
        initial: () => [] as number[],
        reduce: (state, delivery) => [...state, Number(delivery.value)],
      }
    )
    const lease = source(async (snapshot) => {
      snapshots.push([...snapshot.data])
    })
    await lease.ready

    first.push(1)
    await vi.waitFor(() => expect(snapshots.at(-1)).toEqual([1]))
    first.fail(new Error('consumer transport failed'))
    await vi.waitFor(() => expect(lease.inspect().restarts).toBe(1))
    await vi.waitFor(() => expect(mocks.getConsumer).toHaveBeenCalledTimes(2))
    second.push(2)
    await vi.waitFor(() => expect(snapshots.at(-1)).toEqual([1, 2]))

    await lease.close()
    await active.close()
  })
})

describe('defineReducingJetStreamSession', () => {
  const reducer = {
    scope: 'numbers:v1',
    initial: () => [] as number[],
    reduce: (state: number[], delivery: { value: string }) => [...state, Number(delivery.value)],
  }
  const streamOptions = {
    stream: 'EVENTS',
    filter: 'events.>',
    start: 'all' as const,
    codec: natsCodecs.text,
  }

  function arrangeConsumer(controlled: ReturnType<typeof controlledMessages>, pending = 0) {
    mocks.getConsumer.mockReset().mockResolvedValue({
      consume: async () => controlled.messages,
      info: async () => ({ num_pending: pending }),
      delete: async () => true,
    } as unknown as Consumer)
  }

  it('shares one consumer and one reduced state across handles of the same definition', async () => {
    const controlled = controlledMessages()
    arrangeConsumer(controlled)
    const active = fakeConnectionRuntime()
    const registry = createSessionRegistry()
    const definition = () => defineReducingJetStreamSession(active, 'numbers', streamOptions, reducer)
    const first = registry.acquire(definition())
    const second = registry.acquire(definition())
    await first.ready

    controlled.push(1)
    controlled.push(2)

    await vi.waitFor(() => expect(second.getSnapshot().value?.data).toEqual([1, 2]))
    expect(first.getSnapshot().value?.data).toEqual([1, 2])
    expect(mocks.getConsumer).toHaveBeenCalledOnce()
    await first.release()
    await second.release()
    await registry.close()
    await active.close()
  })

  it.each([
    ['reducer scope', { ...reducer, scope: 'numbers:v2' }, streamOptions],
    ['batch policy', reducer, { ...streamOptions, batchPolicy: { maxItems: 3 } }],
  ])('rejects reusing one key with a different %s', async (_name, otherReducer, otherOptions) => {
    const controlled = controlledMessages()
    arrangeConsumer(controlled)
    const active = fakeConnectionRuntime()
    const registry = createSessionRegistry()
    const first = registry.acquire(
      defineReducingJetStreamSession(active, 'numbers', streamOptions, reducer)
    )

    expect(() =>
      registry.acquire(defineReducingJetStreamSession(active, 'numbers', otherOptions, otherReducer))
    ).toThrow(SessionContractMismatchError)

    await first.release()
    await registry.close()
    await active.close()
  })

  it('refuses an event-cursor resume because reduced state is not persisted with it', () => {
    const active = fakeConnectionRuntime()
    const store = { load: vi.fn(), save: vi.fn(), clear: vi.fn() }

    expect(() =>
      defineReducingJetStreamSession(
        active,
        'numbers',
        { ...streamOptions, resume: { key: 'numbers', store } } as never,
        reducer
      )
    ).toThrow('cannot resume')
  })

  it('rejects caughtUp when the lease closes before replay catches up', async () => {
    const controlled = controlledMessages()
    arrangeConsumer(controlled, 5)
    const active = fakeConnectionRuntime()
    const lease = createReducingJetStreamSessionSource(active, streamOptions, reducer)(
      async () => undefined
    )
    await vi.waitFor(() => expect(lease.inspect().phase).toBe('replaying'))

    const caughtUp = expect(lease.caughtUp).rejects.toBeInstanceOf(JetStreamCatchUpCancelledError)
    await lease.close()

    await caughtUp
    await active.close()
  })
})
