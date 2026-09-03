import type { Consumer, ConsumerMessages, JsMsg } from '@nats-io/jetstream'
import type { NatsConnection } from '@nats-io/nats-core'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  createNatsRuntime,
  natsCodecs,
  type NatsailScheduledTask,
  type NatsailScheduler,
} from '@natsail/core'
import { createReducingJetStreamSessionSource } from '@natsail/jetstream'

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

class ManualScheduler implements NatsailScheduler {
  time = 0
  tasks: Array<{ at: number; cancelled: boolean; task: () => void }> = []
  now = () => this.time
  yield = async () => undefined
  schedule(task: () => void, delayMs: number): NatsailScheduledTask {
    const scheduled = { at: this.time + delayMs, cancelled: false, task }
    this.tasks.push(scheduled)
    return { cancel: () => (scheduled.cancelled = true) }
  }
  advance(ms: number): void {
    this.time += ms
    for (const scheduled of this.tasks.splice(0)) {
      if (!scheduled.cancelled && scheduled.at <= this.time) scheduled.task()
      else this.tasks.push(scheduled)
    }
  }
}

function controlledMessages() {
  const queued: JsMsg[] = []
  let wake = deferred()
  let closed = false
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
    closed: vi.fn(async () => undefined),
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
    pulled: () => pulled,
  }
}

function runtime() {
  const closed = deferred()
  const connection = {
    closed: () => closed.promise,
    drain: vi.fn(async () => closed.resolve()),
    getServer: () => 'mock:4222',
    isClosed: () => false,
    status: async function* () {},
  } as unknown as NatsConnection
  return createNatsRuntime({ connect: async () => connection })
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
    const active = runtime()
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
    const active = runtime()
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
    const lease = source(async (snapshot) => snapshots.push([...snapshot.data]))
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
    const active = runtime()
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
    const active = runtime()
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
    const active = runtime()
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
    const lease = source(async (snapshot) => snapshots.push([...snapshot.data]))

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
    const active = runtime()
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
    const lease = source(async (snapshot) => snapshots.push([...snapshot.data]))
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
})
