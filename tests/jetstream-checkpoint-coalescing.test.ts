import { type Consumer, type JsMsg } from '@nats-io/jetstream'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { type CheckpointStore } from '@natsail/checkpoints'
import { natsCodecs, type NatsailScheduler } from '@natsail/core'
import {
  consumeJetStream,
  type JetStreamCheckpointCoalescing,
  type JetStreamDelivery,
} from '@natsail/jetstream'

import { fakeConnectionRuntime, messageSource } from './fixtures/fakes'

const jetStreamMocks = vi.hoisted(() => ({
  getConsumer: vi.fn(),
  streamInfo: vi.fn(),
}))

vi.mock('@nats-io/jetstream', async (importOriginal) => {
  const original = await importOriginal<typeof import('@nats-io/jetstream')>()

  return {
    ...original,
    jetstream: () => ({ consumers: { get: jetStreamMocks.getConsumer } }),
    jetstreamManager: () => ({ streams: { info: jetStreamMocks.streamInfo } }),
  }
})

const stream = 'CHAT'
const epoch = '2026-10-08T00:00:00.000Z'

function message(sequence: number): JsMsg {
  return {
    data: natsCodecs.text.encode(`token-${sequence}`),
    info: { stream, streamSequence: sequence, pending: 0 },
    redelivered: false,
  } as JsMsg
}

function arrangeConsumer(sequences: readonly number[], replayed: number, stayOpen = false) {
  const messages = messageSource(sequences.map(message), undefined, stayOpen)
  jetStreamMocks.getConsumer.mockResolvedValue({
    consume: vi.fn(async () => messages),
    info: vi.fn(async () => ({ num_pending: replayed })),
    delete: vi.fn(async () => true),
  } as unknown as Consumer)
}

function manualScheduler() {
  const tasks: Array<() => void> = []
  const scheduler: NatsailScheduler = {
    now: () => 0,
    schedule: (task) => {
      tasks.push(task)
      return { cancel: () => tasks.splice(tasks.indexOf(task), 1) }
    },
    yield: async () => undefined,
  }
  return { scheduler, tasks }
}

function consume(
  coalesce: JetStreamCheckpointCoalescing,
  handler: (delivery: JetStreamDelivery<string>) => void | Promise<void> = () => undefined
) {
  const saved: number[] = []
  const store: CheckpointStore = {
    load: async () => undefined,
    save: async (_key, checkpoint) => {
      saved.push(checkpoint.sequence)
    },
    clear: async () => undefined,
  }
  const runtime = fakeConnectionRuntime()
  const lease = consumeJetStream(
    runtime,
    {
      stream,
      filter: 'chat.>',
      start: 'all',
      resume: { key: 'conversation-42', store, coalesce },
      codec: natsCodecs.text,
    },
    handler
  )
  return { lease, runtime, saved }
}

describe('JetStream checkpoint coalescing', () => {
  beforeEach(() => {
    jetStreamMocks.getConsumer.mockReset()
    jetStreamMocks.streamInfo.mockReset()
    jetStreamMocks.streamInfo.mockResolvedValue({ created: epoch, state: { first_seq: 1 } })
  })

  it('saves every maxItems deliveries and the remainder when the consumer stops', async () => {
    arrangeConsumer([1, 2, 3, 4, 5], 0)
    const { lease, runtime, saved } = consume({ maxItems: 2 })

    await lease.closed

    expect(saved).toEqual([2, 4, 5])
    await runtime.close()
  })

  it('saves the newest cursor once the time window elapses', async () => {
    arrangeConsumer([1, 2, 3], 0, true)
    const { scheduler, tasks } = manualScheduler()
    const handled: number[] = []
    const { lease, runtime, saved } = consume({ maxWaitMs: 50, scheduler }, (delivery) => {
      handled.push(delivery.cursor.sequence)
    })

    await vi.waitFor(() => expect(handled).toEqual([1, 2, 3]))
    expect(saved).toEqual([])
    expect(tasks).toHaveLength(1)

    tasks[0]!()
    await vi.waitFor(() => expect(saved).toEqual([3]))

    await lease.close()
    expect(saved).toEqual([3])
    await runtime.close()
  })

  it('saves the replayed backlog before caughtUp resolves', async () => {
    arrangeConsumer([1, 2, 3], 3, true)
    const { lease, runtime, saved } = consume({ maxItems: 100 })

    await lease.caughtUp

    expect(saved).toEqual([3])
    await lease.close()
    await runtime.close()
  })

  it('saves handled progress when a later handler fails', async () => {
    arrangeConsumer([1, 2, 3], 0)
    const failure = new Error('render failed')
    const { lease, runtime, saved } = consume({ maxItems: 100 }, (delivery) => {
      if (delivery.cursor.sequence === 3) throw failure
    })

    await expect(lease.closed).rejects.toBe(failure)

    expect(saved).toEqual([2])
    await runtime.close()
  })
})
