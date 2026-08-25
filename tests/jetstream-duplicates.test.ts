import type { Consumer, ConsumerMessages, JsMsg } from '@nats-io/jetstream'
import type { NatsConnection } from '@nats-io/nats-core'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { createMemoryCheckpointStore, type CheckpointStore } from '@natsail/checkpoints'
import { createNatsRuntime, natsCodecs } from '@natsail/core'
import {
  createReducingJetStreamSessionSource,
  createJetStreamSessionSource,
  consumeJetStream,
  defineJetStreamSession,
  JetStreamDuplicateError,
  JetStreamResumeError,
  type JetStreamDelivery,
  type JetStreamDuplicateDeliveryPolicy,
  type JetStreamSubscriptionOptions,
} from '@natsail/jetstream'

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

const stream = 'EVENTS'
const epoch = '2026-08-21T00:00:00.000Z'
function message(sequence: number, value: string, redelivered = false, pending = 0): JsMsg {
  return {
    data: natsCodecs.text.encode(value),
    info: { stream, streamSequence: sequence, pending },
    redelivered,
  } as JsMsg
}

function messageSource(
  deliveries: readonly JsMsg[],
  closedError?: Error,
  stayOpen = false
): ConsumerMessages {
  let closeRequested = false
  let finish!: () => void
  const closeSignal = new Promise<void>((resolve) => {
    finish = resolve
  })

  return {
    async *[Symbol.asyncIterator]() {
      for (const delivery of deliveries) {
        if (closeRequested) break
        yield delivery
      }
      if (stayOpen && !closeRequested) await closeSignal
    },
    close: vi.fn(async () => {
      closeRequested = true
      finish()
    }),
    closed: vi.fn(async () => closedError),
  } as unknown as ConsumerMessages
}

function createConsumer(deliveries: readonly JsMsg[], closedError?: Error, stayOpen = false) {
  const messages = messageSource(deliveries, closedError, stayOpen)
  const consumer = {
    consume: vi.fn(async () => messages),
    info: vi.fn(async () => ({ num_pending: deliveries.length })),
    delete: vi.fn(async () => true),
  } as unknown as Consumer
  return { consumer, messages }
}

function arrangeConsumer(deliveries: readonly JsMsg[]) {
  const { consumer, messages } = createConsumer(deliveries)
  jetStreamMocks.getConsumer.mockResolvedValue(consumer)
  return { consumer, messages }
}

function arrangeRuntime() {
  let resolveClosed!: () => void
  const closed = new Promise<void>((resolve) => {
    resolveClosed = resolve
  })
  const connection = {
    closed: () => closed,
    drain: vi.fn(async () => resolveClosed()),
    getServer: vi.fn(() => 'mock:4222'),
    isClosed: vi.fn(() => false),
    status: async function* () {},
  } as unknown as NatsConnection
  return createNatsRuntime({ connect: async () => connection })
}

function arrangeStore() {
  const save = vi.fn<CheckpointStore['save']>(async () => undefined)
  const store: CheckpointStore = {
    load: async () => ({ stream, epoch, sequence: 2 }),
    save,
    clear: async () => undefined,
  }
  return { save, store }
}

function consumeWithPolicy(
  policy: JetStreamDuplicateDeliveryPolicy | undefined,
  handler: (delivery: JetStreamDelivery<string>) => void | Promise<void>
) {
  const runtime = arrangeRuntime()
  const { save, store } = arrangeStore()
  const lease = consumeJetStream(
    runtime,
    {
      stream,
      filter: 'events.>',
      start: 'all',
      ...(policy === undefined ? {} : { duplicateDeliveryPolicy: policy }),
      resume: { key: 'conversation:one', store },
      codec: natsCodecs.text,
    },
    handler
  )
  return { lease, runtime, save }
}

describe('JetStream duplicate-delivery policy', () => {
  beforeEach(() => {
    jetStreamMocks.getConsumer.mockReset()
    jetStreamMocks.streamInfo.mockReset()
    jetStreamMocks.streamInfo.mockResolvedValue({
      created: epoch,
      state: { first_seq: 1 },
    })
  })

  it('drops already-committed sequences by default', async () => {
    arrangeConsumer([message(2, 'duplicate', true), message(3, 'next')])
    const received: Array<JetStreamDelivery<string>> = []
    const { lease, runtime, save } = consumeWithPolicy(undefined, (delivery) => {
      received.push(delivery)
    })

    await lease.closed

    expect(received).toMatchObject([
      {
        value: 'next',
        cursor: { stream, epoch, sequence: 3 },
        duplicate: false,
        redelivered: false,
      },
    ])
    expect(save).toHaveBeenCalledOnce()
    expect(save).toHaveBeenCalledWith('conversation:one', {
      stream,
      epoch,
      sequence: 3,
      scope: '["events.>"]',
    })
    await runtime.close()
  })

  it('marks the captured backlog and resolves caughtUp after its final accepted delivery', async () => {
    arrangeConsumer([message(1, 'one', false, 1), message(2, 'two')])
    const runtime = arrangeRuntime()
    const deliveries: Array<JetStreamDelivery<string>> = []
    const lease = consumeJetStream(
      runtime,
      {
        stream,
        filter: 'events.>',
        start: 'all',
        codec: natsCodecs.text,
      },
      (delivery) => {
        deliveries.push(delivery)
      }
    )

    await expect(lease.caughtUp).resolves.toEqual({
      cursor: { stream, sequence: 2 },
      delivered: 2,
    })
    expect(deliveries).toMatchObject([
      { value: 'one', replay: 'initial', consumerPending: 1 },
      { value: 'two', replay: 'initial', consumerPending: 0 },
    ])
    await runtime.close()
  })

  it('publishes one atomic reduced state after replay instead of every historical delivery', async () => {
    arrangeConsumer([message(1, 'one', false, 1), message(2, 'two')])
    const runtime = arrangeRuntime()
    const snapshots: Array<{ phase: string; data: string[] }> = []
    const source = createReducingJetStreamSessionSource(
      runtime,
      {
        stream,
        filter: 'events.>',
        start: 'all',
        codec: natsCodecs.text,
        recovery: { maxAttempts: 2, delayMs: 0 },
      },
      {
        scope: 'messages:v1',
        initial: () => [] as string[],
        reduce: (values, delivery) => [...values, delivery.value],
      }
    )
    const lease = source(async (snapshot) => {
      snapshots.push({ phase: snapshot.phase, data: [...snapshot.data] })
    })

    await lease.ready
    expect(snapshots.slice(0, 2)).toEqual([
      { phase: 'replaying', data: [] },
      { phase: 'live', data: ['one', 'two'] },
    ])
    await lease.close()
    await runtime.close()
  })

  it('rejects an event cursor without matching materialized reducer state', () => {
    const runtime = arrangeRuntime()
    expect(() =>
      createReducingJetStreamSessionSource(
        runtime,
        {
          stream,
          filter: 'events.>',
          start: 'all',
          codec: natsCodecs.text,
          resume: { key: 'unsafe-reducer', store: createMemoryCheckpointStore() },
        },
        {
          scope: 'messages:v1',
          initial: () => [] as string[],
          reduce: (values, delivery) => [...values, delivery.value],
        }
      )
    ).toThrow(/materialized state/)
  })

  it('requires a contract scope for custom recovery functions', () => {
    const runtime = arrangeRuntime()
    expect(() =>
      defineJetStreamSession(runtime, 'events:custom-retry', {
        stream,
        filter: 'events.>',
        start: 'all',
        codec: natsCodecs.text,
        recovery: { shouldRetry: () => true },
      })
    ).toThrow(/recovery scope/)
  })

  it('restarts a failed session and resumes from its package-owned in-memory cursor', async () => {
    const first = createConsumer([message(1, 'one')], new Error('consumer failed'))
    const second = createConsumer([message(2, 'two')], undefined, true)
    jetStreamMocks.getConsumer
      .mockResolvedValueOnce(first.consumer)
      .mockResolvedValueOnce(second.consumer)
    const runtime = arrangeRuntime()
    const values: string[] = []
    const source = createJetStreamSessionSource(runtime, {
      stream,
      filter: 'events.>',
      start: 'all',
      codec: natsCodecs.text,
      recovery: { maxAttempts: 2, delayMs: 0 },
    })
    const lease = source(async (delivery) => {
      values.push(delivery.value)
    })

    await vi.waitFor(() => expect(jetStreamMocks.getConsumer).toHaveBeenCalledTimes(2))
    await vi.waitFor(() => expect(values).toEqual(['one', 'two']))
    expect(lease.inspect()).toMatchObject({ restarts: 1 })
    await lease.close()
    await runtime.close()
  })

  it('can deliver a duplicate without regressing the checkpoint', async () => {
    arrangeConsumer([message(3, 'next'), message(3, 'duplicate', true)])
    const received: Array<JetStreamDelivery<string>> = []
    const { lease, runtime, save } = consumeWithPolicy('deliver', (delivery) => {
      received.push(delivery)
    })

    await lease.closed

    expect(received).toMatchObject([
      {
        value: 'next',
        cursor: { stream, epoch, sequence: 3 },
        duplicate: false,
        redelivered: false,
      },
      {
        value: 'duplicate',
        cursor: { stream, epoch, sequence: 3 },
        duplicate: true,
        redelivered: true,
      },
    ])
    expect(save).toHaveBeenCalledOnce()
    expect(save).toHaveBeenCalledWith('conversation:one', {
      stream,
      epoch,
      sequence: 3,
      scope: '["events.>"]',
    })
    await runtime.close()
  })

  it('can stop on a duplicate with sequence details', async () => {
    const { consumer } = arrangeConsumer([message(2, 'duplicate', true), message(3, 'next')])
    const handler = vi.fn()
    const { lease, runtime, save } = consumeWithPolicy('error', handler)

    await lease.ready
    await expect(lease.closed).rejects.toEqual(
      expect.objectContaining<Partial<JetStreamDuplicateError>>({
        name: 'JetStreamDuplicateError',
        sequence: 2,
        committedSequence: 2,
      })
    )

    expect(handler).not.toHaveBeenCalled()
    expect(save).not.toHaveBeenCalled()
    expect(consumer.delete).toHaveBeenCalledOnce()
    await runtime.close()
  })

  it('rejects a checkpoint created for another logical source', async () => {
    arrangeConsumer([])
    const runtime = arrangeRuntime()
    const store: CheckpointStore = {
      load: async () => ({
        stream,
        epoch,
        sequence: 2,
        scope: '["events.other"]:decoder-v1',
      }),
      save: async () => undefined,
      clear: async () => undefined,
    }
    const lease = consumeJetStream(
      runtime,
      {
        stream,
        filter: 'events.>',
        start: 'all',
        resume: { key: 'conversation:one', store, scope: 'decoder-v1' },
        codec: natsCodecs.text,
      },
      () => undefined
    )

    const expectedError = expect.objectContaining<Partial<JetStreamResumeError>>({
      code: 'checkpoint-scope-mismatch',
      checkpointSequence: 2,
    })
    await Promise.all([
      expect(lease.ready).rejects.toEqual(expectedError),
      expect(lease.closed).rejects.toEqual(expectedError),
    ])
    await runtime.close()
  })

  it('bounds pull buffers by bytes and reports the reserved capacity', async () => {
    const { consumer } = arrangeConsumer([])
    const runtime = arrangeRuntime()
    const lease = consumeJetStream(
      runtime,
      {
        stream,
        filter: 'events.>',
        start: 'new',
        maxBufferedBytes: 4_096,
        codec: natsCodecs.text,
      },
      () => undefined
    )

    await lease.ready
    expect(consumer.consume).toHaveBeenCalledWith({ max_bytes: 4_096 })
    expect(runtime.inspect()).toEqual(
      expect.objectContaining({
        activeResources: 1,
        usedJetStreamConsumers: 1,
        usedBufferedMessages: 0,
        usedBufferedBytes: 4_096,
      })
    )
    await lease.closed
    expect(runtime.inspect()).toEqual(
      expect.objectContaining({ activeResources: 0, usedBufferedBytes: 0 })
    )
    await runtime.close()
  })

  it('rejects simultaneous message and byte buffer modes', () => {
    const runtime = arrangeRuntime()

    expect(() =>
      consumeJetStream(
        runtime,
        {
          stream,
          filter: 'events.>',
          start: 'new',
          maxBufferedMessages: 32,
          maxBufferedBytes: 4_096,
          codec: natsCodecs.text,
        } as unknown as JetStreamSubscriptionOptions<string>,
        () => undefined
      )
    ).toThrow('mutually exclusive')
  })

  it('adapts one JetStream consumer into a shareable session source', async () => {
    arrangeConsumer([message(1, 'one')])
    const runtime = arrangeRuntime()
    const source = createJetStreamSessionSource(runtime, {
      stream,
      filter: 'events.>',
      start: 'all',
      codec: natsCodecs.text,
    })
    const accepted: Array<JetStreamDelivery<string>> = []
    const lease = source(async (delivery) => {
      accepted.push(delivery)
    })

    await lease.closed
    expect(accepted).toMatchObject([
      { value: 'one', cursor: { stream, sequence: 1 }, duplicate: false },
    ])
    await runtime.close()
  })
})
