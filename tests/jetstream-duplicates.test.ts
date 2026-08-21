import type { Consumer, ConsumerMessages, JsMsg } from '@nats-io/jetstream'
import type { NatsConnection } from '@nats-io/nats-core'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { CheckpointStore } from '@natsail/checkpoints'
import { createNatsRuntime } from '@natsail/core'
import {
  consumeJetStream,
  JetStreamDuplicateError,
  type JetStreamDelivery,
  type JetStreamDuplicateDeliveryPolicy,
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
const encoder = new TextEncoder()
const decoder = new TextDecoder()

function message(sequence: number, value: string, redelivered = false): JsMsg {
  return {
    data: encoder.encode(value),
    info: { stream, streamSequence: sequence },
    redelivered,
  } as JsMsg
}

function messageSource(deliveries: readonly JsMsg[]): ConsumerMessages {
  let closeRequested = false

  return {
    async *[Symbol.asyncIterator]() {
      for (const delivery of deliveries) {
        if (closeRequested) break
        yield delivery
      }
    },
    close: vi.fn(async () => {
      closeRequested = true
    }),
    closed: vi.fn(async () => undefined),
  } as unknown as ConsumerMessages
}

function arrangeConsumer(deliveries: readonly JsMsg[]) {
  const messages = messageSource(deliveries)
  const consumer = {
    consume: vi.fn(async () => messages),
    delete: vi.fn(async () => true),
  } as unknown as Consumer
  jetStreamMocks.getConsumer.mockResolvedValue(consumer)
  return { consumer, messages }
}

function arrangeRuntime() {
  const connection = {
    drain: vi.fn(async () => undefined),
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
      decode: (delivery) => decoder.decode(delivery.data),
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
    })
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
})
