import {
  AckPolicy,
  DeliverPolicy,
  ReplayPolicy,
  type Consumer,
  type ConsumerMessages,
  type JsMsg,
} from '@nats-io/jetstream'
import type { NatsConnection } from '@nats-io/nats-core'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { createNatsRuntime, natsCodecs, type NatsailTelemetryEvent } from '@natsail/core'
import { processJetStream } from '@natsail/jetstream'

const jetStreamMocks = vi.hoisted(() => ({
  addConsumer: vi.fn(),
  deleteConsumer: vi.fn(),
  getConsumer: vi.fn(),
}))

vi.mock('@nats-io/jetstream', async (importOriginal) => {
  const original = await importOriginal<typeof import('@nats-io/jetstream')>()

  return {
    ...original,
    jetstream: () => ({ consumers: { get: jetStreamMocks.getConsumer } }),
    jetstreamManager: () => ({
      consumers: {
        add: jetStreamMocks.addConsumer,
        delete: jetStreamMocks.deleteConsumer,
      },
    }),
  }
})

const stream = 'EVENTS'
const subject = 'events.>'

function message(sequence: number, value: string): JsMsg {
  return {
    ack: vi.fn(),
    data: natsCodecs.text.encode(value),
    info: {
      deliveryCount: 1,
      stream,
      streamSequence: sequence,
    },
    redelivered: false,
    subject,
  } as unknown as JsMsg
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
    status: async function* () {
      await closeSignal
    },
  } as unknown as ConsumerMessages
}

function consumer(deliveries: readonly JsMsg[], closedError?: Error, stayOpen = false): Consumer {
  const messages = messageSource(deliveries, closedError, stayOpen)
  return {
    consume: vi.fn(async () => messages),
    delete: vi.fn(async () => true),
    info: vi.fn(async () => ({
      config: {
        ack_policy: AckPolicy.Explicit,
        deliver_policy: DeliverPolicy.All,
        durable_name: 'processor',
        filter_subject: subject,
        replay_policy: ReplayPolicy.Instant,
      },
    })),
  } as unknown as Consumer
}

function runtime(telemetryEvents?: NatsailTelemetryEvent[]) {
  let closeConnection!: () => void
  const closed = new Promise<void>((resolve) => {
    closeConnection = resolve
  })
  const connection = {
    closed: () => closed,
    drain: vi.fn(async () => closeConnection()),
    getServer: vi.fn(() => 'mock:4222'),
    isClosed: vi.fn(() => false),
    status: async function* () {},
  } as unknown as NatsConnection
  return createNatsRuntime({
    connect: async () => connection,
    ...(telemetryEvents === undefined
      ? {}
      : { telemetry: { record: (event) => telemetryEvents.push(event) } }),
  })
}

describe('recovering explicit-ack JetStream processor', () => {
  beforeEach(() => {
    jetStreamMocks.addConsumer.mockReset().mockResolvedValue(undefined)
    jetStreamMocks.deleteConsumer.mockReset().mockResolvedValue(true)
    jetStreamMocks.getConsumer.mockReset()
  })

  it('reopens the owned consumer after an infrastructure failure and deletes it only on close', async () => {
    const telemetryEvents: NatsailTelemetryEvent[] = []
    const activeConsumer = consumer([message(2, 'two')], undefined, true)
    jetStreamMocks.getConsumer
      .mockResolvedValueOnce(consumer([message(1, 'one')], new Error('consumer failed')))
      .mockResolvedValueOnce(activeConsumer)
    const nats = runtime(telemetryEvents)
    const received: string[] = []
    const lease = processJetStream(
      nats,
      {
        stream,
        consumer: { mode: 'owned', name: 'processor' },
        filter: subject,
        start: 'all',
        recovery: { maxAttempts: 2, delayMs: 0 },
        codec: natsCodecs.text,
      },
      ({ value }) => {
        received.push(value)
      }
    )

    await lease.ready
    await vi.waitFor(() => expect(jetStreamMocks.getConsumer).toHaveBeenCalledTimes(2))
    await vi.waitFor(() => expect(received).toEqual(['one', 'two']))
    expect(jetStreamMocks.addConsumer).toHaveBeenNthCalledWith(
      1,
      stream,
      expect.objectContaining({ durable_name: 'processor' })
    )
    expect(lease.inspect()).toMatchObject({ phase: 'live', restarts: 1 })
    expect(telemetryEvents).toContainEqual(
      expect.objectContaining({
        type: 'counter',
        name: 'natsail.jetstream.recoveries',
        value: 1,
      })
    )
    expect(jetStreamMocks.deleteConsumer).not.toHaveBeenCalled()

    await lease.close()
    expect(activeConsumer.delete).toHaveBeenCalledOnce()
    await nats.close()
  })

  it('deletes the owned consumer when the runtime closes the recovering processor', async () => {
    const activeConsumer = consumer([], undefined, true)
    jetStreamMocks.getConsumer.mockResolvedValue(activeConsumer)
    const nats = runtime()
    const lease = processJetStream(
      nats,
      {
        stream,
        consumer: { mode: 'owned', name: 'processor' },
        filter: subject,
        start: 'all',
        recovery: { maxAttempts: 2, delayMs: 0 },
        codec: natsCodecs.text,
      },
      async () => undefined
    )

    await lease.ready
    await nats.close()

    expect(activeConsumer.delete).toHaveBeenCalledOnce()
    await expect(lease.closed).resolves.toBeUndefined()
  })

  it('does not retry an application handler failure', async () => {
    const applicationError = new Error('application rejected delivery')
    jetStreamMocks.getConsumer.mockResolvedValue(consumer([message(1, 'one')]))
    const nats = runtime()
    const lease = processJetStream(
      nats,
      {
        stream,
        consumer: { mode: 'ensure', name: 'processor' },
        filter: subject,
        start: 'all',
        recovery: { maxAttempts: 3, delayMs: 0, shouldRetry: () => true },
        codec: natsCodecs.text,
      },
      async () => {
        throw applicationError
      }
    )

    await expect(lease.closed).rejects.toBe(applicationError)
    expect(jetStreamMocks.getConsumer).toHaveBeenCalledOnce()
    expect(jetStreamMocks.deleteConsumer).not.toHaveBeenCalled()
    await nats.close()
  })
})
