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

import type { CheckpointStore } from '@natsail/checkpoints'
import { createNatsRuntime, natsCodecs, type NatsailTelemetryEvent } from '@natsail/core'
import { consumeJetStream, processJetStream } from '@natsail/jetstream'

const mocks = vi.hoisted(() => ({
  addConsumer: vi.fn(),
  getConsumer: vi.fn(),
  streamInfo: vi.fn(),
}))

vi.mock('@nats-io/jetstream', async (importOriginal) => {
  const original = await importOriginal<typeof import('@nats-io/jetstream')>()
  return {
    ...original,
    jetstream: () => ({ consumers: { get: mocks.getConsumer } }),
    jetstreamManager: () => ({
      consumers: { add: mocks.addConsumer },
      streams: { info: mocks.streamInfo },
    }),
  }
})

const stream = 'PRIVATE_STREAM'
const filter = 'private.>'

function message(sequence: number, redelivered = false): JsMsg {
  return {
    ack: vi.fn(),
    data: natsCodecs.text.encode(`value-${sequence}`),
    info: {
      deliveryCount: redelivered ? 2 : 1,
      pending: 0,
      stream,
      streamSequence: sequence,
    },
    redelivered,
    subject: 'private.subject',
  } as unknown as JsMsg
}

function messages(deliveries: readonly JsMsg[]): ConsumerMessages {
  return {
    async *[Symbol.asyncIterator]() {
      yield* deliveries
    },
    close: vi.fn(async () => undefined),
    closed: vi.fn(async () => undefined),
    status: async function* () {},
  } as unknown as ConsumerMessages
}

function runtime(
  events: NatsailTelemetryEvent[],
  clock: { value: number }
): ReturnType<typeof createNatsRuntime> {
  let finish!: () => void
  const closed = new Promise<void>((resolve) => {
    finish = resolve
  })
  const connection = {
    closed: () => closed,
    drain: vi.fn(async () => finish()),
    getServer: () => 'nats://private.example',
    isClosed: () => false,
    status: async function* () {},
  } as unknown as NatsConnection
  return createNatsRuntime({
    connect: async () => connection,
    telemetry: { record: (event) => events.push(event) },
    telemetryClock: { now: () => clock.value },
  })
}

describe('JetStream telemetry', () => {
  beforeEach(() => {
    mocks.addConsumer.mockReset().mockResolvedValue(undefined)
    mocks.getConsumer.mockReset()
    mocks.streamInfo.mockReset().mockResolvedValue({
      created: '2026-09-02T00:00:00.000Z',
      state: { first_seq: 1 },
    })
  })

  it('measures replay, handlers, and checkpoint operations with an injected clock', async () => {
    const clock = { value: 0 }
    const events: NatsailTelemetryEvent[] = []
    const delivery = message(1)
    const source = messages([delivery])
    const consumer = {
      consume: vi.fn(async () => source),
      delete: vi.fn(async () => true),
      info: vi.fn(async () => {
        clock.value += 1
        return { num_pending: 1 }
      }),
    } as unknown as Consumer
    mocks.getConsumer.mockResolvedValue(consumer)
    const checkpoint: CheckpointStore = {
      clear: async () => undefined,
      load: async () => {
        clock.value += 2
        return undefined
      },
      save: async () => {
        clock.value += 3
      },
    }
    const nats = runtime(events, clock)

    const lease = consumeJetStream(
      nats,
      {
        stream,
        filter,
        start: 'all',
        resume: { key: 'private-checkpoint', store: checkpoint },
        codec: natsCodecs.text,
      },
      async () => {
        clock.value += 4
      }
    )

    await lease.closed
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'duration',
        name: 'natsail.checkpoint.operation.duration',
        durationMs: 2,
        attributes: expect.objectContaining({ operation: 'load', outcome: 'success' }),
      })
    )
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'duration',
        name: 'natsail.checkpoint.operation.duration',
        durationMs: 3,
        attributes: expect.objectContaining({ operation: 'save', outcome: 'success' }),
      })
    )
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'duration',
        name: 'natsail.jetstream.handler.duration',
        durationMs: 4,
        attributes: expect.objectContaining({ operation: 'ordered-handler', outcome: 'success' }),
      })
    )
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'duration',
        name: 'natsail.jetstream.replay.duration',
        durationMs: 7,
      })
    )
    expect(
      events
        .filter(
          (event) => event.type === 'gauge' && event.name === 'natsail.jetstream.replay.remaining'
        )
        .map((event) => (event.type === 'gauge' ? event.value : -1))
    ).toEqual([1, 0])
    expect(JSON.stringify(events)).not.toContain(stream)
    expect(JSON.stringify(events)).not.toContain(filter)
    expect(JSON.stringify(events)).not.toContain('private-checkpoint')
    expect(JSON.stringify(events)).not.toContain('private.subject')
    await nats.close()
  })

  it('measures processor redelivery, handler completion, and acknowledgement', async () => {
    const clock = { value: 20 }
    const events: NatsailTelemetryEvent[] = []
    const delivery = message(9, true)
    const source = messages([delivery])
    const consumer = {
      consume: vi.fn(async () => source),
      delete: vi.fn(async () => true),
      info: vi.fn(async () => ({
        config: {
          ack_policy: AckPolicy.Explicit,
          deliver_policy: DeliverPolicy.All,
          durable_name: 'private-consumer',
          filter_subject: filter,
          replay_policy: ReplayPolicy.Instant,
        },
      })),
    } as unknown as Consumer
    mocks.getConsumer.mockResolvedValue(consumer)
    const nats = runtime(events, clock)

    const lease = processJetStream(
      nats,
      {
        stream,
        consumer: { mode: 'ensure', name: 'private-consumer' },
        filter,
        start: 'all',
        codec: natsCodecs.text,
      },
      async () => {
        clock.value += 6
      }
    )

    await lease.closed
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'counter',
        name: 'natsail.jetstream.deliveries',
        attributes: expect.objectContaining({ redelivered: true }),
      })
    )
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'duration',
        name: 'natsail.jetstream.handler.duration',
        durationMs: 6,
        attributes: expect.objectContaining({ operation: 'processor-handler', outcome: 'success' }),
      })
    )
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'counter',
        name: 'natsail.jetstream.acknowledgements',
        attributes: expect.objectContaining({ outcome: 'success', redelivered: true }),
      })
    )
    expect(delivery.ack).toHaveBeenCalledOnce()
    expect(JSON.stringify(events)).not.toContain('private-consumer')
    await nats.close()
  })

  it('reports checkpoint and replay failures without exposing the checkpoint key', async () => {
    const clock = { value: 30 }
    const events: NatsailTelemetryEvent[] = []
    const failure = new Error('checkpoint unavailable')
    const checkpoint: CheckpointStore = {
      clear: async () => undefined,
      load: async () => {
        clock.value += 4
        throw failure
      },
      save: async () => undefined,
    }
    const nats = runtime(events, clock)
    const lease = consumeJetStream(
      nats,
      {
        stream,
        filter,
        start: 'all',
        resume: { key: 'private-failing-key', store: checkpoint },
        codec: natsCodecs.text,
      },
      async () => undefined
    )
    void lease.ready.catch(() => undefined)
    void lease.caughtUp.catch(() => undefined)

    await expect(lease.closed).rejects.toBe(failure)
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'duration',
        name: 'natsail.checkpoint.operation.duration',
        durationMs: 4,
        attributes: expect.objectContaining({ operation: 'load', outcome: 'failure' }),
      })
    )
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'duration',
        name: 'natsail.jetstream.replay.duration',
        durationMs: 4,
        attributes: expect.objectContaining({ outcome: 'failure' }),
      })
    )
    expect(JSON.stringify(events)).not.toContain('private-failing-key')
    await nats.close()
  })

  it('reports processor handler failure and leaves the delivery unacknowledged', async () => {
    const clock = { value: 50 }
    const events: NatsailTelemetryEvent[] = []
    const delivery = message(10, true)
    const source = messages([delivery])
    const consumer = {
      consume: vi.fn(async () => source),
      delete: vi.fn(async () => true),
      info: vi.fn(async () => ({
        config: {
          ack_policy: AckPolicy.Explicit,
          deliver_policy: DeliverPolicy.All,
          durable_name: 'private-consumer',
          filter_subject: filter,
          replay_policy: ReplayPolicy.Instant,
        },
      })),
    } as unknown as Consumer
    mocks.getConsumer.mockResolvedValue(consumer)
    const nats = runtime(events, clock)
    const failure = new Error('application failed')
    const lease = processJetStream(
      nats,
      {
        stream,
        consumer: { mode: 'ensure', name: 'private-consumer' },
        filter,
        start: 'all',
        codec: natsCodecs.text,
      },
      async () => {
        clock.value += 5
        throw failure
      }
    )

    await expect(lease.closed).rejects.toBe(failure)
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'duration',
        name: 'natsail.jetstream.handler.duration',
        durationMs: 5,
        attributes: expect.objectContaining({
          operation: 'processor-handler',
          outcome: 'failure',
          redelivered: true,
        }),
      })
    )
    expect(delivery.ack).not.toHaveBeenCalled()
    await nats.close()
  })
})
