import {
  AckPolicy,
  DeliverPolicy,
  ReplayPolicy,
  type ConsumerConfig,
  type Consumer,
  type ConsumerInfo,
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
  infoConsumer: vi.fn(),
  pauseConsumer: vi.fn(),
  resumeConsumer: vi.fn(),
  updateConsumer: vi.fn(),
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
        info: jetStreamMocks.infoConsumer,
        pause: jetStreamMocks.pauseConsumer,
        resume: jetStreamMocks.resumeConsumer,
        update: jetStreamMocks.updateConsumer,
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
      deliverySequence: sequence,
      pending: 0,
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

function consumer(
  deliveries: readonly JsMsg[],
  closedError?: Error,
  stayOpen = false,
  currentInfo?: ConsumerInfo
): Consumer {
  const messages = messageSource(deliveries, closedError, stayOpen)
  return {
    consume: vi.fn(async () => messages),
    delete: vi.fn(async () => true),
    info: vi.fn(async () =>
      currentInfo === undefined
        ? {
            config: {
              ack_policy: AckPolicy.Explicit,
              deliver_policy: DeliverPolicy.All,
              durable_name: 'processor',
              filter_subject: subject,
              replay_policy: ReplayPolicy.Instant,
            },
          }
        : currentInfo
    ),
  } as unknown as Consumer
}

function consumerInfo(config: Partial<ConsumerConfig>): ConsumerInfo {
  return {
    stream_name: stream,
    name: 'processor',
    created: '2026-09-02T00:00:00.000Z',
    config: {
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      durable_name: 'processor',
      filter_subject: subject,
      replay_policy: ReplayPolicy.Instant,
      ...config,
    },
    delivered: { consumer_seq: 0, stream_seq: 0, last_active: 0 },
    ack_floor: { consumer_seq: 0, stream_seq: 0, last_active: 0 },
    num_ack_pending: 0,
    num_pending: 0,
    num_redelivered: 0,
    num_waiting: 0,
    push_bound: false,
    pause_remaining: 0,
  }
}

function runtime(telemetryEvents?: NatsailTelemetryEvent[], shutdownTimeoutMs?: number) {
  let closeConnection!: () => void
  const closed = new Promise<void>((resolve) => {
    closeConnection = resolve
  })
  const connection = {
    closed: () => closed,
    drain: vi.fn(async () => closeConnection()),
    close: vi.fn(async () => closeConnection()),
    getServer: vi.fn(() => 'mock:4222'),
    isClosed: vi.fn(() => false),
    status: async function* () {},
  } as unknown as NatsConnection
  return createNatsRuntime({
    connect: async () => connection,
    ...(shutdownTimeoutMs === undefined ? {} : { shutdownTimeoutMs }),
    ...(telemetryEvents === undefined
      ? {}
      : { telemetry: { record: (event) => telemetryEvents.push(event) } }),
  })
}

describe('recovering explicit-ack JetStream processor', () => {
  it.each([0, -1, 1.5, Number.POSITIVE_INFINITY, 100])(
    'rejects unsafe progress interval %s before connecting',
    (progressIntervalMs) => {
      const nats = runtime()
      expect(() =>
        processJetStream(
          nats,
          {
            stream,
            consumer: { mode: 'ensure', name: 'processor' },
            filter: subject,
            start: 'all',
            codec: natsCodecs.text,
            ackWaitMs: 100,
            progressIntervalMs,
          },
          () => undefined
        )
      ).toThrow('progressIntervalMs')
      expect(jetStreamMocks.getConsumer).not.toHaveBeenCalled()
    }
  )

  it('retains an earlier retry boundary when later work succeeds before recovery', async () => {
    const first = message(1, 'retry')
    first.nak = vi.fn()
    const second = message(2, 'success')
    const starting = consumerInfo({ deliver_policy: DeliverPolicy.New })
    let active: ConsumerInfo | undefined
    jetStreamMocks.addConsumer.mockImplementation(async (_stream, config) => {
      active = consumerInfo(config)
      return active
    })
    jetStreamMocks.infoConsumer.mockImplementation(async () => {
      if (!active) throw { code: 404 }
      return active
    })
    jetStreamMocks.getConsumer
      .mockResolvedValueOnce(consumer([first, second], new Error('lost consumer'), false, starting))
      .mockResolvedValue(consumer([], undefined, true, starting))
    const nats = runtime()
    const lease = processJetStream(
      nats,
      {
        stream,
        consumer: { mode: 'owned', name: 'processor' },
        filter: subject,
        start: 'new',
        codec: natsCodecs.text,
        recovery: { maxAttempts: 2, delayMs: 0 },
      },
      ({ value }) => {
        if (value === 'retry') return { action: 'retry', delayMs: 250 }
        active = undefined
      }
    )
    try {
      await lease.ready
      await vi.waitFor(() => expect(jetStreamMocks.addConsumer).toHaveBeenCalledTimes(2))
      expect(jetStreamMocks.addConsumer).toHaveBeenLastCalledWith(
        stream,
        expect.objectContaining({ deliver_policy: DeliverPolicy.StartSequence, opt_start_seq: 1 })
      )
    } finally {
      await lease.close().catch(() => undefined)
      await nats.close().catch(() => undefined)
    }
  })
  it.each([false, true])(
    'does not ack late work or delete its consumer after forced shutdown (recovery=%s)',
    async (recovering) => {
      vi.useFakeTimers()
      const delivery = message(1, 'one')
      delivery.working = vi.fn()
      jetStreamMocks.getConsumer.mockResolvedValue(consumer([delivery], undefined, true))
      const nats = runtime(undefined, 50)
      let release!: () => void
      let started!: () => void
      const handling = new Promise<void>((resolve) => {
        started = resolve
      })
      const pending = new Promise<void>((resolve) => {
        release = resolve
      })
      let signal: AbortSignal | undefined
      const lease = processJetStream(
        nats,
        {
          stream,
          consumer: { mode: 'owned', name: 'processor' },
          filter: subject,
          start: 'all',
          codec: natsCodecs.text,
          progressIntervalMs: 10,
          ...(recovering ? { recovery: { delayMs: 0 } } : {}),
        },
        async (_delivery, context) => {
          signal = context.signal
          started()
          await pending
        }
      )
      try {
        await handling
        const closing = nats.close().catch((error: unknown) => error)
        await vi.advanceTimersByTimeAsync(50)
        await expect(closing).resolves.toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError' })
        expect(signal?.aborted).toBe(true)
        const progressBefore = vi.mocked(delivery.working).mock.calls.length
        await vi.advanceTimersByTimeAsync(100)
        expect(delivery.working).toHaveBeenCalledTimes(progressBefore)
        release()
        await lease.closed.catch(() => undefined)
        expect(delivery.ack).not.toHaveBeenCalled()
        expect(jetStreamMocks.deleteConsumer).not.toHaveBeenCalled()
      } finally {
        release()
        await lease.close().catch(() => undefined)
        vi.useRealTimers()
      }
    }
  )

  it('keeps unconfirmed work out of the acknowledged position', async () => {
    const delivery = message(1, 'one')
    delivery.ackAck = vi.fn(async () => false)
    jetStreamMocks.getConsumer.mockResolvedValue(consumer([delivery]))
    const nats = runtime()
    const lease = processJetStream(
      nats,
      {
        stream,
        consumer: { mode: 'ensure', name: 'processor' },
        filter: subject,
        start: 'all',
        codec: natsCodecs.text,
        acknowledgement: { mode: 'confirmed' },
      },
      () => undefined
    )
    await expect(lease.closed).rejects.toMatchObject({ name: 'JetStreamAcknowledgementError' })
    expect(lease.inspect().acknowledged.stream).toBe(0)
    await nats.close()
  })
  it('terminates a poison message only on an explicit handler decision', async () => {
    const first = message(1, 'poison')
    first.term = vi.fn()
    const second = message(2, 'success')
    jetStreamMocks.getConsumer.mockResolvedValue(consumer([first, second], undefined, true))
    const nats = runtime()
    const lease = processJetStream(
      nats,
      {
        stream,
        consumer: { mode: 'ensure', name: 'processor' },
        filter: subject,
        start: 'all',
        codec: natsCodecs.text,
      },
      ({ value }) =>
        value === 'poison' ? { action: 'term', reason: 'unsupported job schema' } : undefined
    )
    try {
      await lease.ready
      await vi.waitFor(() => expect(second.ack).toHaveBeenCalledOnce())
      expect(first.term).toHaveBeenCalledWith('unsupported job schema')
      expect(first.ack).not.toHaveBeenCalled()
    } finally {
      await lease.close().catch(() => undefined)
      await nats.close().catch(() => undefined)
    }
  })
  it('allows an explicit delayed retry without acknowledging the failed delivery', async () => {
    const first = message(1, 'retry')
    first.nak = vi.fn()
    const second = message(2, 'success')
    jetStreamMocks.getConsumer.mockResolvedValue(consumer([first, second], undefined, true))
    const nats = runtime()
    const lease = processJetStream(
      nats,
      {
        stream,
        consumer: { mode: 'ensure', name: 'processor' },
        filter: subject,
        start: 'all',
        codec: natsCodecs.text,
      },
      ({ value }) => (value === 'retry' ? { action: 'retry', delayMs: 250 } : undefined)
    )
    try {
      await lease.ready
      await vi.waitFor(() => expect(second.ack).toHaveBeenCalledOnce())
      expect(first.nak).toHaveBeenCalledWith(250)
      expect(first.ack).not.toHaveBeenCalled()
    } finally {
      await lease.close()
      await nats.close()
    }
  })
  it('keeps a slow handler alive and stops progress updates after processing', async () => {
    vi.useFakeTimers()
    const delivery = message(1, 'one')
    delivery.working = vi.fn()
    const active = consumer([delivery], undefined, true)
    jetStreamMocks.getConsumer.mockResolvedValue(active)
    const nats = runtime()
    let release!: () => void
    let started!: () => void
    const handling = new Promise<void>((resolve) => {
      started = resolve
    })
    const pending = new Promise<void>((resolve) => {
      release = resolve
    })
    const lease = processJetStream(
      nats,
      {
        stream,
        consumer: { mode: 'ensure', name: 'processor' },
        filter: subject,
        start: 'all',
        codec: natsCodecs.text,
        ackWaitMs: 100,
        progressIntervalMs: 25,
      },
      async () => {
        started()
        await pending
      }
    )
    try {
      await handling
      await vi.advanceTimersByTimeAsync(75)
      expect(delivery.working).toHaveBeenCalledTimes(3)
      expect(delivery.ack).not.toHaveBeenCalled()
      expect(active.consume).toHaveBeenCalledWith({ max_messages: 1 })
      release()
      await vi.advanceTimersByTimeAsync(0)
      await lease.close()
      await vi.advanceTimersByTimeAsync(100)
      expect(delivery.working).toHaveBeenCalledTimes(3)
      expect(delivery.ack).toHaveBeenCalledOnce()
    } finally {
      release()
      await lease.close().catch(() => undefined)
      await nats.close()
      vi.useRealTimers()
    }
  })
  it('does not advance the acknowledgement boundary before server confirmation', async () => {
    let confirm!: (accepted: boolean) => void
    const delivery = message(1, 'one')
    delivery.ackAck = vi.fn(
      () =>
        new Promise<boolean>((resolve) => {
          confirm = resolve
        })
    )
    jetStreamMocks.getConsumer.mockResolvedValue(consumer([delivery], undefined, true))
    const nats = runtime()
    const lease = processJetStream(
      nats,
      {
        stream,
        consumer: { mode: 'ensure', name: 'processor' },
        filter: subject,
        start: 'all',
        codec: natsCodecs.text,
        acknowledgement: { mode: 'confirmed', timeoutMs: 500 },
      },
      () => undefined
    )
    try {
      await lease.ready
      await vi.waitFor(() => expect(delivery.ackAck).toHaveBeenCalledWith({ timeout: 500 }))
      expect(lease.inspect().acknowledged.stream).toBe(0)
      confirm(true)
      await vi.waitFor(() => expect(lease.inspect().acknowledged.stream).toBe(1))
      expect(delivery.ack).not.toHaveBeenCalled()
    } finally {
      confirm?.(true)
      await lease.close()
      await nats.close()
    }
  })
  beforeEach(() => {
    let active: ConsumerInfo | undefined
    jetStreamMocks.addConsumer.mockReset().mockImplementation(async (_stream, config) => {
      active = consumerInfo(config)
      return active
    })
    jetStreamMocks.deleteConsumer.mockReset().mockResolvedValue(true)
    jetStreamMocks.infoConsumer.mockReset().mockImplementation(async () => {
      if (active === undefined) throw { code: 404 }
      return active
    })
    jetStreamMocks.pauseConsumer.mockReset()
    jetStreamMocks.resumeConsumer.mockReset()
    jetStreamMocks.updateConsumer.mockReset()
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
    expect(lease.inspect()).toMatchObject({
      phase: 'live',
      restarts: 1,
      delivered: { consumer: 2, stream: 2 },
      acknowledged: { consumer: 2, stream: 2 },
    })
    expect(telemetryEvents).toContainEqual(
      expect.objectContaining({
        type: 'counter',
        name: 'natsail.jetstream.recoveries',
        value: 1,
      })
    )
    expect(jetStreamMocks.deleteConsumer).not.toHaveBeenCalled()

    await lease.close()
    expect(jetStreamMocks.deleteConsumer).toHaveBeenCalledOnce()
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

    expect(jetStreamMocks.deleteConsumer).toHaveBeenCalledOnce()
    await expect(lease.closed).resolves.toBeUndefined()
  })

  it('surfaces an owned-consumer deletion failure from close', async () => {
    const activeConsumer = consumer([], undefined, true)
    jetStreamMocks.getConsumer.mockResolvedValue(activeConsumer)
    const deletionError = new Error('consumer delete failed')
    jetStreamMocks.deleteConsumer.mockRejectedValue(deletionError)
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
    await expect(lease.close()).rejects.toBe(deletionError)
    expect(jetStreamMocks.deleteConsumer).toHaveBeenCalledOnce()
    await nats.close().catch(() => undefined)
  })

  it('recreates a deleted start:new consumer after the last safe acknowledgement boundary', async () => {
    const firstInfo = consumerInfo({
      deliver_policy: DeliverPolicy.New,
      metadata: { 'natsail.io/processor-owner': 'natsail' },
    })
    const resumedInfo = consumerInfo({
      deliver_policy: DeliverPolicy.StartSequence,
      opt_start_seq: 2,
      metadata: { 'natsail.io/processor-owner': 'natsail' },
    })
    jetStreamMocks.infoConsumer
      .mockReset()
      .mockRejectedValueOnce({ code: 404 })
      .mockResolvedValueOnce(firstInfo)
      .mockRejectedValueOnce({ code: 404 })
      .mockResolvedValueOnce(resumedInfo)
      .mockResolvedValue(resumedInfo)
    const activeConsumer = consumer([message(2, 'published-in-gap')], undefined, true)
    jetStreamMocks.getConsumer
      .mockResolvedValueOnce(consumer([message(1, 'before-gap')], new Error('consumer deleted')))
      .mockResolvedValueOnce(activeConsumer)
    const nats = runtime()
    const received: string[] = []
    const lease = processJetStream(
      nats,
      {
        stream,
        consumer: { mode: 'owned', name: 'processor' },
        filter: subject,
        start: 'new',
        recovery: { maxAttempts: 2, delayMs: 0 },
        codec: natsCodecs.text,
      },
      ({ value }) => {
        received.push(value)
      }
    )

    await lease.ready
    await vi.waitFor(() => expect(received).toEqual(['before-gap', 'published-in-gap']))
    expect(jetStreamMocks.addConsumer).toHaveBeenCalledTimes(2)
    expect(jetStreamMocks.addConsumer).toHaveBeenNthCalledWith(
      2,
      stream,
      expect.objectContaining({
        durable_name: 'processor',
        deliver_policy: DeliverPolicy.StartSequence,
        opt_start_seq: 2,
      })
    )
    expect(lease.inspect()).toMatchObject({
      phase: 'live',
      restarts: 1,
      acknowledged: { stream: 2 },
    })

    await lease.close()
    await nats.close()
  })

  it('preserves a start:new creation boundary when recovery happens before the first message', async () => {
    const firstInfo = {
      ...consumerInfo({
        deliver_policy: DeliverPolicy.New,
        metadata: { 'natsail.io/processor-owner': 'natsail' },
      }),
      delivered: { consumer_seq: 0, stream_seq: 41, last_active: 0 },
    }
    const resumedInfo = consumerInfo({
      deliver_policy: DeliverPolicy.StartSequence,
      opt_start_seq: 42,
      metadata: { 'natsail.io/processor-owner': 'natsail' },
    })
    jetStreamMocks.infoConsumer
      .mockReset()
      .mockRejectedValueOnce({ code: 404 })
      .mockResolvedValueOnce(firstInfo)
      .mockRejectedValueOnce({ code: 404 })
      .mockResolvedValueOnce(resumedInfo)
      .mockResolvedValue(resumedInfo)
    jetStreamMocks.getConsumer
      .mockResolvedValueOnce(
        consumer([], new Error('consumer deleted before delivery'), false, firstInfo)
      )
      .mockResolvedValueOnce(consumer([], undefined, true, resumedInfo))
    const nats = runtime()
    const lease = processJetStream(
      nats,
      {
        stream,
        consumer: { mode: 'owned', name: 'processor' },
        filter: subject,
        start: 'new',
        recovery: { maxAttempts: 2, delayMs: 0 },
        codec: natsCodecs.text,
      },
      async () => undefined
    )

    await lease.ready
    await vi.waitFor(() => expect(jetStreamMocks.getConsumer).toHaveBeenCalledTimes(2))
    expect(jetStreamMocks.addConsumer).toHaveBeenNthCalledWith(
      2,
      stream,
      expect.objectContaining({
        deliver_policy: DeliverPolicy.StartSequence,
        opt_start_seq: 42,
      })
    )

    await lease.close()
    await nats.close()
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
