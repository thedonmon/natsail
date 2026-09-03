import {
  AckPolicy,
  DeliverPolicy,
  ReplayPolicy,
  type ConsumerConfig,
  type ConsumerInfo,
} from '@nats-io/jetstream'
import type { NatsConnection } from '@nats-io/nats-core'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { createNatsRuntime, type NatsRuntime } from '@natsail/core'
import {
  classifyJetStreamProcessorDrift,
  createJetStreamProcessorController,
  normalizeJetStreamProcessorActive,
  validateJetStreamProcessorAdminOptions,
  JetStreamProcessorConfigurationError,
  type JetStreamProcessorAdminOptions,
} from '@natsail/jetstream'

const mocks = vi.hoisted(() => ({
  add: vi.fn(),
  delete: vi.fn(),
  info: vi.fn(),
  manager: vi.fn(),
  pause: vi.fn(),
  resume: vi.fn(),
  update: vi.fn(),
}))

vi.mock('@nats-io/jetstream', async (importOriginal) => {
  const original = await importOriginal<typeof import('@nats-io/jetstream')>()
  return {
    ...original,
    jetstreamManager: (connection: NatsConnection) => mocks.manager(connection),
  }
})

const baseOptions: JetStreamProcessorAdminOptions = {
  stream: 'EVENTS',
  consumer: { mode: 'ensure', name: 'processor' },
  filter: 'events.>',
  start: 'all',
}

function info(
  config: Partial<ConsumerConfig> = {},
  state: Partial<ConsumerInfo> = {}
): ConsumerInfo {
  return {
    stream_name: 'EVENTS',
    name: 'processor',
    created: '2026-09-02T00:00:00.000Z',
    config: {
      durable_name: 'processor',
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant,
      filter_subject: 'events.>',
      ...config,
    },
    delivered: { consumer_seq: 12, stream_seq: 20, last_active: 0 },
    ack_floor: { consumer_seq: 10, stream_seq: 18, last_active: 0 },
    num_ack_pending: 2,
    num_pending: 8,
    num_redelivered: 3,
    num_waiting: 0,
    push_bound: false,
    paused: false,
    pause_remaining: 0,
    ...state,
  }
}

function runtime() {
  let close!: () => void
  const closed = new Promise<void>((resolve) => {
    close = resolve
  })
  const connection = {
    closed: () => closed,
    drain: vi.fn(async () => close()),
    getServer: () => 'mock:4222',
    isClosed: () => false,
    status: async function* () {},
  } as unknown as NatsConnection
  return createNatsRuntime({ connect: async () => connection })
}

describe('JetStream processor administration', () => {
  let active: ConsumerInfo | undefined

  beforeEach(() => {
    active = info()
    mocks.info.mockReset().mockImplementation(async () => {
      if (active === undefined) throw { code: 404 }
      return active
    })
    mocks.add.mockReset().mockImplementation(async (_stream, config) => {
      active = info(config)
      return active
    })
    mocks.update.mockReset().mockImplementation(async (_stream, _name, config) => {
      active = info({ ...active!.config, ...config })
      return active
    })
    mocks.delete.mockReset().mockImplementation(async () => {
      active = undefined
      return true
    })
    mocks.pause.mockReset().mockImplementation(async () => {
      active = info(active!.config, { paused: true })
      return { paused: true }
    })
    mocks.resume.mockReset().mockImplementation(async () => {
      active = info(active!.config, { paused: false })
      return { paused: false }
    })
    mocks.manager.mockReset().mockImplementation(() => ({ consumers: mocks }))
  })

  it.each([
    [{ backoffMs: [] }, 'backoffMs'],
    [{ backoffMs: [100, 50] }, 'ordered'],
    [{ backoffMs: [100], ackWaitMs: 50 }, 'first backoffMs'],
    [{ backoffMs: [100, 200], maxDeliver: 1 }, 'must not exceed maxDeliver'],
    [{ ackSamplePercent: 101 }, 'ackSamplePercent'],
    [{ replicas: 0 }, 'replicas'],
    [{ metadata: { _nats_internal: 'no' } }, 'metadata'],
    [{ driftPolicy: 'recreate-owned' }, 'cannot use driftPolicy recreate-owned'],
    [{ start: 'latest' }, 'start must be all, new, or an after sequence'],
    [{ consumer: { mode: 'adopt', name: 'processor' } }, 'consumer.mode'],
    [{ start: { after: -1 } }, 'start.after'],
    [{ start: { after: Number.MAX_SAFE_INTEGER } }, 'room for the next sequence'],
  ] as const)('rejects invalid administration options %#', (patch, message) => {
    expect(() =>
      validateJetStreamProcessorAdminOptions({
        ...baseOptions,
        ...patch,
      } as JetStreamProcessorAdminOptions)
    ).toThrow(message)
  })

  it('compares only explicitly requested optional fields and canonicalizes maps and filters', () => {
    const activeInfo = info({
      replay_policy: ReplayPolicy.Original,
      filter_subjects: ['events.b', 'events.a'],
      metadata: { z: 'last', _nats_level: 'server-owned', a: 'first' },
      ack_wait: 9_000_000_000,
    })
    delete activeInfo.config.filter_subject
    const current = normalizeJetStreamProcessorActive(activeInfo)

    expect(
      classifyJetStreamProcessorDrift(
        { ...baseOptions, filter: ['events.a', 'events.b'], metadata: { a: 'first', z: 'last' } },
        current
      )
    ).toEqual({ editable: [], immutable: [] })
  })

  it('rejects immutable ensure drift without partially applying editable drift', async () => {
    active = info({ deliver_policy: DeliverPolicy.New, max_ack_pending: 10 })
    const controller = createJetStreamProcessorController(runtime(), {
      ...baseOptions,
      maxAckPending: 20,
    })

    await expect(controller.reconcile()).resolves.toMatchObject({
      status: 'rejected',
      editableDrift: ['maxAckPending'],
      immutableDrift: ['deliverPolicy'],
    })
    expect(mocks.update).not.toHaveBeenCalled()
    expect(mocks.delete).not.toHaveBeenCalled()
  })

  it('clears the mutually exclusive filter field when filter cardinality changes', async () => {
    active = info({ filter_subject: 'events.>' })
    const controller = createJetStreamProcessorController(runtime(), {
      ...baseOptions,
      filter: ['events.a', 'events.b'],
    })

    await expect(controller.reconcile()).resolves.toMatchObject({ status: 'updated' })
    expect(mocks.update).toHaveBeenCalledWith(
      'EVENTS',
      'processor',
      expect.objectContaining({ filter_subject: '', filter_subjects: ['events.a', 'events.b'] })
    )
  })

  it('enforces inspect-only bind and ownership deletion guards at runtime', async () => {
    const bind = createJetStreamProcessorController(runtime(), {
      ...baseOptions,
      consumer: { mode: 'bind', name: 'processor' },
    })
    await expect(bind.refresh()).resolves.toMatchObject({ consumer: { mode: 'bind' } })
    await expect(bind.pause(new Date(Date.now() + 60_000))).rejects.toBeInstanceOf(
      JetStreamProcessorConfigurationError
    )
    await expect(bind.resume()).rejects.toBeInstanceOf(JetStreamProcessorConfigurationError)
    await expect(bind.delete()).rejects.toMatchObject({ code: 'ownership-required' })
    expect(mocks.pause).not.toHaveBeenCalled()
    expect(mocks.resume).not.toHaveBeenCalled()
    expect(mocks.delete).not.toHaveBeenCalled()
  })

  it('serializes operations and remains usable after a rejected operation', async () => {
    let release!: () => void
    const blocked = new Promise<void>((resolve) => {
      release = resolve
    })
    mocks.info
      .mockImplementationOnce(async () => {
        await blocked
        throw new Error('first failed')
      })
      .mockImplementation(async () => active!)
    const controller = createJetStreamProcessorController(runtime(), baseOptions)
    const first = controller.refresh()
    const second = controller.refresh()

    await vi.waitFor(() => expect(mocks.info).toHaveBeenCalledOnce())
    release()
    await expect(first).rejects.toThrow('first failed')
    await expect(second).resolves.toBeDefined()
    expect(mocks.info).toHaveBeenCalledTimes(2)
  })

  it('caches rich authoritative inspection state', async () => {
    const controller = createJetStreamProcessorController(runtime(), baseOptions)
    const inspection = await controller.refresh()

    expect(inspection).toMatchObject({
      state: {
        pendingAcknowledgements: 2,
        pendingMessages: 8,
        delivered: { consumer: 12, stream: 20 },
        acknowledged: { consumer: 10, stream: 18 },
        redeliveries: 3,
        paused: false,
      },
    })
    expect(controller.inspect()).toEqual(inspection)
  })

  it('recreates the connection-bound consumer API after runtime connection replacement', async () => {
    const first = {} as NatsConnection
    const second = {} as NatsConnection
    let connection = first
    const activeRuntime = {
      connection: vi.fn(async () => connection),
    } as unknown as NatsRuntime
    const controller = createJetStreamProcessorController(activeRuntime, baseOptions)

    await controller.refresh()
    await controller.refresh()
    expect(mocks.manager).toHaveBeenCalledTimes(1)
    expect(mocks.manager).toHaveBeenLastCalledWith(first)

    connection = second
    await controller.refresh()
    expect(mocks.manager).toHaveBeenCalledTimes(2)
    expect(mocks.manager).toHaveBeenLastCalledWith(second)
  })

  it('recreates only owned immutable drift from the ack-floor boundary and stays stable', async () => {
    active = info(
      {
        deliver_policy: DeliverPolicy.All,
        metadata: { 'natsail.io/processor-owner': 'natsail' },
      },
      {
        ack_floor: {
          consumer_seq: 6,
          stream_seq: 18,
          last_active: 0,
        },
      }
    )
    const controller = createJetStreamProcessorController(runtime(), {
      ...baseOptions,
      consumer: { mode: 'owned', name: 'processor' },
      start: 'new',
    })

    await expect(controller.reconcile()).resolves.toMatchObject({
      status: 'recreated',
      deliveryBoundary: 19,
    })
    expect(mocks.delete).toHaveBeenCalledOnce()
    expect(mocks.add).toHaveBeenCalledWith(
      'EVENTS',
      expect.objectContaining({
        durable_name: 'processor',
        deliver_policy: DeliverPolicy.StartSequence,
        opt_start_seq: 19,
      })
    )
    await expect(controller.reconcile()).resolves.toMatchObject({ status: 'unchanged' })
    expect(mocks.delete).toHaveBeenCalledOnce()
  })

  it('preserves an undelivered start:new creation boundary during recreation', async () => {
    active = info(
      {
        deliver_policy: DeliverPolicy.New,
        metadata: { 'natsail.io/processor-owner': 'natsail' },
      },
      {
        delivered: {
          consumer_seq: 0,
          stream_seq: 41,
          last_active: 0,
        },
        ack_floor: {
          consumer_seq: 0,
          stream_seq: 0,
          last_active: 0,
        },
        num_ack_pending: 0,
      }
    )
    const controller = createJetStreamProcessorController(runtime(), {
      ...baseOptions,
      consumer: { mode: 'owned', name: 'processor' },
      start: 'all',
    })

    await expect(controller.reconcile()).resolves.toMatchObject({
      status: 'recreated',
      deliveryBoundary: 42,
    })
    expect(mocks.add).toHaveBeenCalledWith(
      'EVENTS',
      expect.objectContaining({
        deliver_policy: DeliverPolicy.StartSequence,
        opt_start_seq: 42,
      })
    )
  })

  it('rolls a failed owned recreation back to the acknowledgement boundary', async () => {
    active = info(
      {
        deliver_policy: DeliverPolicy.New,
        metadata: { 'natsail.io/processor-owner': 'natsail' },
      },
      {
        ack_floor: {
          consumer_seq: 6,
          stream_seq: 18,
          last_active: 0,
        },
      }
    )
    const recreationError = new Error('replacement rejected')
    mocks.add
      .mockReset()
      .mockRejectedValueOnce(recreationError)
      .mockImplementationOnce(async (_stream, config) => {
        active = info(config)
        return active
      })
    const controller = createJetStreamProcessorController(runtime(), {
      ...baseOptions,
      consumer: { mode: 'owned', name: 'processor' },
      start: 'all',
    })

    await expect(controller.reconcile()).rejects.toBe(recreationError)
    expect(mocks.delete).toHaveBeenCalledOnce()
    expect(mocks.add).toHaveBeenCalledTimes(2)
    expect(mocks.add).toHaveBeenLastCalledWith(
      'EVENTS',
      expect.objectContaining({
        deliver_policy: DeliverPolicy.StartSequence,
        opt_start_seq: 19,
      })
    )
    expect(active?.config).toMatchObject({
      deliver_policy: DeliverPolicy.StartSequence,
      opt_start_seq: 19,
    })
    expect(controller.inspect()).toMatchObject({
      active: {
        deliverPolicy: DeliverPolicy.StartSequence,
        startSequence: 19,
      },
    })
  })

  it('refuses to recreate start:new after unacknowledged delivery without a safe boundary', async () => {
    active = info(
      {
        deliver_policy: DeliverPolicy.New,
        metadata: { 'natsail.io/processor-owner': 'natsail' },
      },
      {
        delivered: {
          consumer_seq: 1,
          stream_seq: 41,
          last_active: 0,
        },
        ack_floor: {
          consumer_seq: 0,
          stream_seq: 0,
          last_active: 0,
        },
        num_ack_pending: 1,
      }
    )
    const controller = createJetStreamProcessorController(runtime(), {
      ...baseOptions,
      consumer: { mode: 'owned', name: 'processor' },
      start: 'all',
    })

    await expect(controller.reconcile()).rejects.toMatchObject({
      code: 'reconciliation-rejected',
    })
    expect(mocks.delete).not.toHaveBeenCalled()
  })

  it('pauses until a future deadline, resumes, and deletes owned consumers', async () => {
    active = info({ metadata: { 'natsail.io/processor-owner': 'natsail' } })
    const controller = createJetStreamProcessorController(runtime(), {
      ...baseOptions,
      consumer: { mode: 'owned', name: 'processor' },
    })
    await expect(controller.pause(new Date(0))).rejects.toThrow('in the future')
    const until = new Date(Date.now() + 60_000)
    await expect(controller.pause(until)).resolves.toMatchObject({
      status: 'paused',
      until: until.toISOString(),
      inspection: { state: { paused: true } },
    })
    expect(mocks.pause).toHaveBeenCalledWith('EVENTS', 'processor', until)
    await expect(controller.resume()).resolves.toMatchObject({
      status: 'resumed',
      inspection: { state: { paused: false } },
    })
    await expect(controller.delete()).resolves.toEqual({ status: 'deleted' })
    expect(controller.inspect().active).toBeUndefined()
  })

  it('never recreates or deletes an unmarked consumer claimed as owned', async () => {
    const controller = createJetStreamProcessorController(runtime(), {
      ...baseOptions,
      consumer: { mode: 'owned', name: 'processor' },
      start: 'new',
    })

    await expect(controller.reconcile()).resolves.toMatchObject({
      status: 'rejected',
      reason: 'ownership',
    })
    await expect(controller.delete()).rejects.toMatchObject({ code: 'ownership-required' })
    expect(mocks.delete).not.toHaveBeenCalled()
  })
})
