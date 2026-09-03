import {
  AckPolicy,
  DeliverPolicy,
  jetstream,
  jetstreamManager,
  ReplayPolicy,
  StorageType,
} from '@nats-io/jetstream'
import { afterEach, describe, expect, it } from 'vitest'

import { createNatsRuntime, natsCodecs } from '@natsail/core'
import {
  createJetStreamProcessorController,
  processJetStream,
  JetStreamProcessorConfigurationError,
} from '@natsail/jetstream'

import { connectToTestNats, uniqueSubject } from './helpers.js'

describe('durable JetStream processor administration', () => {
  const closeAfterTest: Array<() => Promise<void>> = []

  afterEach(async () => {
    await Promise.allSettled(closeAfterTest.splice(0).map((close) => close()))
  })

  async function fixture(prefix: string) {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const client = jetstream(adminConnection)
    const stream = `ADMIN_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const consumer = `processor_${crypto.randomUUID().replaceAll('-', '_')}`
    const subject = uniqueSubject(prefix)
    await manager.streams.add({ name: stream, subjects: [subject], storage: StorageType.Memory })
    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream).catch(() => false)
      await adminConnection.drain()
    })
    return { client, consumer, manager, runtime, stream, subject }
  }

  it('creates a configured durable consumer, then reports it unchanged', async () => {
    const { consumer, runtime, stream, subject } = await fixture('admin-create')
    const controller = createJetStreamProcessorController(runtime, {
      stream,
      consumer: { mode: 'ensure', name: consumer },
      filter: subject,
      start: 'all',
      ackWaitMs: 250,
      backoffMs: [250, 500],
      maxDeliver: 3,
      maxAckPending: 16,
      metadata: { purpose: 'integration-test' },
      ackSamplePercent: 10,
      replicas: 1,
      memoryStorage: true,
    })

    await expect(controller.reconcile()).resolves.toMatchObject({ status: 'created' })
    await expect(controller.reconcile()).resolves.toMatchObject({ status: 'unchanged' })
    expect(controller.inspect()).toMatchObject({
      active: {
        durableName: consumer,
        backoffMs: [250, 500],
        metadata: { purpose: 'integration-test' },
        ackSamplePercent: 10,
        replicas: 1,
        memoryStorage: true,
      },
      state: { pendingAcknowledgements: 0, paused: false },
      lastReconciliation: { status: 'unchanged' },
    })
  })

  it('updates editable configuration without moving the acknowledgement floor', async () => {
    const { client, consumer, manager, runtime, stream, subject } = await fixture('admin-update')
    const published = await client.publish(subject, 'processed')
    const lease = processJetStream(
      runtime,
      {
        stream,
        consumer: { mode: 'ensure', name: consumer },
        filter: subject,
        start: 'all',
        codec: natsCodecs.text,
      },
      async () => undefined
    )
    await lease.ready
    await expect
      .poll(async () => (await manager.consumers.info(stream, consumer)).ack_floor.stream_seq)
      .toBe(published.seq)
    await lease.close()
    const before = await manager.consumers.info(stream, consumer)

    const controller = createJetStreamProcessorController(runtime, {
      stream,
      consumer: { mode: 'ensure', name: consumer },
      filter: subject,
      start: 'all',
      maxAckPending: 17,
      metadata: { revision: 'two' },
    })
    await expect(controller.reconcile()).resolves.toMatchObject({
      status: 'updated',
      editableDrift: ['maxAckPending', 'metadata'],
    })
    const after = await manager.consumers.info(stream, consumer)
    expect(after.ack_floor).toMatchObject(before.ack_floor)
    expect(after.created).toBe(before.created)
    expect(after.config).toMatchObject({
      max_ack_pending: 17,
      metadata: { revision: 'two' },
    })
  })

  it('rejects immutable ensure drift without mutating editable fields', async () => {
    const { consumer, manager, runtime, stream, subject } = await fixture('admin-reject')
    await manager.consumers.add(stream, {
      durable_name: consumer,
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant,
      filter_subject: subject,
      max_ack_pending: 10,
    })
    const before = await manager.consumers.info(stream, consumer)
    const controller = createJetStreamProcessorController(runtime, {
      stream,
      consumer: { mode: 'ensure', name: consumer },
      filter: subject,
      start: 'new',
      maxAckPending: 20,
    })

    await expect(controller.reconcile()).resolves.toMatchObject({
      status: 'rejected',
      editableDrift: ['maxAckPending'],
      immutableDrift: ['deliverPolicy'],
    })
    const after = await manager.consumers.info(stream, consumer)
    expect(after.created).toBe(before.created)
    expect(after.config).toMatchObject(before.config)
  })

  it('recreates only an owned consumer from its safe ack floor and retains unacked work', async () => {
    const { client, consumer, manager, runtime, stream, subject } = await fixture('admin-recreate')
    const originalController = createJetStreamProcessorController(runtime, {
      stream,
      consumer: { mode: 'owned', name: consumer },
      filter: subject,
      start: 'all',
    })
    await expect(originalController.reconcile()).resolves.toMatchObject({ status: 'created' })
    const first = await client.publish(subject, 'acknowledged')
    const second = await client.publish(subject, 'unacknowledged')
    const original = await client.consumers.get(stream, consumer)
    const firstDelivery = await original.next({ expires: 1_000 })
    expect(firstDelivery?.info.streamSequence).toBe(first.seq)
    firstDelivery?.ack()
    await expect
      .poll(async () => (await manager.consumers.info(stream, consumer)).ack_floor.stream_seq)
      .toBe(first.seq)

    const controller = createJetStreamProcessorController(runtime, {
      stream,
      consumer: { mode: 'owned', name: consumer },
      filter: subject,
      start: 'new',
    })
    await expect(controller.reconcile()).resolves.toMatchObject({
      status: 'recreated',
      deliveryBoundary: second.seq,
    })
    await expect(controller.reconcile()).resolves.toMatchObject({ status: 'unchanged' })

    const recreated = await client.consumers.get(stream, consumer)
    const retained = await recreated.next({ expires: 1_000 })
    expect(retained?.info.streamSequence).toBe(second.seq)
    retained?.ack()
  })

  it('pauses, resumes, refreshes cached state, and enforces delete ownership', async () => {
    const { consumer, manager, runtime, stream, subject } = await fixture('admin-operations')
    const ensure = createJetStreamProcessorController(runtime, {
      stream,
      consumer: { mode: 'ensure', name: consumer },
      filter: subject,
      start: 'all',
    })
    await ensure.reconcile()
    await expect(ensure.pause(new Date(Date.now() + 60_000))).resolves.toMatchObject({
      status: 'paused',
      inspection: { state: { paused: true } },
    })
    await expect(ensure.resume()).resolves.toMatchObject({
      status: 'resumed',
      inspection: { state: { paused: false } },
    })
    await expect(ensure.delete()).rejects.toBeInstanceOf(JetStreamProcessorConfigurationError)
    await expect(manager.consumers.info(stream, consumer)).resolves.toBeDefined()

    const claimedOwned = createJetStreamProcessorController(runtime, {
      stream,
      consumer: { mode: 'owned', name: consumer },
      filter: subject,
      start: 'all',
    })
    await claimedOwned.refresh()
    await expect(claimedOwned.delete()).rejects.toBeInstanceOf(JetStreamProcessorConfigurationError)
    await expect(manager.consumers.info(stream, consumer)).resolves.toBeDefined()

    await manager.consumers.delete(stream, consumer)
    const owned = createJetStreamProcessorController(runtime, {
      stream,
      consumer: { mode: 'owned', name: consumer },
      filter: subject,
      start: 'all',
    })
    await owned.reconcile()
    await expect(owned.delete()).resolves.toEqual({ status: 'deleted' })
    await expect(manager.consumers.info(stream, consumer)).rejects.toThrow()
  })
})
