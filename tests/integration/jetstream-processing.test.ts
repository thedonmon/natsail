import {
  AckPolicy,
  jetstream,
  jetstreamManager,
  ReplayPolicy,
  StorageType,
} from '@nats-io/jetstream'
import { afterEach, describe, expect, it } from 'vitest'

import { createNatsRuntime, natsCodecs } from '@natsail/core'
import { JetStreamProcessorConfigurationError, processJetStream } from '@natsail/jetstream'

import { connectToTestNats, uniqueSubject } from './helpers.js'

describe('managed explicit-ack JetStream processing', () => {
  const closeAfterTest: Array<() => Promise<void>> = []

  afterEach(async () => {
    await Promise.allSettled(closeAfterTest.splice(0).map((close) => close()))
  })

  it('acknowledges only after application processing succeeds and preserves the durable consumer', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const client = jetstream(adminConnection)
    const stream = `PROCESS_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const consumerName = `processor_${crypto.randomUUID().replaceAll('-', '_')}`
    const subject = uniqueSubject('processing')

    await manager.streams.add({
      name: stream,
      subjects: [subject],
      storage: StorageType.Memory,
    })
    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    await client.publish(subject, 'work')
    let finishProcessing!: () => void
    const processing = new Promise<void>((resolve) => {
      finishProcessing = resolve
    })
    closeAfterTest.push(async () => finishProcessing())
    const received: string[] = []

    const lease = processJetStream(
      runtime,
      {
        stream,
        consumer: { mode: 'ensure', name: consumerName },
        filter: subject,
        start: 'all',
        ackWaitMs: 5_000,
        maxDeliver: 3,
        codec: natsCodecs.text,
      },
      async (delivery) => {
        expect(delivery.subject).toBe(subject)
        received.push(delivery.value)
        await processing
      }
    )

    await lease.ready
    await expect.poll(() => received).toEqual(['work'])
    const before = await manager.consumers.info(stream, consumerName)
    expect(before.config.ack_policy).toBe(AckPolicy.Explicit)
    expect(before.ack_floor.stream_seq).toBe(0)

    finishProcessing()
    await expect
      .poll(async () => (await manager.consumers.info(stream, consumerName)).ack_floor.stream_seq)
      .toBe(1)

    await lease.close()
    await expect(manager.consumers.info(stream, consumerName)).resolves.toBeDefined()
  })

  it('reuses an existing named consumer when ensure is called with the same contract', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const stream = `PROCESS_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const consumerName = `processor_${crypto.randomUUID().replaceAll('-', '_')}`
    const subject = uniqueSubject('processing-ensure')

    await manager.streams.add({ name: stream, subjects: [subject], storage: StorageType.Memory })
    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    const options = {
      stream,
      consumer: { mode: 'ensure' as const, name: consumerName },
      filter: subject,
      start: 'all' as const,
      ackWaitMs: 5_000,
      maxDeliver: 3,
      decode: (message: { data: Uint8Array }) => message.data,
    }
    const first = processJetStream(runtime, options, async () => undefined)
    await first.ready
    const created = await manager.consumers.info(stream, consumerName)
    await first.close()

    const second = processJetStream(runtime, options, async () => undefined)
    await second.ready
    const reused = await manager.consumers.info(stream, consumerName)

    expect(reused.created).toBe(created.created)
    expect(reused.config).toMatchObject(created.config)
    await second.close()
  })

  it('reports a typed contract error when ensure finds an incompatible named consumer', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const stream = `PROCESS_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const consumerName = `processor_${crypto.randomUUID().replaceAll('-', '_')}`
    const expectedSubject = uniqueSubject('processing-ensure-expected')
    const actualSubject = uniqueSubject('processing-ensure-actual')

    await manager.streams.add({
      name: stream,
      subjects: [expectedSubject, actualSubject],
      storage: StorageType.Memory,
    })
    await manager.consumers.add(stream, {
      durable_name: consumerName,
      ack_policy: AckPolicy.Explicit,
      deliver_policy: 'all',
      filter_subject: actualSubject,
    })
    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    const lease = processJetStream(
      runtime,
      {
        stream,
        consumer: { mode: 'ensure', name: consumerName },
        filter: expectedSubject,
        start: 'all',
        codec: natsCodecs.bytes,
      },
      async () => undefined
    )

    await expect(lease.ready).rejects.toMatchObject({ code: 'filter-mismatch' })
    await expect(lease.closed).rejects.toBeInstanceOf(JetStreamProcessorConfigurationError)
  })

  it('rejects binding an explicit-ack consumer whose filter does not match', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const stream = `PROCESS_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const consumerName = `processor_${crypto.randomUUID().replaceAll('-', '_')}`
    const expectedSubject = uniqueSubject('processing-expected')
    const actualSubject = uniqueSubject('processing-actual')

    await manager.streams.add({
      name: stream,
      subjects: [expectedSubject, actualSubject],
      storage: StorageType.Memory,
    })
    await manager.consumers.add(stream, {
      durable_name: consumerName,
      ack_policy: AckPolicy.Explicit,
      deliver_policy: 'all',
      filter_subject: actualSubject,
    })
    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    const lease = processJetStream(
      runtime,
      {
        stream,
        consumer: { mode: 'bind', name: consumerName },
        filter: expectedSubject,
        start: 'all',
        codec: natsCodecs.bytes,
      },
      async () => undefined
    )

    await expect(lease.ready).rejects.toMatchObject({ code: 'filter-mismatch' })
    await expect(lease.closed).rejects.toBeInstanceOf(JetStreamProcessorConfigurationError)
  })

  it('rejects binding a consumer that does not use explicit acknowledgements', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const stream = `PROCESS_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const consumerName = `processor_${crypto.randomUUID().replaceAll('-', '_')}`
    const subject = uniqueSubject('processing-config')

    await manager.streams.add({ name: stream, subjects: [subject], storage: StorageType.Memory })
    await manager.consumers.add(stream, {
      durable_name: consumerName,
      ack_policy: AckPolicy.None,
      deliver_policy: 'all',
      filter_subject: subject,
    })
    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    const lease = processJetStream(
      runtime,
      {
        stream,
        consumer: { mode: 'bind', name: consumerName },
        filter: subject,
        start: 'all',
        codec: natsCodecs.bytes,
      },
      async () => undefined
    )

    await expect(lease.ready).rejects.toBeInstanceOf(JetStreamProcessorConfigurationError)
    await expect(lease.closed).rejects.toMatchObject({ code: 'ack-policy' })
  })

  it('deletes an owned consumer when its local lease closes', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const stream = `PROCESS_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const consumerName = `processor_${crypto.randomUUID().replaceAll('-', '_')}`
    const subject = uniqueSubject('processing-owned')

    await manager.streams.add({ name: stream, subjects: [subject], storage: StorageType.Memory })
    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    const lease = processJetStream(
      runtime,
      {
        stream,
        consumer: { mode: 'owned', name: consumerName },
        filter: subject,
        start: 'new',
        codec: natsCodecs.bytes,
      },
      async () => undefined
    )

    await lease.ready
    await expect(manager.consumers.info(stream, consumerName)).resolves.toBeDefined()
    await lease.close()
    await expect(manager.consumers.info(stream, consumerName)).rejects.toThrow()
  })

  it('applies the supported durable-consumer delivery and redelivery configuration', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const stream = `PROCESS_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const consumerName = `processor_${crypto.randomUUID().replaceAll('-', '_')}`
    const subject = uniqueSubject('processing-options')

    await manager.streams.add({ name: stream, subjects: [subject], storage: StorageType.Memory })
    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    const lease = processJetStream(
      runtime,
      {
        stream,
        consumer: { mode: 'ensure', name: consumerName },
        filter: subject,
        start: 'new',
        replayPolicy: ReplayPolicy.Original,
        ackWaitMs: 1_234,
        maxDeliver: 7,
        maxAckPending: 8,
        codec: natsCodecs.bytes,
      },
      async () => undefined
    )

    await lease.ready
    const info = await manager.consumers.info(stream, consumerName)
    expect(info.config).toMatchObject({
      replay_policy: ReplayPolicy.Original,
      ack_wait: 1_234_000_000,
      max_deliver: 7,
      max_ack_pending: 8,
    })
    await lease.close()
  })

  it('leaves a failed delivery unacknowledged so a bound processor receives the redelivery', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const client = jetstream(adminConnection)
    const stream = `PROCESS_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const consumerName = `processor_${crypto.randomUUID().replaceAll('-', '_')}`
    const subject = uniqueSubject('processing-redelivery')

    await manager.streams.add({ name: stream, subjects: [subject], storage: StorageType.Memory })
    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })
    await client.publish(subject, 'retry-me')

    const first = processJetStream(
      runtime,
      {
        stream,
        consumer: { mode: 'ensure', name: consumerName },
        filter: subject,
        start: 'all',
        ackWaitMs: 100,
        codec: natsCodecs.text,
      },
      async () => {
        throw new Error('processing failed')
      }
    )
    await first.ready
    await expect(first.closed).rejects.toThrow('processing failed')
    expect((await manager.consumers.info(stream, consumerName)).ack_floor.stream_seq).toBe(0)

    const redeliveries: Array<{ attempt: number; redelivered: boolean; value: string }> = []
    const second = processJetStream(
      runtime,
      {
        stream,
        consumer: { mode: 'bind', name: consumerName },
        filter: subject,
        start: 'all',
        codec: natsCodecs.text,
      },
      async (delivery) => {
        redeliveries.push({
          attempt: delivery.deliveryAttempt,
          redelivered: delivery.redelivered,
          value: delivery.value,
        })
      }
    )
    await second.ready
    await expect
      .poll(() => redeliveries)
      .toEqual([{ attempt: 2, redelivered: true, value: 'retry-me' }])
    await second.close()
  })
})
