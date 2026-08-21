import { AckPolicy, jetstream, jetstreamManager, StorageType, type JsMsg } from '@nats-io/jetstream'
import { afterEach, describe, expect, it } from 'vitest'

import { createMemoryCheckpointStore } from '@natsail/checkpoints'
import { createNatsRuntime } from '@natsail/core'
import { consumeJetStream, JetStreamResumeError } from '@natsail/jetstream'

import { connectToTestNats, uniqueSubject } from './helpers.js'

describe('JetStream runtime adapter', () => {
  const closeAfterTest: Array<() => Promise<void>> = []

  afterEach(async () => {
    await Promise.allSettled(closeAfterTest.splice(0).map((close) => close()))
  })

  it('replays, stays live, and resumes strictly after a stream cursor', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const client = jetstream(adminConnection)
    const stream = `TEST_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const subject = uniqueSubject('jetstream')

    await manager.streams.add({
      name: stream,
      subjects: [subject],
      storage: StorageType.Memory,
    })

    let connectionRequests = 0
    const runtime = createNatsRuntime({
      connect: async () => {
        connectionRequests += 1
        return runtimeConnection
      },
    })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    const encoder = new TextEncoder()
    const decoder = new TextDecoder()
    await client.publish(subject, encoder.encode('stored'))

    const received: Array<{ sequence: number; value: string }> = []
    const lease = consumeJetStream(
      runtime,
      {
        stream,
        filter: subject,
        start: 'all',
        decode: (message) => decoder.decode(message.data),
      },
      async (delivery) => {
        received.push({
          sequence: delivery.cursor.sequence,
          value: delivery.value,
        })
      }
    )

    await lease.ready
    const orderedConsumers = await manager.consumers.list(stream).next()
    expect(orderedConsumers).toHaveLength(1)
    expect(orderedConsumers[0]!.config.ack_policy).toBe(AckPolicy.None)
    await client.publish(subject, encoder.encode('live'))

    await expect
      .poll(() => received)
      .toEqual([
        { sequence: 1, value: 'stored' },
        { sequence: 2, value: 'live' },
      ])
    expect(connectionRequests).toBe(1)

    await lease.close()

    await client.publish(subject, encoder.encode('while-closed'))
    const resumed = consumeJetStream(
      runtime,
      {
        stream,
        filter: subject,
        start: { after: 2 },
        decode: (message) => decoder.decode(message.data),
      },
      async (delivery) => {
        received.push({
          sequence: delivery.cursor.sequence,
          value: delivery.value,
        })
      }
    )

    await resumed.ready
    await expect
      .poll(() => received)
      .toEqual([
        { sequence: 1, value: 'stored' },
        { sequence: 2, value: 'live' },
        { sequence: 3, value: 'while-closed' },
      ])
    await runtime.close()
    await resumed.closed
    const streamInfo = await manager.streams.info(stream)
    expect(streamInfo.state.consumer_count).toBe(0)
  })

  it('keeps filtered checkpoints in global stream-sequence units', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const client = jetstream(adminConnection)
    const stream = `TEST_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const prefix = uniqueSubject('filtered-sequence')
    const selectedSubject = `${prefix}.selected`
    const otherSubject = `${prefix}.other`

    await manager.streams.add({
      name: stream,
      subjects: [`${prefix}.>`],
      storage: StorageType.Memory,
    })

    const checkpoints = createMemoryCheckpointStore()
    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    const encoder = new TextEncoder()
    const decoder = new TextDecoder()
    const first = await client.publish(selectedSubject, encoder.encode('selected-one'))
    await client.publish(otherSubject, encoder.encode('other-two'))
    const third = await client.publish(selectedSubject, encoder.encode('selected-three'))
    const received: Array<{ sequence: number; value: string }> = []
    const options = {
      stream,
      filter: selectedSubject,
      start: 'all' as const,
      resume: {
        key: 'conversation:filtered-sequence',
        store: checkpoints,
      },
      decode: (message: JsMsg) => decoder.decode(message.data),
    }
    const consume = () =>
      consumeJetStream(runtime, options, ({ cursor, value }) => {
        received.push({ sequence: cursor.sequence, value })
      })

    const initial = consume()
    await initial.ready
    await expect
      .poll(() => received)
      .toEqual([
        { sequence: first.seq, value: 'selected-one' },
        { sequence: third.seq, value: 'selected-three' },
      ])
    await initial.close()

    await client.publish(otherSubject, encoder.encode('other-four'))
    const fifth = await client.publish(selectedSubject, encoder.encode('selected-five'))
    const resumed = consume()
    await resumed.ready
    await expect
      .poll(() => received.at(-1))
      .toEqual({
        sequence: fifth.seq,
        value: 'selected-five',
      })
    await expect
      .poll(async () => (await checkpoints.load('conversation:filtered-sequence'))?.sequence)
      .toBe(fifth.seq)
    await resumed.close()
  })

  it('resumes from a checkpoint and saves only after processing succeeds', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const client = jetstream(adminConnection)
    const stream = `TEST_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const subject = uniqueSubject('checkpoint')

    await manager.streams.add({
      name: stream,
      subjects: [subject],
      storage: StorageType.Memory,
    })
    const streamInfo = await manager.streams.info(stream)
    const checkpoints = createMemoryCheckpointStore()
    await checkpoints.save('conversation:one', {
      stream,
      epoch: streamInfo.created,
      sequence: 1,
    })

    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    const encoder = new TextEncoder()
    const decoder = new TextDecoder()
    await client.publish(subject, encoder.encode('already-processed'))
    await client.publish(subject, encoder.encode('next'))

    let finishProcessing!: () => void
    const processing = new Promise<void>((resolve) => {
      finishProcessing = resolve
    })
    closeAfterTest.push(async () => finishProcessing())
    const received: string[] = []
    const lease = consumeJetStream(
      runtime,
      {
        stream,
        filter: subject,
        start: 'all',
        resume: {
          key: 'conversation:one',
          store: checkpoints,
        },
        decode: (message) => decoder.decode(message.data),
      },
      async (delivery) => {
        received.push(delivery.value)
        await processing
      }
    )

    await lease.ready
    await expect.poll(() => received).toEqual(['next'])
    expect(await checkpoints.load('conversation:one')).toEqual({
      stream,
      epoch: streamInfo.created,
      sequence: 1,
    })

    finishProcessing()
    await expect.poll(async () => (await checkpoints.load('conversation:one'))?.sequence).toBe(2)
    await lease.close()
  })

  it('reports a retention gap or continues from retained data by policy', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const client = jetstream(adminConnection)
    const stream = `TEST_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const subject = uniqueSubject('retention-gap')

    await manager.streams.add({
      name: stream,
      subjects: [subject],
      storage: StorageType.Memory,
    })
    const encoder = new TextEncoder()
    await client.publish(subject, encoder.encode('one'))
    await client.publish(subject, encoder.encode('two'))
    await client.publish(subject, encoder.encode('three'))
    await manager.streams.purge(stream, { keep: 1 })
    const streamInfo = await manager.streams.info(stream)
    expect(streamInfo.state.first_seq).toBe(3)

    const checkpoints = createMemoryCheckpointStore()
    await checkpoints.save('conversation:gap', {
      stream,
      epoch: streamInfo.created,
      sequence: 1,
    })
    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    const lease = consumeJetStream(
      runtime,
      {
        stream,
        filter: subject,
        start: 'all',
        resume: {
          key: 'conversation:gap',
          store: checkpoints,
        },
        decode: (message) => message.data,
      },
      async () => undefined
    )

    await expect(lease.ready).rejects.toBeInstanceOf(JetStreamResumeError)
    await expect(lease.closed).rejects.toMatchObject({
      code: 'retention-gap',
      checkpointSequence: 1,
      firstAvailableSequence: 3,
    })
    expect((await manager.streams.info(stream)).state.consumer_count).toBe(0)

    const received: string[] = []
    const continued = consumeJetStream(
      runtime,
      {
        stream,
        filter: subject,
        start: 'all',
        resume: {
          key: 'conversation:gap',
          store: checkpoints,
          retentionGapPolicy: 'continue',
        },
        decode: (message) => new TextDecoder().decode(message.data),
      },
      async (delivery) => {
        received.push(delivery.value)
      }
    )

    await continued.ready
    await expect.poll(() => received).toEqual(['three'])
    await expect.poll(async () => (await checkpoints.load('conversation:gap'))?.sequence).toBe(3)
    await continued.close()
  })
})
