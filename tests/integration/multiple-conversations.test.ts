import { jetstream, jetstreamManager, StorageType } from '@nats-io/jetstream'
import { afterEach, describe, expect, it } from 'vitest'

import { createNatsRuntime, natsCodecs, NatsRuntimeLimitError } from '@natsail/core'
import { consumeJetStream } from '@natsail/jetstream'

import { connectToTestNats, uniqueSubject } from './helpers.js'

describe('multiple conversations in one runtime', () => {
  const closeAfterTest: Array<() => Promise<void>> = []

  afterEach(async () => {
    await Promise.allSettled(closeAfterTest.splice(0).map((close) => close()))
  })

  it('uses one connection for 64 bounded JetStream consumers', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const client = jetstream(adminConnection)
    const stream = `TEST_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const subjectPrefix = uniqueSubject('conversations')

    await manager.streams.add({
      name: stream,
      subjects: [`${subjectPrefix}.*`],
      storage: StorageType.Memory,
    })

    const conversationCount = 64
    const perConversationBuffer = 4
    let connectionRequests = 0
    const runtime = createNatsRuntime({
      connect: async () => {
        connectionRequests += 1
        return runtimeConnection
      },
      limits: {
        maxJetStreamConsumers: conversationCount,
        maxBufferedMessages: conversationCount * perConversationBuffer,
      },
    })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    const received = new Set<string>()
    const leases = Array.from({ length: conversationCount }, (_, index) => {
      const subject = `${subjectPrefix}.${index}`
      return consumeJetStream(
        runtime,
        {
          stream,
          filter: subject,
          start: 'new',
          maxBufferedMessages: perConversationBuffer,
          codec: natsCodecs.text,
        },
        async (value) => {
          received.add(value.value)
        }
      )
    })

    await Promise.all(leases.map((lease) => lease.ready))
    expect(connectionRequests).toBe(1)
    await expect
      .poll(async () => (await manager.streams.info(stream)).state.consumer_count)
      .toBe(conversationCount)

    await Promise.all(
      Array.from({ length: conversationCount }, (_, index) =>
        client.publish(`${subjectPrefix}.${index}`, `conversation-${index}`)
      )
    )

    await expect.poll(() => received.size).toBe(conversationCount)
    await runtime.close()
    await expect.poll(async () => (await manager.streams.info(stream)).state.consumer_count).toBe(0)
  })

  it('enforces and releases the connection-wide consumer budget', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const stream = `TEST_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const subjectPrefix = uniqueSubject('consumer-budget')

    await manager.streams.add({
      name: stream,
      subjects: [`${subjectPrefix}.*`],
      storage: StorageType.Memory,
    })

    const runtime = createNatsRuntime({
      connect: async () => runtimeConnection,
      limits: { maxJetStreamConsumers: 2 },
    })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    const open = (index: number) =>
      consumeJetStream(
        runtime,
        {
          stream,
          filter: `${subjectPrefix}.${index}`,
          start: 'new',
          maxBufferedMessages: 1,
          codec: natsCodecs.bytes,
        },
        async () => undefined
      )

    const first = open(1)
    const second = open(2)
    await Promise.all([first.ready, second.ready])

    let limitError: unknown
    try {
      open(3)
    } catch (error) {
      limitError = error
    }
    expect(limitError).toBeInstanceOf(NatsRuntimeLimitError)
    expect(limitError).toMatchObject({
      code: 'jetstream-consumers',
      limit: 2,
      used: 2,
      requested: 1,
    })

    await first.close()
    const replacement = open(3)
    await replacement.ready
    await expect.poll(async () => (await manager.streams.info(stream)).state.consumer_count).toBe(2)
  })

  it('enforces and releases the aggregate pull-buffer budget', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const stream = `TEST_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const subjectPrefix = uniqueSubject('buffer-budget')

    await manager.streams.add({
      name: stream,
      subjects: [`${subjectPrefix}.*`],
      storage: StorageType.Memory,
    })

    const runtime = createNatsRuntime({
      connect: async () => runtimeConnection,
      limits: { maxBufferedMessages: 5 },
    })
    closeAfterTest.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream)
      await adminConnection.drain()
    })

    const open = (index: number, maxBufferedMessages: number) =>
      consumeJetStream(
        runtime,
        {
          stream,
          filter: `${subjectPrefix}.${index}`,
          start: 'new',
          maxBufferedMessages,
          codec: natsCodecs.bytes,
        },
        async () => undefined
      )

    const fourMessages = open(1, 4)
    await fourMessages.ready

    let limitError: unknown
    try {
      open(2, 2)
    } catch (error) {
      limitError = error
    }
    expect(limitError).toBeInstanceOf(NatsRuntimeLimitError)
    expect(limitError).toMatchObject({
      code: 'buffered-messages',
      limit: 5,
      used: 4,
      requested: 2,
    })

    const oneMessage = open(2, 1)
    await oneMessage.ready
    await fourMessages.close()
    const replacement = open(3, 4)
    await replacement.ready
    await expect.poll(async () => (await manager.streams.info(stream)).state.consumer_count).toBe(2)
  })
})
