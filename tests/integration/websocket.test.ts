import { jetstream, jetstreamManager, StorageType } from '@nats-io/jetstream'
import { afterEach, describe, expect, it } from 'vitest'

import { createNatsRuntime, natsCodecs } from '@natsail/core'
import { consumeJetStream } from '@natsail/jetstream'

import { connectToTestNats, connectToTestNatsWebSocket, uniqueSubject } from './helpers.js'

describe('WebSocket transport', () => {
  const closeAfterTest: Array<() => Promise<void>> = []

  afterEach(async () => {
    await Promise.allSettled(closeAfterTest.splice(0).map((close) => close()))
  })

  it('delivers Core NATS messages through the browser-compatible transport', async () => {
    const publisher = await connectToTestNats()
    const runtime = createNatsRuntime({ connect: connectToTestNatsWebSocket })
    closeAfterTest.push(async () => {
      await runtime.close()
      await publisher.drain()
    })

    const subject = uniqueSubject('websocket')
    const received: string[] = []
    const lease = runtime.subscribe(
      {
        subject,
        codec: natsCodecs.text,
      },
      async (value) => {
        received.push(value)
      }
    )

    await lease.ready
    const connection = await runtime.connection()
    await connection.flush()
    publisher.publish(subject, 'over-websocket')
    await publisher.flush()
    await expect.poll(() => received).toEqual(['over-websocket'])
  })

  it('uses one browser-compatible WebSocket for 64 bounded consumers', async () => {
    const adminConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const client = jetstream(adminConnection)
    const stream = `TEST_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const subjectPrefix = uniqueSubject('websocket-conversations')
    const conversationCount = 64
    const perConversationBuffer = 2

    await manager.streams.add({
      name: stream,
      subjects: [`${subjectPrefix}.*`],
      storage: StorageType.Memory,
    })

    let connectionRequests = 0
    const runtime = createNatsRuntime({
      connect: async () => {
        connectionRequests += 1
        return connectToTestNatsWebSocket()
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
    const leases = Array.from({ length: conversationCount }, (_, index) =>
      consumeJetStream(
        runtime,
        {
          stream,
          filter: `${subjectPrefix}.${index}`,
          start: 'new',
          maxBufferedMessages: perConversationBuffer,
          codec: natsCodecs.text,
        },
        async (delivery) => {
          received.add(delivery.value)
        }
      )
    )

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
  })
})
