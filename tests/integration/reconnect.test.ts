import { jetstream, jetstreamManager, StorageType } from '@nats-io/jetstream'
import type { NatsConnection } from '@nats-io/nats-core'
import { afterEach, describe, expect, it } from 'vitest'

import { createMemoryCheckpointStore } from '@natsail/checkpoints'
import { createNatsRuntime, type NatsRuntimeEvent } from '@natsail/core'
import { consumeJetStream } from '@natsail/jetstream'

import { connectToTestNats, uniqueSubject } from './helpers.js'

async function forceReconnect(
  connection: NatsConnection,
  onDisconnect?: () => void | Promise<void>
): Promise<readonly string[]> {
  const statuses: string[] = []
  const observed = (async () => {
    for await (const status of connection.status()) {
      statuses.push(status.type)
      if (status.type === 'disconnect') await onDisconnect?.()
      if (status.type === 'reconnect') return
    }
  })()

  await connection.reconnect()
  await observed
  return statuses
}

describe('forced reconnect recovery', () => {
  const closeAfterTest: Array<() => Promise<void>> = []

  afterEach(async () => {
    await Promise.allSettled(closeAfterTest.splice(0).map((close) => close()))
  })

  it('restores a Core NATS subscription on the shared runtime connection', async () => {
    const connection = await connectToTestNats()
    const runtime = createNatsRuntime({ connect: async () => connection })
    closeAfterTest.push(() => runtime.close())

    const encoder = new TextEncoder()
    const decoder = new TextDecoder()
    const subject = uniqueSubject('core-reconnect')
    const received: string[] = []
    const lease = runtime.subscribe(
      {
        subject,
        decode: (message) => decoder.decode(message.data),
      },
      async (value) => {
        received.push(value)
      }
    )

    await lease.ready
    await runtime.publish(subject, encoder.encode('before'))
    await connection.flush()
    await expect.poll(() => received).toEqual(['before'])

    const statuses = await forceReconnect(connection)
    expect(statuses).toEqual(expect.arrayContaining(['forceReconnect', 'disconnect', 'reconnect']))

    await runtime.publish(subject, encoder.encode('after'))
    await connection.flush()
    await expect.poll(() => received).toEqual(['before', 'after'])
  })

  it('continues ordered JetStream delivery across a forced disconnect', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const client = jetstream(adminConnection)
    const stream = `TEST_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const subject = uniqueSubject('jetstream-reconnect')

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

    const checkpoints = createMemoryCheckpointStore()
    const encoder = new TextEncoder()
    const decoder = new TextDecoder()
    await client.publish(subject, encoder.encode('before'))

    const received: Array<{ duplicate: boolean; sequence: number; value: string }> = []
    const lease = consumeJetStream(
      runtime,
      {
        stream,
        filter: subject,
        start: 'all',
        resume: { key: 'reconnect', store: checkpoints },
        decode: (message) => decoder.decode(message.data),
      },
      async (delivery) => {
        received.push({
          duplicate: delivery.duplicate,
          sequence: delivery.cursor.sequence,
          value: delivery.value,
        })
      }
    )

    await lease.ready
    await expect.poll(() => received).toEqual([{ duplicate: false, sequence: 1, value: 'before' }])

    const statuses = await forceReconnect(runtimeConnection, async () => {
      await client.publish(subject, encoder.encode('during'))
    })
    expect(statuses).toEqual(expect.arrayContaining(['forceReconnect', 'disconnect', 'reconnect']))
    await client.publish(subject, encoder.encode('after'))

    await expect
      .poll(() => received)
      .toEqual([
        { duplicate: false, sequence: 1, value: 'before' },
        { duplicate: false, sequence: 2, value: 'during' },
        { duplicate: false, sequence: 3, value: 'after' },
      ])
    await expect.poll(async () => (await checkpoints.load('reconnect'))?.sequence).toBe(3)
  })

  it('reports connection state and structured reconnect diagnostics', async () => {
    const connection = await connectToTestNats()
    const runtime = createNatsRuntime({ connect: async () => connection })
    const events: NatsRuntimeEvent[] = []
    const observing = (async () => {
      for await (const event of runtime.events) events.push(event)
    })()

    await runtime.connection()
    await expect
      .poll(() => events.filter((event) => event.type === 'status').map((event) => event.state))
      .toEqual(['idle', 'connecting', 'connected'])

    await connection.reconnect()
    await expect
      .poll(() => events.filter((event) => event.type === 'status').map((event) => event.state), {
        timeout: 4_000,
      })
      .toEqual(['idle', 'connecting', 'connected', 'disconnected', 'reconnecting', 'connected'])
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'diagnostic',
        source: 'connection',
        code: 'forced-reconnect',
        level: 'info',
      })
    )

    await runtime.close()
    await observing
    expect(events.at(-1)).toEqual(expect.objectContaining({ type: 'status', state: 'closed' }))
  })

  it('reports ordered-consumer loss and recreation diagnostics', async () => {
    const adminConnection = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const client = jetstream(adminConnection)
    const stream = `TEST_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const subject = uniqueSubject('consumer-recreation')

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

    const events: NatsRuntimeEvent[] = []
    const observing = (async () => {
      for await (const event of runtime.events) events.push(event)
    })()
    const received: string[] = []
    const lease = consumeJetStream(
      runtime,
      {
        stream,
        filter: subject,
        start: 'new',
        decode: (message) => new TextDecoder().decode(message.data),
      },
      async (delivery) => {
        received.push(delivery.value)
      }
    )

    await lease.ready
    const consumers = await manager.consumers.list(stream).next()
    expect(consumers).toHaveLength(1)
    await manager.consumers.delete(stream, consumers[0]!.name)
    await forceReconnect(runtimeConnection)
    await client.publish(subject, new TextEncoder().encode('after-recreation'))

    await expect.poll(() => received, { timeout: 5_000 }).toEqual(['after-recreation'])
    await expect
      .poll(
        () =>
          events
            .filter((event) => event.type === 'diagnostic' && event.source === 'jetstream')
            .map((event) => event.code),
        { timeout: 5_000 }
      )
      .toEqual(expect.arrayContaining(['consumer-not-found', 'ordered-consumer-recreated']))

    await runtime.close()
    await observing
  })
})
