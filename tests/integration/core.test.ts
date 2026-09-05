import { afterEach, describe, expect, it } from 'vitest'

import { createNatsRuntime, natsCodecs } from '@natsail/core'

import { connectToTestNats, uniqueSubject } from './helpers.js'

describe('Core NATS runtime', () => {
  const closeAfterTest: Array<() => Promise<void>> = []

  afterEach(async () => {
    await Promise.allSettled(closeAfterTest.splice(0).map((close) => close()))
  })

  it('finishes buffered handlers and flushes their replies before runtime shutdown', async () => {
    const connection = await connectToTestNats()
    const peer = await connectToTestNats()
    const runtime = createNatsRuntime({ connect: async () => connection, shutdownTimeoutMs: 5_000 })
    closeAfterTest.push(
      () => runtime.close(),
      () => peer.close()
    )
    const subject = uniqueSubject('core.drain')
    const reply = uniqueSubject('core.replies')
    const replies: string[] = []
    peer.subscribe(reply, {
      callback: (error, message) => {
        if (!error) replies.push(message.string())
      },
    })
    let started!: () => void
    let release!: () => void
    const handling = new Promise<void>((resolve) => {
      started = resolve
    })
    const finish = new Promise<void>((resolve) => {
      release = resolve
    })
    const processed: string[] = []
    const lease = runtime.subscribe({ subject, codec: natsCodecs.text }, async (value, message) => {
      if (value === 'first') {
        started()
        await finish
      }
      message.respond(`handled:${value}`)
      processed.push(value)
    })
    try {
      await lease.ready
      await connection.flush()
      peer.publish(subject, 'first', { reply })
      peer.publish(subject, 'buffered', { reply })
      await peer.flush()
      await handling
      await connection.flush()
      const closing = runtime.close()
      release()
      await closing
      expect(processed).toEqual(['first', 'buffered'])
      await expect.poll(() => replies).toEqual(['handled:first', 'handled:buffered'])
      expect(connection.isClosed()).toBe(true)
    } finally {
      release()
    }
  })

  it('uses one connection for multiple subscriptions and delivers live messages', async () => {
    const connection = await connectToTestNats()
    let connectionRequests = 0
    const runtime = createNatsRuntime({
      connect: async () => {
        connectionRequests += 1
        return connection
      },
    })
    closeAfterTest.push(() => runtime.close())

    const firstSubject = uniqueSubject('core.first')
    const secondSubject = uniqueSubject('core.second')
    const received: string[] = []

    const first = runtime.subscribe(
      { subject: firstSubject, codec: natsCodecs.text },
      async (value) => {
        received.push(`first:${value}`)
      }
    )
    const second = runtime.subscribe(
      { subject: secondSubject, codec: natsCodecs.text },
      async (value) => {
        received.push(`second:${value}`)
      }
    )

    await Promise.all([first.ready, second.ready])
    await runtime.publish(firstSubject, 'one')
    await runtime.publish(secondSubject, 'two')
    await connection.flush()

    await expect.poll(() => received).toEqual(['first:one', 'second:two'])
    expect(connectionRequests).toBe(1)

    await first.close()
    await second.close()
  })
})
