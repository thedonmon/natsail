import { afterEach, describe, expect, it } from 'vitest'

import { createNatsRuntime, natsCodecs } from '@natsail/core'

import { connectToTestNats, uniqueSubject } from './helpers.js'

describe('Core NATS runtime', () => {
  const closeAfterTest: Array<() => Promise<void>> = []

  afterEach(async () => {
    await Promise.allSettled(closeAfterTest.splice(0).map((close) => close()))
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
