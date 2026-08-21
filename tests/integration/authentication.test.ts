import { afterEach, describe, expect, it } from 'vitest'

import { createNatsRuntime } from '@natsail/core'

import {
  connectToJwtTestNats,
  connectToNkeyTestNats,
  connectToTlsTestNats,
  connectToTokenTestNats,
  connectToUserPasswordTestNats,
  uniqueSubject,
} from './helpers.js'

describe('authenticated connection factories', () => {
  const closeAfterTest: Array<() => Promise<void>> = []

  afterEach(async () => {
    await Promise.allSettled(closeAfterTest.splice(0).map((close) => close()))
  })

  it('uses a token-authenticated connection for runtime delivery', async () => {
    const runtime = createNatsRuntime({ connect: connectToTokenTestNats })
    closeAfterTest.push(() => runtime.close())

    const subject = uniqueSubject('token-auth')
    const received: string[] = []
    const lease = runtime.subscribe(
      {
        subject,
        decode: (message) => new TextDecoder().decode(message.data),
      },
      async (value) => {
        received.push(value)
      }
    )

    await lease.ready
    await runtime.publish(subject, new TextEncoder().encode('authenticated'))
    const connection = await runtime.connection()
    await connection.flush()
    await expect.poll(() => received).toEqual(['authenticated'])
  })

  it('uses a user/password connection for runtime delivery', async () => {
    const runtime = createNatsRuntime({ connect: connectToUserPasswordTestNats })
    closeAfterTest.push(() => runtime.close())

    const subject = uniqueSubject('user-password-auth')
    const received: string[] = []
    const lease = runtime.subscribe(
      {
        subject,
        decode: (message) => new TextDecoder().decode(message.data),
      },
      async (value) => {
        received.push(value)
      }
    )

    await lease.ready
    await runtime.publish(subject, new TextEncoder().encode('authenticated'))
    const connection = await runtime.connection()
    await connection.flush()
    await expect.poll(() => received).toEqual(['authenticated'])
  })

  it('uses an NKey-authenticated connection for runtime delivery', async () => {
    const runtime = createNatsRuntime({ connect: connectToNkeyTestNats })
    closeAfterTest.push(() => runtime.close())

    const subject = uniqueSubject('nkey-auth')
    const received: string[] = []
    const lease = runtime.subscribe(
      {
        subject,
        decode: (message) => new TextDecoder().decode(message.data),
      },
      async (value) => {
        received.push(value)
      }
    )

    await lease.ready
    await runtime.publish(subject, new TextEncoder().encode('authenticated'))
    const connection = await runtime.connection()
    await connection.flush()
    await expect.poll(() => received).toEqual(['authenticated'])
  })

  it('uses a CA-verified TLS connection for runtime delivery', async () => {
    const runtime = createNatsRuntime({ connect: connectToTlsTestNats })
    closeAfterTest.push(() => runtime.close())

    const subject = uniqueSubject('tls')
    const received: string[] = []
    const lease = runtime.subscribe(
      {
        subject,
        decode: (message) => new TextDecoder().decode(message.data),
      },
      async (value) => {
        received.push(value)
      }
    )

    await lease.ready
    await runtime.publish(subject, new TextEncoder().encode('encrypted'))
    const connection = await runtime.connection()
    await connection.flush()
    await expect.poll(() => received).toEqual(['encrypted'])
  })

  it('uses an operator JWT connection for runtime delivery', async () => {
    const runtime = createNatsRuntime({ connect: connectToJwtTestNats })
    closeAfterTest.push(() => runtime.close())

    const subject = uniqueSubject('jwt-auth')
    const received: string[] = []
    const lease = runtime.subscribe(
      {
        subject,
        decode: (message) => new TextDecoder().decode(message.data),
      },
      async (value) => {
        received.push(value)
      }
    )

    await lease.ready
    await runtime.publish(subject, new TextEncoder().encode('authenticated'))
    const connection = await runtime.connection()
    await connection.flush()
    await expect.poll(() => received).toEqual(['authenticated'])
  })
})
