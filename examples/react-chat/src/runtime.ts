import { wsconnect } from '@nats-io/nats-core'
import { createNatsRuntime } from '@natsail/core'
import type { NatsManagedResource } from '@natsail/react'
import { createSessionRegistry } from '@natsail/session'

/** Created after React commits and closed by NatsManagedProvider. */
export const createExampleNatsResource = (): NatsManagedResource => {
  const runtime = createNatsRuntime({
    connect: () => wsconnect({ servers: 'ws://127.0.0.1:9223', timeout: 2_000 }),
    initialConnectRetry: {
      maxAttempts: 3,
      delayMs: 250,
    },
  })
  const sessions = createSessionRegistry({ idleCloseMs: 250 })
  return { runtime, sessions }
}
