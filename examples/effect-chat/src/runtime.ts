import { wsconnect } from '@nats-io/nats-core'
import { createNatsRuntime } from '@natsail/core'
import { makeNatsail } from '@natsail/effect'
import { createSessionRegistry } from '@natsail/session'

export const runtime = createNatsRuntime({
  connect: () => wsconnect({ servers: 'ws://127.0.0.1:9223', timeout: 2_000 }),
  initialConnectRetry: {
    maxAttempts: 3,
    delayMs: 250,
  },
})

export const sessions = createSessionRegistry({ idleCloseMs: 0 })
export const natsail = makeNatsail({ runtime, sessions })

let closePromise: Promise<void> | undefined

export const closeExampleRuntime = (): Promise<void> => {
  closePromise ??= Promise.allSettled([sessions.close(), runtime.close()]).then(() => undefined)
  return closePromise
}
