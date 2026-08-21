import { jetstream, jetstreamManager, StorageType } from '@nats-io/jetstream'
import { wsconnect } from '@nats-io/nats-core'

import { createNatsRuntime, type NatsRuntime } from '@natsail/core'
import { consumeJetStream, type JetStreamLease } from '@natsail/jetstream'

interface BrowserLoadResult {
  connectionRequests: number
  consumerCount: number
  receivedCount: number
  userAgent: string
}

declare global {
  interface Window {
    openNatsailBrowserConnection: () => Promise<number>
    runNatsailBrowserLoad: () => Promise<BrowserLoadResult>
  }
}

const waitFor = async (predicate: () => boolean, description: string): Promise<void> => {
  const deadline = Date.now() + 10_000

  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error(`Timed out waiting for ${description}`)
    }

    await new Promise((resolve) => setTimeout(resolve, 25))
  }
}

window.runNatsailBrowserLoad = async () => {
  const conversationCount = 64
  const perConversationBuffer = 2
  const stream = `BROWSER_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
  const subjectPrefix = `tests.browser.${crypto.randomUUID()}`
  const leases: JetStreamLease<unknown>[] = []
  let runtime: NatsRuntime | undefined
  let streamCreated = false
  let connectionRequests = 0

  try {
    runtime = createNatsRuntime({
      connect: async () => {
        connectionRequests += 1
        return wsconnect({ servers: 'ws://127.0.0.1:9223', timeout: 2_000 })
      },
      limits: {
        maxJetStreamConsumers: conversationCount,
        maxBufferedMessages: conversationCount * perConversationBuffer,
      },
    })

    const connection = await runtime.connection()
    const manager = await jetstreamManager(connection)
    const client = jetstream(connection)

    await manager.streams.add({
      name: stream,
      subjects: [`${subjectPrefix}.*`],
      storage: StorageType.Memory,
    })
    streamCreated = true

    const decoder = new TextDecoder()
    const received = new Set<string>()

    for (let index = 0; index < conversationCount; index += 1) {
      leases.push(
        consumeJetStream(
          runtime,
          {
            stream,
            filter: `${subjectPrefix}.${index}`,
            start: 'new',
            maxBufferedMessages: perConversationBuffer,
            decode: (message) => decoder.decode(message.data),
          },
          async (delivery) => {
            received.add(delivery.value)
          }
        )
      )
    }

    await Promise.all(leases.map((lease) => lease.ready))
    const consumerCount = (await manager.streams.info(stream)).state.consumer_count

    await Promise.all(
      Array.from({ length: conversationCount }, (_, index) =>
        client.publish(`${subjectPrefix}.${index}`, `conversation-${index}`)
      )
    )
    await waitFor(() => received.size === conversationCount, 'all browser deliveries')

    return {
      connectionRequests,
      consumerCount,
      receivedCount: received.size,
      userAgent: navigator.userAgent,
    }
  } finally {
    await Promise.allSettled(leases.map((lease) => lease.close()))

    if (runtime) {
      if (streamCreated) {
        const manager = await jetstreamManager(await runtime.connection())
        await manager.streams.delete(stream).catch(() => undefined)
      }

      await runtime.close()
    }
  }
}

window.openNatsailBrowserConnection = async () => {
  let connectionRequests = 0
  const runtime = createNatsRuntime({
    connect: async () => {
      connectionRequests += 1
      return wsconnect({ servers: 'ws://127.0.0.1:9223', timeout: 2_000 })
    },
  })

  try {
    await runtime.connection()
    return connectionRequests
  } finally {
    await runtime.close()
  }
}

document.documentElement.dataset.natsailReady = 'true'
