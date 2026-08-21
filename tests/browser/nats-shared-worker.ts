/// <reference lib="webworker" />

import { wsconnect } from '@nats-io/nats-core'

import { createNatsRuntime, type NatsRuntime, type SubscriptionLease } from '@natsail/core'

interface WorkerRequest {
  action: 'close' | 'publish' | 'stats' | 'subscribe'
  id: number
  subject?: string
  value?: string
}

interface ClientState {
  leases: Set<SubscriptionLease>
  port: MessagePort
}

const clients = new Set<ClientState>()
const decoder = new TextDecoder()
const encoder = new TextEncoder()
let connectionRequests = 0
let runtime: NatsRuntime | undefined

const getRuntime = (): NatsRuntime => {
  runtime ??= createNatsRuntime({
    connect: async () => {
      connectionRequests += 1
      return wsconnect({ servers: 'ws://127.0.0.1:9223', timeout: 2_000 })
    },
  })

  return runtime
}

const postResult = (port: MessagePort, id: number, result?: unknown): void => {
  port.postMessage({ id, result, type: 'result' })
}

const closeClient = async (client: ClientState): Promise<void> => {
  await Promise.allSettled(Array.from(client.leases, (lease) => lease.close()))
  clients.delete(client)

  if (clients.size === 0 && runtime) {
    await runtime.close()
    runtime = undefined
  }
}

const handleRequest = async (client: ClientState, request: WorkerRequest): Promise<void> => {
  const activeRuntime = getRuntime()

  switch (request.action) {
    case 'subscribe': {
      if (!request.subject) {
        throw new Error('A subject is required')
      }

      const subject = request.subject
      const lease = activeRuntime.subscribe(
        {
          subject,
          decode: (message) => decoder.decode(message.data),
        },
        async (value) => {
          client.port.postMessage({ subject, type: 'delivery', value })
        }
      )
      client.leases.add(lease)
      await lease.ready
      postResult(client.port, request.id)
      return
    }

    case 'publish':
      if (!request.subject || request.value === undefined) {
        throw new Error('A subject and value are required')
      }

      await activeRuntime.publish(request.subject, encoder.encode(request.value))
      postResult(client.port, request.id)
      return

    case 'stats':
      postResult(client.port, request.id, {
        clientCount: clients.size,
        connectionRequests,
        subscriptionCount: Array.from(clients).reduce(
          (total, current) => total + current.leases.size,
          0
        ),
      })
      return

    case 'close':
      await closeClient(client)
      postResult(client.port, request.id)
  }
}

const workerScope = self as unknown as SharedWorkerGlobalScope

workerScope.onconnect = (event) => {
  const port = event.ports[0]
  const client: ClientState = { leases: new Set(), port }
  clients.add(client)

  port.onmessage = (message: MessageEvent<WorkerRequest>) => {
    void handleRequest(client, message.data).catch((error: unknown) => {
      port.postMessage({
        error: error instanceof Error ? error.message : String(error),
        id: message.data.id,
        type: 'result',
      })
    })
  }
  port.start()
}
