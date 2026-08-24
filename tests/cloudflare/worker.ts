import { type NatsConnection, wsconnect } from '@nats-io/nats-core'
import { connect as tcpConnect } from '@nats-io/transport-node'

import { createNatsRuntime, natsCodecs } from '../../packages/core/src/index.js'

const waitFor = async (predicate: () => boolean, description: string): Promise<void> => {
  const deadline = Date.now() + 5_000

  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error(`Timed out waiting for ${description}`)
    }

    await new Promise((resolve) => setTimeout(resolve, 25))
  }
}

const probeConnection = async (
  transport: 'tcp' | 'websocket',
  connect: () => Promise<NatsConnection>
): Promise<Response> => {
  let connectionRequests = 0
  const runtime = createNatsRuntime({
    connect: async () => {
      connectionRequests += 1
      return connect()
    },
  })
  const subject = `tests.cloudflare.${crypto.randomUUID()}`
  let received: string | undefined
  const lease = runtime.subscribe(
    {
      subject,
      codec: natsCodecs.text,
    },
    async (value) => {
      received = value
    }
  )

  try {
    await lease.ready
    const connection = await runtime.connection()
    await connection.flush()
    const expected = `cloudflare-${transport}`
    await runtime.publish(subject, expected)
    await connection.flush()
    await waitFor(() => received !== undefined, 'the Cloudflare Worker delivery')

    return Response.json({
      connectionRequests,
      received,
      transport,
      userAgent: navigator.userAgent,
      webSocketType: typeof WebSocket,
    })
  } finally {
    await lease.close()
    await runtime.close()
  }
}

export default {
  async fetch(request: Request): Promise<Response> {
    const { pathname } = new URL(request.url)

    if (pathname === '/health') {
      return new Response('ok')
    }

    if (pathname === '/probe') {
      return probeConnection('websocket', () =>
        wsconnect({ servers: 'ws://127.0.0.1:9223', timeout: 2_000 })
      )
    }

    if (pathname === '/probe-tcp') {
      return probeConnection('tcp', () =>
        tcpConnect({ servers: 'nats://127.0.0.1:4223', timeout: 2_000 })
      )
    }

    return new Response('Not found', { status: 404 })
  },
}
