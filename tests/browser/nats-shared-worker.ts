/// <reference lib="webworker" />

import { wsconnect } from '@nats-io/nats-core'

import { createBrowserBrokerWorker } from '@natsail/browser-broker'
import { createNatsRuntime, natsCodecs, type NatsRuntime } from '@natsail/core'
import { createCoreSessionSource, createSessionRegistry } from '@natsail/session'

declare const __NATS_WS_URL__: string

const sessions = createSessionRegistry()
let runtime: NatsRuntime | undefined

const broker = createBrowserBrokerWorker({
  sessions,
  createSource: ({ descriptor }) => {
    runtime ??= createNatsRuntime({
      connect: async () => {
        const connection = await wsconnect({ servers: __NATS_WS_URL__, timeout: 2_000 })
        broker.reportConnection('opened')
        void connection.closed().finally(() => broker.reportConnection('closed'))
        void (async () => {
          for await (const status of connection.status()) {
            if (status.type === 'reconnect') broker.reportConnection('reconnected')
          }
        })()
        return connection
      },
    })
    const source = createCoreSessionSource(runtime, {
      subject: descriptor.key,
      codec: natsCodecs.text,
    })
    const encoder = new TextEncoder()
    return (accept) => source(async (value) => accept({ data: encoder.encode(value) }))
  },
  idleTeardownMs: 50,
})

const workerScope = self as unknown as SharedWorkerGlobalScope
workerScope.onconnect = (event) => broker.connect(event.ports[0]!)
