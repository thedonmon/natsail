/// <reference lib="webworker" />

import { wsconnect } from '@nats-io/nats-core'

import { createBrowserBrokerWorker } from '@natsail/browser-broker'
import { createNatsRuntime, natsCodecs, type NatsRuntime } from '@natsail/core'
import { createCoreSessionSource, createSessionRegistry } from '@natsail/session'

declare const __NATS_WS_URL__: string

const sessions = createSessionRegistry()
let runtime: NatsRuntime | undefined

const sourceSubjects = new Map([['acceptance-events', 'tests.browser-broker.acceptance.events']])
const publishSubjects = new Map([
  ['publish-acceptance-event', 'tests.browser-broker.acceptance.events'],
])
const requestSubjects = new Map([['echo', 'tests.browser-broker.acceptance.echo']])

const mappedSubject = (mapping: ReadonlyMap<string, string>, logicalName: string): string => {
  const subject = mapping.get(logicalName)
  if (!subject) throw new Error(`Browser broker operation ${logicalName} is not allowed`)
  return subject
}

const runtimeForWorker = (): NatsRuntime => {
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
  return runtime
}

const broker = createBrowserBrokerWorker({
  sessions,
  createSource: ({ descriptor }) => {
    const source = createCoreSessionSource(runtimeForWorker(), {
      subject: mappedSubject(sourceSubjects, descriptor.key),
      codec: natsCodecs.text,
    })
    const encoder = new TextEncoder()
    return (accept) => source(async (value) => accept({ data: encoder.encode(value) }))
  },
  publish: async ({ operation, data }) => {
    const activeRuntime = runtimeForWorker()
    await activeRuntime.publish(mappedSubject(publishSubjects, operation), data)
    await (await activeRuntime.connection()).flush()
  },
  request: async ({ operation, data }) => {
    const activeRuntime = runtimeForWorker()
    const connection = await activeRuntime.connection()
    const subject = mappedSubject(requestSubjects, operation)
    const responder = connection.subscribe(subject, {
      max: 1,
      callback: (_error, message) => {
        message.respond(message.data)
      },
    })
    try {
      await connection.flush()
      return await activeRuntime.request({ subject, data, codec: natsCodecs.bytes })
    } finally {
      responder.unsubscribe()
    }
  },
  closeIdleResources: async () => {
    const activeRuntime = runtime
    runtime = undefined
    await activeRuntime?.close()
  },
  idleTeardownMs: 50,
})

const workerScope = self as unknown as SharedWorkerGlobalScope
workerScope.onconnect = (event) => broker.connect(event.ports[0]!)
