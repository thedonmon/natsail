---
name: natsail-browser-broker
description: Use @natsail/browser-broker to share SessionSource leases and authorized publish/request operations across same-origin tabs through one SharedWorker, with bounded queues, cursor acknowledgement, credential refresh, and explicit fallback policy.
---

# Use @natsail/browser-broker

Install the broker beside Core and Session:

```sh
pnpm add @natsail/browser-broker @natsail/core @natsail/session
```

Create the NATS runtime and session registry inside the SharedWorker. The broker accepts logical operation names; worker callbacks must map those names to subjects or services allowed for the authenticated identity. Never forward an arbitrary tab string directly to NATS.

```ts
let runtime: NatsRuntime | undefined
const sessions = createSessionRegistry()
const runtimeForWorker = () => (runtime ??= createRuntime())

const broker = createBrowserBrokerWorker({
  sessions,
  createSource: ({ identity, descriptor, credentials, resumeAfter }) =>
    createAllowedSource(runtimeForWorker(), { identity, descriptor, credentials, resumeAfter }),
  publish: ({ identity, operation, data, credentials }) =>
    publishAllowedOperation(runtimeForWorker(), { identity, operation, data, credentials }),
  request: ({ identity, operation, data, credentials }) =>
    requestAllowedOperation(runtimeForWorker(), { identity, operation, data, credentials }),
  closeIdleResources: async () => {
    const active = runtime
    runtime = undefined
    await active?.close()
  },
})

self.onconnect = (event) => broker.connect(event.ports[0])
```

In each tab, create one client and turn descriptors into ordinary `SessionSource` values:

```ts
const client = await createBrowserBrokerClient({
  identity: { tenant: tenantId, authenticationContext: 'interactive-user-v1' },
  credentials: () => credentialStore.current(),
  connect: createSharedWorkerConnector(new URL('./nats-worker.ts', import.meta.url)),
  strict: true,
})

const source = client.createSource({ key: 'conversation-feed', contract: 'events:v2' })
await client.publish('send-message', encodedMessage)
const reply = await client.request('lookup-message', encodedQuery)
```

Keep descriptor contracts stable and include every option that changes delivery. Queue overflow rejects only the lagging tab with `BrowserBrokerResumeRequiredError`; rebuild that source from the error cursor. A reconnect resumes after the last fully accepted and acknowledged batch, so handlers still need normal at-least-once idempotence around a worker failure.

Call `refreshCredentials()` after a credential revision changes. Identity fields stay fixed for the life of a port, and credentials never belong in URLs, keys, contracts, logs, or telemetry. Close session handles before closing the client.

Use `strict: true` when a duplicate tab-local connection would break connection, consumer, or cost limits. Configure `createTabLocalBrokerConnector()` only as a deliberate fallback.

See the [browser broker guide](https://github.com/thedonmon/natsail/tree/main/packages/browser-broker#readme) and [delivery guarantees](https://github.com/thedonmon/natsail/blob/main/docs/DELIVERY.md).
