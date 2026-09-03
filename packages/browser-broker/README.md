# @natsail/browser-broker

`@natsail/browser-broker` lets same-origin tabs share physical NATSail `SessionSource` leases through a `SharedWorker`. It defines a versioned MessagePort protocol, per-tab delivery bounds, cursor acknowledgements, credential refresh, and explicit lag recovery.

## Worker host

Create the NATS runtime and session registry inside the SharedWorker. The source factory returns a normal NATSail `SessionSource`; two tabs that attach with the same tenant, authentication context, logical key, and contract therefore open one physical source.

```ts
/// <reference lib="webworker" />

import { createBrowserBrokerWorker } from '@natsail/browser-broker'
import { createSessionRegistry } from '@natsail/session'

const sessions = createSessionRegistry()
const broker = createBrowserBrokerWorker({
  sessions,
  createSource: ({ identity, descriptor, credentials, resumeAfter }) =>
    createApplicationSessionSource({
      tenant: identity.tenant,
      source: descriptor.key,
      credentials,
      resumeAfter,
    }),
  maxTabQueueItems: 256,
  maxTabQueueBytes: 1024 * 1024,
  maxRetainedItems: 1024,
  maxRetainedBytes: 4 * 1024 * 1024,
  idleTeardownMs: 250,
})

self.onconnect = (event) => broker.connect(event.ports[0])
```

`BrowserBrokerDelivery.data` is encoded bytes. A JetStream source should also supply its stream cursor. The broker retains a bounded source log and keeps at most one transferred batch in flight per tab. It copies each tab's batch into transferable `ArrayBuffer` values, then waits for an acknowledgement before sending another batch.

## Tab client

Credentials originate in the tab. Load them from the application's authenticated bootstrap state, send them during `hello`, and call `refreshCredentials()` after rotating them. Do not put credentials in a worker URL, source key, local storage, telemetry attribute, or diagnostic string.

```ts
import { createBrowserBrokerClient, createSharedWorkerConnector } from '@natsail/browser-broker'

const client = await createBrowserBrokerClient({
  identity: {
    tenant: authenticatedTenantId,
    authenticationContext: 'interactive-user-v1',
  },
  credentials: () => credentialStore.current(),
  // Keep the constructor and static URL together so Vite and similar bundlers
  // compile the worker as a module entry.
  connect: () =>
    new SharedWorker(new URL('./nats-worker.ts', import.meta.url), {
      name: 'application-nats',
      type: 'module',
    }).port,
  strict: true,
})

const source = client.createSource({
  key: 'conversation-feed',
  contract: 'conversation-events:v1',
})

const handle = sessions.acquire({
  key: 'conversation-feed',
  contract: 'browser-broker:conversation-events:v1',
  source,
})
```

Tenant and authentication context are immutable for a connected port and form part of physical source identity. Credentials do not: they have a monotonic revision and can be refreshed without changing logical identity. A conflicting contract for an active identity fails with `code: 'contract-mismatch'` instead of creating a second physical source.

## Lag and restart behavior

Queue limits apply independently to each tab and include its pending items plus its one in-flight batch. If either its item or encoded-byte limit is exceeded, only that tab receives `resume-required` with reason `lagged`; the broker never silently drops a reliable delivery. Recreate the application session from its last accepted cursor. Other tabs continue normally.

`client.reconnect()` opens a replacement worker port, repeats credential bootstrap, and reattaches every active source after its last acknowledged cursor. Message decoding or source failures remain explicit. The client also attempts this path after a MessagePort `messageerror`. The worker heartbeat sweep releases references for tabs that stop responding.

SharedWorker is not available in every browser or embedding mode. Non-strict applications can supply an explicit tab-local fallback that uses the same protocol and SessionSource contract:

```ts
const client = await createBrowserBrokerClient({
  identity,
  credentials,
  connect: createSharedWorkerConnector(workerUrl),
  fallback: createTabLocalBrokerConnector(() =>
    createBrowserBrokerWorker({ sessions: tabSessions, createSource: createTabSource })
  ),
})
```

Use `strict: true` when opening a separate per-tab runtime would violate connection, consumer, or cost limits.

## Protocol v1

Every message carries `protocol: 'natsail.browser-broker'` and `version: 1`. Tab commands are `hello`, `attach`, `detach`, `ack`, `refresh-credentials`, `restart`, `stats`, `heartbeat`, and `close`. Worker messages are `result`, `state`, and `batch`. `parseBrowserBrokerCommand()` and `parseBrowserBrokerMessage()` reject malformed fields and unsupported versions.

The protocol is same-origin transport, not authorization. The worker source factory must still enforce the authenticated tenant's allowed source mapping. Never accept an arbitrary NATS subject, stream, or consumer name from an untrusted page.

## Telemetry

Pass the Stage 1 sink to the worker and client. The package reports active tabs, physical sources, caller-reported upstream connections, aggregate queue item/byte depth, lag, fallback, source restart, connection reconnect, and worker replacement. Attributes contain only stable action/source dimensions; tenant IDs, auth contexts, credentials, source keys, contracts, stream names, and cursors are excluded.

The caller owns the upstream runtime, so call `broker.reportConnection('opened' | 'reconnected' | 'closed')` from its connection lifecycle to populate physical-connection telemetry accurately.

## Scope

This package is for same-origin browser tabs. It does not implement a remote gateway or Cloudflare Durable Object protocol.
