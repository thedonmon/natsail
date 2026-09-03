---
name: natsail-core
description: Use @natsail/core in an application for one managed NATS connection, Core publish and subscribe, request/reply, payload codecs, connection retry or reauthentication, runtime diagnostics, and resource limits. Use natsail-jetstream when messages must replay.
---

# Use @natsail/core

Install Core and an official transport that matches the host:

```sh
# Node.js
pnpm add @natsail/core @nats-io/transport-node

# Browser WebSocket applications
pnpm add @natsail/core @nats-io/nats-core
```

## Create one runtime

```ts
import { connect } from '@nats-io/transport-node'
import { createNatsRuntime, natsCodecs } from '@natsail/core'

const runtime = createNatsRuntime({
  connect: () => connect({ servers: 'nats://127.0.0.1:4222' }),
  initialConnectRetry: {
    maxAttempts: 3,
    delayMs: 500,
  },
})
```

The connection is lazy and shared by subscriptions, publish, request/reply, and optional NATSail adapters. In a browser, replace the connection factory with `wsconnect({ servers: 'wss://…' })`.

## Use codecs at payload boundaries

```ts
const messages = runtime.subscribe(
  { subject: 'chat.room.123', codec: natsCodecs.text },
  async (text) => {
    await renderMessage(text)
  }
)

await messages.ready
await runtime.publish('chat.room.123', 'hello')

const requestCodec = natsCodecs.json<{ id: string }>()
const response = await runtime.request({
  subject: 'users.lookup',
  data: requestCodec.encode({ id: '123' }),
  codec: natsCodecs.json<{ name: string }>(),
  timeoutMs: 2_000,
})
```

Use `natsCodecs.text`, `bytes`, or `json<T>()`, or implement `NatsPayloadCodec<T>`. Supply exactly one of `codec` and `decode`. Reserve `decode(message)` for cases that require subject, headers, reply subject, or other raw-message metadata.

## Handle lifecycle and failures

- Core subscriptions are live-only and handlers run serially. A decode or handler failure rejects the lease's `closed` promise.
- `ready` means the subscription is registered; `closed` settles when delivery ends. Call `close()` when the subscription owner is done.
- A request supports timeout, headers, and `AbortSignal`. NATSail does not retry a request after it may have reached a responder.
- Configure `initialConnectRetry` for bounded startup attempts. After rotating credentials, call `runtime.reconnect()` so the authenticator runs again; do not add an application reconnect loop.
- Consume `runtime.events` for connection state and diagnostics. Use `runtime.inspect()` for current resource use, and configure `limits` when the application needs connection-wide JetStream consumer or buffer budgets.
- `runtime.connection()` is an escape hatch for nats.js operations NATSail does not wrap. Never close or drain that connection.
- Close the application-owned runtime during shutdown; it closes managed resources and drains its connection.

See the [Core package guide](https://github.com/thedonmon/natsail/tree/main/packages/core#readme) and [npm package](https://www.npmjs.com/package/@natsail/core).
