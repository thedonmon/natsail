---
name: natsail-effect
description: Use @natsail/effect to expose a NATSail runtime through an Effect service and Layer, consume scoped Core or JetStream Streams, control buffers and interruption, materialize replayed state, run explicit-ack processors, or share session Streams.
---

# Use @natsail/effect

The adapter currently targets the exact Effect v4 release candidate documented by NATSail and is published under the `next` tag:

```sh
pnpm add effect@4.0.0-rc.112 @natsail/core @natsail/jetstream @natsail/session @natsail/effect@next
```

Keep the installed Effect peer compatible with the version declared by `@natsail/effect`.

## Supply the Natsail service

Prefer a scoped Layer when Effect should own the runtime and registry:

```ts
import { Effect, Stream } from 'effect'

import { createNatsRuntime, natsCodecs } from '@natsail/core'
import { makeNatsailScopedLayer, subscribe } from '@natsail/effect'
import { createSessionRegistry } from '@natsail/session'

const NatsLive = makeNatsailScopedLayer(
  Effect.sync(() => ({
    runtime: createNatsRuntime({ connect: connectToNats }),
    sessions: createSessionRegistry(),
  }))
)
```

`makeNatsailScopedLayer()` closes the registry and runtime when its scope exits. `makeNatsailLayer()` wraps application-owned objects and does not close them. The `Natsail` service also exposes publish, request/reply, reconnect, runtime events/status, connection, and session diagnostics.

## Choose a Stream or processor

- `subscribe()` creates a cold scoped Core subject Stream.
- `jetStreamEvents()` includes deliveries plus the explicit caught-up event.
- `jetStreamDeliveries()` emits ordered replay/live deliveries only.
- `materializeJetStream()` folds replay, emits one hydrated live state at catch-up, then microbatches live updates.
- `sessionSnapshots()` and `sessionValues()` share validated registry definitions.
- `runJetStreamProcessor()` runs named explicit-ack work and waits for the handler Effect before acknowledgement.

```ts
const messages = subscribe(
  {
    subject: 'chat.room.*',
    codec: natsCodecs.json<ChatMessage>(),
  },
  {
    bufferSize: 256,
    overflowStrategy: 'suspend',
  }
)

const program = messages.pipe(
  Stream.runForEach((message) => Effect.log(message)),
  Effect.provide(NatsLive)
)
```

Core subject Streams may use `suspend`, `error`, `dropping`, or `sliding`. Reliable JetStream Streams allow only `suspend` or `error`; dropping after checkpoint acceptance would violate durable delivery. Keep the nats.js pull buffer (`maxBufferedMessages` or `maxBufferedBytes`) separate from the decoded Effect queue (`bufferSize`).

Queue admission completes an ordered-consumer handler. If business completion must control acknowledgement, use `runJetStreamProcessor()` instead. Application Effect errors remain typed and defects remain causes. Processor infrastructure failures surface as `NatsailJetStreamError` with stage `processor`; service operations use `NatsailOperationError` with their operation tag. Interruption propagates to the underlying NATS operation; do not add another retry loop around package-owned recovery.

Avoid collecting an unbounded subject Stream. Use Effect chunking or bounded processing for sustained sources. `materializeJetStream()` rejects cursor-only resume because materialized state and its cursor must be restored atomically.

See the [Effect package guide](https://github.com/thedonmon/natsail/tree/main/packages/effect#readme) and [npm package](https://www.npmjs.com/package/@natsail/effect).
