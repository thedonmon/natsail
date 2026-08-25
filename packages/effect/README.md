# @natsail/effect

`@natsail/effect` gives Effect programs one scoped NATSail service, typed failures, cancellable event Streams, and reference-counted shared-session Streams.

```sh
pnpm add effect @natsail/core @natsail/session @natsail/effect
```

The package targets stable Effect v3. It does not create a second connection, retry loop, checkpoint system, or consumer lifecycle. NATSail still owns those policies; Effect owns dependency injection, structured concurrency, interruption, and scope finalization.

```ts
import { Effect, Stream } from 'effect'

import { createNatsRuntime } from '@natsail/core'
import { Natsail, makeNatsailScopedLayer } from '@natsail/effect'
import { createSessionRegistry } from '@natsail/session'

const NatsLive = makeNatsailScopedLayer(
  Effect.sync(() => ({
    runtime: createNatsRuntime({ connect: connectToNats }),
    sessions: createSessionRegistry({ idleCloseMs: 250 }),
  }))
)

const program = Effect.gen(function* () {
  const nats = yield* Natsail

  yield* nats.publish('chat.room.123', encodedMessage)

  yield* nats
    .sessionValues(conversationDefinition)
    .pipe(Stream.runForEach((conversation) => Effect.log(conversation)))
}).pipe(Effect.provide(NatsLive))
```

`makeNatsailScopedLayer()` closes the session registry and runtime when the Layer scope exits, including failure or fiber interruption. `makeNatsailLayer()` supplies application-owned objects without closing them.

`sessionSnapshots()` and `sessionValues()` are cold Streams. Each Effect consumer retains one registry handle; consumers of the same validated definition still share one underlying NATS source. The handle is released whenever the stream completes, fails, or is interrupted.

Session Streams queue at most 32 snapshots by default. Overflow fails with `NatsailStreamBufferOverflowError` instead of silently losing state. Applications may choose `dropping`, `sliding`, a different bound, or an explicit `unbounded` buffer.

`NatsailOperationError` covers Promise operations and event iterators. `NatsailSessionError` distinguishes session acquisition from source failure. Both retain the original cause for tagged Effect recovery.

The service exposes `runtime` and `sessions` as escape hatches. Advanced nats.js, JetStream manager, explicit-ack processor, and custom session operations remain available without leaving the Layer-owned lifecycle.

## License

Apache-2.0
