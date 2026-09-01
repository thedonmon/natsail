# @natsail/effect

`@natsail/effect` gives Effect v4 programs scoped Core NATS subject Streams, typed operations and failures, and optional shared-session Streams over one NATSail runtime.

The current v4 adapter targets `effect@4.0.0-rc.112`. Install the matching Effect release candidate while this version is published under NATSail's prerelease tag.

```sh
pnpm add effect@4.0.0-rc.112 @natsail/core @natsail/session @natsail/effect
```

## Core subject Streams

Use `subscribe()` when every Effect consumer should own one ephemeral Core NATS subscription:

```ts
import { Effect, Stream } from 'effect'

import { natsCodecs } from '@natsail/core'
import { makeNatsailScopedLayer, subscribe } from '@natsail/effect'

const NatsLive = makeNatsailScopedLayer(
  Effect.sync(() => ({
    runtime: createNatsRuntime({ connect: connectToNats }),
    sessions: createSessionRegistry(),
  }))
)

const messages = subscribe(
  {
    subject: 'chat.room.*',
    queue: 'chat-workers',
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

The Stream is cold. Starting a consumer creates one subscription; completion, failure, or interruption closes it. Wildcard subjects, queue groups, codecs, and metadata-aware `decode(message)` functions use the same Core NATS options as `runtime.subscribe()`.

Subject Streams queue at most 32 decoded messages by default. Their overflow policy is explicit:

- `suspend` is the default and waits for downstream capacity before NATSail's awaited delivery handler returns.
- `error` fails with `NatsailStreamBufferOverflowError` rather than losing a message.
- `dropping` drops a new message when the buffer is full.
- `sliding` keeps the newest messages by dropping the oldest buffered message.

The bound is measured in messages, not bytes. Core NATS remains ephemeral and at-most-once: local backpressure cannot add server retention, acknowledgement, or replay. Use JetStream when an application cannot tolerate missed messages.

`NatsailSubjectError` distinguishes subscription creation, readiness, and source/decoding failure while retaining the original cause.

For sustained streams, compose the subscription with Effect's native chunk operators instead of accumulating the entire stream:

```ts
const processMessages = messages.pipe(
  Stream.rechunk(256),
  Stream.runForEachArray((batch) => persistBatch(batch))
)
```

`Stream.rechunk()` bounds each processing batch, and `runForEachArray()` invokes the application once per non-empty batch. Avoid `Stream.runCollect()` for unbounded subjects because it retains every message until the Stream completes.

## Service and request/reply

The same scoped Layer supplies the `Natsail` service for publish, request/reply, reconnect, connection inspection, and escape hatches:

```ts
const request = Effect.gen(function* () {
  const nats = yield* Natsail

  yield* nats.publish('jobs.created', encodedJob)

  return yield* nats.request({
    subject: 'jobs.lookup',
    data: encodedRequest,
    codec: natsCodecs.json<Job>(),
  })
}).pipe(Effect.provide(NatsLive))
```

An interrupted request aborts the underlying NATS request. `makeNatsailScopedLayer()` closes the session registry and runtime when the Layer scope exits. `makeNatsailLayer()` supplies application-owned objects without closing them.

## Shared sessions

`sessionSnapshots()` and `sessionValues()` remain available for application state that should share one validated `SessionRegistry` source. Each Effect consumer retains one registry handle, and the handle is released whenever the Stream completes, fails, or is interrupted.

Session Streams queue at most 32 snapshots by default. Overflow fails with `NatsailStreamBufferOverflowError`; applications may explicitly choose `dropping`, `sliding`, a different bound, or `unbounded`.

Session snapshot buffering happens after the registry updates. Use direct subject Streams when downstream Effect demand must reach the NATSail delivery handler.

NATSail continues to own connection and authentication recovery. The Effect adapter does not create a second connection or retry loop. The service exposes `runtime` and `sessions` for advanced nats.js, JetStream manager, processor, and custom session operations without leaving the Layer-owned lifecycle.

## License

Apache-2.0
