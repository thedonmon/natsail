# @natsail/effect

`@natsail/effect` gives Effect v4 programs scoped Core NATS and JetStream Streams, typed operations and failures, and optional shared-session Streams over one NATSail runtime.

The current v4 adapter targets `effect@4.0.0-rc.112`. Install the matching Effect release candidate while this version is published under NATSail's prerelease tag.

```sh
pnpm add effect@4.0.0-rc.112 @natsail/core @natsail/jetstream @natsail/session @natsail/effect
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

## JetStream delivery

Use `jetStreamEvents()` when the application needs both ordered deliveries and the exact replay-to-live boundary. `jetStreamDeliveries()` exposes the same delivery path without the control event:

```ts
import { natsCodecs } from '@natsail/core'
import { jetStreamEvents } from '@natsail/effect'

const events = jetStreamEvents(
  {
    stream: 'CHAT',
    filter: 'chat.room.>',
    start: 'all',
    codec: natsCodecs.json<ChatEvent>(),
    maxBufferedMessages: 64,
    recovery: { delayMs: 500 },
  },
  {
    bufferSize: 256,
    overflowStrategy: 'suspend',
  }
)
```

The Stream is cold and scoped. NATSail owns the ordered consumer, checkpoint, duplicate policy, and package-level recovery. Effect owns downstream demand and structured cancellation. The default local Effect buffer is 32 events and suspends the awaited JetStream handler when full. Durable delivery deliberately supports only `suspend` and `error`; dropping or sliding would incorrectly allow the source checkpoint to advance past an event that was never admitted to the Effect Stream.

Delivery into the bounded Stream is the ordered-consumer acceptance boundary. If durable completion must mean that a business Effect succeeded—not merely that the event entered the Stream—use `runJetStreamProcessor()` and its explicit-ack consumer.

`maxBufferedMessages` or `maxBufferedBytes` controls the nats.js pull buffer. `bufferSize` controls the second, decoded Effect queue. Both bounds remain explicit.

## Atomic replay materialization

Use `jetStreamStates()` with a reducing session when Effect, React, or RxJS
should share one keyed JetStream consumer. Replay, reconnect, and the first
hydrated live state are immediate. Later cumulative live states are coalesced
to the latest value in one bounded presentation window:

```ts
import { Stream } from 'effect'

import { jetStreamStates } from '@natsail/effect'
import { defineReducingJetStreamSession } from '@natsail/jetstream'

const conversationDefinition = defineReducingJetStreamSession(
  runtime,
  `conversation:${conversationId}`,
  {
    stream: 'CHAT',
    filter: `chat.room.${conversationId}`,
    start: 'all',
    codec: chatEventCodec,
    recovery: {},
  },
  {
    scope: 'conversation:v1',
    initial: () => emptyConversation(),
    reduce: (state, delivery) => reduceConversation(state, delivery.value),
  }
)

const render = jetStreamStates(conversationDefinition, {
  liveBatchWithin: '16 millis',
}).pipe(
  Stream.filter((snapshot) => snapshot.phase === 'live'),
  Stream.runForEach((snapshot) => updateConversation(snapshot.data))
)
```

Every delivery still reaches the shared reducer serially. Coalescing only
limits downstream state notifications. Concurrent Effect consumers retain
separate registry handles but share the same source and latest cumulative
state. Set `liveBatchWithin` to zero to observe every reduced state.

Use `materializeJetStream()` instead when each Effect consumer should own a
cold JetStream source and the batch reducer itself needs typed Effect failures
or services. It avoids rendering every intermediate historical state, emits an
initial `replaying` state, silently reduces bounded replay batches, emits one
complete `live` state at catch-up, and then emits microbatched live updates:

```ts
import { Effect, Stream } from 'effect'

import { materializeJetStream } from '@natsail/effect'

const conversation = materializeJetStream(
  {
    stream: 'CHAT',
    filter: `chat.room.${roomId}`,
    start: 'all',
    codec: chatEventCodec,
    maxBufferedMessages: 64,
    recovery: {},
  },
  {
    initial: () => emptyConversation(),
    reduceBatch: (state, deliveries) =>
      Effect.sync(() => reduceConversationBatch(state, deliveries)),
  },
  {
    bufferSize: 256,
    batchPolicy: { maxItems: 256, maxWaitMs: 16 },
    workBudget: { yieldAfterMs: 4, scheduler },
  }
)

const render = conversation.pipe(Stream.runForEach((snapshot) => updateConversation(snapshot.data)))
```

`materializeNatsJetStreamEvents(events, materializer, options)` applies the same batching and atomic replay rules to an existing `NatsailJetStreamEvent` Stream. Use it when another component already owns event acquisition; `materializeJetStream()` remains the managed NATS source path.

`reduceBatch` is a native Effect. Its typed error and service requirements remain in the returned Stream type. Package-owned recovery can resume admitted events while the current materialized state remains alive. A fresh materializer rebuilds from replay, so `resume` is rejected until a state store can commit the materialized state and cursor atomically.

`batchSize` and `batchWithin` remain compatible aliases. The shared policy additionally supports byte bounds with `sizeOf`. Durable JetStream queues remain lossless: their only overflow modes are backpressure (`suspend`) or a typed `error`.

## Explicit-ack processing

Use `runJetStreamProcessor()` for durable work queues or other named consumer workflows:

```ts
import { runJetStreamProcessor } from '@natsail/effect'

const worker = runJetStreamProcessor(
  {
    stream: 'JOBS',
    consumer: { mode: 'ensure', name: 'email-workers' },
    filter: 'jobs.email',
    start: 'all',
    codec: emailJobCodec,
    maxAckPending: 32,
    recovery: { delayMs: 500 },
  },
  (delivery) => sendEmail(delivery.value)
)
```

The package waits for the handler Effect before the underlying processor acknowledges the message. Package-owned recovery reopens the named consumer after infrastructure failures while preserving its acknowledgement floor. Typed application failures remain terminal typed Effect failures; consumer-contract and final processor lifecycle failures use `NatsailJetStreamError`.

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

`jetStreamStates()` is the replay-aware presentation adapter for shared reducing JetStream definitions. `sessionSnapshots()` and `sessionValues()` remain available for other application state that should share one validated `SessionRegistry` source. Each Effect consumer retains one registry handle, and the handle is released whenever the Stream completes, fails, or is interrupted.

Session Streams queue at most 32 snapshots by default. Overflow fails with `NatsailStreamBufferOverflowError`; applications may explicitly choose `dropping`, `sliding`, a different bound, or `unbounded`.

When Core telemetry is enabled, `error` overflow paths report `natsail.buffer.signals` before the typed stream failure. The measurement contains only the stable source and overflow signal; it does not contain the subject, stream, or session key.

Session snapshot buffering happens after the registry updates. Use direct subject Streams when downstream Effect demand must reach the NATSail delivery handler.

NATSail continues to own connection, authentication, ordered-consumer recovery, and checkpoints. The Effect adapter does not create a second connection or retry loop. The service exposes `runtime` and `sessions` for JetStream manager calls and other advanced nats.js or custom session operations without leaving the Layer-owned lifecycle.

## License

Apache-2.0
