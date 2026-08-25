# @natsail/jetstream

`@natsail/jetstream` adds ordered replay, application checkpoints, named explicit-ack processing, duplicate policies, and retention-gap handling to a NATSail runtime.

```sh
pnpm add @natsail/core @natsail/checkpoints @natsail/session @natsail/jetstream
```

`consumeJetStream()` uses an ordered consumer with `AckPolicy.None`. It saves the application checkpoint only after the handler succeeds. Its lease exposes `caughtUp`, `inspect()`, and lifecycle notifications. Every delivery includes the server pending count and a stable `replay: 'initial' | 'live'` classification based on the backlog captured when the consumer opens.

`createJetStreamSessionSource()` adapts the consumer for one shared React and RxJS session. Set `recovery` to let the package replace a failed ordered consumer after its last successfully processed cursor. Permanent configuration, retention, decode, duplicate, and application-handler failures stay terminal by default.

Custom `recovery.delayMs` or `recovery.shouldRetry` functions require a stable `recovery.scope` when used in a validated definition. The scope prevents two callers from sharing a key while silently using different retry semantics.

`defineJetStreamSession()` combines that source with a validated contract. `defineReducingJetStreamSession()` also folds the initial replay without publishing partially assembled application state, emits one atomic live snapshot at catch-up, and then emits every serially reduced live state:

```ts
import { defineReducingJetStreamSession } from '@natsail/jetstream'

const conversation = defineReducingJetStreamSession(
  runtime,
  'conversation:123',
  {
    stream: 'CONVERSATIONS',
    filter: 'conversations.123.events',
    start: 'all',
    recovery: { delayMs: 500 },
    codec: natsCodecs.json<ConversationEvent>(),
  },
  {
    scope: 'conversation-view:v2',
    initial: () => emptyConversation,
    reduce: applyConversationEvent,
  }
)
```

The reduced snapshot reports `replaying`, `reconnecting`, or `live`, the last cursor, initial replay count, and package recovery count. React and RxJS can acquire the same definition without duplicating the consumer.

A reducing session does not accept `resume` yet. An event cursor is safe only when the matching materialized reducer state is restored atomically with it. Package-owned recovery preserves both within the active source lease; a fresh lease reconstructs state from an atomic replay.

Normal consumers select a package-owned payload codec instead of constructing text encoders or decoders:

```ts
import { natsCodecs } from '@natsail/core'
import { consumeJetStream } from '@natsail/jetstream'

consumeJetStream(
  runtime,
  {
    stream: 'CHAT',
    filter: 'chat.room.123',
    start: 'all',
    codec: natsCodecs.json<ChatMessage>(),
  },
  async ({ value, subject, cursor }) => chat.apply(value, subject, cursor)
)
```

The same `codec` option works with `processJetStream()`. Supply any `NatsPayloadCodec<T>` for another wire format. Use `decode(message)` only when the application needs the raw `JsMsg`; ordinary deliveries already include `subject`, cursor, duplicate, and redelivery metadata.

`processJetStream()` is the work-processing seam. `ensure` creates or reuses a retained named pull consumer, `bind` validates and attaches to an existing consumer, and `owned` deletes its named consumer before the lease closes. Every mode requires `AckPolicy.Explicit`; the package acknowledges only after the handler succeeds. Existing consumers must match the requested filter, start position, and any explicitly supplied acknowledgement or replay settings. A mismatch fails with `JetStreamProcessorConfigurationError` instead of consuming under a different server contract.

Configure `ackWaitMs`, `maxDeliver` (`-1` means unlimited), `maxAckPending`, replay policy, start position, and pull-buffer capacity. Invalid policies fail before a connection is acquired. A failed handler stops the lease without acknowledging its delivery, allowing server redelivery when the consumer is rebound or restarted.

Use `maxBufferedMessages` or `maxBufferedBytes` to bound the nats.js pull loop. These modes are mutually exclusive. The runtime reserves the selected capacity before it opens the consumer.

The checkpoint scope includes normalized filters. Set `resume.scope` when a codec, decoder, or domain-model change must invalidate an old checkpoint.

See the [NATSail README](https://github.com/thedonmon/natsail#explicit-ack-processing-example) for the explicit-ack example and the separate ordered-consumer acknowledgement boundary.

## License

Apache-2.0
