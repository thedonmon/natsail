# @natsail/jetstream

`@natsail/jetstream` adds ordered replay, application checkpoints, named explicit-ack processing, duplicate policies, and retention-gap handling to a NATSail runtime.

```sh
pnpm add @natsail/core @natsail/checkpoints @natsail/session @natsail/jetstream
```

`consumeJetStream()` uses an ordered consumer with `AckPolicy.None`. It saves the application checkpoint only after the handler succeeds.

`createJetStreamSessionSource()` adapts the consumer for one shared React and RxJS session. Each delivery retains its cursor and duplicate metadata.

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
