# @natsail/core

`@natsail/core` provides one shared NATS connection for managed subscriptions, publish, and request/reply. It also owns retry, cleanup, status, limits, and runtime diagnostics.

```sh
pnpm add @natsail/core @nats-io/transport-node
```

The application supplies the connection factory. Core NATS does not require JetStream.

## Payloads

NATSail owns the common byte conversion. Choose the payload contract at the subscription or request boundary:

```ts
import { natsCodecs } from '@natsail/core'

runtime.subscribe({ subject: 'chat.messages', codec: natsCodecs.text }, async (message) =>
  chat.append(message)
)

const result = await runtime.request({
  subject: 'users.lookup',
  data: natsCodecs.json<{ id: string }>().encode({ id: '123' }),
  codec: natsCodecs.json<{ name: string }>(),
})

await runtime.publish('chat.messages', 'hello')
```

`natsCodecs` includes `text`, `bytes`, and `json<T>()`. Implement `NatsPayloadCodec<T>` to use Protobuf, MessagePack, validation, or a different text policy without changing NATSail. The same codec works with Core NATS, JetStream, React, and RxJS.

Use the lower-level `decode(message)` option only when decoding depends on NATS metadata such as the subject, headers, or reply subject. Exactly one of `codec` or `decode` is required.

The runtime replaces a permanently closed connection by default. Set `connectionRecovery.onPermanentClose` to `wait` to defer replacement until the next caller.

Call `runtime.reconnect()` after an authenticator receives new credentials. A live connection starts a new handshake and calls the authenticator again.

The reconnect can interrupt in-flight messages and requests. Normal NATS reconnect settings still apply.

The retry policy accepts fixed or computed delays. It also accepts `shouldRetry` for error-specific retry decisions.

Call `runtime.inspect()` to read the current connection generation, resource reservations, and configured limits.

`runtime.request()` uses the shared connection with a response codec or raw-message decoder and optional timeout, headers, and abort signal. It does not replay a request whose outcome may be ambiguous.

`runtime.connection()` exposes the runtime-owned nats.js connection for operations without a managed NATSail seam. Consumers must not close or drain that connection.

See the [NATSail README](https://github.com/thedonmon/natsail#core-nats-example) for an example and delivery guarantees.

## License

Apache-2.0
