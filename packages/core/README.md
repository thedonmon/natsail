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

Call `runtime.reconnect()` after an authenticator receives new credentials. A live connection starts a new handshake and calls the authenticator again. The promise resolves after the runtime observes the disconnect-to-connected cycle, so a subsequent publish does not race the offline socket.

The reconnect can interrupt in-flight messages and requests. Normal NATS reconnect settings still apply.

The retry policy accepts fixed or computed delays. It also accepts `shouldRetry` for error-specific retry decisions.

Call `runtime.inspect()` to read the current connection generation, resource reservations, and configured limits.

## Telemetry

Pass an optional synchronous `NatsailTelemetrySink` to observe counters, gauges, and durations without adding an observability dependency to Core:

```ts
import { createNatsRuntime, type NatsailTelemetryEvent } from '@natsail/core'

const runtime = createNatsRuntime({
  connect: connectToNats,
  telemetry: {
    record(event: NatsailTelemetryEvent) {
      measurementQueue.push(event)
    },
  },
  telemetryAttributes: { service: 'orders-api', region: 'us-west' },
})
```

The runtime reports connection attempts and recovery, publish/request duration and outcome, resource allocations, current reservations, configured limits, and slow-consumer signals. JetStream and Effect add their measurements through the same reporter.

Caller attributes must be primitive and low-cardinality. NATSail's internal attributes win on key collisions. Default events never contain subjects, payloads, credentials, session/checkpoint keys, stream names, or consumer names. Measurements do not enter `runtime.events`.

The sink runs inline. Enqueue work and avoid blocking I/O. Sink exceptions are ignored, but NATSail cannot preempt a blocking callback. Tests and deterministic hosts can supply `telemetryClock: { now }`.

Use [`@natsail/opentelemetry`](../opentelemetry/README.md) to send the same events to OpenTelemetry.

## Batching and cooperative work

`NatsailBatchPolicy<T>` supplies optional `maxItems`, `maxBytes`, and `maxWaitMs` bounds; at least one bound is required and byte policies require a finite, non-negative `sizeOf` result. `createNatsailBatcher()` serializes count, byte, time, explicit, and normal-completion flushes. `cancel()` discards only the pending partial batch and never interrupts an already applying batch.

`NatsailWorkBudget` combines `yieldAfterMs` with one injectable `NatsailScheduler` (`now`, `schedule`, and `yield`). `createNatsailWorkController()` lets serial reducer loops yield cooperatively without running reducer calls concurrently. The default scheduler uses the host monotonic clock and timers; tests can provide a manual scheduler.

`runtime.request()` uses the shared connection with a response codec or raw-message decoder and optional timeout, headers, and abort signal. It does not replay a request whose outcome may be ambiguous.

`runtime.connection()` exposes the runtime-owned nats.js connection for operations without a managed NATSail seam. Consumers must not close or drain that connection.

See the [Core NATS example](https://github.com/thedonmon/natsail#core-nats-example) and the [delivery model](https://github.com/thedonmon/natsail/blob/main/docs/DELIVERY.md).

## Shutdown and event buffering

`shutdownTimeoutMs` bounds runtime close, including drain (default 30 seconds). Expiry requests cancellation, force-closes the connection, and rejects with `NatsRuntimeShutdownTimeoutError`. Resource cleanup failures reject with `AggregateError`. Core handlers receive a third `{ signal }` argument for cooperative cancellation; direct lease close still waits for in-flight handling.

`maxBufferedEvents` bounds each runtime-event iterator (default 256). Slow iterators lose oldest events and receive an `event-buffer-overflow` diagnostic before retained events. `runtime.inspect()` supplies current state.

See the [production guide](https://github.com/thedonmon/natsail/blob/main/docs/PRODUCTION.md) for supported versions, worker tuning, and deployment checks.

## License

Apache-2.0
