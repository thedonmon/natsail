---
'@natsail/effect': minor
---

Fix Stream and processor hangs, and add Effect v4 tracing, layer statics and a Schema codec.

Breaking:

- `makeNatsailLayer` and `makeNatsailScopedLayer` are removed. Use `Natsail.layer(resource)` and `Natsail.layerScoped(acquire)`.
- Invalid stream options now surface as a defect when the Stream runs, consistently through the service and the free functions. Previously `service.subscribe(options, { bufferSize: 0 })` threw at construction, so code that wrapped Stream construction in try/catch must handle the defect at run time (`Stream.catchCause` or `Effect.catchDefect`).

Fixes:

- Stopping a subject, JetStream or processor Stream early (`Stream.take`, interruption, Layer shutdown) while the buffer is full no longer hangs. The producer is stopped before the subscription closes, and a JetStream delivery that never reached the Stream now rejects so its resume checkpoint cannot advance past it.
- `sessionSnapshots`, `sessionValues` and `jetStreamStates` now fail with the typed `NatsailSessionError` (`stage: 'acquire'`) when the session cannot be acquired, and a defect during stream setup now fails the Stream instead of hanging it.
- Interrupting `runJetStreamProcessor` aborts the in-flight handler instead of waiting for it. The in-flight message is not acknowledged and redelivers after `ack_wait`, so handlers must be idempotent.
- Layer shutdown closes the registry and runtime concurrently via `closeNatsResources`.

Additions:

- `Natsail.layer` and `Natsail.layerScoped`.
- `natsSchemaCodec` from `@natsail/effect/schema`, a JSON payload codec backed by an Effect `Schema` (supports `Schema.Date`, `BigInt` and classes).
- Spans for `publish`, `request` and other operations, and a root `process <stream>` consumer span per processor delivery that links to the producer trace from a W3C `traceparent` header. Spans carry the subject as `messaging.destination.name` and are exported only when a tracer is installed.
