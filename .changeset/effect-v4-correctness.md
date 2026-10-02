---
'@natsail/effect': minor
---

Fix Stream and processor hangs, and add Effect v4 tracing, layer statics and a Schema codec.

- Stopping a subject, JetStream or processor Stream early (`Stream.take`, interruption, Layer shutdown) while the buffer is full no longer hangs. The producer is stopped before the subscription closes, and a JetStream delivery that never reached the Stream now rejects so its resume checkpoint cannot advance past it.
- `sessionSnapshots`, `sessionValues` and `jetStreamStates` now fail with the typed `NatsailSessionError` (`stage: 'acquire'`) when the session cannot be acquired, instead of never emitting.
- Invalid stream options now surface as a defect when the Stream runs, consistently through the service and the free functions. Previously `service.subscribe(options, { bufferSize: 0 })` threw at construction.
- Interrupting `runJetStreamProcessor` aborts the in-flight handler instead of waiting for it to finish.
- `makeNatsailScopedLayer` now closes the registry and runtime concurrently via `closeNatsResources`.
- Added `Natsail.layer` and `Natsail.layerScoped`, `natsSchemaCodec` (from `@natsail/effect/schema`), and OpenTelemetry-style spans for `publish`, `request` and each processor delivery.
