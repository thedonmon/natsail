# @natsail/effect

## 0.7.0

### Minor Changes

- 53dc2ff: Fix Stream and processor hangs, and add Effect v4 tracing, layer statics and a Schema codec.

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

### Patch Changes

- Updated dependencies [cb3e03c]
- Updated dependencies [28984b3]
- Updated dependencies [28984b3]
  - @natsail/jetstream@0.7.0
  - @natsail/core@0.6.0
  - @natsail/session@0.5.1

## 0.6.0

### Minor Changes

- 75263c6: Support stable Effect v4 with an `effect@^4.0.0` peer dependency and publish the adapter under the default `latest` tag. Update the workspace, chat example, and package smoke check to Effect 4.0.0.

## 0.5.1

### Patch Changes

- Updated dependencies [4bca28d]
  - @natsail/core@0.5.0
  - @natsail/session@0.5.0
  - @natsail/jetstream@0.6.1

## 0.5.0

### Minor Changes

- ca0e5c3: Bound runtime shutdown and event buffering; add cooperative handler cancellation, optional processor progress heartbeats and confirmed acknowledgements, and explicit retry/terminal handler outcomes. Forward cancellation and outcomes through framework adapters.

  Runtime close now defaults to a 30-second grace period and rejects on timeout or resource cleanup failure. Slow event observers receive an overflow diagnostic after exceeding their configured capacity. Callback mocks that manually invoke Core or processor handlers must supply the new cancellation context. Default processor acknowledgement and thrown-handler-error behavior remain unchanged.

  Normal Core lease close drains buffered and already-in-flight messages before closing the subscription. Runtime shutdown waits for these handlers before draining the connection; explicit cancellation and deadline expiry stop further delivery instead.

### Patch Changes

- Updated dependencies [ca0e5c3]
  - @natsail/core@0.4.0
  - @natsail/jetstream@0.6.0
  - @natsail/session@0.4.1

## 0.4.0

### Minor Changes

- 8273c15: Add failure-isolated dependency-free runtime, session, JetStream, checkpoint, processor, and buffer telemetry with deterministic clocks, plus an optional OpenTelemetry metrics sink. Effect remains published on the `next` tag.
- 8273c15: Add shared count/byte/time batching and cooperative work budgets, atomic bounded JetStream reducer hydration with fresh retry batches, 16ms cumulative live coalescing, a public Effect event-stream materializer, and deterministic adapter scheduling while preserving legacy adapter batch options.

### Patch Changes

- Updated dependencies [8273c15]
- Updated dependencies [8273c15]
- Updated dependencies [8273c15]
- Updated dependencies [8273c15]
  - @natsail/jetstream@0.5.0
  - @natsail/core@0.3.0
  - @natsail/session@0.4.0

## 0.3.0

### Minor Changes

- ceda618: Add `jetStreamStates()` for registry-shared reducing JetStream definitions with immediate replay and reconnect boundaries plus bounded cumulative live-state coalescing.

## 0.2.1

### Patch Changes

- 478ae86: Add package-owned recovery for named explicit-ack JetStream processors. Processor leases now expose lifecycle inspection and restart counts, React reports reconnecting processors without a remount and serializes lease replacement, and Effect processors can use the same recovery policy while preserving terminal application failures.
- Updated dependencies [478ae86]
  - @natsail/jetstream@0.4.0

## 0.2.0

### Minor Changes

- 0f8c6e1: Move the adapter to Effect v4 and add cold, scoped Core NATS and JetStream Streams. Core subjects support wildcard and queue-group subscriptions with typed lifecycle failures and explicit buffer policies. JetStream adds reliable bounded delivery, replay-to-live events, atomic batched state materialization, recovery-aware resumability, and named explicit-ack processors whose handlers are native Effects.

  This release is published under the `next` dist-tag while Effect v4 remains a release candidate.

## 0.1.0

### Minor Changes

- a644006: Add scoped Effect v3 services, typed operation and session failures, cancellable runtime and registry event Streams, and bounded shared-session snapshot and value Streams.

### Patch Changes

- Updated dependencies [6a5d994]
  - @natsail/session@0.3.0
  - @natsail/core@0.2.1
