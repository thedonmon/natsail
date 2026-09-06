# @natsail/effect

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
