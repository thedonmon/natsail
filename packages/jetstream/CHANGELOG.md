# @natsail/jetstream

## 0.6.0

### Minor Changes

- ca0e5c3: Bound runtime shutdown and event buffering; add cooperative handler cancellation, optional processor progress heartbeats and confirmed acknowledgements, and explicit retry/terminal handler outcomes. Forward cancellation and outcomes through framework adapters.

  Runtime close now defaults to a 30-second grace period and rejects on timeout or resource cleanup failure. Slow event observers receive an overflow diagnostic after exceeding their configured capacity. Callback mocks that manually invoke Core or processor handlers must supply the new cancellation context. Default processor acknowledgement and thrown-handler-error behavior remain unchanged.

  Normal Core lease close drains buffered and already-in-flight messages before closing the subscription. Runtime shutdown waits for these handlers before draining the connection; explicit cancellation and deadline expiry stop further delivery instead.

### Patch Changes

- Updated dependencies [ca0e5c3]
  - @natsail/core@0.4.0
  - @natsail/session@0.4.1

## 0.5.0

### Minor Changes

- 8273c15: Add ownership-aware durable processor administration with discriminated operation results, connection-aware management, surfaced cleanup failures, validated public modes, normalized drift reconciliation, rich cached inspection, safe acknowledgement/start-boundary recreation and rollback, and advanced consumer configuration.
- 8273c15: Add failure-isolated dependency-free runtime, session, JetStream, checkpoint, processor, and buffer telemetry with deterministic clocks, plus an optional OpenTelemetry metrics sink. Effect remains published on the `next` tag.
- 8273c15: Add shared count/byte/time batching and cooperative work budgets, atomic bounded JetStream reducer hydration with fresh retry batches, 16ms cumulative live coalescing, a public Effect event-stream materializer, and deterministic adapter scheduling while preserving legacy adapter batch options.

### Patch Changes

- Updated dependencies [8273c15]
- Updated dependencies [8273c15]
- Updated dependencies [8273c15]
  - @natsail/core@0.3.0
  - @natsail/session@0.4.0

## 0.4.0

### Minor Changes

- 478ae86: Add package-owned recovery for named explicit-ack JetStream processors. Processor leases now expose lifecycle inspection and restart counts, React reports reconnecting processors without a remount and serializes lease replacement, and Effect processors can use the same recovery policy while preserving terminal application failures.

## 0.3.0

### Minor Changes

- 6a5d994: Add validated shared session definitions, replay catch-up metadata, atomic JetStream reducers, cursor-preserving consumer recovery, managed React ownership, coalesced reducer selectors, RxJS reducer adapters, and session lifecycle diagnostics.

  `runtime.reconnect()` now resolves after the forced disconnect completes instead of returning while the connection is still offline.

### Patch Changes

- Updated dependencies [6a5d994]
  - @natsail/session@0.3.0
  - @natsail/core@0.2.1

## 0.2.0

### Minor Changes

- 1c07051: Add permanent connection recovery, explicit reconnect, runtime inspection, byte limits, and managed request/reply.

  Add source-scoped checkpoints, restartable sessions, and shared JetStream helpers for React and RxJS.

  Add validated named explicit-ack JetStream processors with bind, ensure, and owned lifecycles, redelivery controls, and a React processor hook.

  Add injectable text, JSON, and byte payload codecs across Core NATS and JetStream, accept strings directly when publishing, and expose delivery subjects without requiring raw-message decoders.

### Patch Changes

- Updated dependencies [1c07051]
  - @natsail/core@0.2.0
  - @natsail/checkpoints@0.2.0
  - @natsail/session@0.2.0

## 0.1.0

### Minor Changes

- 7a89d94: Prepare the first public NATSail package set with npm metadata, verified tarballs, Changesets versioning, bootstrap publication, and provenance-aware release automation.

### Patch Changes

- Updated dependencies [7a89d94]
  - @natsail/checkpoints@0.1.0
  - @natsail/core@0.1.0
