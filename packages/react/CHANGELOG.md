# @natsail/react

## 0.6.1

### Patch Changes

- 4bca28d: Coordinate package connection lifecycle during startup, reconnect, and disposal.

  Core adds an opt-in `connect: { create(context) }` factory with a pending-attempt cancellation signal and attempt identifiers. Legacy function factories still receive no arguments. Concurrent explicit reconnect calls coalesce, startup reconnect still forces a fresh handshake, and disposal settles reconnect callers without starting another connection. Pending unopened Core leases terminate at deadline cancellation; queued transport status cannot announce a connection after disposal. Forced cleanup runs once.

  Session exports `closeNatsResources({ runtime, sessions })` to start both lifetimes' shutdown without letting registry cleanup postpone the runtime deadline. Managed React providers use this helper by default, retaining StrictMode safety and permitting replacement connections while prior resources drain. Custom providers can delegate to the same helper. Session close preserves values delivered during graceful lease drain.

  Existing events and telemetry now distinguish connection attempts, explicit reconnect outcomes, disposal outcomes, and discarded late connections. Failure diagnostics retain bounded categories while omitting raw errors. Runtime disposal from telemetry callbacks prevents subsequent factory invocation or connected status publication. See `docs/UPGRADING-LIFECYCLE.md` for cancellation limitations, shutdown failure handling, and compatibility notes.

- Updated dependencies [4bca28d]
  - @natsail/core@0.5.0
  - @natsail/session@0.5.0
  - @natsail/jetstream@0.6.1

## 0.6.0

### Minor Changes

- ca0e5c3: Bound runtime shutdown and event buffering; add cooperative handler cancellation, optional processor progress heartbeats and confirmed acknowledgements, and explicit retry/terminal handler outcomes. Forward cancellation and outcomes through framework adapters.

  Runtime close now defaults to a 30-second grace period and rejects on timeout or resource cleanup failure. Slow event observers receive an overflow diagnostic after exceeding their configured capacity. Callback mocks that manually invoke Core or processor handlers must supply the new cancellation context. Default processor acknowledgement and thrown-handler-error behavior remain unchanged.

  Normal Core lease close drains buffered and already-in-flight messages before closing the subscription. Runtime shutdown waits for these handlers before draining the connection; explicit cancellation and deadline expiry stop further delivery instead.

### Patch Changes

- Updated dependencies [ca0e5c3]
  - @natsail/core@0.4.0
  - @natsail/jetstream@0.6.0
  - @natsail/session@0.4.1

## 0.5.0

### Minor Changes

- 8273c15: Add shared count/byte/time batching and cooperative work budgets, atomic bounded JetStream reducer hydration with fresh retry batches, 16ms cumulative live coalescing, a public Effect event-stream materializer, and deterministic adapter scheduling while preserving legacy adapter batch options.

### Patch Changes

- Updated dependencies [8273c15]
- Updated dependencies [8273c15]
- Updated dependencies [8273c15]
- Updated dependencies [8273c15]
  - @natsail/jetstream@0.5.0
  - @natsail/core@0.3.0
  - @natsail/session@0.4.0

## 0.4.0

### Minor Changes

- 478ae86: Add package-owned recovery for named explicit-ack JetStream processors. Processor leases now expose lifecycle inspection and restart counts, React reports reconnecting processors without a remount and serializes lease replacement, and Effect processors can use the same recovery policy while preserving terminal application failures.

### Patch Changes

- Updated dependencies [478ae86]
  - @natsail/jetstream@0.4.0

## 0.3.0

### Minor Changes

- 6a5d994: Add validated shared session definitions, replay catch-up metadata, atomic JetStream reducers, cursor-preserving consumer recovery, managed React ownership, coalesced reducer selectors, RxJS reducer adapters, and session lifecycle diagnostics.

  `runtime.reconnect()` now resolves after the forced disconnect completes instead of returning while the connection is still offline.

### Patch Changes

- Updated dependencies [6a5d994]
  - @natsail/jetstream@0.3.0
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
  - @natsail/jetstream@0.2.0
  - @natsail/session@0.2.0

## 0.1.0

### Minor Changes

- 7a89d94: Prepare the first public NATSail package set with npm metadata, verified tarballs, Changesets versioning, bootstrap publication, and provenance-aware release automation.

### Patch Changes

- Updated dependencies [7a89d94]
  - @natsail/core@0.1.0
  - @natsail/session@0.1.0
