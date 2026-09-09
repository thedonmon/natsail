# @natsail/session

## 0.5.0

### Minor Changes

- 4bca28d: Coordinate package connection lifecycle during startup, reconnect, and disposal.

  Core adds an opt-in `connect: { create(context) }` factory with a pending-attempt cancellation signal and attempt identifiers. Legacy function factories still receive no arguments. Concurrent explicit reconnect calls coalesce, startup reconnect still forces a fresh handshake, and disposal settles reconnect callers without starting another connection. Pending unopened Core leases terminate at deadline cancellation; queued transport status cannot announce a connection after disposal. Forced cleanup runs once.

  Session exports `closeNatsResources({ runtime, sessions })` to start both lifetimes' shutdown without letting registry cleanup postpone the runtime deadline. Managed React providers use this helper by default, retaining StrictMode safety and permitting replacement connections while prior resources drain. Custom providers can delegate to the same helper. Session close preserves values delivered during graceful lease drain.

  Existing events and telemetry now distinguish connection attempts, explicit reconnect outcomes, disposal outcomes, and discarded late connections. Failure diagnostics retain bounded categories while omitting raw errors. Runtime disposal from telemetry callbacks prevents subsequent factory invocation or connected status publication. See `docs/UPGRADING-LIFECYCLE.md` for cancellation limitations, shutdown failure handling, and compatibility notes.

### Patch Changes

- Updated dependencies [4bca28d]
  - @natsail/core@0.5.0

## 0.4.1

### Patch Changes

- Updated dependencies [ca0e5c3]
  - @natsail/core@0.4.0

## 0.4.0

### Minor Changes

- 8273c15: Add failure-isolated dependency-free runtime, session, JetStream, checkpoint, processor, and buffer telemetry with deterministic clocks, plus an optional OpenTelemetry metrics sink. Effect remains published on the `next` tag.
- 8273c15: Add shared count/byte/time batching and cooperative work budgets, atomic bounded JetStream reducer hydration with fresh retry batches, 16ms cumulative live coalescing, a public Effect event-stream materializer, and deterministic adapter scheduling while preserving legacy adapter batch options.

### Patch Changes

- Updated dependencies [8273c15]
- Updated dependencies [8273c15]
- Updated dependencies [8273c15]
  - @natsail/core@0.3.0

## 0.3.0

### Minor Changes

- 6a5d994: Add validated shared session definitions, replay catch-up metadata, atomic JetStream reducers, cursor-preserving consumer recovery, managed React ownership, coalesced reducer selectors, RxJS reducer adapters, and session lifecycle diagnostics.

  `runtime.reconnect()` now resolves after the forced disconnect completes instead of returning while the connection is still offline.

### Patch Changes

- Updated dependencies [6a5d994]
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

## 0.1.0

### Minor Changes

- 7a89d94: Prepare the first public NATSail package set with npm metadata, verified tarballs, Changesets versioning, bootstrap publication, and provenance-aware release automation.

### Patch Changes

- Updated dependencies [7a89d94]
  - @natsail/core@0.1.0
