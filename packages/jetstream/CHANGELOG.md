# @natsail/jetstream

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
