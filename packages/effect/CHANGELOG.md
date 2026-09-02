# @natsail/effect

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
