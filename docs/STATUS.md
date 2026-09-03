# Project status and roadmap

NATSail is a public `0.x` package family. The repository publishes eight packages under the `@natsail` npm scope.

The test suite uses NATS 2.14.4. Separate fixtures cover anonymous, token, user/password, NKey, operator JWT, and TLS connections.

## Tested capabilities

### Runtime and Core NATS

- One runtime connection for many Core NATS subscriptions
- Publish, live delivery, and request/reply
- Bounded initial connection retry and cleanup
- Recovery after forced reconnect or permanent close
- Forced reauthentication after credential changes
- Structured status, diagnostics, and resource inspection
- Browser WebSocket transport in Node.js and Chromium
- Connection-wide limits for consumers, buffered messages, and buffered bytes
- Dependency-free counters, gauges, and deterministic duration telemetry isolated from runtime operations
- Optional OpenTelemetry metrics adapter with no Core OpenTelemetry dependency

### JetStream

- Ordered replay and live delivery through one consumer
- Explicit replay-boundary metadata and catch-up completion
- Resume strictly after an application cursor
- Atomic replay-to-live state reduction
- Cursor-preserving consumer recovery
- Duplicate drop, deliver, and error policies
- Retention-gap error and continue policies
- Named explicit-ack processing with redelivery
- Package-owned infrastructure recovery for named explicit-ack processors
- Bound, retained, and lease-owned processor lifecycles
- Message-count and byte-capacity pull limits
- Replay/remaining, handler, redelivery, acknowledgement, checkpoint, recovery, and buffer-signal measurements

### Sessions and framework adapters

- Keyed session sharing with final-release cleanup
- Contract checks for shared logical sources
- Memory and IndexedDB checkpoint stores
- Strict-Mode-safe React runtime ownership
- React selectors, reducers, status, and processor hooks, including processor restart state
- Frame-coalesced React JetStream state
- RxJS runtime, session, Core, and JetStream Observables
- Frame-coalesced RxJS cumulative state
- Effect v4 Layers, bounded Streams, replay materialization, and processors
- One logical session shared by Effect, React, and RxJS

### Examples and packaging

- Direct React rooms application
- RxJS and Effect chat performance labs with histories of up to 5,000 messages
- AI SDK and TanStack AI transports over Core NATS and JetStream
- Full-page AI reply recovery with retained state
- Local Cloudflare Worker WebSocket and TCP transport proofs
- Local Durable Object fan-out and restart replay
- SharedWorker cross-tab connection proof
- Changesets, package checks, provenance, and trusted publishing
- Machine-readable local 1,000/5,000 replay and configurable live-burst benchmark foundation

## Current limits

### Materialized state resume

Package-owned recovery preserves reducer state and its cursor while the source lease remains active. A fresh lease rebuilds its state from stream replay.

Fast persisted reducer resume needs a state store that commits materialized state and its cursor together. Reducing sessions reject a normal `resume` configuration until this contract exists.

### Checkpoint coordination

Memory and IndexedDB checkpoints coordinate one client-side backing store. They do not coordinate distributed writers or replace server acknowledgement.

### AI stream recovery

The AI example restores framework messages and active runs after a page reload. It does not include a server-owned run registry or cross-device recovery.

AI SDK can replay one retained native run. It cannot continue from an arbitrary native chunk without the earlier run events.

### Cloudflare

The repository proves official WebSocket and Node TCP transports in local workerd. Remote endpoints, production authentication, and Workers VPC access still need deployment tests.

The Durable Object gateway remains a prototype. A published package needs production authentication, backpressure, eviction, and cost policy.

### Cross-tab sharing

The `SharedWorker` harness proves that two tabs can share one connection. It does not define a supported broker protocol, authentication model, or complete failure behavior.

### Effect tuning

The Effect adapter provides bounded Core and JetStream Streams. Sustained slow-consumer and recovery tests still need published measurements and tuning guidance.

The local benchmark and browser labs now expose stable measurement fields, but the repository does not yet publish representative hardware baselines or NATS server throughput claims.

### Processor policy

Named processors support explicit acknowledgements and redelivery. Progress heartbeats, confirmed acknowledgements, and higher-level `nak` or terminal-message policy are not packaged yet.

## Roadmap

1. Measure a production migration that replaces application-owned NATS effects with shared definitions, reducers, and managed ownership.
2. Publish Effect v4 buffer and batching guidance from slow-consumer, replay, processor-failure, and recovery tests.
3. Prove a materialized-state store that commits reducer state and its JetStream cursor together.
4. Add processor progress heartbeats and explicit terminal-message policy after their failure semantics are tested.
5. Prove an atomic catch-up-to-live handoff in the Durable Object gateway.
6. Add gateway authentication, backpressure, eviction tests, and cost measurements.
7. Run the Cloudflare examples against a remote NATS endpoint and test Workers VPC access.
8. Define the `SharedWorker` broker protocol, authentication model, lifecycle, and failure behavior.

## Publication status

All eight package tarballs pass repository checks. Routine releases use Changesets and GitHub trusted publishing.

A new npm package still needs its first publication and trusted-publisher configuration before routine OIDC releases can publish it.
