---
'@natsail/jetstream': minor
'@natsail/session': minor
'@natsail/react': minor
'@natsail/rxjs': minor
'@natsail/core': patch
---

Add validated shared session definitions, replay catch-up metadata, atomic JetStream reducers, cursor-preserving consumer recovery, managed React ownership, coalesced reducer selectors, RxJS reducer adapters, and session lifecycle diagnostics.

`runtime.reconnect()` now resolves after the forced disconnect completes instead of returning while the connection is still offline.
