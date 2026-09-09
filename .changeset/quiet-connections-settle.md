---
'@natsail/core': minor
'@natsail/react': patch
'@natsail/session': minor
---

Coordinate package connection lifecycle during startup, reconnect, and disposal.

Core adds an opt-in `connect: { create(context) }` factory with a pending-attempt cancellation signal and attempt identifiers. Legacy function factories still receive no arguments. Concurrent explicit reconnect calls coalesce, startup reconnect still forces a fresh handshake, and disposal settles reconnect callers without starting another connection. Pending unopened Core leases terminate at deadline cancellation; queued transport status cannot announce a connection after disposal. Forced cleanup runs once.

Session exports `closeNatsResources({ runtime, sessions })` to start both lifetimes' shutdown without letting registry cleanup postpone the runtime deadline. Managed React providers use this helper by default, retaining StrictMode safety and permitting replacement connections while prior resources drain. Custom providers can delegate to the same helper. Session close preserves values delivered during graceful lease drain.

Existing events and telemetry now distinguish connection attempts, explicit reconnect outcomes, disposal outcomes, and discarded late connections. Failure diagnostics retain bounded categories while omitting raw errors. Runtime disposal from telemetry callbacks prevents subsequent factory invocation or connected status publication. See `docs/UPGRADING-LIFECYCLE.md` for cancellation limitations, shutdown failure handling, and compatibility notes.
