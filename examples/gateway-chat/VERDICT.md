# Prototype verdict

Date: 2026-08-21

Question: Is a tenant-scoped NATSail Durable Object gateway valuable when exercised by a real multi-room TanStack application?

Verdict: Yes. The application makes the gateway seam more convincing, and it also sharpens the limits enough to prevent premature package promotion.

## Proven locally

The one-command browser proof ran with two Chromium tabs, NATS Server 2.14.4, Wrangler 4.125.0, TanStack Router 1.170.31, and TanStack Query 5.101.4.

- Router loaders prefetched room metadata and retained history into a QueryClient passed through router context.
- TanStack Query remained the only application cache. WebSocket frames, connection phases, optimistic sends, and catch-up deliveries updated that cache.
- Four logical rooms shared one tenant Durable Object and one upstream NATSail connection.
- Two browser tabs attached to the same room feed.
- One tab deliberately disconnected while the other published a new room message.
- The stale tab reconnected with its prior applied cursor, received `resume-required`, fetched the missing retained frame, and returned to `live` at the new cursor.
- Three substantially different layouts exercised the same room and delivery model.

## What the app clarified

The gateway cursor and each tab's applied cursor are different domain concepts. Making both visible is useful product behavior, not debug-only instrumentation. A user can now tell whether a room is live, reconnecting, catching up, or outside the retained window.

TanStack Router and Query are a good consumer shape for this package. Loaders own initial history, Query owns the timeline cache, and the gateway is a cache writer. No gateway-specific React state container was necessary.

The prototype models rooms inside the JSON event envelope over one tenant subject. It does not yet prove per-room NATS subjects, room authorization, or room-specific retention policy. Those belong in the next gateway protocol proof.

## Production blockers exposed by the proof

- The Durable Object writes and lists storage on every delivery. A production retained log needs an efficient sequence index, batching, byte caps, and measured cost.
- A reconnect retries if new live deliveries advance the gateway during history fetch. This converges in the local proof but is not an atomic catch-up-to-live handoff under continuous traffic.
- The 128-message limit is a scenario constant, not a justified product policy.
- Slow clients, outbound byte backpressure, authentication, room authorization, credential rotation, and forced eviction remain unproven.
- A temporary ordered JetStream consumer may still be the better long-gap strategy.

## Recommendation

Keep the experiment as a separate prototype and continue toward a small `@natsail/cloudflare-gateway` interface. Before promotion, compare a temporary per-client catch-up consumer with a small retained fast path, and prove an atomic handoff to shared live fan-out under continuous traffic.
