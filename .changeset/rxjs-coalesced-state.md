---
'@natsail/rxjs': minor
---

Add `observeNatsJetStreamState()` for cumulative reducer state with immediate atomic replay hydration, duplicate lifecycle suppression, and bounded live-state coalescing. Every delivery remains reducer-processed; only presentation notifications are batched for smoother UI rendering.
