---
'@natsail/effect': minor
---

Move the adapter to Effect v4 and add cold, scoped Core NATS and JetStream Streams. Core subjects support wildcard and queue-group subscriptions with typed lifecycle failures and explicit buffer policies. JetStream adds reliable bounded delivery, replay-to-live events, atomic batched state materialization, recovery-aware resumability, and named explicit-ack processors whose handlers are native Effects.

This release is published under the `next` dist-tag while Effect v4 remains a release candidate.
