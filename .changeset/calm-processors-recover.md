---
'@natsail/jetstream': minor
'@natsail/react': minor
'@natsail/effect': patch
---

Add package-owned recovery for named explicit-ack JetStream processors. Processor leases now expose lifecycle inspection and restart counts, React reports reconnecting processors without a remount and serializes lease replacement, and Effect processors can use the same recovery policy while preserving terminal application failures.
