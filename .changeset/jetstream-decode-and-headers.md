---
'@natsail/jetstream': minor
---

Processors accept `onDecodeFailure`, which returns `retry` or `term` for a payload the codec rejects. Unset, a decode failure still stops the processor. Every JetStream delivery (ordered, processor, and reducing session) now carries `headers?: MsgHdrs`.

Breaking: six helpers that were exported only for internal use are removed: `classifyJetStreamProcessorDrift`, `normalizeJetStreamProcessorActive`, `normalizeJetStreamProcessorDesired`, `jetStreamProcessorConsumerConfig`, `inspectJetStreamProcessorConsumerState`, and `validateJetStreamProcessorAdminOptions`. Migration: use `createJetStreamProcessorController(runtime, options)`, which validates options synchronously and throws `JetStreamProcessorConfigurationError`, and read `controller.reconcile()` or `controller.inspect()` for normalized configuration and drift.

The README adds tested recipes for dead-lettering and for watching a KV bucket.
