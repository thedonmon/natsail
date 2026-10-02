---
'@natsail/jetstream': minor
---

Processors accept `onDecodeFailure`, which returns `retry` or `term` for a payload the codec rejects. Unset, a decode failure still stops the processor.

Decode failures are now typed and terminal everywhere: they surface as `JetStreamDecodeError` (stream, sequence, subject, and original message in the text; `subject`, `cursor`, `deliveryAttempt`, and `cause` as fields). On ordered consumers and reducing sessions a decode error is no longer retried by session `recovery`; before, a non-`TypeError` was retried against the same message. A processor's stop reason (`handlerFailure`, `closed` rejection) is now the `JetStreamDecodeError` wrapper, so check `error.cause` for the original error. `onDecodeFailure` still receives the raw error. Every JetStream delivery (ordered, processor, and reducing session) now carries `headers?: MsgHdrs`.

Breaking: six helpers that were exported only for internal use are removed: `classifyJetStreamProcessorDrift`, `normalizeJetStreamProcessorActive`, `normalizeJetStreamProcessorDesired`, `jetStreamProcessorConsumerConfig`, `inspectJetStreamProcessorConsumerState`, and `validateJetStreamProcessorAdminOptions`. Migration: use `createJetStreamProcessorController(runtime, options)`, which validates options synchronously and throws `JetStreamProcessorConfigurationError`, and read `controller.reconcile()` or `controller.inspect()` for normalized configuration and drift.

The README adds tested recipes for dead-lettering and for watching a KV bucket.
