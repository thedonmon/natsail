---
'@natsail/rxjs': minor
---

Add `batchWithPolicy(policy, { scheduler? })`, an RxJS operator that batches by the shared `NatsailBatchPolicy` (count, bytes, time). The timer starts at the first value of a batch, so idle subscriptions schedule nothing and never emit empty arrays. `observeNatsJetStreamState` now shares the same bookkeeping; an oversized live state now errors with `NatsailBatchItemTooLargeError` instead of a `RangeError`.
