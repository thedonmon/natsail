---
'@natsail/jetstream': minor
---

Add `resume.coalesce` to `consumeJetStream` and JetStream sessions. It saves the checkpoint once per item or time window instead of after every delivery, which cuts IndexedDB writes on high-rate subjects such as streamed model tokens. The default still saves after every delivery.
