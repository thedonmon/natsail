---
'@natsail/core': minor
'@natsail/jetstream': minor
'@natsail/react': minor
'@natsail/effect': minor
---

Bound runtime shutdown and event buffering; add cooperative handler cancellation, optional processor progress heartbeats and confirmed acknowledgements, and explicit retry/terminal handler outcomes. Forward cancellation and outcomes through framework adapters.

Runtime close now defaults to a 30-second grace period and rejects on timeout or resource cleanup failure. Slow event observers receive an overflow diagnostic after exceeding their configured capacity. Callback mocks that manually invoke Core or processor handlers must supply the new cancellation context. Default processor acknowledgement and thrown-handler-error behavior remain unchanged.
