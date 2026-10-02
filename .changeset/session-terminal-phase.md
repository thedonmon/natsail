---
'@natsail/session': patch
---

A session whose source closed or failed before it became ready now stays in the `closed` or `error` phase. Previously a late `ready` moved it back to `live`, hiding the failure from subscribers until the next restart.
