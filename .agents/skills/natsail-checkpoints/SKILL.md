---
name: natsail-checkpoints
description: Use @natsail/checkpoints for NATSail JetStream cursor persistence in memory or IndexedDB, including stable resume keys, scope invalidation, monotonic writes, and checkpoint conflict handling. Use with natsail-jetstream for replay and retention policy.
---

# Use @natsail/checkpoints

```sh
pnpm add @natsail/checkpoints
```

Choose a store by lifetime:

```ts
import { createIndexedDbCheckpointStore, createMemoryCheckpointStore } from '@natsail/checkpoints'

const sessionOnly = createMemoryCheckpointStore()
const acrossReloads = createIndexedDbCheckpointStore({
  databaseName: 'my-application',
})
```

- Use the memory store for tests, short-lived workers, or recovery only while the process remains alive.
- Use the IndexedDB store in browsers when resume state must survive page reloads. It is unavailable in hosts without `globalThis.indexedDB`.
- The built-in memory and IndexedDB stores are client-local and do not coordinate distributed writers. Applications may implement `CheckpointStore` with different storage or coordination, but a checkpoint is still not a replacement for JetStream server acknowledgement.

## Pass the store to ordered replay

Applications normally let `@natsail/jetstream` create and advance checkpoint records:

```ts
import { consumeJetStream } from '@natsail/jetstream'

const lease = consumeJetStream(
  runtime,
  {
    stream: 'CONVERSATIONS',
    filter: 'conversations.123.events',
    start: 'all',
    codec: conversationEventCodec,
    resume: {
      key: 'conversation:123',
      store: acrossReloads,
      scope: 'conversation-event:v2',
    },
  },
  applyConversationEvent
)
```

Choose a stable, application-specific `key` for one logical source. Set `scope` when a decoder, filter interpretation, or domain event version changes so an incompatible cursor is rejected instead of silently reused.

The store rejects invalid records, sequence regressions within the same stream epoch, and incompatible scopes. Handle `CheckpointValidationError`, `CheckpointConflictError`, or `CheckpointScopeConflictError` if application code writes records directly.

JetStream owns stream/epoch mismatch, duplicate, and retention-gap decisions. Its default retention-gap policy is `error`; use `continue` only when the application can safely rebuild from the first retained message.

See the [checkpoint package guide](https://github.com/thedonmon/natsail/tree/main/packages/checkpoints#readme) and [npm package](https://www.npmjs.com/package/@natsail/checkpoints).
