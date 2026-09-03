---
name: natsail-jetstream
description: Use @natsail/jetstream for ordered replay and live delivery, application checkpoint resume, duplicate and retention-gap policy, atomic reducing sessions, package recovery, or named explicit-ack work processors with redelivery.
---

# Use @natsail/jetstream

Install the packages imported by the application:

```sh
pnpm add @natsail/core @natsail/checkpoints @natsail/session @natsail/jetstream
```

## Replay, then stay live

`consumeJetStream()` opens one ordered consumer. It uses `AckPolicy.None`; successful handler completion is the boundary for advancing an application checkpoint.

```ts
import { createIndexedDbCheckpointStore } from '@natsail/checkpoints'
import { natsCodecs } from '@natsail/core'
import {
  consumeJetStream,
  defineReducingJetStreamSession,
  processJetStream,
} from '@natsail/jetstream'

const checkpoints = createIndexedDbCheckpointStore({ databaseName: 'my-app' })

const conversation = consumeJetStream(
  runtime,
  {
    stream: 'CONVERSATIONS',
    filter: 'conversations.123.events',
    start: 'all',
    codec: natsCodecs.json<ConversationEvent>(),
    resume: {
      key: 'conversation:123',
      store: checkpoints,
      scope: 'conversation-event:v2',
    },
  },
  async (delivery) => applyEvent(delivery.value)
)

await conversation.ready
await conversation.caughtUp
```

`ready` means the consumer opened. `caughtUp` resolves after the backlog captured at open is processed; new traffic does not move that boundary. Each delivery reports its cursor, subject, duplicate/redelivery flags, pending count, and `replay: 'initial' | 'live'`.

Direct `consumeJetStream()` does not configure package-owned consumer recovery. Use `createJetStreamSessionSource()`, `defineJetStreamSession()`, or `defineReducingJetStreamSession()` with `recovery` when a shared logical source should reopen after infrastructure failure.

The default duplicate policy is `drop`. Choose `deliver` to receive marked duplicates or `error` to stop. The default retention-gap policy is `error`; choose `continue` only when starting at the first retained message is safe. Use either `maxBufferedMessages` or `maxBufferedBytes`, never both.

## Share atomic application state

Use `defineJetStreamSession()` for shared deliveries. Use `defineReducingJetStreamSession()` when consumers need one complete state after replay and serial live reductions:

```ts
const conversationState = defineReducingJetStreamSession(
  runtime,
  'conversation:123',
  {
    stream: 'CONVERSATIONS',
    filter: 'conversations.123.events',
    start: 'all',
    recovery: { delayMs: 500 },
    codec: natsCodecs.json<ConversationEvent>(),
  },
  {
    scope: 'conversation-view:v2',
    initial: () => emptyConversation,
    reduce: (state, delivery) => reduceConversation(state, delivery.value),
  }
)
```

Give the same definition and session registry to React, RxJS, or Effect. A reducing session deliberately rejects cursor-only `resume`; a cursor is unsafe unless the matching materialized state is restored atomically.

## Process durable work

```ts
const processor = processJetStream(
  runtime,
  {
    stream: 'JOBS',
    consumer: { mode: 'ensure', name: 'billing-workers' },
    filter: 'jobs.billing',
    start: 'all',
    codec: natsCodecs.json<BillingJob>(),
    ackWaitMs: 60_000,
    maxDeliver: 10,
    maxAckPending: 64,
    recovery: { delayMs: 500 },
  },
  async ({ value, deliveryAttempt }) => applyBillingJob(value, deliveryAttempt)
)
```

The handler must succeed before acknowledgement. Make handlers idempotent because unacknowledged work is redelivered and a deleted retained consumer may be recreated from its configured start position.

Choose consumer ownership explicitly:

- `bind`: attach to an administrator-managed consumer and validate its contract.
- `ensure`: create or reuse a retained consumer.
- `owned`: create a named consumer and delete it when the logical processor lease closes.

Recovery retries infrastructure failures. Decoder, handler, and consumer-contract failures remain terminal. Use the lease's `inspect()` and `subscribe()` to show `connecting`, `live`, `reconnecting`, `closed`, or `error` state and restart counts. Close every lease when its owner is done.

Use `createJetStreamProcessorController()` for serialized administration without a handler loop. `pause()`, `resume()`, and `delete()` return discriminated `paused`, `resumed`, and `deleted` results. Owned-consumer deletion failures reject the operation or lease close; do not treat a rejected close as successful cleanup.

See the [JetStream package guide](https://github.com/thedonmon/natsail/tree/main/packages/jetstream#readme), [delivery guarantees](https://github.com/thedonmon/natsail/blob/main/docs/DELIVERY.md), and [npm package](https://www.npmjs.com/package/@natsail/jetstream).
