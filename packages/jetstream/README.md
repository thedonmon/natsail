# @natsail/jetstream

`@natsail/jetstream` adds ordered replay, application checkpoints, named explicit-ack processing, duplicate policies, and retention-gap handling to a NATSail runtime.

```sh
pnpm add @natsail/core @natsail/checkpoints @natsail/session @natsail/jetstream
```

`consumeJetStream()` uses an ordered consumer with `AckPolicy.None`. It saves the application checkpoint only after the handler succeeds. Its lease exposes `caughtUp`, `inspect()`, and lifecycle notifications. Every delivery includes the server pending count and a stable `replay: 'initial' | 'live'` classification based on the backlog captured when the consumer opens.

`createJetStreamSessionSource()` adapts the consumer for one shared React and RxJS session. Set `recovery` to let the package replace a failed ordered consumer after its last successfully processed cursor. Permanent configuration, retention, decode, duplicate, and application-handler failures stay terminal by default. Decode failures are terminal too; see [Decode failures](#decode-failures).

Custom `recovery.delayMs` or `recovery.shouldRetry` functions require a stable `recovery.scope` when used in a validated definition. The scope prevents two callers from sharing a key while silently using different retry semantics.

`defineJetStreamSession()` combines that source with a validated contract. `defineReducingJetStreamSession()` also folds the initial replay without publishing partially assembled application state, emits one atomic live snapshot at catch-up, and then emits every serially reduced live state:

```ts
import { defineReducingJetStreamSession } from '@natsail/jetstream'

const conversation = defineReducingJetStreamSession(
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
    reduce: applyConversationEvent,
  }
)
```

The reduced snapshot reports `replaying`, `reconnecting`, or `live`, the last cursor, initial replay count, and package recovery count. React and RxJS can acquire the same definition without duplicating the consumer.

A reducing session does not accept `resume` yet. An event cursor is safe only when the matching materialized reducer state is restored atomically with it. Package-owned recovery preserves both within the active source lease; a fresh lease reconstructs state from an atomic replay.

Reducing sessions accept the shared `batchPolicy`, `scheduler`, and `workBudget`. They rebuild replay in bounded batches but expose one atomic hydration commit. After catch-up, the default live policy is 256 deliveries or 16ms; `liveBatchMs: 0` disables the time window. Count and byte bounds remain hard intake limits. Only one batch may apply at once, reducer calls stay serial, and cursor/checkpoint advancement waits for successful downstream batch publication. Closing waits for an in-flight batch and discards a pending partial batch.

Normal consumers select a package-owned payload codec instead of constructing text encoders or decoders:

```ts
import { natsCodecs } from '@natsail/core'
import { consumeJetStream } from '@natsail/jetstream'

consumeJetStream(
  runtime,
  {
    stream: 'CHAT',
    filter: 'chat.room.123',
    start: 'all',
    codec: natsCodecs.json<ChatMessage>(),
  },
  async ({ value, subject, cursor }) => chat.apply(value, subject, cursor)
)
```

The same `codec` option works with `processJetStream()`. Supply any `NatsPayloadCodec<T>` for another wire format. Use `decode(message)` only when the application needs the raw `JsMsg`; ordinary deliveries already include `subject`, cursor, `redelivered`, and `headers`; ordered deliveries also carry `duplicate`, `consumerPending`, and `replay`, and processor deliveries carry `deliveryAttempt`.

`processJetStream()` is the work-processing seam. `ensure` creates or reuses a retained durable pull consumer, `bind` validates and attaches to an existing consumer without mutating it, and `owned` creates a durable consumer that its lease deletes on close. Every mode requires `AckPolicy.Explicit`; the package acknowledges only after the handler succeeds.

Existing consumers are reconciled through one ownership-aware implementation. `bind` is inspect-only. `ensure` updates editable drift by default but rejects immutable drift without applying a partial update. `owned` may recreate only its own consumer when immutable drift is present. Override those defaults with `driftPolicy: 'error' | 'update-editable' | 'recreate-owned'`; invalid mode/policy combinations fail synchronously.

Editable fields are filters, acknowledgement wait/backoff, maximum deliveries, maximum pending acknowledgements, metadata, acknowledgement sampling, and replicas. Durable identity, pull/push kind, acknowledgement policy, delivery/start policy, replay policy, and memory/file consumer storage are immutable. Optional fields participate in drift checks only when supplied. A rejection is a discriminated reconciliation result in the controller API and a `JetStreamProcessorReconciliationError` (also a `JetStreamProcessorConfigurationError`) when processor startup cannot honor the contract.

Use `createJetStreamProcessorController(runtime, options)` for administration without starting a handler loop. Its cached `inspect()` is synchronous; `refresh()` reads authoritative state. `reconcile()`, `pause(futureDate)`, `resume()`, and `delete()` are serialized. A bound controller rejects pause, resume, and delete, and the delete guard checks ownership at runtime. Reconciliation returns `unchanged`, `created`, `updated`, `recreated`, or `rejected`, including editable/immutable drift plus normalized before/after state. A recreation reports the first eligible `deliveryBoundary` sequence.

Owned recreation starts after the last acknowledged stream sequence. For an undelivered `start: 'new'` consumer, it preserves the creation-time stream tail. If deliveries exist but no safe acknowledgement or start boundary is available, reconciliation refuses deletion. If replacement creation fails, rollback recreates the prior consumer at that same safe boundary and cached inspection reflects the rollback state.

Pause, resume, and delete return discriminated `paused`, `resumed`, or `deleted` results. A failed owned-consumer deletion rejects the operation or processor close instead of reporting cleanup as complete. When runtime recovery replaces the NATS connection, later administration calls use a consumer manager created from the replacement connection.

Set `recovery` to reopen the same named consumer after a connection or consumer-loop failure. When the named consumer is retained, its server-side acknowledgement floor determines where processing resumes, so acknowledged work stays complete and an interrupted delivery remains eligible for redelivery. If an owned `start: 'new'` consumer is deleted during recovery, NATSail recreates it from the last locally safe acknowledgement boundary instead of taking a new tail snapshot; messages published during the gap remain eligible. Handler and decoder failures are terminal and are not hidden by infrastructure retries. An `owned` recovering processor retains its consumer between attempts and deletes it only when the logical lease closes.

The processor lease exposes `inspect()` and `subscribe()`. Its phase is `connecting`, `live`, `reconnecting`, `closed`, or `error`. Cached inspection includes ownership, pending messages and acknowledgements, delivered and acknowledged consumer/stream sequences, redeliveries, pause state, handler failure, restarts, normalized desired/active configuration, and the last reconciliation. Inspection never performs network I/O.

Configure `ackWaitMs`, ordered `backoffMs`, `maxDeliver` (`-1` means unlimited), `maxAckPending`, `metadata`, `ackSamplePercent`, `replicas`, `memoryStorage`, replay policy, start position, pull-buffer capacity, and recovery attempts or delay. The first backoff is the effective acknowledgement wait. `ackWaitMs`, `backoffMs` entries, `maxAckPending`, and `replicas` must be positive integers. `maxDeliver` is `-1` or a positive integer. `backoffMs` must be non-empty and non-decreasing, `ackWaitMs` (if set) must equal its first entry, and its length must not exceed a finite `maxDeliver`. Invalid ranges, backoff relationships, metadata, and policies fail before a connection is acquired. A failed handler is cached before the lease stops and leaves its delivery unacknowledged.

Use `maxBufferedMessages` or `maxBufferedBytes` to bound the nats.js pull loop. These modes are mutually exclusive. The runtime reserves the selected capacity before it opens the consumer.

When the runtime has a telemetry sink, this package reports replay duration and remaining work, initial/live delivery counts, handler duration/outcome, redelivery and acknowledgement counts, checkpoint load/save duration/outcome, recovery attempts, and consumer discard/limit signals. Measurements use the Core reporter and never contain stream, filter, subject, checkpoint key, or consumer names. The existing runtime diagnostic stream retains detailed operator-facing context.

The checkpoint scope includes normalized filters. Set `resume.scope` when a codec, decoder, or domain-model change must invalidate an old checkpoint.

By default the checkpoint is saved after every delivery. For high-rate subjects such as streamed model tokens, set `resume.coalesce` to save at most once per window:

```ts
resume: {
  key: 'conversation-123',
  store: checkpoints,
  coalesce: { maxWaitMs: 250, maxItems: 64 },
}
```

The newest handled cursor is saved when either bound is reached, before `caughtUp` resolves, when the lease closes, and when it stops with an error. If the page closes or crashes between saves, the next open redelivers the deliveries handled since the last save, so the handler must tolerate repeats.

See the [NATSail README](https://github.com/thedonmon/natsail#explicit-ack-processing-example) for the explicit-ack example and the separate ordered-consumer acknowledgement boundary.

## Long-running and failure-aware processors

Set `progressIntervalMs` below the effective acknowledgement wait to send in-progress updates during a handler. With heartbeats enabled, the default pull buffer is one message. Use `acknowledgement: { mode: 'confirmed', timeoutMs: 5_000 }` to wait for acknowledgement receipt before advancing the local acknowledged position.

Handlers receive a second `{ signal }` argument for cancellation. Returning nothing acknowledges success; returning `{ action: 'retry', delayMs: 500 }` requests delayed redelivery; `{ action: 'term', reason: 'unsupported schema' }` explicitly stops redelivery. Thrown errors remain terminal for the processor. Confirmed acknowledgement does not apply to retry or terminal commands, and external side effects still require idempotency.

Forced runtime shutdown preserves owned consumers and prevents late handler results from being acknowledged. See the [production guide](https://github.com/thedonmon/natsail/blob/main/docs/PRODUCTION.md) for the full shutdown contract and configuration examples.

## Decode failures

A payload that the `codec` or `decode` function rejects stops a processor by default, like a handler failure. With an unlimited `maxDeliver`, restarting it replays the same message and stalls again. Set `onDecodeFailure` to decide per message instead. It receives `{ error, subject, cursor, deliveryAttempt, data, headers }` and returns the same `retry` or `term` disposition as a handler:

```ts
processJetStream(runtime, {
  stream: 'JOBS',
  consumer: { mode: 'ensure', name: 'billing' },
  filter: 'jobs.billing',
  start: 'all',
  codec: natsCodecs.json<BillingJob>(),
  onDecodeFailure: ({ error, data, cursor }) => {
    quarantine(cursor, data, error)
    return { action: 'term', reason: 'malformed payload' }
  },
}, handle)
```

A throwing hook, or one that returns nothing, stops the processor.

Decode failures are terminal by default on every path. A processor without a hook, `consumeJetStream()`, and reducing sessions all end with a `JetStreamDecodeError`, and session `recovery` does not retry it. Recognise it with `error instanceof JetStreamDecodeError`. Its message names the stream, sequence, subject, and the original error's type, but never the payload or the original message (codec errors can quote the payload). It also carries `subject`, `cursor`, and, for processors, `deliveryAttempt`. The original error is `error.cause`. Ordered consumers do not acknowledge, so only processors can skip a bad message with `onDecodeFailure`. Elsewhere, decode defensively: have `decode` return a tagged value and let the handler or reducer skip the bad entry.

## Delivery headers

Every JetStream delivery, from `consumeJetStream()`, `processJetStream()`, and reducing sessions, carries `headers?: MsgHdrs`. It is absent when the message has no headers. Use `@natsail/opentelemetry` to continue a trace from `headers`.

## Dead-letter recipe

NATSail has no built-in dead-letter publisher. `term` discards the message, and publish-then-`term` is not atomic. Persist the failure first, then terminate:

```ts
const maxAttempts = 5

processJetStream(
  runtime,
  { stream: 'JOBS', consumer: { mode: 'ensure', name: 'billing' }, filter: 'jobs.billing',
    start: 'all', maxDeliver: maxAttempts, backoffMs: [1_000, 5_000, 30_000], codec },
  async (delivery) => {
    try {
      await work(delivery.value)
    } catch (error) {
      if (delivery.deliveryAttempt < maxAttempts) return { action: 'retry', delayMs: 1_000 }
      const hdrs = headers()
      hdrs.set('x-original-sequence', String(delivery.cursor.sequence))
      await js.publish('dlq.jobs.billing', codec.encode(delivery.value), { headers: hdrs })
      return { action: 'term', reason: 'exhausted' }
    }
  }
)
```

Add a safety net for deliveries that never reach the handler's last attempt, such as repeated ack-wait timeouts. Capture the server's max-deliveries advisory in a stream, with the subject `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>`. Each advisory carries `stream_seq`, and `jsm.streams.getMessage(stream, { seq })` returns the original.

`term` does not raise the max-deliveries advisory. It raises `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.<stream>.<consumer>`, and the server treats it as an acknowledgement, so on `WorkQueue` and `Interest` streams the original can be gone before a dead-letter worker fetches it. Limits and age can also remove it. The handler-side publish above is the reliable path. `tests/integration/jetstream-failure-handling.test.ts` runs the handler-side recipe against a real server.

## Watching a KV bucket

A KV bucket is a stream named `KV_<bucket>` with subjects `$KV.<bucket>.<key>` and a `KV-Operation` header (`PUT`, `DEL`, or `PURGE`). A reducing session over that stream replays the current entries, goes live, and gives React, RxJS, and Effect the same shared map, with no extra package:

```ts
const settings = defineReducingJetStreamSession(
  runtime,
  'kv:settings',
  {
    stream: 'KV_settings',
    filter: '$KV.settings.>',
    start: 'all',
    decode: (message): KvChange => ({
      key: message.subject.slice('$KV.settings.'.length),
      operation: message.headers?.get('KV-Operation') || 'PUT',
      value: new TextDecoder().decode(message.data),
    }),
  },
  {
    scope: 'kv-map:v1',
    initial: () => new Map<string, string>(),
    reduce: (state, { value: change }) => {
      const next = new Map(state)
      if (change.operation === 'PUT') next.set(change.key, change.value)
      else next.delete(change.key)
      return next
    },
  }
)
```

`start: 'all'` yields the last value per key only for buckets with the default `history: 1`. Buckets with more history replay every revision, and the reducer keeps the last. This assumes the default codecs and stream name: a bucket opened with a key or value codec (such as `Base64KeyCodec()`), a custom `streamName`, or a mirror needs the same decoding and prefix here. Write with `@nats-io/kv` as usual. `tests/integration/jetstream-kv.test.ts` covers put, delete, and purge.

## License

Apache-2.0
