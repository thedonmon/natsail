# NATS resumable streams: evidence and a session primitive

Research date: 2026-08-21. The repository pins `@nats-io/jetstream`, `@nats-io/nats-core`, and `@nats-io/transport-node` 3.4.0.

This document records the initial research. See the [main README](../../README.md#current-limits) for the implemented interface and current limitations.

## Conclusion

nats.js already implements the difficult _live-connection_ part of ordered recovery. An ordered pull consumer tracks the consumer delivery sequence, remembers the last JetStream stream sequence, and recreates its ephemeral consumer at `last stream sequence + 1` when delivery becomes inconsistent. We should build on that behavior, not replace it.

The missing layer is a durable, application-visible checkpoint. An ordered consumer's cursor lives only in the nats.js object, uses `AckPolicy.None`, and advances as the library yields a message—not after React state, a WebSocket client, or another side effect has safely processed it. A browser reload, process crash, new gateway instance, or replacement `NatsConnection` creates a new ordered consumer from its initial delivery policy. Consequently:

- `DeliverPolicy.New` can skip events that arrived while the app had no live consumer.
- `DeliverPolicy.All` avoids loss but can replay the whole retained history.
- Neither choice records application progress or defines duplicate/gap behavior.

The strongest open-source opportunity is therefore a thin checkpointing and policy layer over nats.js ordered pull consumers: a serializable cursor, explicit duplicate/gap policies, handler-completion checkpointing, bounded pull buffering, and unified recovery status. That session primitive should live inside a connection-wide NATS runtime that schedules many feeds. It should not be another NATS transport or a generalized RxJS wrapper.

## Clarified product scope

TanStack AI is inspiration for delivery invariants only. It is neither a production dependency nor the product's organizing model. The private chat example proves a TanStack AI `SubscribeConnectionAdapter`, a separate AI SDK `ChatTransport`, conversation reconstruction with `start: "all"`, and active-answer recovery through `consumeJetStream()`. The recovery test closes the reply consumer while native frames continue to publish, then recreates it from the last application checkpoint. It also makes `AckPolicy.None`, global filtered sequence units, and duplicate policy explicit. Full-page restoration remains separate because it needs persisted framework and run state.

The public module should be NATS-native and framework-neutral:

- one runtime multiplexes many logical stream sessions over one NATS connection;
- one independently resumable conversation normally has one ordered consumer while active;
- session priorities and aggregate limits prevent several history replays from flooding the browser;
- paused/background sessions can release their ephemeral consumers and resume from the last accepted cursor;
- React, RxJS, Vue, Svelte, workers, and Node adapt the same callback/async-iterable seam;
- RxJS remains useful for local composition and fan-out but is not a core dependency or history store;
- a later `SharedWorker` host can multiplex one runtime across real same-origin browser tabs.

The session interface below remains the implementation primitive. The architecture proposal wraps it in `NatsStreamRuntime` so connection ownership, session scheduling, and total buffering are not repeated by every caller.

## Findings from NATS and nats.js

### 1. WebSocket changes the transport, not JetStream semantics

The NATS server speaks the same NATS protocol and exposes the same JetStream API over `ws://`/`wss://`; WebSocket is the browser-compatible transport and carries framing overhead compared with TCP. [Official WebSocket documentation](https://docs.nats.io/learn/websocket/)

The JavaScript client reconnects by default, exposes `disconnect`, `reconnecting`, `reconnect`, `error`, `staleConnection`, and `close` through `NatsConnection.status()`, and re-sends subscriptions after a successful reconnect. Its reconnect settings include attempt limits, delay, jitter, first-connect behavior, and authentication-error handling. [nats.js core documentation](https://nats-io.github.io/nats.js/core/index.html), [3.3.1 reconnect implementation](https://github.com/nats-io/nats.js/blob/v3.3.1/core/src/protocol.ts)

That transport recovery is not message recovery by itself. Core NATS is temporally coupled: subscribers do not receive messages published while they are disconnected. JetStream supplies persistence and replay. NATS explicitly recommends JetStream or application acknowledgements when delivery matters across reconnects. [JetStream concepts](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/README.md), [reconnect buffering documentation](https://docs.nats.io/using-nats/developer/connecting/reconnect/buffer)

### 2. Ordered consumers are a client-managed replay cursor, not a durable worker

The modern nats.js consumer API creates an ordered consumer when `consumers.get(stream, orderedOptions)` is called without a consumer name. The public contract says that an inconsistency causes the underlying consumer to be recreated at the correct sequence, and that ordered messages cannot be acknowledged. [nats.js `Consumers` API](https://nats-io.github.io/nats.js/jetstream/types/Consumers.html)

The 3.3.1 implementation makes the mechanism concrete:

1. It reads `JsMsg.info.deliverySequence` and expects that consumer-local sequence to increase by exactly one.
2. It separately records `JsMsg.info.streamSequence`.
3. On a delivery-sequence mismatch, it deletes/recreates the consumer using `DeliverPolicy.StartSequence` and `opt_start_seq = last stream sequence + 1`.
4. It listens to connection reconnect events, resets pending pulls, watches idle heartbeats, and emits notifications such as `heartbeats_missed`, `consumer_deleted`, `stream_not_found`, and `ordered_consumer_recreated`.
5. The recreated consumer uses `AckPolicy.None`, `max_deliver = 1`, memory storage, and one replica.

See the [first-party 3.3.1 ordered-consumer implementation](https://github.com/nats-io/nats.js/blob/v3.3.1/jetstream/src/consumer.ts) and [ordered-consumer construction](https://github.com/nats-io/nats.js/blob/v3.3.1/jetstream/src/jsmstream_api.ts). NATS describes an ordered consumer as recreating itself at the expected sequence when a message is out of order. [nats.js JetStream guide](https://nats-io.github.io/nats.js/jetstream/index.html)

This is a good fit for a single UI or read model that needs its own ordered copy. It is deliberately different from a named durable consumer with explicit acknowledgements, which is the right primitive for shared workers, redelivery after failed processing, or a server-owned processing position. Pull consumers provide demand-driven batching and finer control, and NATS recommends them for scalable processing and avoiding slow-consumer problems. [Official JetStream development guidance](https://github.com/nats-io/nats.docs/blob/master/using-nats/jetstream/develop_jetstream.md), [first-party nats.go consumer guidance](https://github.com/nats-io/nats.go/blob/main/jetstream/README.md)

### 3. Start policies seed a consumer; a stored stream sequence resumes it

JetStream supports delivery of all retained messages, the last message, the last message per subject, only new messages, a specific start sequence, or a start time. `StartSequence` requires `opt_start_seq`; `New` begins with messages published after consumer creation. [nats.js `DeliverPolicy`](https://nats-io.github.io/nats.js/jetstream/variables/DeliverPolicy.html), [JetStream replay concepts](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/README.md)

A resumable wrapper should use `All` or `New` only when no checkpoint exists. Once a checkpoint exists, it should create the ordered consumer from `checkpoint.sequence + 1`.

The persistent cursor must be the _stream_ sequence, not the consumer delivery sequence:

- `streamSequence` identifies the stored message in the stream and survives consumer recreation.
- `deliverySequence` is local to an individual consumer incarnation and resets when that consumer is recreated.
- `deliveryCount`/`redelivered` describe attempts, and `pending` is a point-in-time estimate.

These values are carried in `JsMsg.info`; the NATS acknowledgement subject likewise includes delivery count, stream sequence, and consumer sequence. [nats.js `JsMsg`](https://nats-io.github.io/nats.js/jetstream/index.html), [JetStream protocol reference](https://github.com/nats-io/nats.docs/blob/master/using-nats/jetstream/nats_api_reference.md)

Subject filtering creates valid jumps in global stream sequence. For example, matching messages may be stream sequences 10 and 19 because 11–18 belong to other subjects. A wrapper must not call that a gap. nats.js correctly detects transport loss with the contiguous _delivery_ sequence while using the _stream_ sequence only as the resume location.

### 4. Acknowledgements define processing semantics and duplicates remain possible

`AckPolicy.None` requires no acknowledgements; `Explicit` requires each sequence to be acknowledged; `All` acknowledges the selected message and lower sequences. `ack()`, `nak()`, `working()`, `term()`, and `ackAck()` expose success, retry, progress, terminal failure, and a server-confirmed double acknowledgement. [nats.js `AckPolicy`](https://nats-io.github.io/nats.js/jetstream/variables/AckPolicy.html), [nats.js `JsMsg` API](https://nats-io.github.io/nats.js/jetstream/types/JsMsg.html)

Base JetStream quality of service is at least once. Duplicate publication can result from a lost publish acknowledgement, and duplicate delivery can result from a lost consumer acknowledgement. NATS publisher message IDs and subscriber double acknowledgements narrow those failure windows, but they do not atomically couple an arbitrary application side effect with the broker. [Official JetStream exactly-once discussion](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/README.md)

Therefore an open-source wrapper should say “ordered, resumable, at-least-once at the application boundary,” expose the stream sequence as an idempotency key, and make duplicates explicit. It should not advertise exactly once unless the user's state mutation and cursor write share an atomic transaction.

### 5. Backpressure must be bounded at both JetStream and WebSocket layers

JetStream can replay much faster than the original publish rate and can overwhelm a client. nats.js `consume()` maintains an internal pull buffer; `consume({ max_messages: 1 })` is the documented low-buffer pattern, while larger message/byte limits trade memory for throughput. The async iterator does not yield another item until the current loop body completes. [nats.js processing guide](https://nats-io.github.io/nats.js/jetstream/index.html)

NATS servers protect themselves by disconnecting clients that do not drain data. Client pending limits bound memory, but overflow must also be surfaced; otherwise the application can silently lose Core NATS data. [Official slow-consumer documentation](https://docs.nats.io/learn/resilient-clients/slow-consumers)

For a NATS-to-WebSocket gateway, bounded NATS pulling is only half the solution. Each downstream socket also needs a bounded outbound queue. A slow socket should be closed with its last delivered cursor so it can reconnect, rather than allowing an unbounded per-client queue. Direct browser consumers similarly need serial handling or an explicit concurrency limit.

## Failure and recovery semantics

| Failure                                | nats.js behavior                                                                            | Wrapper/application responsibility                                                                      |
| -------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Short WebSocket/TCP disconnect         | Core client reconnects and restores subscriptions. Ordered pulls are reset/reissued.        | Report status; do not create a second competing session.                                                |
| Out-of-order/missed ordered delivery   | nats.js detects a `deliverySequence` mismatch and recreates from `last streamSequence + 1`. | Observe and count recreation; do not duplicate this algorithm.                                          |
| Ordered consumer deleted/inactive      | Ordered client recreates it; status describes deletion/recreation.                          | Continue from the in-memory cursor, then from persisted cursor after a process replacement.             |
| Browser reload/process crash           | Ordered cursor disappears with the JS object.                                               | Load a durable cursor and start at `sequence + 1`.                                                      |
| Handler throws before checkpoint       | Ordered consumer itself cannot NAK because it is `AckNone`.                                 | Retry the same in-memory item or recreate from the last committed cursor; never advance the checkpoint. |
| Cursor predates stream retention       | Requested history no longer exists.                                                         | Emit/fail with an explicit retention gap according to configured policy.                                |
| Stream delete and recreate             | Sequence numbers can restart, making an old numeric cursor ambiguous.                       | Bind the cursor to the stream creation timestamp (an epoch) and reject an epoch mismatch.               |
| Subject-filtered stream sequence jumps | Normal: other subjects occupy the skipped global sequences.                                 | Never infer a gap merely from `current streamSequence > previous + 1`.                                  |
| Authentication expires                 | Client normally aborts after repeated identical auth errors unless configured otherwise.    | Refresh credentials deliberately; surface terminal auth versus transient reconnect.                     |
| Slow downstream WebSocket/client       | Buffers grow or connection can be dropped.                                                  | Bound pull size and downstream queue; disconnect with a resumable cursor.                               |

## Current-code implications

### What is already aligned

- [`NatsContext.tsx`](../../src/app/contexts/NatsContext.tsx) enables built-in reconnect indefinitely, monitors connection status, uses a token authenticator backed by a mutable token ref, and separately retries failed initial connections. That is a sound connection owner for an incremental wrapper.
- [`nats-rx.ts`](../../src/lib/nats/nats-rx.ts) already offers `{ ordered: true }`, drains `ConsumerMessages.status()`, and uses the current nats.js consumer API.
- [`event-stream.ts`](../../src/lib/caspian-v2/services/event-stream.ts) and [`use-conversation-tail.ts`](../../src/hooks/caspian-v2/use-conversation-tail.ts) already use ordered consumers for canonical Caspian events. Core NATS token deltas remain explicitly ephemeral/display-only, while the canonical final event comes through JetStream; that separation is sensible.
- `shareReplay({ refCount: true })` fans a single ordered consumer out inside the application rather than attempting concurrent reads from one ordered consumer, which nats.js forbids.

### Gaps and corrections

1. **The comments overstate the lifetime of ordered recovery.** Ordered resume is transparent while the ordered consumer object and its cursor survive. It does not persist progress through reload, process crash, a brand-new connection/service instance, or a cold gateway handoff.

2. **`DeliverPolicy.New` is not reload-safe.** The conversation tail starts a new ordered consumer “from now.” If no saved cursor is supplied after a full teardown, events published during that absence are skipped. The full event stream's `DeliverPolicy.All` takes the opposite risk and can replay retained history.

3. **There is a units mismatch in the ordered path.** `createJetStreamObservable` accepts `Partial<ConsumerConfig>`, whose `inactive_threshold` is nanoseconds. In the ordered branch it forwards that number into `OrderedConsumerOptions`; nats.js 3.3.1 then calls `nanos(opts.inactive_threshold)`, treating the option as milliseconds. The current `300_000_000_000` “5 minutes in nanoseconds” therefore becomes roughly 9.5 years before conversion, rather than five minutes. The wrapper should expose `Duration`/milliseconds at its boundary and perform the conversion in exactly one place. [First-party construction code](https://github.com/nats-io/nats.js/blob/v3.3.1/jetstream/src/jsmstream_api.ts), [consumer-config nanosecond documentation](https://nats-io.github.io/nats.js/jetstream/types/ConsumerConfig.html)

4. **Downstream success is not the ordered cursor.** nats.js updates its cursor before `observer.next(msg)`. RxJS processing/parsing and UI state changes happen afterward. There is no checkpoint tied to successful downstream processing.

5. **No stable cursor is exposed.** `JsMsg.info.streamSequence` is currently discarded when events are parsed. It should remain attached as event metadata and be persisted only after the handler succeeds.

6. **Observable caching is not cursor-aware.** `CaspianEventStreamService.activeStreams` is keyed only by organization/workspace, not connection identity, start/checkpoint, or initial delivery policy. The entry is not removed when `shareReplay` ref-count reaches zero. A migration should make session identity and teardown explicit before removing the existing watchdogs.

7. **The legacy push-consumer service is a separate migration.** [`jetstream-service.ts`](../../src/lib/nats/jetstream-service.ts) manually creates deliver inboxes and Core subscriptions. Do not expand the new wrapper to preserve that low-level API. Keep it temporarily, migrate read/replay feeds to ordered pull consumers first, and use named durable pull consumers for processing workloads that require explicit acknowledgement.

## Proposed session primitive: one start operation, one status stream, one close operation

The design is intentionally callback-first instead of Observable-first. A serial awaited handler gives the library a precise point at which it may advance the application checkpoint and naturally applies backpressure.

```ts
import type { JsMsg } from '@nats-io/jetstream'
import type { NatsConnection } from '@nats-io/nats-core'

export type ResumeCursor = Readonly<{
  v: 1
  stream: string
  streamCreated: string // epoch from StreamInfo.created
  subject: string
  sequence: number // JsMsg.info.streamSequence
}>

export type CursorStore = {
  load(key: string): Promise<ResumeCursor | null>
  save(key: string, cursor: ResumeCursor): Promise<void>
}

export type ResumeStatus =
  | { type: 'connecting' | 'live' | 'reconnecting' | 'closed' }
  | { type: 'consumer-recreated'; at: number }
  | { type: 'duplicate'; sequence: number }
  | { type: 'gap'; reason: 'retention' | 'stream-recreated'; after: number }
  | { type: 'error'; error: Error; terminal: boolean }

export type ResumableMessage<T> = Readonly<{
  value: T
  cursor: ResumeCursor
  duplicate: boolean
  raw: JsMsg
}>

export type ResumeSession = {
  readonly status: AsyncIterable<ResumeStatus>
  close(): Promise<void>
}

// Starts immediately. This is the package's only constructor/start operation.
export function consumeResumable<T>(
  options: {
    nc: NatsConnection
    stream: string
    subject: string
    key?: string
    start?: 'all' | 'new'
    cursorStore?: CursorStore
    after?: ResumeCursor
    duplicates?: 'deliver' | 'drop' | 'error'
    gaps?: 'error' | 'start-earliest' | 'start-new'
    maxBuffered?: number
    decode(msg: JsMsg): T
    signal?: AbortSignal
  },
  handle: (message: ResumableMessage<T>) => void | Promise<void>
): ResumeSession
```

The three operations a normal caller sees are `consumeResumable(...)`, iteration of `session.status` when observability is wanted, and `session.close()`. Cursor storage is dependency injection, so browser `localStorage`, IndexedDB, Redis, a database transaction, or a test fake can implement the same two-function port without becoming a package-level persistence opinion.

### React/Next.js usage

```ts
const cursorStore: CursorStore = {
  async load(key) {
    const json = localStorage.getItem(key)
    return json ? JSON.parse(json) : null
  },
  async save(key, cursor) {
    localStorage.setItem(key, JSON.stringify(cursor))
  },
}

const session = consumeResumable(
  {
    nc,
    stream: 'CASPIAN_EVENTS',
    subject: `caspian.events.${organizationId}.${workspaceId}`,
    key: `conversation:${organizationId}:${workspaceId}`,
    start: 'all',
    cursorStore,
    duplicates: 'deliver', // reducer uses event id/stream sequence idempotently
    gaps: 'error',
    maxBuffered: 32,
    decode: (msg) => parseJetStreamEvent(msg),
    signal: abortController.signal,
  },
  async ({ value, cursor }) => {
    // Resolve only after the application accepted the event. The wrapper saves
    // `cursor` after this promise succeeds.
    applyEventIdempotently(value, cursor.sequence)
  }
)

return () => session.close()
```

For an application WebSocket gateway, `after` comes from the authenticated reconnect handshake and each outgoing frame includes `message.cursor`. The browser stores the last applied cursor and presents it on the next connection. The server must bind/validate the cursor against the route's stream and subject; a cursor is a position, not authorization. A slow client is disconnected once its bounded send queue fills and resumes with that cursor later.

### Hidden internals

The small interface hides the following state machine:

1. Load `after` or the cursor store and fetch `StreamInfo`.
2. Verify stream name, subject, and `StreamInfo.created`. Detect a definite retention gap when the requested cursor is older than `state.first_seq - 1`; do not infer gaps from filtered sequence jumps.
3. With no cursor, seed from `start`. With a cursor, create a nats.js ordered pull consumer at `opt_start_seq = cursor.sequence + 1`.
4. Consume through an async iterator with bounded `max_messages`/bytes, while merging `NatsConnection.status()` and `ConsumerMessages.status()` into `session.status`.
5. Decode and call exactly one handler at a time. Update the in-memory committed cursor and then the store only after the handler resolves. If the handler fails, retain/retry the same message or recreate from the last committed sequence.
6. Compare incoming stream sequences with the last committed sequence for explicit duplicate policy. Let nats.js use delivery sequence for transport-gap detection.
7. Apply exponential reconnect/handler retry with jitter and terminal classification for authorization, invalid cursor, deleted/recreated stream, and abort.
8. On close, stop the message iterator, await its closure, delete the ephemeral ordered consumer best-effort, stop status iterators, and leave the caller-owned `NatsConnection` open.

## Tradeoffs

- **Ordered ephemeral plus external cursor versus durable consumer:** external cursors are natural for per-browser/per-view replay and WebSocket resume tokens, and they avoid one server consumer per inactive browser. Durable consumers are stronger for server jobs with explicit processing acknowledgements and shared workers. The wrapper should support the first use case, not blur both into one mode.
- **At least once versus loss:** checkpoint-after-handler can duplicate an event if the effect succeeds but the checkpoint write fails. Checkpoint-before-handler can lose it. Default to the former and require idempotent handlers keyed by stream sequence/event ID.
- **Strict gap failure versus availability:** `gaps: 'error'` protects correctness and should be the default. Resetting to earliest/newest is useful for disposable UI projections but must emit an observable gap event.
- **Per-subject gap certainty:** a global stream sequence jump is not proof that a filtered subject lost a message. Stronger proofs require retention/epoch checks, subject-specific stream design, delete markers, or application-level event ordinals.
- **Cursor storage:** local storage survives reload but not another device; server storage enables cross-device resume but creates identity, expiry, and tenancy concerns. Keeping storage behind a port lets adopters choose.
- **Cursor trust:** an unsigned client cursor can request older data and amplify replay. Gateways should validate bounds and subject/tenant binding, optionally signing opaque cursor tokens.
- **One ordered consumer per logical feed:** ordered consumers reject concurrent consume/fetch activity. Fan out after the wrapper (React store, RxJS adapter, or gateway hub), not by sharing the consumer API concurrently.
- **Retention is the real offline limit:** no reconnect wrapper can recover an event already removed by stream limits, purge, work-queue retention, or stream recreation. Retention and cursor expiry must be designed together.

## Incremental migration and validation

1. First extract the ordered branch of `createJetStreamObservable` behind `consumeResumable`, while continuing to accept the existing `NatsConnection` from `NatsContext`.
2. Fix the ordered `inactive_threshold` unit boundary and add tests that inspect the server consumer configuration.
3. Preserve `streamSequence` and stream epoch through `parseJetStreamEvent`; add an RxJS adapter locally if consumers still need `Observable`, but keep RxJS outside the open-source core.
4. Migrate `useConversationTail` with a per-conversation cursor. Keep the current Core NATS token-delta channel as best-effort UI data and reconcile it with canonical JetStream events.
5. Migrate the full Caspian event stream, remove stale cache entries on teardown/connection replacement, and only then simplify watchdogs.
6. Leave processing consumers on named durable pull consumers with explicit acknowledgements; migrate the manual push service separately.

Before presenting the package as reliable, run deterministic fault tests for disconnect during delivery, reconnect to another server, consumer deletion, missed heartbeats, handler failure before/after checkpoint save, page reload, stale token refresh, stream purge/retention expiry, stream delete/recreate, duplicate replay, filtered sequence jumps, and a downstream WebSocket that stops reading. The key assertions are ordered values, explicit duplicate/gap events, bounded memory, no leaked consumers/status iterators, and restart from the last successfully committed stream sequence.
