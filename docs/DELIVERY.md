# Delivery model and guarantees

This document describes where NATSail starts and stops. It also explains which delivery contract applies to each consumer type.

## Core NATS

Core NATS delivers live messages. It does not replay messages that arrive while a subscriber is offline.

The runtime uses one connection for Core subscriptions, publish, and request/reply. Each subscription handler runs serially.

`runtime.request()` supports a payload codec, timeout, headers, and an abort signal. NATSail never retries an ambiguous request after it can reach a responder.

Core subscriptions, requests, ordered consumers, and explicit-ack processors accept the same `NatsPayloadCodec<T>` interface. The built-in codecs support text, JSON, and bytes.

## Ordered JetStream replay

`consumeJetStream()` uses one ordered pull consumer for replay and live delivery. Each delivery includes its stream cursor.

The consumer captures the initial pending count when it opens. Each delivery identifies initial replay or live traffic.

The `caughtUp` promise resolves after the consumer processes its captured replay. New messages can arrive during that replay without changing its boundary.

Ordered consumers use `AckPolicy.None`. Their saved application cursor is not a durable server acknowledgement.

If a consumer has a resume configuration, NATSail saves its checkpoint after the handler succeeds. A failed handler does not advance the checkpoint.

The checkpoint contains the stream name, stream epoch, sequence, and source scope. The epoch identifies a recreated stream.

The source scope includes normalized filters and an optional application version. A mismatched scope stops resume with a typed error.

Memory and IndexedDB stores reject sequence regressions. A stale writer cannot replace a newer checkpoint with an older sequence.

## Explicit-ack processing

`processJetStream()` uses a named pull consumer with `AckPolicy.Explicit`. It acknowledges a message after the handler succeeds.

A failed handler leaves the message unacknowledged for server redelivery. The application can configure acknowledgement wait/backoff, maximum deliveries, maximum pending acknowledgements, metadata, acknowledgement sampling, replicas, memory storage, replay policy, and start position.

Consumer ownership has three modes:

- `bind` attaches to an administrator-managed consumer and never mutates it.
- `ensure` creates or reuses a retained consumer and may update editable settings.
- `owned` creates a durable consumer that the lease deletes when it closes and may safely recreate it.

`createJetStreamProcessorController()` exposes cached inspection plus authoritative refresh, reconciliation, pause, resume, and ownership-guarded delete. Operations are serialized. Reconciliation results distinguish unchanged, created, updated, recreated, and rejected outcomes and report normalized desired/active configuration plus editable and immutable drift. `error`, `update-editable`, and `recreate-owned` policies cannot grant ownership that the consumer mode does not have.

Set `recovery` to reopen the named consumer after an infrastructure failure. When that consumer is retained, recovery uses its server-side acknowledgement floor: acknowledged messages remain complete, and an interrupted unacknowledged message remains eligible for redelivery. A deleted owned `start: 'new'` consumer is recreated from the last safe acknowledgement boundary, so messages published during the deletion gap are not skipped. Handler, decoder, and consumer-contract failures stay terminal.

An `owned` recovering processor retains its named consumer between attempts and deletes it when the logical processor lease closes.

Processor `inspect()` is synchronous and cached. It includes phase, ownership, restart count, pending messages and acknowledgements, consumer and stream delivery/acknowledgement sequences, redeliveries, pause state, last handler failure, normalized desired/active configuration, and the last reconciliation. Controller `refresh()` performs the explicit management read.

## Duplicate and retention policy

An incoming sequence at or behind the application cursor is a duplicate. The default `duplicateDeliveryPolicy` is `drop`.

Use `deliver` to receive the event with `delivery.duplicate === true`. Use `error` to stop with `JetStreamDuplicateError`.

The NATS `redelivered` flag remains separate from application-cursor duplication.

NATSail stops with a `retention-gap` error when stream retention removes required messages. The `continue` policy starts from the first retained message instead.

## Replay reduction and rendering

`defineReducingJetStreamSession()` folds the initial replay without publishing partial application state. It publishes one complete snapshot at catch-up, then publishes serially reduced live state.

React selectors and RxJS state Observables can coalesce live notifications. Every delivery still reaches the reducer in stream order.

Effect materialization reduces bounded replay batches and supports bounded live queues. Effect owns downstream demand and structured interruption.

A fresh reducing session rebuilds its state from the retained stream. Persisted reducer resume needs a state store that commits the materialized state and cursor together.

## Recovery

The runtime replaces a permanently closed connection by default. Its event stream remains active until `runtime.close()` completes.

Shared JetStream sources can use package-owned consumer recovery. Recovery starts after the last successfully processed cursor.

Named explicit-ack processors can also use package-owned recovery. They resume from the server acknowledgement floor instead of an application checkpoint.

Configuration, decode, retention-gap, duplicate-policy, and application-handler failures remain terminal by default.

Call `runtime.reconnect()` after an authenticator receives new credentials. The promise resolves after the runtime observes a new connected generation.

A reconnect can interrupt in-flight messages and requests. The configured nats.js reconnect behavior still applies.

## Resource limits and buffering

The JetStream adapter bounds each nats.js pull loop to 32 buffered messages by default. Set `maxBufferedMessages` to change this bound.

Set `maxBufferedBytes` to use a byte limit. A consumer cannot use message and byte limits at the same time.

The runtime tracks message capacity and byte capacity separately. It can also limit the active JetStream consumer count.

Effect Streams have a second decoded queue. Reliable JetStream Streams support `suspend` and `error` overflow policies.

Core NATS Effect Streams can also use dropping or sliding policies. These policies do not add server retention to Core NATS.

## Batch and reducer boundaries

`NatsailBatchPolicy<T>` provides count, byte, and time bounds. Normal completion flushes a partial batch; cancellation discards it. A full batch already applying is allowed to finish, and close does not resolve until that application settles.

Reducing JetStream sessions admit at most one applying batch before backpressuring intake. Reducers run serially and may yield through `NatsailWorkBudget`; yields never introduce concurrent reducer calls. Replay batches remain private until one caught-up state commit. Live cumulative state uses a 16ms default window, while replay/recovery phase notifications remain immediate. Cursor and checkpoint advancement follows successful downstream batch application.

## Telemetry and diagnostics

`runtime.events` is the low-frequency lifecycle and diagnostic stream. High-frequency measurements never enter it.

Pass a `NatsailTelemetrySink` to `createNatsRuntime()` to observe connection attempts and recovery, publish/request durations and outcomes, resource reservations and configured limits, JetStream replay and remaining work, handler/redelivery/acknowledgement outcomes, checkpoint load/save durations, recovery attempts, and slow-consumer or overflow signals. Pass the same sink to `createSessionRegistry()` for active-session, reference-count, and lifecycle measurements.

Telemetry events contain fixed NATSail dimensions plus optional caller-supplied low-cardinality primitive attributes. NATSail does not copy diagnostic details into telemetry, so subjects, payloads, credentials, session/checkpoint keys, stream names, and consumer names are absent by default. NATSail's reserved per-event dimensions take precedence over colliding caller attributes.

The sink is synchronous and runs inline around the observed operation. It should enqueue measurements and avoid network or filesystem I/O. NATSail catches sink exceptions, but it cannot preempt a callback that blocks the JavaScript thread. Duration tests can inject `telemetryClock`; production defaults to the host monotonic performance clock.

## Browser connection model

Many conversations in one browser tab can share one runtime connection. Each active JetStream conversation still has its own consumer and bounded pull loop.

Each browser tab has a separate JavaScript realm. One runtime in each tab creates one connection in each tab unless the application opts into `@natsail/browser-broker`.

The browser broker runs caller-supplied `SessionSource` definitions inside a `SharedWorker`. The stable tenant, authentication context, logical key, and contract select one physical source. Contract conflicts fail deterministically. Credentials are transferred from authenticated tab bootstrap, use monotonic revisions, and can refresh without changing source identity.

Each tab has independent item and encoded-byte bounds and at most one transferred batch in flight. It acknowledges the batch cursor only after its local SessionSource handler accepts every item. A tab that exceeds its bound receives `resume-required` with reason `lagged`; no reliable item is silently discarded. A bounded physical-source log supports catch-up from retained per-tab JetStream cursors.

Worker replacement reconnects active tab sources after their last acknowledged cursor. Heartbeats release references for abandoned ports, and final-reference teardown honors the configured idle delay. Applications may configure an explicit tab-local fallback, but strict mode rejects environments without SharedWorker when duplicate connections would violate policy.

The protocol is versioned and same-origin. It does not replace authorization: worker source factories must map authenticated identities to allowed application sources instead of accepting arbitrary subjects, streams, or consumer names.

The Durable Object prototype owns one upstream JetStream consumer for multiple clients. It also stores an upstream checkpoint and supports bounded client catch-up after restart.

## Escape hatch

`runtime.connection()` returns the runtime-owned nats.js connection. Use it for manager calls or protocol operations that NATSail does not manage yet.

Application code must not close or drain this connection. NATSail continues to own its lifecycle.
