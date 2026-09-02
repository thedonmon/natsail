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

A failed handler leaves the message unacknowledged for server redelivery. The application can configure the acknowledgement wait, maximum deliveries, maximum pending acknowledgements, replay policy, and start position.

Consumer ownership has three modes:

- `bind` attaches to an administrator-managed consumer.
- `ensure` creates or reuses a retained consumer.
- `owned` creates a consumer that the lease deletes when it closes.

NATSail checks an existing consumer against the requested filter and start position. It also checks each supplied acknowledgement or replay setting.

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

Configuration, decode, retention-gap, duplicate-policy, and application-handler failures remain terminal by default.

Call `runtime.reconnect()` after an authenticator receives new credentials. The promise resolves after the runtime observes a new connected generation.

A reconnect can interrupt in-flight messages and requests. The configured nats.js reconnect behavior still applies.

## Resource limits and buffering

The JetStream adapter bounds each nats.js pull loop to 32 buffered messages by default. Set `maxBufferedMessages` to change this bound.

Set `maxBufferedBytes` to use a byte limit. A consumer cannot use message and byte limits at the same time.

The runtime tracks message capacity and byte capacity separately. It can also limit the active JetStream consumer count.

Effect Streams have a second decoded queue. Reliable JetStream Streams support `suspend` and `error` overflow policies.

Core NATS Effect Streams can also use dropping or sliding policies. These policies do not add server retention to Core NATS.

## Browser connection model

Many conversations in one browser tab can share one runtime connection. Each active JetStream conversation still has its own consumer and bounded pull loop.

Each browser tab has a separate JavaScript realm. One runtime in each tab creates one connection in each tab.

The repository contains a `SharedWorker` proof that lets two tabs share one connection. It is not a published browser-broker package.

The Durable Object prototype owns one upstream JetStream consumer for multiple clients. It also stores an upstream checkpoint and supports bounded client catch-up after restart.

## Escape hatch

`runtime.connection()` returns the runtime-owned nats.js connection. Use it for manager calls or protocol operations that NATSail does not manage yet.

Application code must not close or drain this connection. NATSail continues to own its lifecycle.
