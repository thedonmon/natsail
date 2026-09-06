---
name: natsail-rxjs
description: Use @natsail/rxjs to consume NATSail runtime events, Core subscriptions, shared sessions, JetStream deliveries, or reduced JetStream state as cancellable RxJS Observables with controlled presentation batching.
---

# Use @natsail/rxjs

This branch targets RxJS `9.0.0-beta.0` for evaluation. Use its local workspace build; don't install released `@natsail/rxjs` with RxJS 9 or publish this branch. Released NATSail packages still target RxJS 7.

```sh
pnpm install --frozen-lockfile
pnpm --filter @natsail/rxjs... build
```

Create one runtime and session registry for the application. Observable subscribers with the same registry and validated definition share the underlying source through `@natsail/session`.

## Choose an Observable

- `observeNatsRuntimeEvents(runtime)` for structured connection and diagnostic events.
- `observeNatsRuntimeStatus(runtime)` for distinct connection-state changes.
- `observeNatsCoreSubscription(sessions, runtime, key, options)` for a shared live subject.
- `observeNatsJetStreamSubscription(sessions, runtime, key, options)` for full replay/live deliveries.
- `observeNatsJetStreamReducer(sessions, definition)` for exact session snapshots from a reducing definition.
- `observeNatsJetStreamState(sessions, definition, options)` for cumulative application state with presentation batching.
- `observeNatsSession()` for lifecycle snapshots and `observeNatsSessionValues()` for delivered values only.

```ts
import { map } from 'rxjs/map'
import { observeNatsJetStreamState } from '@natsail/rxjs'

const messageCount$ = observeNatsJetStreamState(sessions, conversationState, {
  liveBatchMs: 16,
})[map]((snapshot) => snapshot.data.messages.length)

const controller = new AbortController()
messageCount$.subscribe(renderCount, { signal: controller.signal })

// Releases this subscriber's session handle.
controller.abort()
```

`observeNatsJetStreamState()` emits replay/recovery phases and the first hydrated live state immediately, then coalesces later cumulative live states to the newest value per window. Set `liveBatchMs: 0` when every reduced live state must be observed. Every JetStream delivery is still reduced serially; batching affects presentation only.

Each direct subscription to a session-backed `ColdObservable` owns a registry handle and releases it on abort. RxJS 9's `subscribe()` returns `undefined`; create the controller before subscribing so synchronous callbacks can cancel safely. Exact Symbol operators preserve this per-subscriber ownership. Native string-named Observable methods return platform Observables with a different shared producer contract.

Equal consecutive application values remain distinct deliveries in `observeNatsSessionValues()`; lifecycle-only changes do not duplicate a value. Pending live state flushes before reconnect or completion; abort and error discard it. Custom batching schedulers use NATSail's `schedule(task, delayMs)` contract returning `{ cancel() }`, not RxJS 7 `SchedulerLike`.

There is no RxJS wrapper for explicit-ack processors. Use `processJetStream()` from `@natsail/jetstream` for durable work, then adapt application state deliberately if an Observable is needed.

Read the branch's [RxJS package guide](../../../packages/rxjs/README.md) for the known upstream beta lifecycle differences and release gates. Don't add an RxJS 7 compatibility shim or assume foreign-realm Observable inputs are supported.
