---
name: natsail-rxjs
description: Use @natsail/rxjs to consume NATSail runtime events, Core subscriptions, shared sessions, JetStream deliveries, or reduced JetStream state as cancellable RxJS Observables with controlled presentation batching.
---

# Use @natsail/rxjs

```sh
pnpm add rxjs @natsail/core @natsail/session @natsail/jetstream @natsail/rxjs
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
import { map } from 'rxjs'
import { observeNatsJetStreamState } from '@natsail/rxjs'

const messageCount$ = observeNatsJetStreamState(sessions, conversationState, {
  liveBatchMs: 16,
}).pipe(map((snapshot) => snapshot.data.messages.length))

const subscription = messageCount$.subscribe(renderCount)

// Releases this subscriber's session handle.
subscription.unsubscribe()
```

`observeNatsJetStreamState()` emits replay/recovery phases and the first hydrated live state immediately, then coalesces later cumulative live states to the newest value per window. Set `liveBatchMs: 0` when every reduced live state must be observed. Every JetStream delivery is still reduced serially; batching affects presentation only.

Each Observable subscription owns one registry handle and releases it on unsubscribe. Equal consecutive application values remain distinct deliveries in `observeNatsSessionValues()`; lifecycle-only changes do not duplicate a value. Pending live state is flushed before reconnect, completion, or error.

There is no RxJS wrapper for explicit-ack processors. Use `processJetStream()` from `@natsail/jetstream` for durable work, then adapt application state deliberately if an Observable is needed.

See the [RxJS package guide](https://github.com/thedonmon/natsail/tree/main/packages/rxjs#readme) and [npm package](https://www.npmjs.com/package/@natsail/rxjs).
