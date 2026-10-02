# @natsail/rxjs

`@natsail/rxjs` provides Observables for Core subscriptions, JetStream subscriptions, shared sessions, runtime events, and connection status.

```sh
pnpm add rxjs @natsail/core @natsail/session @natsail/jetstream @natsail/rxjs
```

The adapter uses the framework-neutral session registry. React and RxJS consumers can share one source without a bridge package.

`batchWithPolicy()` groups any Observable into arrays bounded by the same `NatsailBatchPolicy` that Core, React, and Effect use. The timer starts at the first value of a batch, so an idle subscription schedules nothing and never emits an empty array:

```ts
import { batchWithPolicy, observeNatsCoreSubscription } from '@natsail/rxjs'

const batches$ = observeNatsCoreSubscription(sessions, runtime, 'tokens:room-1', {
  subject: 'chat.tokens.room-1',
  decode: decodeToken,
}).pipe(batchWithPolicy({ maxItems: 500, maxWaitMs: 50 }))
```

A batch flushes when `maxItems` or `maxBytes` is reached, before a value that would overflow `maxBytes`, and when `maxWaitMs` elapses. Source completion and source errors flush the pending batch first. A `sizeOf` that throws or returns a negative or non-finite number, or one item larger than `maxBytes` (`NatsailBatchItemTooLargeError`), errors the stream and drops the pending batch, as unsubscribe does. The policy is validated when the operator is created. Pass `{ scheduler }` to use a custom RxJS scheduler.

`observeNatsJetStreamSubscription()` emits full deliveries from the same keyed session that React hooks use. `observeNatsJetStreamReducer()` consumes the same validated atomic state definition as `useNatsJetStreamReducer()` and exposes exact session lifecycle snapshots.

For rendering cumulative application state, prefer `observeNatsJetStreamState()`. It removes duplicate session-lifecycle notifications, emits replay and the first hydrated live state immediately, and coalesces subsequent cumulative live states to the latest value once per 16ms window:

```ts
import { distinctUntilChanged, map } from 'rxjs'

import { observeNatsJetStreamState } from '@natsail/rxjs'

const conversation$ = observeNatsJetStreamState(sessions, conversationDefinition, {
  liveBatchMs: 16,
})

const messageCount$ = conversation$.pipe(
  map((snapshot) => snapshot.data.messages.length),
  distinctUntilChanged()
)
```

Every JetStream delivery is still applied serially by the validated reducer. Only cumulative state presentation is coalesced, so a slow or busy browser does not lose events. The initial history rebuild remains one atomic hydrated state instead of hundreds of partial conversation states. Set `liveBatchMs: 0` when every reduced live state must be observed. A custom RxJS scheduler may be supplied for host integration or deterministic tests.

`batchPolicy` is the shared alternative to `liveBatchMs` and can bound cumulative notifications by count, bytes, and time. The legacy option remains supported. Replay, reconnect, and the first hydrated live value are immediate; normal completion flushes the latest pending live state, while unsubscribe discards it.

`observeNatsSession()` and `observeNatsSessionValues()` accept validated definitions as well as the legacy key/source pair. `observeNatsSessionEvents()` adapts registry lifecycle and reference-count diagnostics into a cancellable Observable.

See the [NATSail README](https://github.com/thedonmon/natsail#shared-session-adapters) for RxJS examples.

## License

Apache-2.0
