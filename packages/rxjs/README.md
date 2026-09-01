# @natsail/rxjs

`@natsail/rxjs` provides Observables for Core subscriptions, JetStream subscriptions, shared sessions, runtime events, and connection status.

```sh
pnpm add rxjs @natsail/core @natsail/session @natsail/jetstream @natsail/rxjs
```

The adapter uses the framework-neutral session registry. React and RxJS consumers can share one source without a bridge package.

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

`observeNatsSession()` and `observeNatsSessionValues()` accept validated definitions as well as the legacy key/source pair. `observeNatsSessionEvents()` adapts registry lifecycle and reference-count diagnostics into a cancellable Observable.

See the [NATSail README](https://github.com/thedonmon/natsail#shared-session-adapters) for RxJS examples.

## License

Apache-2.0
