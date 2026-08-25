# @natsail/rxjs

`@natsail/rxjs` provides Observables for Core subscriptions, JetStream subscriptions, shared sessions, runtime events, and connection status.

```sh
pnpm add rxjs @natsail/core @natsail/session @natsail/jetstream @natsail/rxjs
```

The adapter uses the framework-neutral session registry. React and RxJS consumers can share one source without a bridge package.

`observeNatsJetStreamSubscription()` emits full deliveries from the same keyed session that React hooks use. `observeNatsJetStreamReducer()` consumes the same validated atomic state definition as `useNatsJetStreamReducer()`.

`observeNatsSession()` and `observeNatsSessionValues()` accept validated definitions as well as the legacy key/source pair. `observeNatsSessionEvents()` adapts registry lifecycle and reference-count diagnostics into a cancellable Observable.

See the [NATSail README](https://github.com/thedonmon/natsail#shared-session-adapters) for RxJS examples.

## License

Apache-2.0
