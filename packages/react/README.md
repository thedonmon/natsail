# @natsail/react

`@natsail/react` provides a NATSail provider, runtime status hooks, session selectors, Core hooks, JetStream hooks, and serial reducer hooks.

```sh
pnpm add react @natsail/core @natsail/session @natsail/jetstream @natsail/react
```

The provider owns neither the runtime nor the session registry. The application closes both objects.

`useNatsJetStreamSubscription()` and its selector variant open one registry-shared, checkpointed JetStream source.

`useNatsConnection()` follows the runtime-owned connection for advanced nats.js operations without adding an application connection effect.

`useNatsJetStreamProcessor()` owns one explicit-ack processor lease. Pass `null` options to disable it without conditionally calling a hook; change the key when its consumer configuration changes.

See the [NATSail README](https://github.com/thedonmon/natsail#shared-session-adapters) for React examples.

## License

Apache-2.0
