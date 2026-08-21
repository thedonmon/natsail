# @natsail/core

`@natsail/core` provides one shared NATS connection for many managed subscriptions. It also owns retry, cleanup, status, limits, and runtime diagnostics.

```sh
pnpm add @natsail/core @nats-io/transport-node
```

The application supplies the connection factory. Core NATS does not require JetStream.

See the [NATSail README](https://github.com/thedonmon/natsail#core-nats-example) for an example and delivery guarantees.

## License

Apache-2.0
