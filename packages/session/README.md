# @natsail/session

`@natsail/session` shares one managed source across keyed consumers. It owns source lifecycle, immutable snapshots, serial reducers, and idle cleanup.

```sh
pnpm add @natsail/core @natsail/session
```

React and RxJS adapters can use the same registry and key. This sharing prevents duplicate source subscriptions.

See the [NATSail README](https://github.com/thedonmon/natsail#shared-session-adapters) for the registry model.

## License

Apache-2.0
