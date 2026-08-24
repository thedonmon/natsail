# @natsail/session

`@natsail/session` shares one managed source across keyed consumers. It owns source lifecycle, immutable snapshots, serial reducers, and idle cleanup.

```sh
pnpm add @natsail/core @natsail/session
```

React and RxJS adapters can use the same registry and key. This sharing prevents duplicate source subscriptions.

Call `handle.restart()` or `registry.restart(key)` to reopen a terminal source. The session keeps its handle and latest accepted value.

A restart rejects deliveries from the prior source generation. A restart does not occur automatically after an application handler fails.

See the [NATSail README](https://github.com/thedonmon/natsail#shared-session-adapters) for the registry model.

## License

Apache-2.0
