# @natsail/checkpoints

`@natsail/checkpoints` provides memory and IndexedDB stores for NATSail stream cursors. Both stores reject sequence regressions and source-scope conflicts.

```sh
pnpm add @natsail/checkpoints
```

The memory store fits tests and session-only recovery. The IndexedDB store keeps checkpoints across browser reloads.

A checkpoint can contain a logical source scope. The JetStream package derives this scope from normalized filters and an optional application version.

See the [delivery model](https://github.com/thedonmon/natsail/blob/main/docs/DELIVERY.md) for checkpoint semantics.

## License

Apache-2.0
