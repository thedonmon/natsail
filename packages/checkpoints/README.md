# @natsail/checkpoints

`@natsail/checkpoints` provides memory and IndexedDB stores for NATSail stream cursors. Both stores reject sequence regressions and validate stream identity.

```sh
pnpm add @natsail/checkpoints
```

The memory store fits tests and session-only recovery. The IndexedDB store keeps checkpoints across browser reloads.

See the [NATSail README](https://github.com/thedonmon/natsail#delivery-guarantees) for checkpoint semantics.

## License

Apache-2.0
