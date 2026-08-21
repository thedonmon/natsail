# @natsail/jetstream

`@natsail/jetstream` adds ordered replay, application checkpoints, duplicate policies, and retention-gap handling to a NATSail runtime.

```sh
pnpm add @natsail/core @natsail/checkpoints @natsail/jetstream
```

`consumeJetStream()` uses an ordered consumer with `AckPolicy.None`. It saves the application checkpoint only after the handler succeeds.

See the [NATSail README](https://github.com/thedonmon/natsail#jetstream-resume-example) for an example and the current acknowledgement boundary.

## License

Apache-2.0
