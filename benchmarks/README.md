# Local benchmark foundation

`pnpm benchmark` prints one versioned JSON document. It runs without NATS or Docker so results can be captured on developer machines and CI hosts before an integration benchmark is added.

The default suite exercises 1,000- and 5,000-event replay models plus 40-, 250-, and 1,000-event live bursts:

```sh
pnpm benchmark > benchmark.json
```

Choose replay sizes, live bursts, reducer batch size, and sample count with explicit arguments:

```sh
pnpm benchmark -- --replay=1000,5000 --bursts=100,500 --batch-size=256 --iterations=10
```

The JSON schema is identified by `schemaVersion`. Results include the scenario kind, message count, batch size, iterations, average/minimum/maximum duration, throughput, and a checksum that prevents the modeled work from becoming dead code.

This command is a comparison foundation, not a server-throughput claim. Use the RxJS and Effect browser labs for end-to-end retained replay, adapter batching, telemetry, and React rendering measurements.
