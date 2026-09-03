# Local benchmark foundation

`pnpm benchmark` runs without NATS or Docker. It feeds the same logical workloads through the public RxJS `observeNatsJetStreamState()` path and the public Effect `materializeNatsJetStreamEvents()` path. The RxJS scenario uses the shared Core batcher to build cumulative state; the Effect scenario runs the adapter's bounded `reduceBatch` materializer.

The default suite exercises 1,000- and 5,000-event replays plus 40-, 250-, and 1,000-event live bursts. Use pnpm's silent mode when capturing the single JSON document:

```sh
pnpm --silent benchmark > benchmark.json
```

Choose replay sizes, live bursts, reducer batch size, and sample count with explicit arguments:

```sh
pnpm --silent benchmark -- --replay=1000,5000 --bursts=100,500 --batch-size=256 --iterations=10
```

The JSON schema is identified by `schemaVersion`. Every RxJS and Effect result reports deliveries, downstream emissions, completed reducer commits verified by the final downstream cursor, replay time, the longest synchronous reducer slice, observed batch sizes, cooperative yield count, average/minimum/maximum duration, throughput, and a checksum taken from the final emitted state. Counts and batch sizes describe one scenario run; timings are aggregated across the requested iterations.

This is a local synthetic adapter-path comparison, not a NATS server-throughput claim or a representative hardware baseline. Use the RxJS and Effect browser labs for end-to-end retained replay, adapter batching, telemetry, and rendering measurements.
