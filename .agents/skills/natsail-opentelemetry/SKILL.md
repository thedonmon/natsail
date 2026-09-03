---
name: natsail-opentelemetry
description: Use @natsail/opentelemetry to map dependency-free NATSail counters, gauges, and durations into OpenTelemetry metrics without adding OpenTelemetry to Core.
---

# Use @natsail/opentelemetry

Install the adapter and its peer dependency explicitly:

```sh
pnpm add @natsail/opentelemetry @opentelemetry/api
```

Configure the application's MeterProvider and exporter first, then create one sink and share it with the runtime and session registry.

```ts
const telemetry = createOpenTelemetrySink({ meterName: 'orders-api' })

const runtime = createNatsRuntime({
  connect,
  telemetry,
  telemetryAttributes: { service: 'orders-api', region: 'us-west' },
})

const sessions = createSessionRegistry({
  telemetry,
  telemetryAttributes: { service: 'orders-api', region: 'us-west' },
})
```

NATSail maps counters and gauges to matching instruments and records durations in millisecond histograms. Instrument instances stay cached by measurement name.

Use only stable primitive attributes with low cardinality. Subjects, stream and consumer names, session and checkpoint keys, tenant identifiers, payloads, and credentials must stay out of attributes. The sink runs inline; exporters should work through the MeterProvider rather than doing network or file I/O inside `record()`.

Core catches sink exceptions, so a telemetry failure can't change publish, request, checkpoint, or handler results. It can't interrupt a sink that blocks the JavaScript thread.

See the [OpenTelemetry adapter guide](https://github.com/thedonmon/natsail/tree/main/packages/opentelemetry#readme).
