# @natsail/opentelemetry

Optional OpenTelemetry metrics for NATSail. Core stays dependency-free: only applications that use this adapter install `@opentelemetry/api` and configure a MeterProvider/exporter.

```sh
pnpm add @natsail/core @natsail/opentelemetry @opentelemetry/api
```

Create one sink and pass it to the runtime and session registry. Use only stable, low-cardinality primitive attributes.

```ts
import { createNatsRuntime } from '@natsail/core'
import { createOpenTelemetrySink } from '@natsail/opentelemetry'
import { createSessionRegistry } from '@natsail/session'

const telemetry = createOpenTelemetrySink()

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

The sink maps counters and gauges directly and maps duration events to millisecond histograms. NATSail never includes subjects, payloads, credentials, session/checkpoint keys, stream names, or consumer names in default telemetry attributes.

Telemetry sinks run synchronously around observed operations. OpenTelemetry instruments are non-blocking, but custom sinks should enqueue measurements and must not perform blocking I/O. NATSail catches sink failures so telemetry cannot change publish, request, delivery, checkpoint, or lifecycle outcomes.

## Trace propagation

NATS messages carry W3C trace context in headers. `injectTraceContext(headers, context?)` writes the active context (or the one you pass) with the globally registered propagator and returns the headers. `extractTraceContext(headers, context?)` reads it back. Both are no-ops unless the application registers a propagator, such as `W3CTraceContextPropagator` from `@opentelemetry/core`.

```ts
import { headers } from '@nats-io/nats-core'
import { context } from '@opentelemetry/api'
import { extractTraceContext, injectTraceContext } from '@natsail/opentelemetry'

await runtime.publish('orders.created', data, { headers: injectTraceContext(headers()) })

// In a JetStream handler; `delivery.headers` is undefined when the message has none.
await context.with(extractTraceContext(delivery.headers), () => handle(delivery.value))
```

Core stays free of tracing hooks. NATSail does not wrap `js.publish`, so inject into headers wherever you publish.
