---
'@natsail/opentelemetry': minor
---

Adds `injectTraceContext(headers, context?)` and `extractTraceContext(headers, context?)` to carry W3C trace context in NATS message headers with the globally registered OpenTelemetry propagator.
