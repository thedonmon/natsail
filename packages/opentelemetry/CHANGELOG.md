# @natsail/opentelemetry

## 0.2.0

### Minor Changes

- cb3e03c: Adds `injectTraceContext(headers, context?)` and `extractTraceContext(headers, context?)` to carry W3C trace context in NATS message headers with the globally registered OpenTelemetry propagator.

### Patch Changes

- Updated dependencies [28984b3]
  - @natsail/core@0.6.0

## 0.1.2

### Patch Changes

- Updated dependencies [4bca28d]
  - @natsail/core@0.5.0

## 0.1.1

### Patch Changes

- Updated dependencies [ca0e5c3]
  - @natsail/core@0.4.0

## 0.1.0

### Minor Changes

- 8273c15: Add failure-isolated dependency-free runtime, session, JetStream, checkpoint, processor, and buffer telemetry with deterministic clocks, plus an optional OpenTelemetry metrics sink. Effect remains published on the `next` tag.

### Patch Changes

- Updated dependencies [8273c15]
- Updated dependencies [8273c15]
- Updated dependencies [8273c15]
  - @natsail/core@0.3.0
