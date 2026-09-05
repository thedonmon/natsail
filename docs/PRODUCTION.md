# Production deployment guide

NATSail manages application lifecycle around the official NATS clients. Choose the delivery contract and failure policy before tuning throughput. A passing CI workload is evidence for that scenario, not a general capacity guarantee.

## Compatibility and verification

| Component               | Supported contract / verification baseline                                                                                                                            |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| JavaScript              | ESM; Node 22.22 in CI, Node 24.15 for releases; other hosts need application verification                                                                             |
| NATS JavaScript clients | `^3.4.0`; tests lock 3.4.0                                                                                                                                            |
| NATS server             | Tests use 2.14.4, including a three-node JetStream cluster; older servers are not covered by this matrix                                                              |
| React                   | Peer range 18 or 19; repository tests currently use 19                                                                                                                |
| RxJS                    | Peer range 7; repository lockfile supplies the tested patch                                                                                                           |
| Effect                  | Exactly `4.0.0-rc.112`, published under `next`; another release candidate requires separate validation                                                                |
| Browser broker          | CI exercises module SharedWorkers in Chromium, Firefox, and WebKit on Linux; mobile browsers, OS suspension, and browser-specific eviction require deployment testing |

The package manifests are authoritative for installable dependency ranges. Package versions are independent. Do not upgrade an Effect peer separately from its adapter.

## Runtime shutdown and observation

```ts
const runtime = createNatsRuntime({
  connect: () => connect({ servers: process.env.NATS_URL }),
  shutdownTimeoutMs: 15_000,
  maxBufferedEvents: 256,
  limits: { maxJetStreamConsumers: 32, maxBufferedMessages: 1_024 },
})
```

`close()` first stops new delivery and lets in-flight work finish. Its grace period includes connection drain and defaults to 30 seconds. On expiry, the runtime requests cancellation, force-closes its connection, and rejects with `NatsRuntimeShutdownTimeoutError`. Treat this as incomplete shutdown, not successful processing. Cleanup failures reject with `AggregateError`.

Core handlers receive a third `{ signal }` argument; processor handlers receive it as their second argument. The signal is aborted on explicit cancellation or expiry of the runtime grace period. Pass it to cancellable I/O. Effect processor execution receives the same interruption through Effect's native abort signal. Ordinary handler functions that ignore the extra argument remain valid; mocks that invoke these callback types manually must supply the context.

No JavaScript library can stop arbitrary application code that ignores cancellation. Direct lease `close()` still waits for its handler; the runtime supplies the bounded shutdown boundary. Custom adapter resources may implement `abort(reason)` to cooperate. Forced shutdown leaves owned processor consumers intact for operational recovery, and late processor results are not acknowledged. Inspect retained consumers before deciding to remove them.

Each `runtime.events` iterator retains at most `maxBufferedEvents` events, default 256. A slow iterator loses the oldest events and receives `event-buffer-overflow` with a dropped count before the retained events. Use `runtime.inspect()` to recover current state. Close abandoned iterators with `return()` or exit their `for await` loop.

JetStream pull budgets reserve capacity; they are not a cap on total JavaScript heap. Core NATS transport queues, decoded objects, framework queues, and application state need their own limits. A slow Core subscriber cannot backpressure the publisher reliably; choose JetStream when dropping live traffic is unacceptable.

## Durable worker recipe

```ts
const worker = processJetStream(
  runtime,
  {
    stream: 'JOBS',
    filter: 'jobs.billing',
    start: 'all',
    consumer: { mode: 'ensure', name: 'billing-workers' },
    codec: natsCodecs.json<BillingJob>(),
    ackWaitMs: 30_000,
    progressIntervalMs: 5_000,
    maxBufferedMessages: 1,
    acknowledgement: { mode: 'confirmed', timeoutMs: 5_000 },
    maxDeliver: 10,
    recovery: { delayMs: 500 },
  },
  async ({ value }, { signal }) => {
    await applyBillingJobIdempotently(value, { signal })
  }
)
await worker.ready
```

- `progressIntervalMs` sends progress acknowledgements while a handler runs. It must be shorter than the active consumer's first backoff or acknowledgement wait. It defaults the pull buffer to one message. Larger explicit buffers need acknowledgement windows that also cover time spent waiting behind other messages. Progress updates cannot prevent redelivery during an outage or an event-loop stall.
- `acknowledgement.mode: 'confirmed'` advances the local acknowledged position only after a positive server response. Failure is ambiguous and may lead to redelivery. The default `sent` mode keeps the lower-overhead fire-and-forget path.
- Returning nothing acknowledges success. Return `{ action: 'retry', delayMs: 500 }` to request delayed redelivery, or `{ action: 'term', reason: 'unsupported schema' }` to stop redelivery of that message. Thrown errors remain terminal for the processor; they are not automatically converted into retries.
- `term` is an explicit discard decision, not a dead-letter queue. Store any required failure record before returning it. NAK and terminal commands are not server-confirmed by the acknowledgement option.
- A retry creates an acknowledgement gap. Recovery must not skip that unfinished delivery merely because later messages succeed; conservative replay can produce duplicates.

Neither confirmed acknowledgements nor progress heartbeats make external side effects exactly once. Use stable job IDs and application-level idempotency. Use retained consumers (`ensure` or administrator-managed `bind`) for jobs that must survive worker lifecycle changes.

## What CI proves

The regular workflow builds packages, typechecks tests/configuration, runs service-backed integration tests and browser acceptance tests, then installs the nine packed tarballs into a clean consumer project.

The isolated Resilience workflow kills a consumer leader, kills a worker during a handler, and runs a slow-handler load scenario against three file-backed JetStream nodes. Its `sustained-load` artifact contains published/processed counts, duplicate count, heap samples, queue-capacity reservations, delivery latency, and event-loop delay. The default publishing window is ten seconds, followed by draining. This is a regression test, not a long-duration soak certification. To increase its duration in an isolated test environment, set `NATSAIL_LOAD_SECONDS` (5–600) when running `scripts/verify-sustained-load.mjs` with `NATSAIL_CLUSTER_TEST=1`.

Browser tests cover two-tab sharing, a closed tab's heartbeat cleanup, and explicit reconnection after terminating a worker realm. They do not reproduce every mobile/background suspension policy. Authorize logical source and operation mappings in the application; same-origin tab identity is not a security boundary.

Packed-consumer bundle budgets cover every package's exported NATSail code and its NATSail dependencies, excluding host libraries and transports. Separate codec-only and runtime-only checks enforce tree-shaking and keep optional integrations out of Core. These numbers are not total application bundle sizes. Review budget changes alongside measured code growth; do not raise a limit just to silence a failure.

Before a broad rollout, repeat these scenarios with your own traffic, payload sizes, authentication rotation, infrastructure, and shutdown grace periods. Persisted materialized-state resume remains a separate feature: fresh reducers replay retained history because safe fast resume requires an atomic state-and-cursor store.
