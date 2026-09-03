# NATSail

[![CI](https://github.com/thedonmon/natsail/actions/workflows/ci.yml/badge.svg)](https://github.com/thedonmon/natsail/actions/workflows/ci.yml) [![npm](https://img.shields.io/npm/v/%40natsail%2Fcore)](https://www.npmjs.com/package/@natsail/core)

NATS has a small API. Keeping a NATS connection healthy inside a real application takes more work.

NATSail gives TypeScript applications one managed NATS runtime. It owns connection reuse, retries, cleanup, resource limits, and diagnostics. Add JetStream when you need replay, checkpoints, or durable work processing.

React, RxJS, and Effect adapters can share the same runtime and logical sessions. Your components do not need connection effects, custom retry loops, or byte-decoding boilerplate.

Browser applications can also move those logical sessions into a `SharedWorker`, so same-origin tabs share physical sources without hiding lag or resume requirements.

NATSail is useful when:

- many parts of an application must share one NATS connection
- subscriptions must close when their owners disappear
- a JetStream replay must finish before the user interface receives live updates
- reconnects must continue from the last processed stream cursor
- React, RxJS, or Effect needs NATS data without owning transport policy

The packages use version `0.x`. Interfaces can change before version `1.0.0`.

## Start with Core NATS

Install the runtime and an official NATS transport:

```sh
pnpm add @natsail/core @nats-io/transport-node
```

Create one runtime for the application. Every subscription uses its shared connection.

```ts
import { connect } from '@nats-io/transport-node'
import { createNatsRuntime, natsCodecs } from '@natsail/core'

const runtime = createNatsRuntime({
  connect: () => connect({ servers: 'nats://127.0.0.1:4222' }),
  initialConnectRetry: {
    maxAttempts: 3,
    delayMs: 500,
  },
})

const messages = runtime.subscribe(
  {
    subject: 'chat.room.123',
    codec: natsCodecs.text,
  },
  async (message) => {
    console.log(message)
  }
)

await messages.ready
await runtime.publish('chat.room.123', 'hello')

// Close every managed subscription, then drain the shared connection.
await runtime.close()
```

Core NATS is live-only. Use JetStream when the application must recover messages that arrived while it was offline.

## Add JetStream replay

Install the JetStream, checkpoint, and session packages:

```sh
pnpm add @natsail/core @natsail/checkpoints @natsail/session @natsail/jetstream
```

The following consumer starts at the beginning of one conversation subject. It resumes after its last successful application checkpoint.

```ts
import { createIndexedDbCheckpointStore } from '@natsail/checkpoints'
import { natsCodecs } from '@natsail/core'
import { consumeJetStream } from '@natsail/jetstream'

const checkpoints = createIndexedDbCheckpointStore()

const conversation = consumeJetStream(
  runtime,
  {
    stream: 'CONVERSATIONS',
    filter: 'conversations.123.events',
    start: 'all',
    codec: natsCodecs.json<ConversationEvent>(),
    resume: {
      key: 'conversation-123',
      store: checkpoints,
    },
  },
  async (delivery) => {
    await applyConversationEvent(delivery.value)
  }
)

await conversation.ready
await conversation.caughtUp
```

NATSail saves the cursor after the handler succeeds. A failed handler does not advance the checkpoint.

Use a reducing session when a user interface needs one complete state after replay. The reducer processes every event, but subscribers do not receive partial historical state.

```ts
import { defineReducingJetStreamSession } from '@natsail/jetstream'

const conversationState = defineReducingJetStreamSession(
  runtime,
  'conversation:123',
  {
    stream: 'CONVERSATIONS',
    filter: 'conversations.123.events',
    start: 'all',
    codec: natsCodecs.json<ConversationEvent>(),
    recovery: { delayMs: 500 },
  },
  {
    scope: 'conversation-view:v1',
    initial: () => ({ messages: [] }),
    reduce: (state, delivery) => reduceConversation(state, delivery.value),
  }
)
```

React, RxJS, and Effect can attach to this definition without creating duplicate JetStream consumers.

## Choose the packages you need

- [`@natsail/core`](packages/core/README.md) owns the shared connection, Core subscriptions, publish, request/reply, retries, limits, and diagnostics.
- [`@natsail/jetstream`](packages/jetstream/README.md) adds ordered replay, recovery, duplicate policy, checkpoints, and named explicit-ack processors.
- [`@natsail/session`](packages/session/README.md) shares keyed logical sources and closes them after the last owner leaves.
- [`@natsail/checkpoints`](packages/checkpoints/README.md) stores monotonic cursors in memory or IndexedDB.
- [`@natsail/react`](packages/react/README.md) provides an ownership-safe provider, status hooks, selectors, reducers, and processor hooks.
- [`@natsail/rxjs`](packages/rxjs/README.md) exposes cancellable Observables and frame-coalesced JetStream state.
- [`@natsail/effect`](packages/effect/README.md) provides scoped Effect v4 Streams with bounded buffers and structured interruption.
- [`@natsail/opentelemetry`](packages/opentelemetry/README.md) maps optional dependency-free measurements to OpenTelemetry metrics.
- [`@natsail/browser-broker`](packages/browser-broker/README.md) shares bounded, cursor-aware `SessionSource` leases across same-origin tabs through a versioned SharedWorker protocol.

Applications install only the packages that they use. The Core package does not import JetStream, Effect, React, or RxJS.

## Telemetry

Core accepts an optional synchronous `NatsailTelemetrySink`. The same sink can observe a session registry. Measurements are discriminated counters, gauges, and durations; they do not enter `runtime.events`, which remains the low-volume connection and diagnostic stream.

```ts
const telemetry = {
  record(event: NatsailTelemetryEvent) {
    measurementQueue.push(event)
  },
}

const runtime = createNatsRuntime({
  connect,
  telemetry,
  telemetryAttributes: { service: 'orders-api', region: 'us-west' },
})

const sessions = createSessionRegistry({ telemetry })
```

Use only stable, low-cardinality primitive attributes. NATSail does not include subjects, payloads, credentials, session/checkpoint keys, stream names, or consumer names in its default telemetry attributes. Internal operation attributes override colliding caller attributes. A sink runs inline, so it should enqueue measurements and avoid blocking I/O; sink exceptions are isolated from observed operations.

Install [`@natsail/opentelemetry`](packages/opentelemetry/README.md) to map the same interface to OpenTelemetry without adding an OpenTelemetry dependency to Core.

## Share sources across browser tabs

Install `@natsail/browser-broker` when same-origin tabs should share one physical Core or JetStream source. The SharedWorker host uses the normal session contract, transfers one batch at a time to each tab, and requires a cursor acknowledgement before advancing that tab.

```ts
const browser = await createBrowserBrokerClient({
  identity: { tenant: tenantId, authenticationContext: 'interactive-user-v1' },
  credentials: () => credentialStore.current(),
  connect: () =>
    new SharedWorker(new URL('./nats-worker.ts', import.meta.url), {
      name: 'application-nats',
      type: 'module',
    }).port,
  strict: true,
})

const conversationSource = browser.createSource({
  key: 'conversation-feed',
  contract: 'conversation-events:v1',
})

await browser.publish('send-message', encodedMessage)
const reply = await browser.request('lookup-message', encodedQuery)
```

Per-tab item and encoded-byte queues are bounded independently. A lagging tab receives an explicit resume-required error from its last acknowledged JetStream cursor; other tabs continue. Tenant/authentication context is immutable source identity, while credentials can be refreshed through the authenticated tab bootstrap. Publish and request use logical operation names that the worker maps to authorized NATS subjects or services. See the [browser broker guide](packages/browser-broker/README.md) for worker setup, idle runtime cleanup, fallback policy, and protocol v1.

## Batching and cooperative reducers

Core exports one dependency-free `NatsailBatchPolicy<T>` for count, byte, and time bounds plus `NatsailWorkBudget` for cooperative serial reducer yields. Reducing JetStream sessions apply replay in bounded batches but publish one atomic hydrated state, then coalesce live cumulative state with a 16ms default. RxJS, React, and Effect accept the same policy while keeping their native scheduling APIs and legacy options. Ordered cursor/checkpoint advancement remains behind successful batch application; close discards a pending partial batch and waits for in-flight work.

## Shared session adapters

Create one session registry beside the runtime. A short idle delay prevents subscription churn during React Strict Mode remounts.

```ts
import { createSessionRegistry } from '@natsail/session'

const sessions = createSessionRegistry({ idleCloseMs: 250 })
```

### React

`NatsManagedProvider` creates the runtime after React commits. It closes the runtime and registry after the final unmount.

```tsx
import { NatsManagedProvider, useNatsJetStreamReducerSelector } from '@natsail/react'

function NatsRoot({ children }) {
  return (
    <NatsManagedProvider
      identity={accountId}
      create={() => ({
        runtime: createRuntime(accountId),
        sessions: createSessionRegistry({ idleCloseMs: 250 }),
      })}
    >
      {children}
    </NatsManagedProvider>
  )
}

function MessageCount() {
  return useNatsJetStreamReducerSelector(
    conversationState,
    (snapshot) => snapshot.value?.data.messages.length ?? 0,
    Object.is,
    { notifications: 'animation-frame' }
  )
}
```

The selector can coalesce React notifications without dropping JetStream deliveries.

### RxJS

RxJS can observe the same reducing definition. Initial history arrives as one state, and live state can be limited to one emission per frame.

```ts
import { observeNatsJetStreamState } from '@natsail/rxjs'

const conversation$ = observeNatsJetStreamState(sessions, conversationState, {
  liveBatchMs: 16,
})
```

### Effect

Effect can observe the same reducing definition through a scoped Stream. Like the RxJS adapter, it emits replay and reconnect boundaries immediately and coalesces later cumulative live state without skipping reducer work.

```ts
import { Effect, Stream } from 'effect'
import { jetStreamStates, makeNatsailScopedLayer } from '@natsail/effect'

const NatsLive = makeNatsailScopedLayer(Effect.sync(() => ({ runtime, sessions })))

const program = jetStreamStates(conversationState, {
  liveBatchWithin: '16 millis',
}).pipe(
  Stream.runForEach((snapshot) => Effect.log(snapshot.data)),
  Effect.provide(NatsLive)
)
```

The adapter also provides cold Core and JetStream Streams with bounded queues, a native Effect replay materializer with typed reducer requirements, and explicit-ack processors.

The current Effect adapter targets the version in its [package guide](packages/effect/README.md) and uses the npm `next` tag.

## Explicit-ack processing example

Ordered replay uses `AckPolicy.None` because its cursor records application progress. Use `processJetStream()` when server acknowledgement and redelivery are part of the work contract.

```ts
import { natsCodecs } from '@natsail/core'
import { processJetStream } from '@natsail/jetstream'

const processor = processJetStream(
  runtime,
  {
    stream: 'JOBS',
    consumer: { mode: 'ensure', name: 'billing_workers' },
    filter: 'jobs.billing',
    start: 'all',
    ackWaitMs: 60_000,
    maxDeliver: 10,
    maxAckPending: 64,
    recovery: { delayMs: 500 },
    codec: natsCodecs.json<BillingJob>(),
  },
  async ({ value, deliveryAttempt }) => {
    await applyBillingJob(value, deliveryAttempt)
  }
)

await processor.ready
```

The processor acknowledges a message after its handler succeeds. A failed handler leaves the message available for server redelivery. With `recovery` enabled, NATSail reopens the same named consumer after infrastructure failures and exposes `reconnecting` plus rich cached progress through the processor lease.

Named consumers also have an administration seam:

```ts
import { createJetStreamProcessorController } from '@natsail/jetstream'

const jobs = createJetStreamProcessorController(runtime, {
  stream: 'JOBS',
  consumer: { mode: 'ensure', name: 'billing_workers' },
  filter: 'jobs.billing',
  start: 'all',
  ackWaitMs: 60_000,
  backoffMs: [60_000, 120_000, 300_000],
  maxDeliver: 10,
  metadata: { team: 'billing' },
})

const result = await jobs.reconcile()
const authoritative = await jobs.refresh()
```

`bind` is inspect-only, `ensure` may update editable settings but never recreates a consumer, and `owned` may recreate or delete only its own durable consumer. Reconciliation is serialized and returns normalized before/after configuration plus editable and immutable drift. Safe owned recreation resumes at the first sequence after the previous acknowledgement floor.

## Run the examples

Clone the repository, install its dependencies, and start one example:

```sh
pnpm install
pnpm example:react-chat
```

Open <http://127.0.0.1:4175/rooms/gateway-lab>. The example shows rooms, history replay, live replies, connection state, and recovery.

Other examples exercise specific integrations:

- `pnpm example:rxjs-chat` opens the RxJS performance lab at <http://127.0.0.1:4177>.
- `pnpm example:effect-chat` opens the matching Effect lab at <http://127.0.0.1:4178>.
- `pnpm example:ai-transport` compares Core NATS and JetStream with AI SDK and TanStack AI transports.
- `pnpm example:gateway-chat` runs the local Durable Object gateway and its two-tab catch-up flow.

The RxJS and Effect labs contain the same conversations, including 1,000-message and 5,000-message histories. Their counters report replay, batching, and React commit behavior.

Each command starts its required local NATS fixtures. Read the [examples guide](examples/README.md) for ports and behavior.

## Core NATS example

The runtime accepts any official NATS connection factory. Browser applications can supply `wsconnect()`, while Node.js applications can use `@nats-io/transport-node`.

`natsCodecs.text`, `natsCodecs.json<T>()`, and `natsCodecs.bytes` handle common payloads. A custom `NatsPayloadCodec<T>` can add validation, Protobuf, MessagePack, or another wire format.

`runtime.request()` uses the shared connection and accepts a timeout or abort signal. NATSail does not retry a request after its outcome becomes ambiguous.

`runtime.connection()` exposes the runtime-owned nats.js connection for operations without a managed NATSail API. Application code must not close or drain it.

## Documentation

- [Delivery model and guarantees](docs/DELIVERY.md) explains Core delivery, JetStream replay, checkpoints, acknowledgements, duplicate policy, recovery, and resource limits.
- [Project status and roadmap](docs/STATUS.md) lists the tested capabilities, current limits, prototypes, and next proofs.
- [Package guides](packages) document each public package and its full API surface.
- [Examples guide](examples/README.md) explains the React, RxJS, Effect, AI transport, and Cloudflare examples.
- [Resumable-stream research](docs/research/nats-resumable-streams.md) records the problem analysis and source material.
- [Resumable-stream architecture](docs/architecture/nats-resumable-streams-proposal.md) records the proposed protocol and tradeoffs.
- [Release guide](docs/RELEASING.md) covers Changesets, trusted publishing, package checks, and provenance.

## Development

The workspace requires Node.js 22.14 or newer, pnpm, Docker, and Chrome for local browser tests.

```sh
pnpm install
pnpm nats:up
pnpm test
pnpm test:browser
pnpm check
pnpm benchmark
```

The main test server uses native port 4223, monitoring port 8223, and WebSocket port 9223. Authentication fixtures use ports 4224 through 4228.

`pnpm nats:up` writes disposable test credentials to the ignored `.generated/` directory. Git does not store private test credentials.

Run `pnpm nats:down` to stop the fixture servers. Run `pnpm benchmark` for a local machine-readable 1,000/5,000-event replay and configurable live-burst baseline. Run `pnpm release:check` to build and inspect all nine package tarballs.

## License

Apache-2.0
