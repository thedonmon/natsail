# NATSail

[![CI](https://github.com/thedonmon/natsail/actions/workflows/ci.yml/badge.svg)](https://github.com/thedonmon/natsail/actions/workflows/ci.yml)

NATSail provides reliable NATS sessions from the edge to the user interface.

The runtime uses one NATS connection for many logical subscriptions. Core NATS works without JetStream. JetStream support is an optional package.

The packages use version `0.x`. Their interfaces can change before version `1.0.0`.

The source repository is public. Published packages are available under the `@natsail` npm scope.

See the [resumable-stream research](docs/research/nats-resumable-streams.md) and [architecture proposal](docs/architecture/nats-resumable-streams-proposal.md) for the design evidence.

## Current status

| Capability                                                   | Status      |
| ------------------------------------------------------------ | ----------- |
| One connection for many Core NATS subscriptions              | Tested      |
| Core NATS publish and live delivery                          | Tested      |
| Runtime-managed Core NATS request/reply                      | Tested      |
| Core and JetStream recovery after forced reconnect           | Tested      |
| Connection status and structured runtime diagnostics         | Tested      |
| Permanent connection replacement and forced reauthentication | Tested      |
| Browser WebSocket transport in Node and Chromium             | Tested      |
| Token, user/password, NKey, JWT, and TLS connections         | Tested      |
| Cloudflare Workers WebSocket and TCP connections             | Local proof |
| GitHub Actions workspace check                               | Passing     |
| JetStream replay and live delivery through one consumer      | Tested      |
| Named explicit-ack JetStream processing and redelivery       | Tested      |
| Durable, bound, and lease-owned processor lifecycles         | Tested      |
| Resume strictly after a stream sequence                      | Tested      |
| Runtime cleanup of Core NATS and JetStream resources         | Tested      |
| Sixty-four conversations on one runtime connection           | Tested      |
| Connection-wide consumer and pull-buffer limits              | Tested      |
| Message-count and byte-capacity pull limits                  | Tested      |
| Bounded initial connection retry and safe retry cleanup      | Tested      |
| Keyed logical sessions with final-release cleanup            | Tested      |
| React provider, selectors, status, and external store        | Tested      |
| React connection and explicit-ack processor hooks            | Tested      |
| React reducer sessions that preserve every delivery          | Tested      |
| RxJS session value, runtime event, and status streams        | Tested      |
| Effect, React, and RxJS sharing one logical session          | Tested      |
| Effect scoped Layer, typed operations, and session Streams   | Tested      |
| Effect JetStream replay, materialization, and processing     | Tested      |
| Memory and IndexedDB checkpoint stores                       | Tested      |
| Filter-scoped checkpoints and scope-conflict detection       | Tested      |
| Shared JetStream sessions for React and RxJS                 | Tested      |
| Replay boundary metadata and explicit catch-up completion    | Tested      |
| Atomic JetStream replay-to-live state reduction              | Tested      |
| Cursor-preserving shared-session recovery                    | Tested      |
| Validated session contracts and resource diagnostics         | Tested      |
| Strict-Mode-safe managed React runtime ownership             | Tested      |
| Frame-coalesced React JetStream state selectors              | Tested      |
| Checkpoint save after successful processing                  | Tested      |
| Checkpoint sequence-regression protection                    | Tested      |
| Duplicate drop, deliver, and error policies                  | Tested      |
| Retention-gap error and recovery policy                      | Tested      |
| SharedWorker connection across browser tabs                  | Prototype   |
| Durable Object fan-out, checkpoint, and restart replay       | Prototype   |
| Direct TanStack/React rooms example                          | Example     |
| TanStack gateway rooms with two-tab retained catch-up        | Example     |
| Guided shadcn workbenches with visible proof receipts        | Example     |
| Native AI SDK `ChatTransport` over Core and JetStream        | Example     |
| Native TanStack AI adapter over Core and JetStream           | Example     |
| Full-page AI reply recovery with persisted chat state        | Example     |
| RxJS rooms with shared JetStream recovery                    | Example     |
| Public npm metadata and seven-package tarball installation   | Tested      |

The integration tests use NATS 2.14.4. Separate fixtures cover anonymous, token, user/password, NKey, operator JWT, and TLS connections.

## Goals

- Support Core NATS without JetStream.
- Add resumable JetStream delivery as an optional package.
- Use one connection for many application streams.
- Keep retry, cleanup, status, and resource limits in one runtime.
- Keep Effect, React, RxJS, and other frameworks out of the core package.
- Remove unused adapters from the application dependency graph.
- Prove behavior with a real local NATS server.

## Packages

| Package                                                                      | Purpose                                    | Required dependencies                                  |
| ---------------------------------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------ |
| [`@natsail/core`](https://www.npmjs.com/package/@natsail/core)               | Shared runtime and Core NATS subscriptions | `@nats-io/nats-core`                                   |
| [`@natsail/checkpoints`](https://www.npmjs.com/package/@natsail/checkpoints) | Memory and IndexedDB checkpoint stores     | None                                                   |
| [`@natsail/jetstream`](https://www.npmjs.com/package/@natsail/jetstream)     | Ordered JetStream replay and resume        | Core package, checkpoint package, `@nats-io/jetstream` |
| [`@natsail/session`](https://www.npmjs.com/package/@natsail/session)         | Keyed session sharing and lifecycle        | Core package                                           |
| [`@natsail/effect`](https://www.npmjs.com/package/@natsail/effect)           | Scoped Effect Core and JetStream Streams   | Core, JetStream, session packages, Effect peer         |
| [`@natsail/react`](https://www.npmjs.com/package/@natsail/react)             | React provider, status, and session hooks  | Core, session package, React peer                      |
| [`@natsail/rxjs`](https://www.npmjs.com/package/@natsail/rxjs)               | Runtime and session Observable bindings    | Core, session package, RxJS peer                       |

Applications install only the packages that they use.

Each package sets `sideEffects` to `false`. Each package also has a separate export and dependency graph. The Core package does not import JetStream, Effect, React, or RxJS.

### Adapter packages

| Package                         | Status     | Peer libraries              | Responsibility                                                  |
| ------------------------------- | ---------- | --------------------------- | --------------------------------------------------------------- |
| `@natsail/effect`               | Prerelease | Effect v4 RC                | Bounded Core and JetStream Streams, materialization, processors |
| `@natsail/react`                | Tested     | React                       | Hooks and external-store bindings                               |
| `@natsail/rxjs`                 | Tested     | RxJS                        | Observable bindings and cancellation                            |
| `@natsail/react-rxjs`           | Deferred   | React and RxJS              | Repeated integration patterns, if real applications need them   |
| `@natsail/transport-cloudflare` | Deferred   | Cloudflare Workers          | Add only if official transports need Worker-specific policy     |
| `@natsail/cloudflare-gateway`   | Prototype  | Workers and Durable Objects | Browser fan-in, restart replay, and downstream cursor policy    |

Applications can install any combination of the Effect, React, and RxJS adapters. Connection, checkpoint, consumer-recovery, and logical-session policy stays in the framework-neutral packages below them. Effect Streams, React hooks, and RxJS Observables can attach to the same logical session without opening duplicate NATS consumers.

The package set does not include a React–RxJS bridge. A bridge earns a package after repeated integration logic appears in multiple applications.

The local Wrangler proof passes with official NATS `wsconnect()` and with the official Node transport on Cloudflare's current `node:net` compatibility layer. NATSail does not need a custom Cloudflare transport package yet.

The [Durable Object gateway prototype](prototypes/cloudflare-durable-object-gateway/README.md) proves that one tenant object can fan out one NATSail JetStream consumer to two clients, persist its upstream checkpoint in Durable Object storage, and replay after a local workerd restart. It also proves that a late client needs an explicit catch-up path when its cursor trails the shared gateway cursor.

The [gateway chat example](examples/gateway-chat/README.md) exercises that path in a real TanStack application. One tab disconnects, another publishes, and the stale tab applies its missing retained delivery before rejoining the shared live feed. This behavior belongs in a separate Cloudflare package because it owns lifecycle, cost, authorization, multiplexing, and downstream replay policy.

The [direct React chat example](examples/react-chat/README.md) uses `NatsManagedProvider`, `useNatsRuntimeStatus()`, and `useNatsCoreSubscriptionReducer()` against the local NATS WebSocket. Both rooms applications use the repository [chat UI package](examples/chat-ui/README.md), which contains shadcn primitives.

The [RxJS chat example](examples/rxjs-chat/README.md) loads one retained multi-room feed from JetStream. Two RxJS projections share the same keyed session source.

The recovery action forces a visible transport reconnect and then publishes three messages through the recovered runtime. Two validated Observable projections continue through the same atomic reducer session and ordered consumer.

The repository [AI chat example](examples/ai-transport/README.md) loads earlier messages from a JetStream conversation subject. Native AI SDK or TanStack AI events carry each new answer.

The **Run recovery test** action pauses and recreates the ordered consumer. The **Reload page mid-reply** action restores the complete chat after a page reload.

AI SDK rebuilds the active answer from its retained native run. TanStack AI continues after its IndexedDB checkpoint.

Core NATS remains the live-only comparison. The deterministic responder uses `@shadcn/helpers`, so no model, route, external API, or API key is necessary.

The public repository contains all examples. The workspace does not publish them as npm packages.

```text
application
├── React hooks ────────┐
├── RxJS Observables ───┤
├── Effect Streams ─────┴── shared session registry
├── checkpoint stores ───── memory or IndexedDB
└── one NATS runtime
    ├── one shared NATS connection
    ├── Core NATS subscriptions
    └── optional JetStream consumers
```

## Delivery guarantees

Core NATS delivers live messages. It does not replay messages that arrive while a subscriber is offline.

`runtime.request()` sends one request through the shared connection, applies a payload codec or raw-message decoder, and supports timeout and abort signals. NATSail never retries an ambiguous request after it may have reached a responder.

Core subscriptions, requests, ordered consumers, and explicit-ack processors accept the same `NatsPayloadCodec<T>` interface. `natsCodecs.text`, `natsCodecs.json<T>()`, and `natsCodecs.bytes` cover common payloads without application-owned `TextEncoder` or `TextDecoder` plumbing. A custom codec can add validation, Protobuf, MessagePack, or another format without a NATSail release. Raw `decode(message)` remains available when decoding needs NATS message metadata.

JetStream stores messages. The adapter uses one ordered pull consumer for replay and live delivery. Each delivery contains a stream cursor.

The consumer captures its initial pending count when it opens. Each delivery reports that server pending count and whether it belongs to the captured initial replay or later live traffic. `lease.caughtUp` resolves only after the captured replay is processed, including when new messages arrive during catch-up.

The persistent checkpoint contains the stream name, stream epoch, stream sequence, and logical source scope. The epoch detects a recreated stream.

The source scope contains normalized filters and an optional application version. A mismatched scope stops resume with a typed error.

Both stores reject sequence regressions. A stale writer cannot replace a newer checkpoint with an older sequence.

Ordered consumers use `AckPolicy.None`. A delivered cursor is not a durable application acknowledgement.

The resume option saves a checkpoint only after the application callback resolves. A failed callback does not advance the checkpoint.

`processJetStream()` is the separate work-processing path. It requires a named pull consumer with `AckPolicy.Explicit` and acknowledges only after the handler resolves. A failed handler leaves the message unacknowledged for server redelivery. Consumers can be bound, ensured and retained, or owned and deleted with the lease. Existing named consumers are validated against the requested filter, start position, and explicitly supplied acknowledgement and replay settings before processing begins. Ack wait, maximum deliveries, maximum pending acknowledgements, replay policy, start position, and pull-buffer capacity are configurable.

An incoming sequence at or behind the application-committed cursor is a duplicate. The default `duplicateDeliveryPolicy` is `drop`. Set it to `deliver` to call the handler with `delivery.duplicate === true`, or to `error` to stop with `JetStreamDuplicateError`. The NATS `redelivered` flag remains available separately.

NATSail stops with a `retention-gap` error when stream retention removed required messages. The `continue` policy processes the retained messages.

The adapter bounds each nats.js pull loop to 32 buffered messages by default. Set `maxBufferedMessages` for a different limit.

Set `maxBufferedBytes` to select byte-based buffering instead. The message and byte modes are mutually exclusive for each consumer.

The runtime tracks aggregate message and byte capacity separately across consumers.

The runtime can limit active JetStream consumers and total pull-buffer capacity. It rejects excess leases with `NatsRuntimeLimitError`.

The runtime replaces a permanently closed connection by default. Its event stream remains active until `runtime.close()` completes.

Shared JetStream sources can opt into package-owned consumer recovery. Recovery resumes after the last successfully processed cursor and retries infrastructure closure according to the configured delay and attempt policy. Configuration, decode, retention-gap, duplicate-policy, and application-handler failures remain terminal unless the consumer explicitly overrides retry classification.

Call `runtime.reconnect()` after an authenticator receives new credentials. A live connection starts a new handshake and calls the authenticator again. Its promise resolves after the runtime observes the disconnect-to-connected cycle.

The reconnect can interrupt in-flight messages and requests. Normal NATS reconnect settings still apply.

`runtime.connection()` remains the escape hatch for nats.js operations that do not have a NATSail policy yet. The returned connection is still runtime-owned; application code must not close or drain it.

`runtime.events` reports the current connection state first. It then reports connection changes and structured diagnostics from Core and JetStream.

## Browser connection model

Many conversations in one browser tab share one runtime connection. Each active JetStream conversation still has its own consumer and bounded pull loop.

Each browser tab has a separate JavaScript realm. One runtime in each tab creates one connection in each tab.

The browser suite compares that baseline with a `SharedWorker`: two tabs create two direct connections, but both tabs share one connection when the worker owns the runtime. This is an integration proof, not a published broker package. Authentication, worker lifecycle, and the application message protocol still need a production design.

This design keeps the primitive useful in Node.js, browsers, React, Vue, Svelte, and other environments.

An ordinary session stores only the latest immutable snapshot. `createReducingSessionSource()` and `useNatsCoreSubscriptionReducer()` fold every delivery serially when an application needs a bounded collection or other accumulated state. `defineReducingJetStreamSession()` additionally hides partially assembled initial replay state, publishes one atomic snapshot at catch-up, preserves the cursor across package-owned recovery, and can be shared by React and RxJS. Applications still choose the reducer and retention policy.

## Examples

Run the direct React primitives example:

```sh
pnpm example:react-chat
```

Open <http://127.0.0.1:4175/rooms/gateway-lab> and follow the left test rail.

Run the Durable Object gateway example:

```sh
pnpm example:gateway-chat
```

Open <http://127.0.0.1:4174/rooms/gateway-lab> and follow the two-tab recovery test.

Run the real chat example over the AI SDK and TanStack AI transports:

```sh
pnpm example:ai-transport
```

Open <http://127.0.0.1:4176> and send the suggested message. During the answer, select **Run recovery test** or **Reload page mid-reply**.

The optional **Transport details** panel changes frameworks, compares JetStream with Core NATS, configures duplicates, and injects a random stream message.

Run the RxJS rooms example:

```sh
pnpm example:rxjs-chat
```

Open <http://127.0.0.1:4177>. Send a message or select **Reconnect and publish 3** to run the transport-recovery scenario.

Each command starts the local NATS fixtures when necessary and stops only fixtures that it started. See the [examples index](examples/README.md) for the behavioral difference.

## Development

Install the dependencies:

```sh
pnpm install
```

Start the local NATS server matrix:

```sh
pnpm nats:up
```

The main server uses native port 4223, monitoring port 8223, and WebSocket port 9223.

The authentication fixtures use ports 4224 through 4228.

`pnpm nats:up` creates disposable NKey, JWT, and TLS credentials in the ignored `.generated/` directory. Git does not store private test credentials.

Run all tests:

```sh
pnpm test
```

Run the real-browser WebSocket load test:

```sh
pnpm test:browser
```

Run formatting, builds, and tests:

```sh
pnpm check
```

Build, inspect, and install all seven publication tarballs:

```sh
pnpm release:check
```

See the [release guide](docs/RELEASING.md) for Changesets, trusted publishing, and provenance.

Stop the server:

```sh
pnpm nats:down
```

The browser test uses installed Chrome outside CI. CI installs Playwright Chromium.

The test server enables JetStream. Core NATS tests do not use JetStream.

## Core NATS example

The runtime accepts a connection factory. The application selects the NATS transport and authentication options.

```ts
import { connect } from '@nats-io/transport-node'
import { createNatsRuntime, natsCodecs } from '@natsail/core'

const runtime = createNatsRuntime({
  connect: () => connect({ servers: 'nats://127.0.0.1:4223' }),
  initialConnectRetry: {
    maxAttempts: 3,
    delayMs: 500,
  },
  limits: {
    maxJetStreamConsumers: 64,
    maxBufferedMessages: 256,
    maxBufferedBytes: 16 * 1024 * 1024,
  },
})

const lease = runtime.subscribe(
  {
    subject: 'events.orders',
    codec: natsCodecs.json<OrderEvent>(),
  },
  async (event) => {
    await orders.accept(event)
  }
)

await lease.ready
```

The callback runs serially for each subscription. `runtime.close()` stops every managed lease before it drains the connection.

Each `connection()` call starts at most one bounded connection series. Concurrent callers share it. A later call can start a fresh series after failure.

The connection factory must enforce its own timeout for each attempt.

Set `connectionRecovery.onPermanentClose` to `wait` to disable immediate replacement. Call `runtime.inspect()` to read the current connection generation and resource reservations.

## JetStream resume example

If the application needs stored delivery, install the checkpoint and JetStream packages.

```ts
import { createIndexedDbCheckpointStore } from '@natsail/checkpoints'
import { natsCodecs } from '@natsail/core'
import { consumeJetStream } from '@natsail/jetstream'

const checkpoints = createIndexedDbCheckpointStore()

const lease = consumeJetStream(
  runtime,
  {
    stream: 'CONVERSATIONS',
    filter: 'conversations.123.events',
    start: 'all',
    duplicateDeliveryPolicy: 'deliver',
    resume: {
      key: 'conversation-123',
      store: checkpoints,
    },
    codec: natsCodecs.json<ConversationEvent>(),
  },
  async (delivery) => {
    await conversation.applyIdempotently(delivery.value, delivery.cursor.sequence)
  }
)

await lease.ready
```

Use `createMemoryCheckpointStore()` for tests or session-only resume. IndexedDB keeps checkpoints across page reloads.

## Explicit-ack processing example

Use a named processor when server acknowledgement and redelivery are part of the application contract:

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
    maxBufferedMessages: 32,
    codec: natsCodecs.json<BillingJob>(),
  },
  async ({ value, deliveryAttempt }) => {
    await billing.apply(value, { deliveryAttempt })
  }
)

await processor.ready
```

Use `mode: 'bind'` to attach to an administrator-managed consumer. Use `mode: 'owned'` for a named consumer that must be deleted before the lease closes.

## Shared session adapters

Create one registry for an application runtime. A short idle delay prevents development-only subscription churn during React Strict Mode remounts.

```ts
import { defineReducingJetStreamSession } from '@natsail/jetstream'
import { natsCodecs } from '@natsail/core'
import { createSessionRegistry } from '@natsail/session'

const sessions = createSessionRegistry({ idleCloseMs: 250 })
const conversation = defineReducingJetStreamSession(
  runtime,
  'conversation:123',
  {
    stream: 'CONVERSATIONS',
    filter: 'conversations.123.events',
    start: 'all',
    recovery: { delayMs: 500 },
    codec: natsCodecs.json<ConversationEvent>(),
  },
  {
    scope: 'conversation-view:v2',
    initial: () => emptyConversation,
    reduce: applyConversationEvent,
  }
)
```

Supply the runtime and registry once near the React root. `NatsManagedProvider` creates both after commit and closes them after final unmount. Its resource survives React Strict Mode effect replay:

```tsx
import {
  NatsManagedProvider,
  useNatsCoreSubscriptionReducer,
  useNatsCoreSubscriptionSelector,
  useNatsJetStreamReducerSelector,
  useNatsRuntimeStatus,
} from '@natsail/react'
import { natsCodecs } from '@natsail/core'

function NatsRoot({ children }) {
  return (
    <NatsManagedProvider
      identity={accountId}
      create={() => ({ runtime: createRuntime(accountId), sessions: createSessionRegistry() })}
    >
      {children}
    </NatsManagedProvider>
  )
}

function ConversationStatus() {
  const connection = useNatsRuntimeStatus()
  const phase = useNatsJetStreamReducerSelector(
    conversation,
    (snapshot) => snapshot.value?.phase ?? snapshot.phase
  )

  return `${connection.state}:${phase}`
}

function LatestConversationSequence() {
  return useNatsJetStreamReducerSelector(
    conversation,
    (snapshot) => snapshot.value?.cursor?.sequence,
    Object.is,
    { notifications: 'animation-frame' }
  )
}

function LatestOrder() {
  const order = useNatsCoreSubscriptionSelector(
    'orders',
    { subject: 'events.orders', codec: natsCodecs.json<Order>() },
    (snapshot) => snapshot.value
  )

  return order?.id ?? 'waiting'
}

function RecentOrders() {
  const orders = useNatsCoreSubscriptionReducer(
    'orders:recent',
    { subject: 'events.orders', codec: natsCodecs.json<Order>() },
    () => [],
    (current, order) => [...current, order].slice(-100)
  )

  return orders.value ?? []
}
```

Use `useNatsSession()` when a component needs the complete immutable snapshot. `useNatsSessionSelector()` skips renders when the selected value does not change.

The Core and JetStream hooks create session sources from their subscription options. `useNatsCoreSubscriptionReducer()` processes every delivery before React renders accumulated state.

RxJS can read snapshots, value deliveries, runtime events, or distinct connection states:

```ts
import {
  observeNatsCoreSubscription,
  observeNatsJetStreamReducer,
  observeNatsRuntimeStatus,
  observeNatsSession,
  observeNatsSessionEvents,
  observeNatsSessionValues,
} from '@natsail/rxjs'
import { natsCodecs } from '@natsail/core'

const snapshots$ = observeNatsSession(sessions, conversation)
const values$ = observeNatsSessionValues(sessions, conversation)
const connection$ = observeNatsRuntimeStatus(runtime)
const conversation$ = observeNatsJetStreamReducer(sessions, conversation)
const sessionEvents$ = observeNatsSessionEvents(sessions)
const orders$ = observeNatsCoreSubscription(sessions, runtime, 'orders', {
  subject: 'events.orders',
  codec: natsCodecs.json<Order>(),
})
```

The value Observable replays the latest value once to a new subscriber. It emits equal consecutive deliveries. It errors or completes with the session.

The React, RxJS, and Effect helpers share one underlying subscription when they use the same registry and validated definition.

Effect v4 programs can consume Core NATS subjects and ordered JetStream replay directly from scoped, bounded Streams. NATSail continues to own connection, recovery, and checkpoint policy; Effect owns demand, structured cancellation, application effects, and finalization:

```ts
import { Effect, Stream } from 'effect'
import { makeNatsailScopedLayer, subscribe } from '@natsail/effect'
import { natsCodecs } from '@natsail/core'

const NatsLive = makeNatsailScopedLayer(Effect.sync(() => ({ runtime, sessions })))

const program = subscribe(
  {
    subject: 'chat.room.*',
    codec: natsCodecs.json<ChatMessage>(),
  },
  {
    bufferSize: 256,
    overflowStrategy: 'suspend',
  }
).pipe(
  Stream.runForEach((message) => Effect.log(message)),
  Effect.provide(NatsLive)
)
```

Each consumer owns one cold subscription that closes after success, failure, or interruption. Subject Streams suspend the awaited Core NATS handler when their bounded buffer is full by default. Applications can instead choose a typed overflow error, dropping, or sliding for best-effort data. JetStream Streams expose the replay-to-live boundary, reliable suspend-or-error buffering, atomic batched materialization, and named explicit-ack processors whose handlers are native Effects.

Session Streams remain available when consumers should share one validated registry source. The raw runtime and registry remain available on the service for advanced operations.

The definition contract records source configuration that affects delivery semantics. Reusing an active key with a different contract throws `SessionContractMismatchError` instead of silently attaching to the wrong consumer. `sessions.inspect()` and `sessions.events` expose reference counts, phases, revisions, idle resources, and restarts for diagnostics.

## Current limits

The current packages cover client runtime, replay, checkpoints, sessions, Effect, React, and RxJS. They do not cover every delivery or deployment model.

| Boundary                     | Supported now                                                                                                                                                          | Not supported yet                                                                                                                         |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| JetStream consumer model     | Ordered `AckPolicy.None` replay plus named explicit-ack processors with bind, ensure, owned, redelivery, and bounded pull policies.                                    | Packaged progress heartbeats, confirmed acknowledgements, and higher-level `nak` or terminal-message policy.                              |
| Checkpoint coordination      | Monotonic memory and IndexedDB checkpoints for one client-side backing store.                                                                                          | Distributed coordination between multiple writers or a server-owned acknowledgement record.                                               |
| Framework stream restoration | The AI example restores framework messages and active runs after a page reload. AI SDK replays one retained run. TanStack AI continues after its IndexedDB checkpoint. | A server-owned run registry and cross-device recovery. AI SDK cannot continue from an arbitrary native chunk without earlier run events.  |
| Cloudflare gateway           | Local Durable Object fan-out, storage-backed upstream checkpoints, restart replay, and bounded client catch-up.                                                        | A published gateway package with production authentication, backpressure, eviction, and cost policy.                                      |
| Cloudflare transport         | Official NATS WebSocket and Node TCP transports in local workerd.                                                                                                      | Remote endpoint, production authentication, and Workers VPC validation.                                                                   |
| Cross-tab connection sharing | A `SharedWorker` harness proves that two tabs can share one connection.                                                                                                | A supported browser-broker protocol with defined authentication, lifecycle, and failure behavior.                                         |
| Adapter ergonomics           | Effect v4 directly owns bounded Core and JetStream Streams, atomic replay materialization, explicit-ack Effect handlers, and scoped cleanup.                           | Sustained slow-consumer and recovery load proofs plus production tuning guidance for the new Effect-native JetStream path.                |
| Materialized state resume    | Package-owned recovery preserves the reducer state and processed cursor within an active source lease. A fresh lease atomically rebuilds state from stream replay.     | Persisted reducer resume requires a state store committed atomically with its event cursor. Passing `resume` to a reducing session fails. |
| Package availability         | Seven package tarballs, Changesets versioning, release checks, provenance, and trusted-publishing automation.                                                          | A new npm package needs its one-time bootstrap publication and trusted-publisher setting before routine OIDC releases.                    |

These boundaries define the next proofs. The examples and prototypes can continue to test them.

## Roadmap

1. Replace the remaining application-owned NATS effects and service wrappers in a production worktree with validated definitions, atomic reducers, and managed ownership. Measure replay time, renders, resource counts, and recovery behavior before merging the migration.
2. Prove Effect v4 Core and JetStream Streams under slow-consumer, replay, processor-failure, and recovery load; publish measured buffer and batching guidance.
3. Design and prove a materialized-state store that commits reducer state and its JetStream cursor together, then allow fast persisted reducer resume without replaying the full retained stream.
4. Add processor progress heartbeats, confirmed acknowledgements, and explicit `nak` or terminal-message policy after proving their failure semantics.
5. Prove an atomic catch-up-to-live handoff in the Durable Object gateway. Add authentication, backpressure, eviction tests, and cost measurements.
6. Deploy the Cloudflare examples against a remote NATS endpoint. Test authentication, reconnect, JetStream resume, and Workers VPC independently.
7. Turn the `SharedWorker` harness into a supported browser broker after its protocol, authentication, lifecycle, and failure tests are explicit.

## License

Apache-2.0. This license matches the main NATS server and JavaScript client projects.
