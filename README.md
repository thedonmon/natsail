# NATSail

[![CI](https://github.com/thedonmon/natsail/actions/workflows/ci.yml/badge.svg)](https://github.com/thedonmon/natsail/actions/workflows/ci.yml)

NATSail provides reliable NATS sessions from the edge to the user interface.

The runtime uses one NATS connection for many logical subscriptions. Core NATS works without JetStream. JetStream support is an optional package.

The packages use version `0.x`. Their interfaces can change before version `1.0.0`.

The source repository is public. All six packages are available on npm at version `0.1.0`.

See the [resumable-stream research](docs/research/nats-resumable-streams.md) and [architecture proposal](docs/architecture/nats-resumable-streams-proposal.md) for the design evidence.

## Current status

| Capability                                              | Status      |
| ------------------------------------------------------- | ----------- |
| One connection for many Core NATS subscriptions         | Tested      |
| Core NATS publish and live delivery                     | Tested      |
| Core and JetStream recovery after forced reconnect      | Tested      |
| Connection status and structured runtime diagnostics    | Tested      |
| Browser WebSocket transport in Node and Chromium        | Tested      |
| Token, user/password, NKey, JWT, and TLS connections    | Tested      |
| Cloudflare Workers WebSocket and TCP connections        | Local proof |
| GitHub Actions workspace check                          | Passing     |
| JetStream replay and live delivery through one consumer | Tested      |
| Resume strictly after a stream sequence                 | Tested      |
| Runtime cleanup of Core NATS and JetStream resources    | Tested      |
| Sixty-four conversations on one runtime connection      | Tested      |
| Connection-wide consumer and pull-buffer limits         | Tested      |
| Bounded initial connection retry and safe retry cleanup | Tested      |
| Keyed logical sessions with final-release cleanup       | Tested      |
| React provider, selectors, status, and external store   | Tested      |
| React reducer sessions that preserve every delivery     | Tested      |
| RxJS session value, runtime event, and status streams   | Tested      |
| React and RxJS sharing one logical session              | Tested      |
| Memory and IndexedDB checkpoint stores                  | Tested      |
| Checkpoint save after successful processing             | Tested      |
| Checkpoint sequence-regression protection               | Tested      |
| Duplicate drop, deliver, and error policies             | Tested      |
| Retention-gap error and recovery policy                 | Tested      |
| SharedWorker connection across browser tabs             | Prototype   |
| Durable Object fan-out, checkpoint, and restart replay  | Prototype   |
| Direct TanStack/React rooms example                     | Example     |
| TanStack gateway rooms with two-tab retained catch-up   | Example     |
| Guided shadcn workbenches with visible proof receipts   | Example     |
| Native AI SDK `ChatTransport` over Core and JetStream   | Example     |
| Native TanStack AI adapter over Core and JetStream      | Example     |
| Six public npm packages and tarball installation        | Released    |

The integration tests use NATS 2.14.4. Separate fixtures cover anonymous, token, user/password, NKey, operator JWT, and TLS connections.

## Goals

- Support Core NATS without JetStream.
- Add resumable JetStream delivery as an optional package.
- Use one connection for many application streams.
- Keep retry, cleanup, status, and resource limits in one runtime.
- Keep React, RxJS, and other frameworks out of the core package.
- Remove unused adapters from the application dependency graph.
- Prove behavior with a real local NATS server.

## Packages

| Package                                                                      | Purpose                                    | Required dependencies                                  |
| ---------------------------------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------ |
| [`@natsail/core`](https://www.npmjs.com/package/@natsail/core)               | Shared runtime and Core NATS subscriptions | `@nats-io/nats-core`                                   |
| [`@natsail/checkpoints`](https://www.npmjs.com/package/@natsail/checkpoints) | Memory and IndexedDB checkpoint stores     | None                                                   |
| [`@natsail/jetstream`](https://www.npmjs.com/package/@natsail/jetstream)     | Ordered JetStream replay and resume        | Core package, checkpoint package, `@nats-io/jetstream` |
| [`@natsail/session`](https://www.npmjs.com/package/@natsail/session)         | Keyed session sharing and lifecycle        | Core package                                           |
| [`@natsail/react`](https://www.npmjs.com/package/@natsail/react)             | React provider, status, and session hooks  | Core, session package, React peer                      |
| [`@natsail/rxjs`](https://www.npmjs.com/package/@natsail/rxjs)               | Runtime and session Observable bindings    | Core, session package, RxJS peer                       |

Applications install only the packages that they use.

Each package sets `sideEffects` to `false`. Each package also has a separate export and dependency graph. The Core package does not import JetStream, React, or RxJS.

### Adapter packages

| Package                         | Status    | Peer libraries              | Responsibility                                                |
| ------------------------------- | --------- | --------------------------- | ------------------------------------------------------------- |
| `@natsail/react`                | Tested    | React                       | Hooks and external-store bindings                             |
| `@natsail/rxjs`                 | Tested    | RxJS                        | Observable bindings and cancellation                          |
| `@natsail/react-rxjs`           | Deferred  | React and RxJS              | Repeated integration patterns, if real applications need them |
| `@natsail/transport-cloudflare` | Deferred  | Cloudflare Workers          | Add only if official transports need Worker-specific policy   |
| `@natsail/cloudflare-gateway`   | Prototype | Workers and Durable Objects | Browser fan-in, restart replay, and downstream cursor policy  |

A React application can install both adapter packages. Neither adapter owns the NATS connection, checkpoints, or the logical-session registry. Those features stay in a framework-neutral module below both adapters. React hooks and RxJS Observables can then attach to the same logical session without opening duplicate NATS consumers.

The `0.1.x` package set does not include a React–RxJS bridge. A bridge earns a package after repeated integration logic appears in multiple applications.

The local Wrangler proof passes with official NATS `wsconnect()` and with the official Node transport on Cloudflare's current `node:net` compatibility layer. NATSail does not need a custom Cloudflare transport package yet.

The [Durable Object gateway prototype](prototypes/cloudflare-durable-object-gateway/README.md) proves that one tenant object can fan out one NATSail JetStream consumer to two clients, persist its upstream checkpoint in Durable Object storage, and replay after a local workerd restart. It also proves that a late client needs an explicit catch-up path when its cursor trails the shared gateway cursor.

The [gateway chat example](examples/gateway-chat/README.md) exercises that path in a real TanStack application. One tab disconnects, another publishes, and the stale tab applies its missing retained delivery before rejoining the shared live feed. This behavior belongs in a separate Cloudflare package because it owns lifecycle, cost, authorization, multiplexing, and downstream replay policy.

The [direct React chat example](examples/react-chat/README.md) uses `NatsProvider`, `useNatsRuntimeStatus()`, and `useNatsCoreSubscriptionReducer()` against the local NATS WebSocket without the gateway. Both rooms applications use a guided private [chat UI package](examples/chat-ui/README.md) built from shadcn primitives.

The private [AI chat example](examples/ai-transport/README.md) is a real multi-turn conversation that loads earlier messages from a JetStream conversation subject and carries native AI SDK `UIMessageChunk` or TanStack AI AG-UI reply events through Core NATS or JetStream. Each framework's real `useChat` hook owns the active message state. **Run recovery test** pauses and recreates the ordered reply consumer from its processed checkpoint while the responder keeps publishing, making retained-frame recovery visible for two seconds. The collapsed details panel can inject a random live stream message, compare global history/reply sequences, and select the duplicate policy. Core remains available as the live-only comparison. The deterministic responder uses `@shadcn/helpers`, so no model, external API, route, or key is required. Nothing under `examples/` is published.

```text
application
├── React hooks ────────┐
├── RxJS Observables ───┴── shared session registry
├── checkpoint stores ───── memory or IndexedDB
└── one NATS runtime
    ├── one shared NATS connection
    ├── Core NATS subscriptions
    └── optional JetStream consumers
```

## Delivery guarantees

Core NATS delivers live messages. It does not replay messages that arrive while a subscriber is offline.

JetStream stores messages. The adapter uses one ordered pull consumer for replay and live delivery. Each delivery contains a stream cursor.

The persistent checkpoint contains the stream name, stream epoch, and stream sequence. The epoch detects a stream that was deleted and recreated.

Both stores reject sequence regressions. A stale writer cannot replace a newer checkpoint with an older sequence.

Ordered consumers use `AckPolicy.None`. A delivered cursor is not a durable application acknowledgement.

The resume option saves a checkpoint only after the application callback resolves. A failed callback does not advance the checkpoint.

An incoming sequence at or behind the application-committed cursor is a duplicate. The default `duplicateDeliveryPolicy` is `drop`. Set it to `deliver` to call the handler with `delivery.duplicate === true`, or to `error` to stop with `JetStreamDuplicateError`. The NATS `redelivered` flag remains available separately.

NATSail stops with a `retention-gap` error when stream retention removed required messages. The `continue` policy processes the retained messages.

The adapter bounds each nats.js pull loop to 32 buffered messages by default. Set `maxBufferedMessages` for a different limit.

The runtime can limit active JetStream consumers and total pull-buffer capacity. It rejects excess leases with `NatsRuntimeLimitError`.

`runtime.events` reports the current connection state first. It then reports connection changes and structured diagnostics from Core and JetStream.

## Browser connection model

Many conversations in one browser tab share one runtime connection. Each active JetStream conversation still has its own consumer and bounded pull loop.

Each browser tab has a separate JavaScript realm. One runtime in each tab creates one connection in each tab.

The browser suite compares that baseline with a `SharedWorker`: two tabs create two direct connections, but both tabs share one connection when the worker owns the runtime. This is an integration proof, not a published broker package. Authentication, worker lifecycle, and the application message protocol still need a production design.

This design keeps the primitive useful in Node.js, browsers, React, Vue, Svelte, and other environments.

An ordinary session stores only the latest immutable snapshot. `createReducingSessionSource()` and `useNatsCoreSubscriptionReducer()` fold every delivery serially when an application needs a bounded collection or other accumulated state. Applications still choose the reducer and retention policy.

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

Open <http://127.0.0.1:4176>, watch the earlier conversation load from JetStream, send the suggested message, and click **Run recovery test** while the answer is streaming. The optional **Transport details** panel switches frameworks, compares JetStream with Core, configures duplicates, and injects a random stream message.

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

Build, inspect, and install all six publication tarballs:

```sh
pnpm release:check
```

See the [release guide](docs/RELEASING.md) for Changesets, the first publication, trusted publishing, and provenance.

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
import { createNatsRuntime } from '@natsail/core'

const runtime = createNatsRuntime({
  connect: () => connect({ servers: 'nats://127.0.0.1:4223' }),
  initialConnectRetry: {
    maxAttempts: 3,
    delayMs: 500,
  },
  limits: {
    maxJetStreamConsumers: 64,
    maxBufferedMessages: 256,
  },
})

const decoder = new TextDecoder()
const lease = runtime.subscribe(
  {
    subject: 'events.orders',
    decode: (message) => decoder.decode(message.data),
  },
  async (event) => {
    await orders.accept(event)
  }
)

await lease.ready
```

The callback runs serially for each subscription. `runtime.close()` stops every managed lease before it drains the connection.

Each `connection()` call starts at most one shared, bounded connection series. Concurrent callers share that series. If all attempts fail, a later call can start a fresh series. The connection factory must still enforce its own per-attempt timeout.

## JetStream resume example

If the application needs stored delivery, install the checkpoint and JetStream packages.

```ts
import { createIndexedDbCheckpointStore } from '@natsail/checkpoints'
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
    decode: decodeConversationEvent,
  },
  async (delivery) => {
    await conversation.applyIdempotently(delivery.value, delivery.cursor.sequence)
  }
)

await lease.ready
```

Use `createMemoryCheckpointStore()` for tests or session-only resume. IndexedDB keeps checkpoints across page reloads.

## Shared session adapters

Create one registry for an application runtime. A short idle delay prevents development-only subscription churn during React Strict Mode remounts.

```ts
import { createSessionRegistry } from '@natsail/session'

const sessions = createSessionRegistry({ idleCloseMs: 250 })
const source = (accept) =>
  consumeJetStream(
    runtime,
    {
      stream: 'CONVERSATIONS',
      filter: 'conversations.123.events',
      start: 'all',
      decode: decodeConversationEvent,
    },
    accept
  )
```

Supply the runtime and registry once near the React root. The provider owns neither object, so the application remains responsible for closing both:

```tsx
import {
  NatsProvider,
  useNatsCoreSubscriptionReducer,
  useNatsCoreSubscriptionSelector,
  useNatsRuntimeStatus,
  useNatsSessionSelector,
} from '@natsail/react'

function NatsRoot({ children }) {
  return (
    <NatsProvider runtime={runtime} sessions={sessions}>
      {children}
    </NatsProvider>
  )
}

function ConversationStatus() {
  const connection = useNatsRuntimeStatus()
  const phase = useNatsSessionSelector('conversation:123', source, (snapshot) => snapshot.phase)

  return `${connection.state}:${phase}`
}

function LatestOrder() {
  const order = useNatsCoreSubscriptionSelector(
    'orders',
    { subject: 'events.orders', decode: decodeOrder },
    (snapshot) => snapshot.value
  )

  return order?.id ?? 'waiting'
}

function RecentOrders() {
  const orders = useNatsCoreSubscriptionReducer(
    'orders:recent',
    { subject: 'events.orders', decode: decodeOrder },
    () => [],
    (current, order) => [...current, order].slice(-100)
  )

  return orders.value ?? []
}
```

Use `useNatsSession()` when a component needs the complete immutable snapshot. `useNatsSessionSelector()` avoids a React render when the selected value did not change. `useNatsCoreSubscription()` and `useNatsCoreSubscriptionSelector()` create the session source directly from Core NATS subscription options. `useNatsCoreSubscriptionReducer()` processes every delivery serially before React renders accumulated state.

RxJS can read snapshots, value deliveries, runtime events, or distinct connection states:

```ts
import {
  observeNatsCoreSubscription,
  observeNatsRuntimeStatus,
  observeNatsSession,
  observeNatsSessionValues,
} from '@natsail/rxjs'

const snapshots$ = observeNatsSession(sessions, 'conversation:123', source)
const values$ = observeNatsSessionValues(sessions, 'conversation:123', source)
const connection$ = observeNatsRuntimeStatus(runtime)
const orders$ = observeNatsCoreSubscription(sessions, runtime, 'orders', {
  subject: 'events.orders',
  decode: decodeOrder,
})
```

The value Observable replays the latest value once to a new subscriber. It still emits equal consecutive deliveries, errors when the session fails, and completes when the session closes.

The React and RxJS Core helpers share one underlying subscription when they use the same registry and key.

The session key identifies the source configuration. If the source configuration changes, change the session key.

## Current limits

The current packages solve client runtime, replay, checkpoint, session, React, and RxJS concerns. They do not yet solve every delivery or deployment model.

| Boundary                     | Supported now                                                                                                   | Not supported yet                                                                                         |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| JetStream consumer model     | Ordered consumers, `AckPolicy.None`, application checkpoints, duplicate policies, and retention-gap detection.  | Named durable consumers, explicit server acknowledgements, redelivery controls, and work-queue ownership. |
| Checkpoint coordination      | Monotonic memory and IndexedDB checkpoints for one client-side backing store.                                   | Distributed coordination between multiple writers or a server-owned acknowledgement record.               |
| Framework stream restoration | The AI example recreates a consumer from its processed checkpoint while the page stays open.                    | Complete AI SDK or TanStack AI message and run restoration after a page reload.                           |
| Cloudflare gateway           | Local Durable Object fan-out, storage-backed upstream checkpoints, restart replay, and bounded client catch-up. | A published gateway package with production authentication, backpressure, eviction, and cost policy.      |
| Cloudflare transport         | Official NATS WebSocket and Node TCP transports in local workerd.                                               | Remote endpoint, production authentication, and Workers VPC validation.                                   |
| Cross-tab connection sharing | A `SharedWorker` harness proves that two tabs can share one connection.                                         | A supported browser-broker protocol with defined authentication, lifecycle, and failure behavior.         |
| Adapter ergonomics           | Direct Core NATS helpers for React and RxJS; framework-neutral sessions can wrap JetStream or other sources.    | Direct JetStream convenience helpers and a full RxJS application example.                                 |
| Package availability         | Six public packages at `0.1.0`, Changesets versioning, and active trusted-publishing automation.                | A completed OIDC publication and npm provenance record. Version `0.1.0` used the manual bootstrap path.   |

These boundaries define the next proofs. They do not block continued experimentation in the examples and prototypes.

## Roadmap

1. Use the next consumer-visible change to prove OIDC publication. Confirm the npm provenance records, package tags, and GitHub releases.
2. Prove a separate named durable-consumer API with explicit acknowledgement, redelivery, ownership, and shutdown semantics. Keep `consumeJetStream()` focused on ordered replay.
3. Add a full-page AI recovery scenario that restores framework state and resumes after the last processed checkpoint.
4. Prove an atomic client catch-up-to-live handoff in the Durable Object gateway. Add authentication, byte limits, backpressure, forced-eviction tests, and cost measurements before package promotion.
5. Deploy the Cloudflare examples against a remote NATS endpoint. Test authentication, reconnect, JetStream resume, and Workers VPC independently.
6. Build a full RxJS rooms or chat example. Use it to find missing composition APIs and add direct JetStream helpers where they remove repeated application code.
7. Turn the `SharedWorker` harness into a supported browser broker after its protocol, authentication, lifecycle, and failure tests are explicit.

## License

Apache-2.0. This license matches the main NATS server and JavaScript client projects.
