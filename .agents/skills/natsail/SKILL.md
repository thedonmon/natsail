---
name: natsail
description: Build TypeScript applications with the NATSail package family. Use when choosing packages, creating a shared runtime, deciding between live Core NATS, ordered JetStream replay, and explicit-ack processing, or composing React, RxJS, and Effect integrations.
---

# Use NATSail in an application

NATSail adds application-owned lifecycle, recovery, delivery, and framework integration around the official NATS JavaScript clients. Start with one runtime and add only the packages the application uses.

## Choose the delivery contract first

| Application need                 | API                   | Contract                                                                                                                                                |
| -------------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Live, ephemeral messages         | `runtime.subscribe()` | Core NATS does not replay messages missed while offline. Handlers run serially.                                                                         |
| Replay followed by live delivery | `consumeJetStream()`  | An ordered consumer uses `AckPolicy.None`; an application checkpoint advances after successful handling. `caughtUp` marks the captured replay boundary. |
| Durable work and redelivery      | `processJetStream()`  | A named consumer uses `AckPolicy.Explicit`; acknowledgement follows successful handling, and failed work remains eligible for redelivery.               |

Do not use Core NATS when missed messages are unacceptable. Do not treat an ordered-consumer cursor as a server acknowledgement. Use an explicit-ack processor when business completion must control acknowledgement.

## Choose packages

| Package                | Add it when                                                              | Detailed skill                                         |
| ---------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------ |
| `@natsail/core`        | Runtime-backed messaging; it owns the shared runtime and Core operations | [natsail-core](../natsail-core/SKILL.md)               |
| `@natsail/jetstream`   | Replay, checkpoints, reducing sessions, or durable processors are needed | [natsail-jetstream](../natsail-jetstream/SKILL.md)     |
| `@natsail/checkpoints` | The application configures memory or IndexedDB resume state              | [natsail-checkpoints](../natsail-checkpoints/SKILL.md) |
| `@natsail/session`     | Multiple consumers or adapters should share one keyed source             | [natsail-session](../natsail-session/SKILL.md)         |
| `@natsail/react`       | React providers, hooks, and selectors are needed                         | [natsail-react](../natsail-react/SKILL.md)             |
| `@natsail/rxjs`        | NATS sources should be consumed as Observables                           | [natsail-rxjs](../natsail-rxjs/SKILL.md)               |
| `@natsail/effect`      | NATS sources should use Effect services, Layers, and scoped Streams      | [natsail-effect](../natsail-effect/SKILL.md)           |

Install every package imported by application code instead of relying on transitive dependency hoisting. NATSail package versions are independent; do not assume every package has the same version number. For Effect, follow the exact peer version and npm tag in the Effect skill.

## Create shared application resources

Supply an official NATS connection factory to `createNatsRuntime()`. Node applications normally use `connect()` from `@nats-io/transport-node`; browser applications use `wsconnect()` from `@nats-io/nats-core`.

```ts
import { connect } from '@nats-io/transport-node'
import { createNatsRuntime } from '@natsail/core'
import { createSessionRegistry } from '@natsail/session'

const runtime = createNatsRuntime({
  connect: () => connect({ servers: 'nats://127.0.0.1:4222' }),
  initialConnectRetry: { maxAttempts: 3, delayMs: 500 },
})

// Create this only when logical sources need to be shared.
const sessions = createSessionRegistry({ idleCloseMs: 250 })
```

Create one runtime per application or JavaScript realm. Create one registry beside it when using shared sessions. Define a logical source once, then give the same definition and registry to React, RxJS, or Effect so they share the underlying subscription or consumer.

## Respect ownership and lifecycle

- Direct Core subscriptions and JetStream consumers or processors return leases. Await `ready`, observe `closed`, and call the idempotent `close()` when the owner is done. Use `caughtUp` when initial replay completion matters.
- Session callers release their handles. Application-owned registries are closed before their runtime.
- Prefer `NatsManagedProvider` in React and `makeNatsailScopedLayer` in Effect when those integrations should own cleanup. RxJS consumers release ownership by unsubscribing.
- The runtime owns the nats.js connection. Never close or drain the object returned by `runtime.connection()`.
- Use exactly one `codec` or metadata-aware `decode` function at each payload boundary.
- Use package-owned recovery. Decoder, configuration, contract, and application-handler failures remain terminal instead of being hidden by retry loops.
- React and RxJS may coalesce presentation notifications, but every delivery must still reach a reducer serially.
- A reducing session rebuilds state from replay and cannot safely use cursor-only `resume` without atomically restored materialized state.

See the [NATSail guide](https://github.com/thedonmon/natsail#readme) and [delivery guarantees](https://github.com/thedonmon/natsail/blob/main/docs/DELIVERY.md) for the current public contract.
