---
name: natsail-react
description: Use @natsail/react to provide a NATSail runtime and session registry to React, consume Core or JetStream sources with hooks and selectors, coalesce rendering safely, and own explicit-ack processors through component lifecycle.
---

# Use @natsail/react

```sh
pnpm add react @natsail/core @natsail/session @natsail/jetstream @natsail/react
```

## Prefer managed application ownership

```tsx
import type { PropsWithChildren } from 'react'

import { NatsManagedProvider } from '@natsail/react'
import { createSessionRegistry } from '@natsail/session'

function NatsRoot({ accountId, children }: PropsWithChildren<{ accountId: string }>) {
  return (
    <NatsManagedProvider
      identity={accountId}
      create={() => ({
        runtime: createRuntime(accountId),
        sessions: createSessionRegistry({ idleCloseMs: 250 }),
      })}
      fallback={null}
    >
      {children}
    </NatsManagedProvider>
  )
}
```

`NatsManagedProvider` creates resources after commit, survives React Strict Mode effect replay, and closes the registry and runtime after final unmount. Change `identity` when the connection identity changes. Use `NatsProvider` only when the application creates and closes both resources itself.

## Choose the narrowest hook

- `useNatsCoreSubscription()` for the latest value from a shared live Core subject.
- `useNatsCoreSubscriptionReducer()` when React needs accumulated Core state.
- `useNatsJetStreamSubscription()` for shared replay/live deliveries.
- `useNatsJetStreamReducer()` for a validated atomic reducing definition.
- `useNatsJetStreamReducerSelector()` or another selector hook to render only a projection.
- `useNatsRuntimeStatus()` for connection state and `useNatsConnection()` for advanced runtime-owned nats.js operations.

Use a stable key that represents every source option. Change the key when subject, filter, decoder, credentials, start/resume, or delivery policy changes. Prefer a validated definition from `defineSession()`, `defineJetStreamSession()`, or `defineReducingJetStreamSession()` when the same source can be used by more than one caller or adapter.

For reducing JetStream state, the default `animation-frame` notification mode coalesces React renders, not reducer deliveries. Selectors receive session snapshots; account for connection phase and the possibility that `value` is not present yet.

## Own processors with a hook

`useNatsJetStreamProcessor(key, options, handler)` owns one explicit-ack processor lease. Pass `null` to disable it without calling the hook conditionally. Change the key whenever consumer configuration changes; handler changes do not require a restart. Replacements wait for the old lease to close, which matters for `owned` consumers. Render the returned phase, restart count, and error instead of adding a React retry loop.

The runtime continues to own the connection returned by `useNatsConnection()`; never close or drain it.

See the [React package guide](https://github.com/thedonmon/natsail/tree/main/packages/react#readme) and [npm package](https://www.npmjs.com/package/@natsail/react).
