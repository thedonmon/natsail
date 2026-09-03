---
name: natsail-session
description: Use @natsail/session to share one keyed NATS source across components or framework adapters, define stable delivery contracts, manage reference-counted handles, reduce values serially, restart terminal sources, and avoid subscription churn.
---

# Use @natsail/session

```sh
pnpm add @natsail/core @natsail/session
```

Create one registry beside the application runtime. A small idle delay is useful when React Strict Mode or route transitions briefly release and reacquire a source.

```ts
import { natsCodecs } from '@natsail/core'
import { createCoreSessionSource, createSessionRegistry, defineSession } from '@natsail/session'

const sessions = createSessionRegistry({ idleCloseMs: 250 })

const conversation = defineSession({
  key: 'conversation:123',
  contract: 'core:chat.room.123:text:v1',
  source: createCoreSessionSource(runtime, {
    subject: 'chat.room.123',
    codec: natsCodecs.text,
  }),
})
```

Pass the same definition and registry to every consumer that should share one underlying source. Prefer validated definitions over the lower-level, unvalidated `key, source` form when more than one caller can acquire the session.

## Design keys and contracts deliberately

- `key` identifies the logical source instance.
- `contract` describes every option that changes delivery semantics: subject or filters, decoder version, start/resume policy, recovery policy, and reducer shape where applicable.
- Reusing a key with a different contract throws `SessionContractMismatchError`; this prevents silently sharing the wrong source.
- Changing only a source closure does not redefine an already active session. Use a new key when delivery identity changes while the prior session may still exist; alternatively, fully release and close the prior session before reacquiring its key with a new contract.

## Own handles correctly

```ts
const handle = sessions.acquire(conversation)
await handle.ready

const renderSnapshot = () => render(handle.getSnapshot())
const unsubscribe = handle.subscribe(renderSnapshot)
renderSnapshot()

unsubscribe()
await handle.release()
```

Each caller releases its handle; the source closes after the final owner leaves and the idle delay expires. Use `handle.restart()` or `sessions.restart(key)` to reopen a terminal source while keeping the logical session and latest value. Restarts are explicit after application-handler failures.

Use `createReducingSessionSource()` when values must be folded serially before consumers see state. Use `sessions.inspect()` and `sessions.events` to diagnose leaked references, unexpected restarts, and lifecycle changes. Close an application-owned registry before closing its runtime.

See the [session package guide](https://github.com/thedonmon/natsail/tree/main/packages/session#readme) and [npm package](https://www.npmjs.com/package/@natsail/session).
