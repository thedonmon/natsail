# @natsail/session

`@natsail/session` shares one managed source across keyed consumers. It owns source lifecycle, immutable snapshots, serial reducers, and idle cleanup.

```sh
pnpm add @natsail/core @natsail/session
```

React and RxJS adapters can use the same registry and key. This sharing prevents duplicate source subscriptions.

Prefer a validated definition when more than one caller can acquire a session. The registry rejects a key reused with a different delivery contract instead of silently sharing the wrong source:

```ts
import { defineSession } from '@natsail/session'

const conversation = defineSession({
  key: 'conversation:123',
  contract: 'conversation-events:v2',
  source,
})

const handle = sessions.acquire(conversation)
```

Call `handle.restart()` or `registry.restart(key)` to reopen a terminal source. The session keeps its handle and latest accepted value.

A restart rejects deliveries from the prior source generation. A restart does not occur automatically after an application handler fails.

`registry.inspect()` reports active keys, contracts, phases, reference counts, revisions, and idle state. `registry.events` emits lifecycle and reference-count changes so applications can detect leaks and unexpected restarts without reaching into an adapter.

See the [NATSail README](https://github.com/thedonmon/natsail#shared-session-adapters) for the registry model.

## License

Apache-2.0
