# @natsail/react

`@natsail/react` provides a NATSail provider, runtime status hooks, session selectors, Core hooks, JetStream hooks, and serial reducer hooks.

```sh
pnpm add react @natsail/core @natsail/session @natsail/jetstream @natsail/react
```

`NatsProvider` owns neither the runtime nor the session registry. The application closes both objects. `NatsManagedProvider` is the ownership-safe alternative: it creates both after commit, reuses the resource during React Strict Mode effect replay, and closes it after final unmount.

```tsx
<NatsManagedProvider
  identity={accountId}
  create={() => ({
    runtime: createNatsRuntime(runtimeOptions),
    sessions: createSessionRegistry({ idleCloseMs: 250 }),
  })}
>
  <App />
</NatsManagedProvider>
```

`useNatsJetStreamSubscription()` and its selector variant open one registry-shared, checkpointed JetStream source.

`useNatsJetStreamReducer()` accepts a validated reducing definition and returns its atomic replay/live snapshot. `useNatsJetStreamReducerSelector()` can schedule React notifications immediately, in a microtask, or on the next animation frame. The underlying session still reduces every delivery serially; only rendering is coalesced.

Both reducer hooks also accept the shared `batchPolicy` and an optional `NatsailScheduler`. Count, byte, and time bounds affect React notification delivery only. Replay and recovery phase changes bypass live coalescing.

`useNatsConnection()` follows the runtime-owned connection for advanced nats.js operations without adding an application connection effect.

`useNatsJetStreamProcessor()` owns one explicit-ack processor lease. Pass `null` options to disable it without conditionally calling a hook; change the key when its consumer configuration changes. A replacement waits for the previous lease to close, so an owned consumer cannot be recreated while its prior lease is still deleting it. When processor recovery is enabled, the hook reports `reconnecting` and the restart count without requiring a React remount.

See the [NATSail README](https://github.com/thedonmon/natsail#shared-session-adapters) for React examples.

## License

Apache-2.0
