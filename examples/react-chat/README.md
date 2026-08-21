# React rooms/chat example

> PRIVATE EXAMPLE — repository application that uses `@natsail/react` directly. It is not published.

## Run

```sh
pnpm example:react-chat
```

Open <http://127.0.0.1:4175/rooms/gateway-lab> and follow the three checks in the left test rail.

The command starts the repository's local NATS Docker fixtures when necessary and runs the Vite application. The browser connects directly to the NATS WebSocket endpoint at `ws://127.0.0.1:9223`.

## What it demonstrates

- One module-level `NatsRuntime` and `SessionRegistry` supplied through `NatsProvider`.
- `useNatsRuntimeStatus()` for connection state.
- `useNatsCoreSubscriptionReducer()` for one wildcard room subscription that serially folds every delivery into an immutable timeline.
- Core NATS publish through the provider runtime.
- TanStack Router loaders prefetched into a QueryClient passed through router context.
- Four logical rooms in one guided shadcn workbench.
- Explicit browser-observed receipts for connection, publish round trip, and shared multi-room state.

Core NATS is intentionally live-only. Reloading the page does not replay old messages. Run the gateway example when you want to exercise retained catch-up and Durable Object reconstruction.

## Verify

```sh
pnpm example:react-chat:verify
```
