# TanStack gateway rooms/chat example

> PRIVATE EXAMPLE — repository application used to exercise the NATSail Durable Object gateway prototype. It is not published.

## Question

Does the gateway seam remain useful when a real browser application has multiple rooms, multiple tabs, optimistic sends, explicit transport state, and a client that must catch up before rejoining live fan-out?

## Run

```sh
pnpm example:gateway-chat
```

Open <http://127.0.0.1:4174/rooms/gateway-lab>. The command starts the local NATS fixture when necessary, creates a scratch JetStream stream, starts the Durable Object gateway, seeds five room messages, and runs the Vite application. It removes the scratch stream when stopped.

Follow the three checks in the left test rail. The final check tells you to open a second tab, simulate a gap in the first tab, publish from the second tab, and reconnect the first tab.

Run the browser hypothesis proof with:

```sh
pnpm example:gateway-chat:verify
```

## What the application exercises

- TanStack Router loaders prefetch room metadata and retained history.
- The QueryClient is passed through router context.
- TanStack Query is the only application cache.
- The WebSocket client writes deliveries and connection phases into that cache.
- Each browser tab has its own applied cursor.
- The Durable Object has one shared upstream cursor and a bounded 128-delivery retained log.
- “Simulate gap” disconnects one tab without stopping the upstream.
- A reconnecting tab fetches its missing delivery, applies it, and then attaches at the current shared cursor.
- The guided workbench reports each observation and keeps cursor, catch-up, and delivery evidence visible.
