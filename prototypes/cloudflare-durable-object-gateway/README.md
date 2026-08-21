# Cloudflare Durable Object gateway prototype

> PROTOTYPE — throwaway code used to decide whether `@natsail/cloudflare-gateway` earns a production package.

## Question

Can one tenant-scoped Durable Object:

1. accept multiple application WebSocket clients;
2. fan out one NATSail JetStream consumer over one upstream NATS connection;
3. persist the upstream checkpoint in Durable Object storage;
4. reconstruct the NATSail runtime after a local workerd restart;
5. replay messages stored while the object was down; and
6. detect, rather than silently ignore, a client whose cursor is behind the shared gateway cursor; and
7. catch that client up from a bounded Durable Object retained-delivery log?

## Run

```sh
pnpm prototype:cloudflare-gateway
```

The command starts the repository's NATS Docker fixtures when necessary, creates a scratch JetStream stream, runs Wrangler twice against the same scratch Durable Object storage, and prints the complete gateway state after every action. It removes only `.wrangler/PROTOTYPE_WIPE_ME_cloudflare_gateway` before a run.

This is not production gateway code. The token, route map, administrative restart controls, and message limits are intentionally fixed for the scenario.

## Promotion criteria

Promote the validated behavior into a separate package only if the prototype shows useful leverage. A production module must hide Durable Object reconstruction, NATSail runtime ownership, storage checkpoints, channel authorization, downstream cursor validation, and bounded client delivery behind a small interface.

The prototype retains the latest 128 deliveries. A client inside that window can fetch the missing frames before it reconnects at the current shared cursor. A client outside the window receives an explicit incomplete-history result.

This follow-up proves the retained-log strategy is usable for short gaps. It does not settle the production choice. Durable Object storage cost, byte limits, continuous-traffic races, and a temporary JetStream catch-up consumer still need comparison.
