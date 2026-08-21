# Cloudflare and NATSail

Research date: 2026-08-21

Status: WebSocket, TCP, and a tenant-scoped Durable Object gateway are proven in local workerd. Remote deployment and production gateway policy remain unproven.

## Executive finding

Cloudflare now has enough network primitives to run NATS clients at the edge. There are two practical transports:

- A Worker can use the standard WebSocket client API. The local Wrangler test now proves official NATS `wsconnect()` delivery in workerd. Remote deployment, authentication, and reconnect behavior still need tests. [Cloudflare outbound WebSocket example](https://developers.cloudflare.com/workers/examples/websockets/), [NATS `wsconnect()` documentation](https://nats-io.github.io/nats.js/core/index.html)
- Current Workers compatibility dates provide `node:net` on top of Cloudflare sockets. The local Wrangler test now proves the official `@nats-io/transport-node` package can deliver over TCP in workerd. A custom transport is not justified unless remote or VPC tests expose a gap. [Cloudflare `node:net`](https://developers.cloudflare.com/workers/runtime-apis/nodejs/net/), [Cloudflare TCP sockets](https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/)

The main constraint is lifecycle, not protocol support. A Durable Object can hibernate accepted browser WebSockets, but an outbound NATS TCP or WebSocket connection prevents hibernation. It keeps the object in memory and creates duration charges for up to 15 minutes per connection. [Durable Object WebSockets](https://developers.cloudflare.com/durable-objects/best-practices/websockets/), [Durable Object lifecycle](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/)

The local [Durable Object gateway prototype](../../prototypes/cloudflare-durable-object-gateway/README.md) now proves one object can accept two clients, share one NATSail WebSocket connection and JetStream consumer, save its checkpoint in Durable Object storage, reconstruct after a local workerd restart, and replay a message stored during downtime. The proof also exposes the remaining interface decision: a client behind the shared gateway cursor needs a separate catch-up path.

The first Cloudflare work belongs at the transport seam. It does not belong in React, RxJS, or the runtime core.

## What Cloudflare provides

### Workers WebSockets

Workers can accept a browser WebSocket with `WebSocketPair`. Workers and Durable Objects can also act as WebSocket clients. Cloudflare supports both `new WebSocket(url)` and an upgrade request made with `fetch()`. A received WebSocket message is limited to 32 MiB. [Workers WebSocket API](https://developers.cloudflare.com/workers/runtime-apis/websockets/), [Workers WebSocket example](https://developers.cloudflare.com/workers/examples/websockets/)

Cloudflare also proxies WebSockets to an origin without extra protocol configuration. A Cloudflare network release can terminate those connections, so reconnect handling remains mandatory. [Cloudflare proxied WebSockets](https://developers.cloudflare.com/network/websockets/)

NATS Server has a native WebSocket listener. TLS is required unless `no_tls` is enabled. Operators can restrict browser origins with `same_origin` or `allowed_origins`. NATS also supports cookie names for user, password, token, and client JWT authentication on this listener. [NATS WebSocket configuration](https://docs.nats.io/reference/config/websocket/)

The official NATS JavaScript core supports browsers and other W3C WebSocket runtimes through `wsconnect()`. It provides the same `NatsConnection` interface as the native transports. [NATS JavaScript core](https://nats-io.github.io/nats.js/core/index.html)

### Durable Objects and hibernation

A Durable Object is a globally unique, single-threaded coordination unit with persistent storage. Cloudflare recommends one object per logical coordination unit instead of one global singleton. [Durable Object rules](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)

The Hibernation WebSocket API keeps accepted client WebSockets connected while the object leaves memory. In-memory state is lost. Small per-socket state can survive in a serialized attachment, with a 16,384-byte maximum. Larger or longer-lived state belongs in Durable Object storage. [Durable Object WebSockets](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)

Hibernation applies only when the Durable Object is the WebSocket server. It does not apply to an outbound WebSocket. An active outbound TCP socket or WebSocket prevents hibernation and incurs duration while it keeps the object alive. [Durable Object lifecycle](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/), [Durable Object pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)

One Durable Object can accept up to 32,768 hibernatable WebSockets, but CPU and memory can lower the useful limit. An individual object has a soft limit of 1,000 requests per second. Cloudflare recommends batching small logical messages because each WebSocket frame has execution overhead. [Durable Object state API](https://developers.cloudflare.com/durable-objects/api/state/), [Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/), [WebSocket batching guidance](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)

### Outbound TCP and private NATS

`connect()` from `cloudflare:sockets` opens an outbound TCP socket with readable and writable streams. A Worker cannot accept raw inbound TCP today. Public outbound connections cannot target `localhost`, private IP addresses, Cloudflare IP ranges, or the same Worker. [Workers TCP sockets](https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/)

Workers VPC changes the private-network case. A VPC Network binding can open raw TCP to a service behind Cloudflare Tunnel, Mesh, or a Cloudflare WAN on-ramp. Workers VPC is in beta, and its VPC `connect()` supports plaintext TCP only today. [Workers VPC overview](https://developers.cloudflare.com/workers-vpc/), [Workers VPC binding API](https://developers.cloudflare.com/workers-vpc/api/)

This means a private NATS server is reachable through a VPC Network in principle. NATS authentication still runs in the NATS protocol. End-to-end NATS TLS over the VPC socket is not documented as supported because the binding currently exposes plaintext TCP only. The tunnel itself maintains an encrypted connection to Cloudflare, but that is a network property and not NATS transport TLS. [Workers VPC tunnel](https://developers.cloudflare.com/workers-vpc/configuration/tunnel/), [Workers VPC binding API](https://developers.cloudflare.com/workers-vpc/api/)

### Service bindings

A Service Binding lets one Worker call another without a public URL. It supports HTTP-style forwarding and JavaScript RPC. Calls count as subrequests and toward a maximum chain of 32 Worker invocations. They do not count as simultaneous open connections. [Service bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/service-bindings/)

Service bindings are useful for isolating application authentication, gateway routing, and NATS credential access. They are not a NATS transport and do not create a shared connection by themselves.

### Queues

Cloudflare Queues provides at-least-once delivery, so duplicate processing is possible. It does not guarantee publish order. A message is limited to 128 KB, a queue is limited to 5,000 messages per second, and retention can be configured up to 14 days on the paid plan. [Queues delivery guarantees](https://developers.cloudflare.com/queues/reference/delivery-guarantees/), [How Queues works](https://developers.cloudflare.com/queues/reference/how-queues-works/), [Queues limits](https://developers.cloudflare.com/queues/platform/limits/)

Queues is therefore a useful ingress buffer or HTTP bridge. It is not a transparent substitute for Core NATS subjects or JetStream sequence replay.

### Tunnel and Spectrum

A normal Cloudflare Tunnel TCP published application requires `cloudflared` on the client and is not a browser-direct NATS transport. [Tunnel published protocols](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/routing-to-tunnel/protocols/)

Workers VPC is the relevant Tunnel integration for Worker-to-private-NATS access because it provides a Worker binding and raw TCP routing. It is currently beta. [Workers VPC](https://developers.cloudflare.com/workers-vpc/)

Spectrum can proxy arbitrary public TCP traffic, but custom TCP and UDP applications require Enterprise with a paid Spectrum add-on. It can expose native NATS TCP without a client-side tunnel, but it does not solve browser TCP support or application-level authorization. [Spectrum overview](https://developers.cloudflare.com/spectrum/), [Spectrum protocols by plan](https://developers.cloudflare.com/spectrum/protocols-per-plan/)

## Architecture comparison

| Architecture                                       | Technical status                                                                                                                                            | Connections and multiplexing                                                                                                                                                                                                                                          | Reliability and security                                                                                                                                                                                                                                                                                                                                                                                                                              | Assessment                                                                                                                                                                      |
| -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Browser direct to NATS WebSocket                   | NATS Server and nats.js officially support browser WSS. The Chromium integration test proves the composition locally.                                       | One NATS connection per tab by default. The browser test proves a `SharedWorker` can reduce two per-tab connections to one connection per origin and browser profile.                                                                                                 | Core NATS can lose messages across disconnects. JetStream can replay. Browser credentials are visible to browser code, so use narrow subject permissions and short-lived identities. NATS can also read a secure client JWT cookie on its WebSocket listener. [NATS reconnect guidance](https://docs.nats.io/using-nats/developer/connecting/reconnect/buffer), [NATS WebSocket auth configuration](https://docs.nats.io/reference/config/websocket/) | Best first production path when browser-direct NATS access is acceptable. It keeps native protocol behavior and has the fewest moving parts.                                    |
| Worker or Durable Object WebSocket gateway to NATS | The local prototype proves a Durable Object can own NATSail over WebSocket, fan out to two clients, persist a checkpoint, and replay after workerd restart. | A per-user or per-tenant Durable Object can multiplex many tabs and conversations over one upstream NATS connection. Do not use one global object. [Durable Object rules](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/) | The gateway keeps NATS credentials off the browser and can enforce an application protocol. A live NATS upstream prevents hibernation and adds duration cost. JetStream recovers the shared upstream cursor, but a late client behind that cursor still needs an explicit catch-up strategy.                                                                                                                                                          | The package is valuable and technically feasible. Per-client catch-up, cost, authentication, byte backpressure, remote deployment, and eviction tests remain before production. |
| Worker TCP socket to NATS                          | The official NATS Node transport passes in local workerd through Cloudflare's current `node:net` compatibility layer.                                       | One Worker or Durable Object runtime can use one NATS connection for many subscriptions. A stateless Worker does not provide a dependable cross-request connection owner.                                                                                             | Public sockets need a public non-Cloudflare destination. Workers VPC can reach private NATS in beta, with plaintext TCP at the binding today. A persistent socket prevents Durable Object hibernation.                                                                                                                                                                                                                                                | No custom transport is justified yet. Remote routing, authentication, reconnect, and lifecycle still need proof.                                                                |
| HTTP or Queue bridge                               | Fully supported Cloudflare primitives, but NATS has no native HTTP bridge in this design. A separate NATS-connected service is required.                    | Browser tabs use HTTP or one gateway WebSocket. The bridge owns NATS connections elsewhere.                                                                                                                                                                           | Queues is at-least-once, unordered, and bounded by message and retention limits. Add idempotency keys. It does not preserve Core NATS or JetStream semantics automatically.                                                                                                                                                                                                                                                                           | Good for commands, ingestion, and coarse buffering. Poor as the primary low-latency conversation stream.                                                                        |

## The existing Cloudflare transport proposal

The open [NATS JavaScript issue #405](https://github.com/nats-io/nats.js/issues/405) proposes a Cloudflare Workers TCP transport. Its author published the unofficial Apache-2.0 package [`@eulerstream/nats-cloudflare`](https://github.com/EulerStream/nats-cloudflare-transport). The implementation adapts `cloudflare:sockets`, including direct TLS and STARTTLS, to the NATS transport contract. It currently imports `@nats-io/nats-core/internal`, which makes it sensitive to NATS internal changes. [Transport source](https://github.com/EulerStream/nats-cloudflare-transport/blob/main/src/cloudflare_transport.ts), [package manifest](https://github.com/EulerStream/nats-cloudflare-transport/blob/main/package.json)

A NATS maintainer reported successful Wrangler tests for basics, JetStream, and Services, then recommended incubation in Synadia Orbit for usage feedback. The proposer agreed. The issue remained open with no linked release or pull request at the research date. [Maintainer result and Orbit recommendation](https://github.com/nats-io/nats.js/issues/405#issuecomment-4412787824), [proposal follow-up](https://github.com/nats-io/nats.js/issues/405#issuecomment-4413601893), [latest issue follow-up](https://github.com/nats-io/nats.js/issues/405#issuecomment-4896401801)

This work is evidence that TCP is feasible. It is not evidence of an official, stable NATS Cloudflare transport. The monorepo should collaborate with this proposal instead of silently copying it under a second API.

## Authentication and credential boundary

For browser-direct WSS, every credential available to the NATS client is available to browser JavaScript unless the NATS WebSocket listener obtains a client JWT from a secure cookie. NATS supports subject-level permissions, accounts, user credentials, tokens, NKeys, and JWTs. NKey seeds are private keys and must be guarded as secrets. [NATS WebSocket configuration](https://docs.nats.io/reference/config/websocket/), [NATS NKeys](https://docs.nats.io/using-nats/nats-tools/nk)

For a Worker or Durable Object gateway, keep the NATS credential in a Worker Secret or Secrets Store binding. Cloudflare says not to put sensitive values in plaintext variables. Secret values are hidden from Wrangler and the dashboard after definition. [Workers secrets](https://developers.cloudflare.com/workers/configuration/secrets/), [Secrets Store Workers integration](https://developers.cloudflare.com/secrets-store/integrations/workers/)

The gateway still needs its own browser authentication, authorization, subject allowlist, payload limits, and rate limits. NATS permissions remain defense in depth rather than a replacement for that boundary.

## Limits that affect the design

- Workers have 128 MB per isolate. The default paid Worker CPU limit is 30 seconds and can be configured up to five minutes. Incoming HTTP requests have no hard wall-time limit while the client remains connected. [Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
- A Worker invocation can have six connections in the connection-establishment phase. Outbound TCP and WebSocket connections count during establishment. Once response headers arrive, established connections no longer count against that six-connection limit. [Workers simultaneous connection limits](https://developers.cloudflare.com/workers/platform/limits/#simultaneous-open-connections)
- A Durable Object has the same six outgoing connections per request, a 32 MiB received WebSocket message limit, and a soft 1,000-request-per-second limit per object. [Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- Hibernation saves duration only for accepted client WebSockets. An outbound NATS socket removes that benefit while it keeps the object alive. [Durable Object WebSockets](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)

These limits favor one runtime connection with many NATS subscriptions. They also favor small bounded buffers and batched browser frames. They do not favor one NATS connection per conversation.

## Recommended first monorepo seam

Do not create `@natsail/transport-cloudflare` yet. The official WebSocket and Node transports both pass the local workerd proof through the runtime's existing connection-factory seam.

Continue with three proofs:

1. Deploy a Worker and run official WebSocket and TCP transports against non-local NATS endpoints. Cover authentication, forced reconnect, and JetStream replay.
2. Run the TCP transport through a Workers VPC private NATS target. Confirm routing and decide whether plaintext inside the VPC is acceptable.
3. Extend the successful Durable Object prototype with a per-client catch-up strategy. Then measure duration with an outbound socket, forced eviction, buffer pressure, and remote JetStream resume by sequence.

Do not put a Durable Object gateway protocol in the first transport package. A later `@natsail/cloudflare-gateway` package can depend on the stable transport and expose Cloudflare-specific session handling. React and RxJS adapters can then consume the same framework-agnostic runtime from their own packages.

## Supported, inferred, and unproven summary

| Finding                                                                         | Classification                                                                                                                                                |
| ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Browser to NATS Server over WSS                                                 | Supported by NATS. Cloudflare WebSocket proxying is supported. Test the combined deployment.                                                                  |
| `wsconnect()` inside a Worker                                                   | Proven locally in workerd through Wrangler. Remote deployment, auth, and reconnect tests remain.                                                              |
| Worker to public NATS over raw TCP                                              | The official NATS Node transport passes locally through Cloudflare's `node:net` implementation. Remote endpoint behavior remains unproven.                    |
| Worker to private NATS over Workers VPC TCP                                     | Cloudflare supports raw plaintext TCP through a VPC Network in beta. NATS-specific operation is inferred and needs an end-to-end test.                        |
| Hibernating browser sockets while retaining a live NATS upstream                | Not supported. Outbound TCP and WebSocket connections prevent hibernation.                                                                                    |
| One Durable Object multiplexing tabs and conversations over one NATS connection | Proven locally with two clients, one NATS connection, a storage-backed checkpoint, workerd restart, and JetStream replay. Production policy remains unproven. |
| Cloudflare Queues as a transparent JetStream replacement                        | Not supported by the semantics. Queues is at-least-once and unordered, with different limits and cursor behavior.                                             |
| Tunnel as direct arbitrary TCP for browser NATS                                 | Not appropriate. Standard published TCP routes require client-side `cloudflared`. Workers VPC is the relevant Worker-side option.                             |
