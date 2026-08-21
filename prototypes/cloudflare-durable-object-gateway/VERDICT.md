# Prototype verdict

Date: 2026-08-21

Question: Does a tenant-scoped Durable Object gateway provide enough behavior behind a small enough seam to justify a separate NATSail package?

Verdict: Yes. Keep it separate from transport and runtime Core. The retained-log catch-up is valuable enough to continue, but not yet production-ready.

## Proven locally

The one-command scenario completed against NATS Server 2.14.4 and Wrangler 4.125.0:

- Two WebSocket clients attached to one Durable Object.
- Both clients received one delivery from one NATSail runtime and one upstream connection.
- The NATSail JetStream checkpoint was stored through a Durable Object storage adapter.
- Wrangler/workerd stopped and restarted against the same scratch storage.
- JetStream stored a message while workerd was down.
- The reconstructed object opened one new upstream connection and replayed the stored message after the persisted cursor.
- A late client with cursor 1, after the gateway reached cursor 2, received `resume-required` instead of silently joining at cursor 2.
- The same client fetched exactly the missing delivery from a bounded 128-delivery Durable Object log.
- Two current clients resumed shared live delivery at cursor 3.

The final state reported two object instances, two total upstream connection attempts, three deliveries, and checkpoint 3.

## Interface-defining result

One shared upstream consumer is sufficient for current-client fan-out and object reconstruction. It is not sufficient by itself for every client reconnect.

The shared gateway checkpoint answers “what has entered the gateway.” A client checkpoint answers “what this client applied.” These cursors can differ. A production module must preserve that distinction.

The retained-log follow-up now proves option 2 works for a short client gap. The remaining design comparison is:

1. a temporary ordered catch-up consumer per behind client, followed by attachment to the shared live fan-out;
2. the proven bounded Durable Object retained-delivery log, after adding explicit byte limits and measuring storage cost; and
3. one consumer per client, used as the correctness baseline but expected to lose multiplexing leverage.

The likely production shape is still temporary per-client catch-up plus the shared live consumer for long gaps, possibly with a small retained log for the common short-gap path. The implementation must close the race between the catch-up high-water mark and new live deliveries with a bounded per-client queue or an atomic handoff protocol.

## Still unproven

- Remote Cloudflare deployment and non-local NATS connectivity
- Real browser authentication and channel authorization
- Forced Durable Object eviction through Cloudflare's test integration
- Outbound-socket duration cost
- Byte-based backpressure, batching, and slow-client handling
- NATS authentication and credential rotation inside the gateway
- Multi-tenant capacity and overload behavior
- Retained-log storage cost, byte caps, and expiry under sustained traffic
- Atomic catch-up-to-live handoff while the room feed never becomes quiet

## Repository status

The prototype remains under `prototypes/`. It is evidence for a future package, not a production gateway implementation.
