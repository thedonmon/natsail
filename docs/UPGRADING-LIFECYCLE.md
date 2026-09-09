# Connection lifecycle upgrade notes

These changes build on the lifecycle behavior in Core 0.4.0, React 0.6.0, and Session 0.4.1. They do not change application authentication policy or the NATS transport's connection timeout.

## Factory cancellation is opt-in

Existing function factories still receive **no arguments**, including functions that accept optional transport options. Multiple `connection()` callers share one pending factory and its configured retry series.

Use the object form to receive a cancellation context:

```ts
const runtime = createNatsRuntime({
  connect: {
    async create({ signal, attempt, attemptId }) {
      // Supply the signal only to operations that support cancellation.
      // This factory must return the connection it creates, or clean it up itself.
      return createApplicationConnection({ signal })
    },
  },
  shutdownTimeoutMs: 15_000,
})
```

`attempt` starts at one for each initial-connect retry series. `attemptId` increases across all factory attempts within one runtime; neither identifier is globally unique. The runtime aborts the signal on disposal only while that factory is pending. It does not abort the signal after adopting the result, so a factory listener cannot accidentally bypass the connection's graceful drain period.

Signaling cancellation does **not** automatically abort an underlying WebSocket. In particular, wrapping `wsconnect()` in this object form does not give the NATS transport new abort support. The transport still owns its handshake timeout and reconnection behavior. NATSail has not added a second connection timeout or retry loop; `initialConnectRetry` remains an explicit, optional policy for rejected factory calls.

If a factory ignores cancellation, pending `connection()` promises still depend on that factory settling. Runtime `close()` does not wait indefinitely: it rejects at its shutdown deadline. A later result is never adopted or announced as connected; the runtime calls its immediate `close()`, not `drain()`, because it never owned any delivered work on that connection. Late rejection and cleanup rejection are observed internally. Factories must not create hidden connections and then discard them without returning or cleaning them up.

Code that directly invokes `NatsRuntimeOptions.connect` must now narrow the function/object union. Ordinary `createNatsRuntime({ connect: () => … })` call sites need no change.

## Reconnect coordination

`reconnect()` during startup waits for the shared initial factory, then forces a fresh handshake. It does not assume that the pending handshake used current credentials. Concurrent calls share one reconnect operation and its result; only the first call's diagnostic reason is used. A subsequent call after settlement starts a new operation.

Success requires both the native reconnect operation and the observed disconnect-to-connected cycle. If the connection closes permanently, the runtime shares one replacement factory with other callers, even if the old native reconnect promise remains pending. Rejection propagates to every coalesced caller. Disposal rejects pending reconnect callers and prevents further native reconnect or replacement-factory calls.

A concurrent request joins the cycle already in progress; it does not queue another handshake. If application inputs change again during that cycle, the application can request another reconnect after settlement. NATSail does not decide when credentials need rotation or replay failed requests. Reconnection can interrupt in-flight traffic; it does not make an ambiguous publish or request safe to retry.

## Managed provider disposal

The default `NatsManagedProvider` cleanup now calls `closeNatsResources({ runtime, sessions })` from `@natsail/session`, which starts `sessions.close()` and `runtime.close()` together. A registry waiting for connection startup or a stalled handler can no longer postpone the runtime deadline and retry cancellation. Cleanup observes both promises, and `onCloseError` receives a shutdown failure without waiting forever for registry cleanup.

Identity changes may temporarily overlap the replacement connection with the previous connection's drain or pending handshake. Replacements do not wait for the old drain. React StrictMode effect replay continues to reuse the same resource and cancels the replayed cleanup; a final unmount disposes it. This is lifecycle ownership, not a rule about when application authentication is ready.

A custom managed resource `close()` still overrides the default cleanup and owns both lifetimes. Delegate to the same framework-independent helper if you supply one:

```ts
import { closeNatsResources } from '@natsail/session'

function closeResource() {
  return closeNatsResources({ runtime, sessions })
}
```

The helper starts both operations even if one throws synchronously, observes both rejections, and rejects on the first failure. Repeated calls rely on the runtime and registry's idempotent `close()` methods. It does not call a resource's custom `close()`, so delegation cannot recurse. The runtime still owns the only shutdown deadline; the helper does not bound arbitrary custom cleanup that waits outside these managed lifetimes.

## Graceful completion versus cancellation

Core lease close drains buffered and already-in-flight delivery. Session close now keeps accepting that draining generation until its lease finishes, preserving the final snapshot. Restart still invalidates the previous generation. Explicit cancellation stops further Core handler delivery; it is not a graceful drain.

The runtime's grace period covers resource cleanup, a pending factory, and connection drain. If a disconnected transport recovers within that period, NATS can flush queued outbound messages. Otherwise shutdown rejects with `NatsRuntimeShutdownTimeoutError` and forces connection close: unsent or unfinished work may be lost. Do not treat that rejection as proof of successful processing. See the [NATS connection API](https://nats-io.github.io/nats.js/core/interfaces/NatsConnection.html#drain).

Pending unopened Core leases terminate at deadline cancellation. A handler that ignores cancellation can still run after the deadline; JavaScript cannot stop arbitrary application code, and its resource remains tracked until it settles. A factory may likewise outlive disposal, with the runtime retaining responsibility for its eventual result. Handle `close()` rejection, and do not infer completed external side effects from a closed runtime.

All deadlines depend on event-loop scheduling. Background browser throttling, suspended tabs, or synchronous JavaScript can delay timeout processing. Tests cover handshake completion before and after timeout processing, including a clock advance without timer processing; these are not hard wall-clock guarantees.

## Lifecycle diagnostics

Subscribe to `runtime.events` before starting work to see attempt history. Existing status snapshots remain available, but consumers must tolerate additional diagnostic events between them.

| Diagnostic code                                                  | Meaning                                                                                                   |
| ---------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `connection-attempt-started`                                     | Factory invocation begins.                                                                                |
| `connection-attempt-succeeded`                                   | Runtime adopts the result.                                                                                |
| `connection-attempt-failed`                                      | Attempt ends without adoption, including disposal.                                                        |
| `connection-retry-scheduled`                                     | Existing initial-connect retry policy schedules another attempt.                                          |
| `connection-failed`                                              | The current initial-connect retry series ends unsuccessfully.                                             |
| `reconnect-requested`, `reconnect-completed`, `reconnect-failed` | One coalesced explicit reconnect operation and its outcome.                                               |
| `disposal-requested`, `disposal-completed`                       | Shutdown starts or finishes gracefully.                                                                   |
| `disposal-timed-out`, `disposal-failed`                          | Shutdown ends without graceful completion.                                                                |
| `late-connection-discarded`                                      | A factory returned a connection after disposal began.                                                     |
| `late-connection-cleanup-failed`                                 | Closing a discarded connection failed; pending shutdown rejects instead of reporting graceful completion. |

Details include the adopted connection generation and, where relevant, `attempt`, `attemptId`, or `reconnectId`. Connection-attempt and retry diagnostics no longer include raw factory error objects; callers still receive the original rejection and retry callbacks still receive the error. Keep credentials and CONNECT payloads out of diagnostic reasons and application logging.

Attempt failure, retry, exhausted-retry, and explicit reconnect failure diagnostics include `details.failureCategory`: `authentication`, `timeout`, `connection`, `cancelled`, or `unknown`. Classification recognizes a fixed set of NATS error names without inspecting messages, causes, or CONNECT payloads; disposal takes precedence as `cancelled`. Custom or unrecognized errors remain `unknown`. These categories aid diagnosis, not authentication or retry policy, and cannot identify every underlying cause.

The event stream ends with the terminal `closed` status, even after a failed shutdown. It does not reopen for late factory results. The existing `natsail.connection.transitions` counter also records these lifecycle codes as its `state` attribute and includes `failureCategory` when present, so an optional telemetry sink can observe late results after the stream ends. Telemetry excludes attempt IDs, generation IDs, reasons, raw errors, and transport payloads to keep labels bounded and avoid disclosing credentials.

## Verification boundary

Focused tests use the real runtime, registry, and React provider, controlled handler/factory promises, real nats.js subscription buffering, and the installed NATS 3.4.0 WebSocket transport with an in-process fake socket. They cover timeout ordering, disconnected outbound draining, cancellation, late settlement, StrictMode, replacement overlap, and reconnect races. They do not certify browser suspension behavior, production authentication flows, or a deployed NATS cluster.
