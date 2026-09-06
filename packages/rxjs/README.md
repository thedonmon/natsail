# @natsail/rxjs

This branch evaluates **RxJS `9.0.0-beta.0`**, not an RC or stable release. Don't publish or deploy this branch yet. Released `@natsail/rxjs` packages still target RxJS 7; the API below applies only to this branch's local build.

`@natsail/rxjs` provides Observables for Core subscriptions, JetStream subscriptions, shared sessions, runtime events, and connection status.

Use the branch's pinned workspace dependencies to try it:

```sh
pnpm install --frozen-lockfile
pnpm --filter @natsail/rxjs... build
```

The adapter uses the framework-neutral session registry. Each direct subscription to a session-backed `ColdObservable` acquires its own registry handle; React and RxJS consumers with the same registry and key still share one underlying NATS source. Aborting one subscription doesn't cancel another. After the final handle releases and the source closes, subscribing again starts a fresh source without replaying the old session's cached value.

## Cancellation and operators

RxJS 9's `subscribe()` returns `undefined`, not a `Subscription`. Create an `AbortController` before subscribing and pass its signal, including when cancellation can happen inside the first synchronous callback. Operators use Symbol methods imported from individual RxJS subpaths instead of `.pipe()`.

`observeNatsJetStreamSubscription()` emits full deliveries from the same keyed session that React hooks use. `observeNatsJetStreamReducer()` consumes the same validated atomic state definition as `useNatsJetStreamReducer()` and exposes exact session lifecycle snapshots.

For rendering cumulative application state, prefer `observeNatsJetStreamState()`. It removes duplicate session-lifecycle notifications, emits replay and the first hydrated live state immediately, and coalesces subsequent cumulative live states to the latest value once per 16ms window:

```ts
import { distinctUntilChanged } from 'rxjs/distinct-until-changed'
import { map } from 'rxjs/map'

import { observeNatsJetStreamState } from '@natsail/rxjs'

const conversation$ = observeNatsJetStreamState(sessions, conversationDefinition, {
  liveBatchMs: 16,
})

const counts$ = conversation$[map]((snapshot) => snapshot.data.messages.length)
const messageCount$ = counts$[distinctUntilChanged]()

const controller = new AbortController()
messageCount$.subscribe((count) => console.log(count), { signal: controller.signal })

// When the component or owner stops consuming:
controller.abort()
```

Every JetStream delivery is still applied serially by the validated reducer. Only cumulative state presentation is coalesced, so a slow or busy browser does not lose events. The initial history rebuild remains one atomic hydrated state instead of hundreds of partial conversation states. Set `liveBatchMs: 0` when every reduced live state must be observed.

RxJS 9 removes the RxJS 7 scheduler API. The adapter now uses NATSail's host-timer scheduler. Its optional `scheduler` accepts `Pick<NatsailScheduler, 'schedule'>`: `schedule(task, delayMs)` must return a task with a `cancel()` method. Existing custom `SchedulerLike` implementations need to adopt that contract.

`batchPolicy` is the shared alternative to `liveBatchMs` and can bound cumulative notifications by count, bytes, and time. The legacy option remains supported. Replay, reconnect, and the first hydrated live value are immediate; normal completion flushes the latest pending live state, while abort and error discard it.

`observeNatsSession()` and `observeNatsSessionValues()` accept validated definitions as well as the legacy key/source pair. `observeNatsSessionEvents()` adapts registry lifecycle and reference-count diagnostics into a cancellable Observable.

## Evaluation status

The migration follows the [RxJS migration guide](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/MIGRATION.md), but verification uses the published `9.0.0-beta.0` artifact, not an unreleased checkout of RxJS. The guide can change independently of that artifact.

Public adapter tests cover delivered values (including equal consecutive values), late subscribers, independent session handles, cancellation, batching, errors, and completion. New lifecycle tests first passed on RxJS 7 before the dependency change. Beta-specific checks cover an already-aborted signal, cancellation inside the initial callback, and synchronous scheduling. The example also builds against emitted package declarations rather than test-only source aliases.

Direct probes of the beta's `ColdObservable` found lifecycle differences that still need upstream clarification:

- An already-aborted signal still starts the producer, and its newly registered teardown doesn't run. NATSail checks `subscriber.active` before acquiring resources.
- Teardowns run in registration order, and a teardown added after completion doesn't run. The guide describes platform cleanup in reverse order and immediate cleanup for an already-inactive subscriber. NATSail registers its cleanup before emitting the first value; it doesn't patch the RxJS runtime.

The browser acceptance suite runs the built public package and its exact Symbol operators in isolated Chromium realms, once with native Observable and once with the global removed so RxJS installs its fallback. It checks independent handles, shared sources, late joins, restart, terminal delivery, host-timer batching, and cancellation. The fixture bundles RxJS and the polyfill without external imports; it doesn't use a dev server or NATS server. The tarball verifier separately compiles and runs a Node consumer against shipped declarations, without workspace source aliases or a direct RxJS import.

```sh
pnpm --filter @natsail/rxjs... build
pnpm exec playwright install chromium
pnpm test:browser-rxjs
pnpm typecheck:tests
pnpm vitest run --exclude 'tests/integration/**'
pnpm release:check
```

The [unsupported-surface catalog](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/docs/UNSUPPORTED_RXJS_7_SURFACES.md) and [migration evidence ledger](https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/docs/MIGRATION_EVIDENCE_LEDGER.md) inform the scope audit. This adapter doesn't use RxJS Subjects, marble schedulers, custom Observable inputs, legacy interop, deep imports, or foreign-realm Observable bridging. Those features don't need substitute implementations. The migration tool isn't a runtime dependency, and its bounded pipe-expression transform has no remaining work in the migrated source.

| Adapter surface                                                               | Lifecycle contract                                                                                                                                                                                   | Evidence                                                                          |
| ----------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Session snapshots/values, Core/JetStream subscriptions, and reducer snapshots | A direct subscriber owns one handle; the registry shares the source by key. Final release closes it, and a later subscriber can start fresh.                                                         | Public adapter tests, Symbol-composed browser fixture, packed Node consumer       |
| Registry events and runtime events/status                                     | Each direct subscriber owns an iterator; abort or termination closes it once. Iteration failures reach the error handler; cleanup rejection after termination doesn't create an unhandled rejection. | Event/status and cleanup-failure tests                                            |
| Reduced JetStream state presentation                                          | Each subscriber owns its pending state and cancellable host task. Completion flushes; abort/error discard. Scheduler creation failure errors the output and releases its handle.                     | Fake-time and injected-scheduler tests, native/fallback browser host-timer checks |

### Before merging for release

- Select the actual supported RC or stable artifact, update the exact peer/dev/example/smoke-test pins and lockfile, then rerun the same gates. Don't replace an exact beta pin with a broad range as a substitute for verification.
- Recheck the upstream `ColdObservable` differences above against that artifact. Passing NATSail's guarded cases isn't proof that all upstream lifecycle differences have resolved.
- Run the complete CI suite, including NATS-backed integration and browser-broker coverage, on the final release commit. The no-server RxJS browser suite doesn't replace transport tests.
- Add the breaking-change Changeset and consumer migration notes, and remove evaluation-only warnings only after accepting the target version. Update the reusable RxJS skill with the final supported version too.

This branch doesn't provide dual RxJS 7/9 compatibility and isn't approval to publish a stable NATSail release against the beta.

## License

Apache-2.0
