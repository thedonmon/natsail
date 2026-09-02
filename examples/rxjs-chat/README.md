# RxJS chat example

> REPOSITORY EXAMPLE — included in the public repository. It is not published as an npm package.

## Run

```sh
pnpm example:rxjs-chat
```

Open <http://127.0.0.1:4177>. The app loads five retained messages from `NATSAIL_RXJS_CHAT` and displays their global stream cursors.

Send a message in any room. The publish returns through the shared RxJS state session before the transcript displays it.

Select **Reconnect and publish 3** to force the runtime transport through a visible disconnect and then publish three messages through the recovered runtime.

The ordered consumer continues from its processed checkpoint. The transcript displays all three messages without a second session or consumer.

## What it proves

- `defineReducingJetStreamSession()` owns the in-lease recovery cursor, bounded byte capacity, retry policy, and timeline reducer.
- Initial retained delivery is reduced privately and published as one atomic live timeline after `caughtUp` resolves.
- `observeNatsJetStreamState()` adapts the validated definition, removes duplicate lifecycle notifications, and frame-coalesces cumulative live state without a custom source controller.
- One shared state Observable feeds two independent RxJS projections from one keyed consumer and session reference.
- The reduced state preserves the complete multi-room timeline, room projection, replay range, and global stream cursors.
- `shareReplay()` gives both projections and the React external store the latest frame-batched view.
- The ordered consumer survives a forced transport reconnect and continues receiving through the same reduced session afterward.
- Package diagnostics expose active session references and consumer restart counts.
- A normal message publish returns through the same Observable before the chat displays it.

## Current boundary

The reducer still belongs to the application because it defines the domain view and retention policy. Consumer lifecycle, checkpoint recovery, replay-to-live handoff, and framework adaptation belong to NATSail.

The React bridge is also local. A separate `@natsail/react-rxjs` package remains unnecessary until more applications repeat this external-store adapter.

## Verify

```sh
pnpm example:rxjs-chat:verify
```
