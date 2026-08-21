# RxJS chat example

> REPOSITORY EXAMPLE — included in the public repository. It is not published as an npm package.

## Run

```sh
pnpm example:rxjs-chat
```

Open <http://127.0.0.1:4177>. The app loads five retained messages from `NATSAIL_RXJS_CHAT` and displays their global stream cursors.

Send a message in any room. The publish returns through the shared RxJS feed before the transcript displays it.

Select **Pause stream and publish 3** to run the recovery scenario. The app closes the ordered consumer and publishes three messages during the gap.

The same Observable resumes after its processed checkpoint. The transcript displays all three retained messages without a new session source.

## What it proves

- `observeNatsSessionValues()` adapts a JetStream `SessionSource` without a React-specific NATS hook.
- `scan()` preserves the complete multi-room timeline and each global stream cursor.
- A second RxJS projection counts observed rooms from the same delivery stream.
- `share()` keeps both projections on one keyed NATSail session source.
- `shareReplay()` gives the React external store the latest combined view.
- The ordered consumer can close, publish into the gap, and resume after the application checkpoint.
- A normal message publish returns through the same Observable before the chat displays it.

## Current boundary

`@natsail/rxjs` has a direct Core NATS helper. JetStream uses the framework-neutral `SessionSource` seam in this example.

A direct JetStream helper can remove a small adapter function. It can also add a required dependency from the RxJS package to the JetStream package.

This example keeps the adapter local until another application shows repeated code. The recovery controller is example logic, not a general package interface.

The React bridge is also local. A separate `@natsail/react-rxjs` package remains unnecessary until more applications repeat this external-store adapter.

## Verify

```sh
pnpm example:rxjs-chat:verify
```
