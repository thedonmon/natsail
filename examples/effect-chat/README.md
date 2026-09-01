# Effect chat lab

> REPOSITORY EXAMPLE — included in the public repository. It is not published as an npm package.

## Run

```sh
pnpm example:effect-chat
```

Open <http://127.0.0.1:4178>. The default conversation reconstructs 240 retained user and assistant messages through `materializeJetStream()`.

Switch between 8, 48, 96, 240, 1,000, and 5,000-message conversations to compare replay and render cost. Initial history is reduced privately and appears as one complete transcript at catch-up. Live state is reduced in bounded Effect batches within a 16ms window.

Open another tab and send a message. The inactive tab receives a Core NATS notification, marks the conversation unread, and reconstructs durable history when selected. Use **Simulate busy room** to publish 40, 250, or 1,000 compact live updates and inspect the adapter-ready, React-rendered, batch-size, and commit counters. Exact React commit duration is available in development or profiling builds.

The UI and fixture scenario are identical to the RxJS chat lab. Only the streaming adapter and stream name differ.

## Verify

```sh
pnpm example:effect-chat:verify
```
