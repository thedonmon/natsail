# RxJS chat lab

> REPOSITORY EXAMPLE — included in the public repository. It is not published as an npm package.

## Run

```sh
pnpm example:rxjs-chat
```

Open <http://127.0.0.1:4177>. The default conversation reconstructs 240 retained user and assistant messages through a reducing JetStream session and `observeNatsJetStreamState()`.

Switch between 8, 48, 96, 240, 1,000, and 5,000-message conversations to compare replay and render cost. Initial history remains private until catch-up and appears as one complete transcript. Cumulative live state is frame-coalesced with a 16ms window.

The lab uses the shared 256-item/16ms batch policy and a 4ms cooperative reducer budget. Its counters separate replay hydration work from cumulative live presentation updates.

Open another tab and send a message. The inactive tab receives a Core NATS notification, marks the conversation unread, and reconstructs durable history when selected. Use **Simulate busy room** to publish 40, 250, or 1,000 compact live updates and inspect the adapter-ready, React-rendered, batch-size, commit, NATSail replay/handler/publish, and buffer-signal counters. Exact React commit duration is available in development or profiling builds.

The UI and fixture scenario are identical to the Effect chat lab. Only the streaming adapter and stream name differ.

## Verify

```sh
pnpm example:rxjs-chat:verify
```
