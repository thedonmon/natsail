# Effect chat lab

> REPOSITORY EXAMPLE — included in the public repository. It is not published as an npm package.

## Run

```sh
pnpm example:effect-chat
```

Open <http://127.0.0.1:4178>. The default conversation reconstructs 240 retained user and assistant messages through a shared reducing session and `jetStreamStates()`.

Switch between 8, 48, 96, 240, 1,000, and 5,000-message conversations to compare replay and render cost. Initial history is reduced privately and appears as one complete transcript at catch-up. Every live event is reduced serially while cumulative presentation updates are coalesced within a 16ms window.

The materializer demonstrates the same shared 256-item/16ms policy and 4ms cooperative work budget as the RxJS lab while retaining native Effect Streams.

Open another tab and send a message. The inactive tab receives a Core NATS notification, marks the conversation unread, and reconstructs durable history when selected. Use **Simulate busy room** to publish 40, 250, or 1,000 compact live updates and inspect the adapter-ready, React-rendered, batch-size, commit, NATSail replay/handler/publish, and buffer-signal counters. Exact React commit duration is available in development or profiling builds.

The UI, fixture scenario, shared reducing-session contract, and presentation window are identical to the RxJS chat lab. Only the streaming adapter and stream name differ. The package guide separately demonstrates `materializeJetStream()` for cold sources whose reducers need typed Effect failures or services.

## Verify

```sh
pnpm example:effect-chat:verify
```
