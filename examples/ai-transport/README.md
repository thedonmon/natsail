# AI chat example

> REPOSITORY EXAMPLE — included in the public repository. It is not published as an npm package.

## Run

```sh
pnpm example:ai-transport
```

Open <http://127.0.0.1:4176>. The app first replays a seeded conversation from `NATSAIL_AI_CONVERSATIONS`. Each message displays its global stream sequence.

Send the suggested prompt or write a message. During the answer, use one of these recovery actions:

- **Run recovery test** closes the ordered consumer for two seconds. The responder continues to publish while the connection reconnects.
- **Reload page mid-reply** reloads the browser page. The app restores the selected framework, chat messages, active run, and JetStream position.

JetStream sends the retained native chunks back into the real framework stream. The same chat transcript remains visible after each recovery.

Open **Transport details** below the composer to change the framework, delivery mode, or duplicate policy. The panel also displays the reply and history cursors.

**Inject random message** publishes an event into the local conversation stream. The transcript displays the event with its assigned sequence.

No model, route, external API, or API key is required. The local responder uses deterministic `@shadcn/helpers` conversations to produce each framework's native stream events.

## What it proves

- A normal multi-turn chat can stream and recover over NATSail through either framework without exposing transport diagnostics to the person chatting.
- `NatsAiSdkChatTransport` implements the AI SDK `ChatTransport` contract and returns `ReadableStream<UIMessageChunk>` through Core NATS or an ordered JetStream consumer.
- `NatsTanStackConnection` implements TanStack AI's long-lived `SubscribeConnectionAdapter` contract and returns native AG-UI `StreamChunk` events through the same delivery choices.
- Each framework's real `useChat` hook owns and reconstructs its message state.
- The browser restores its stable client ID, selected framework, messages, and active-run identity after a page reload.
- The AI SDK adapter implements `reconnectToStream()` and replays the complete retained run from its native `start` event.
- The TanStack AI adapter persists messages and continues strictly after its IndexedDB checkpoint.
- `consumeJetStream()` checkpoints each native frame only after the framework adapter processes it and resumes strictly after that cursor.
- Closing and recreating the ordered consumer during an active answer recovers frames retained during the two-second gap without replacing already-rendered messages.
- `start: "all"` reconstructs earlier application-level conversation messages; the native reply consumers use `start: "new"` until a checkpoint exists.
- Filtered consumers expose global stream-sequence units, including jumps caused by other filtered subjects.
- The app exposes the package's `drop`, `deliver`, and `error` duplicate policies.
- Core NATS remains available as the explicit live-only comparison.
- NATSail remains the transport and runtime boundary. It does not introduce a third chat-message model.
- The interface uses the repository's private shadcn chat primitives as a realistic application surface.

## Current boundary

NATS fixes ordered consumers to `AckPolicy.None`. The server cursor can advance before application processing.

NATSail saves a separate application checkpoint after the handler succeeds. Explicit acknowledgements, redelivery, and named ownership require a separate durable-consumer API.

The two frameworks require different page-recovery strategies. AI SDK persisted messages do not contain the transient state that assembles streamed message parts.

As a result, the AI SDK adapter replays the complete retained native run. Each reply uses a unique subject, which limits this replay to one run.

TanStack AI can hydrate an existing message part by its message ID. It continues after the last processed checkpoint without replaying the earlier run events.

The example stores checkpoints in IndexedDB. Browser storage owns the chat and run identity. Cross-device recovery requires a server-owned run registry and checkpoint.

Core NATS resumes live delivery after a reconnect. It does not replay chunks that arrive while the browser is disconnected.

The UI disables both recovery actions in Core mode.

The example pins `@tanstack/ai` 0.40 and `@tanstack/ai-client` 0.20. `@shadcn/helpers` 0.2 declares this compatibility range.

Upgrade the helper and TanStack packages together.

## Verify

```sh
pnpm example:ai-transport:verify
```
