# AI chat example

> PRIVATE EXAMPLE — repository application for exercising NATSail as a real chat transport. It is not published.

## Run

```sh
pnpm example:ai-transport
```

Open <http://127.0.0.1:4176>. The app first replays a seeded conversation from `NATSAIL_AI_CONVERSATIONS` and displays each global stream sequence. Use the suggested prompt or write your own message. While the reply is streaming, click **Run recovery test**. The test closes the reply's ordered consumer for two visible seconds while the responder keeps publishing, forces a real connection reconnect, then recreates the consumer from its last processed checkpoint. JetStream replays the retained native chunks into the same framework stream and transcript.

Open **Transport details** below the composer when you want to switch between **AI SDK** and **TanStack AI**, compare **JetStream** with live-only **Core NATS**, select the duplicate-delivery policy, or inspect reply/history cursors and acknowledgement semantics. **Inject random message** publishes another event directly into the running local conversation stream and renders it with its assigned sequence.

No model, route, external API, or API key is required. The local responder uses deterministic `@shadcn/helpers` conversations to produce each framework's native stream events.

## What it proves

- A normal multi-turn chat can stream and recover over NATSail through either framework without exposing transport diagnostics to the person chatting.
- `NatsAiSdkChatTransport` implements the AI SDK `ChatTransport` contract and returns `ReadableStream<UIMessageChunk>` through Core NATS or an ordered JetStream consumer.
- `NatsTanStackConnection` implements TanStack AI's long-lived `SubscribeConnectionAdapter` contract and returns native AG-UI `StreamChunk` events through the same delivery choices.
- Each framework's real `useChat` hook owns and reconstructs its message state.
- `consumeJetStream()` checkpoints each native frame only after the framework adapter processes it and resumes strictly after that cursor.
- Closing and recreating the ordered consumer during an active answer recovers frames retained during the two-second gap without replacing already-rendered messages.
- `start: "all"` reconstructs earlier application-level conversation messages; the native reply consumers use `start: "new"` until a checkpoint exists.
- Filtered consumers expose global stream-sequence units, including jumps caused by other filtered subjects.
- The app exposes the package's `drop`, `deliver`, and `error` duplicate policies.
- Core NATS remains available as the explicit live-only comparison.
- NATSail remains the transport and runtime boundary. It does not introduce a third chat-message model.
- The interface uses the repository's private shadcn chat primitives as a realistic application surface.

## Current boundary

Ordered consumers are fixed to `AckPolicy.None` by NATS. Their server cursor can advance before application processing, so NATSail separately saves its application checkpoint only after the handler succeeds. Explicit acknowledgements, NAK/redelivery, and named durable ownership require a separate durable-consumer API; they are not presented as a fake option on `consumeJetStream()`.

This example proves gap recovery while an active framework stream and browser page remain alive. The AI SDK adapter still returns `null` from `reconnectToStream()`: restoring the same answer after a full page reload also requires persisted chat/run identity and reconstructed framework state. The example uses an in-memory checkpoint store; a production browser would use the IndexedDB checkpoint store or a server-owned checkpoint.

Core NATS resumes live delivery after reconnect, but it does not replay chunks published while the browser was disconnected. The UI disables the reconnect exercise in Core mode instead of implying otherwise.

The example pins `@tanstack/ai` 0.40 and `@tanstack/ai-client` 0.20 because `@shadcn/helpers` 0.2 declares that compatibility range. Upgrade the helper and TanStack packages together.

## Verify

```sh
pnpm example:ai-transport:verify
```
