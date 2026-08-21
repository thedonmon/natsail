# Private examples

Every package under `examples/` is private workspace code. None is published.

| Example                                | Run                         | What it proves                                                                                          |
| -------------------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------- |
| [React chat](react-chat/README.md)     | `pnpm example:react-chat`   | A guided TanStack rooms app using `NatsProvider`, reducer-backed Core subscriptions, and direct publish |
| [Gateway chat](gateway-chat/README.md) | `pnpm example:gateway-chat` | A guided two-tab proof for Durable Object retained catch-up and live fan-out                            |
| [AI chat](ai-transport/README.md)      | `pnpm example:ai-transport` | Native AI SDK and TanStack AI chat streams with JetStream cursor recovery during an active reply        |
| [Shared chat UI](chat-ui/README.md)    | Used by all three apps      | Private room model, guided proof UI, and shadcn chat primitives                                         |

The React example is the shortest path for application developers. The gateway example tests browsers that must not connect directly to NATS or need downstream retained catch-up. The AI example tests framework-native chat streams, processed-frame checkpoints, and JetStream gap recovery through an ordinary conversation without taking over either framework's state model.
