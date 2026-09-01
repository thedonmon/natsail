# Repository examples

Every package under `examples/` is repository-only workspace code. The public repository contains this code, but npm does not publish it.

| Example                                | Run                         | What it proves                                                                                          |
| -------------------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------- |
| [React chat](react-chat/README.md)     | `pnpm example:react-chat`   | A guided TanStack rooms app using `NatsProvider`, reducer-backed Core subscriptions, and direct publish |
| [Gateway chat](gateway-chat/README.md) | `pnpm example:gateway-chat` | A guided two-tab proof for Durable Object retained catch-up and live fan-out                            |
| [AI chat](ai-transport/README.md)      | `pnpm example:ai-transport` | Native AI SDK and TanStack AI streams with reconnect and full-page JetStream recovery                   |
| [RxJS chat](rxjs-chat/README.md)       | `pnpm example:rxjs-chat`    | Realistic conversation replay, live batching, and multi-tab notices through RxJS                        |
| [Effect chat](effect-chat/README.md)   | `pnpm example:effect-chat`  | The identical chat workload through Effect materialization, backpressure, and scoped cancellation       |
| [Shared chat UI](chat-ui/README.md)    | Used by all five apps       | Shared product and workbench surfaces plus shadcn chat primitives                                       |

The React example is the shortest path for application developers. The gateway example tests browsers that cannot connect directly to NATS.

The gateway example also tests downstream retained catch-up. The AI example tests native chat streams, checkpoints, reconnects, and page reloads.

The RxJS and Effect examples deliberately use the same four conversations, seeded message counts, interaction design, assistant responder, multi-tab flow, and performance counters. Switch among 240, 96, 48, and 8-message histories, then use the 40-message busy-room action to compare presentation batching without changing the product workload.
