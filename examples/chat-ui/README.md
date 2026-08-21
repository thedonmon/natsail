# Shared chat example UI

> PRIVATE EXAMPLE PACKAGE — not published.

This package holds the room model, transport-visible timeline types, guided rooms workbench, and shadcn primitives shared by the examples. The AI example composes the primitives into a focused chat window instead of using the rooms workbench.

It intentionally contains no NATS runtime, React adapter, TanStack Query cache, or gateway client. The rooms examples supply those behaviors through `WorkspaceProps`; the AI example imports only the UI primitives it needs.

The workbench tells the user what to do, what result to wait for, and whether the browser observed it. The transport adapters and application state stay in the consuming examples.
