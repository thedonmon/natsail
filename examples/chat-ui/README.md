# Shared chat example UI

> REPOSITORY EXAMPLE PACKAGE — included in the public repository. It is not published to npm.

This package holds the room model, transport-visible timeline types, guided rooms workbench, realistic performance-chat surface, and shadcn primitives shared by the examples. The Effect and RxJS labs render the same multi-conversation product UI so their streaming behavior can be compared without changing the React workload.

It intentionally contains no NATS runtime, React adapter, TanStack Query cache, or gateway client. The rooms examples supply those behaviors through `WorkspaceProps`; the AI example imports only the UI primitives it needs.

The workbench tells the user what to do, what result to wait for, and whether the browser observed it. The performance-chat surface adds conversation switching, atomic loading, assistant messages, unread state, tab presence, update notices, and matching replay/render counters. Transport adapters and application state stay in the consuming examples.
