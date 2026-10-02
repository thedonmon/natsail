---
'@natsail/core': minor
'@natsail/react': minor
---

Remove two public exports that had no consumers.

`@natsail/react` no longer exports `useNatsJetStreamSubscriptionSelector`. Migrate by calling `useNatsSessionSelector(key, createJetStreamSessionSource(runtime, options), selector, isEqual)` with `runtime` from `useNatsRuntime()`, or use `useNatsJetStreamReducerSelector` when the state is a fold of deliveries.

`@natsail/core` no longer exports `defineNatsailWorkBudget`. Work budgets are still validated when you pass `workBudget` options to the APIs that accept them; there is no need to pre-validate a budget.
