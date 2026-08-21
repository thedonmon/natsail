# Changesets

Add one changeset for each pull request that changes a public package interface or behavior.

```sh
pnpm changeset
```

Select each affected package and its semantic-version change. Describe the consumer-visible result and any required migration.

Documentation, tests, examples, and internal refactors do not require a changeset when published behavior stays the same.

The release workflow maintains the version pull request. See [the release guide](../docs/RELEASING.md) for publication steps.
