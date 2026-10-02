# Release NATSail packages

NATSail uses Changesets for versions and changelogs. The release workflow publishes through npm trusted publishing and does not use an npm token. npm creates provenance automatically for trusted publication from this public repository.

## Trusted publisher settings

Each package has a GitHub Actions trusted publisher with these exact values:

- Organization or user: `thedonmon`
- Repository: `natsail`
- Workflow filename: `release.yml`
- Environment name: leave blank
- Allowed action: `npm publish`

Each package also requires two-factor authentication and disallows tokens. The GitHub repository variable `NPM_RELEASES_ENABLED` must be `true`:

```sh
gh variable set NPM_RELEASES_ENABLED --body true --repo thedonmon/natsail
```

Read the [npm trusted-publishing guide](https://docs.npmjs.com/trusted-publishers/) before you change the package settings.

## Routine release

1. Run `pnpm changeset` in each pull request that changes published behavior.
2. Select all affected packages and the correct semantic-version change.
3. Describe the consumer-visible result and migration in the changeset.
4. Merge the pull request after CI passes.
5. Review the Changesets version pull request. The release workflow creates it as a draft.
6. Mark the draft version pull request ready for review. Until then CI does not run on it, because the bot-created pull request cannot start the workflow. The `ready_for_review` event starts a real CI run.
7. Merge the version pull request after CI passes and its package and changelog changes are correct. The Resilience workflow does not listen for `ready_for_review`, so it can stay red on version pull requests.

The release workflow builds and packs all nine packages. It installs every tarball together before publication.

The NATSail publisher compares each local version with npm. It packs missing versions with pnpm so no `workspace:` dependency reaches npm. It then publishes each tarball with the npm CLI and GitHub Actions OIDC. The Changesets action creates the package tags and GitHub releases. npm attaches provenance to each trusted publication.

An ordinary push with no new package version is a successful no-op. You can inspect the same plan locally without publishing anything:

```sh
pnpm release:plan
```

Read the [Changesets guide](https://github.com/changesets/changesets/blob/main/docs/intro-to-using-changesets.md) for version and publication semantics.

## Safety gates

When `NPM_RELEASES_ENABLED` is false or absent, the workflow creates only the version pull request.

The publish step requires all of these conditions:

- The repository variable is `true`.
- The workflow runs from `main` of `thedonmon/natsail` in GitHub Actions on a GitHub-hosted runner.
- The workflow has `id-token: write` permission, so OIDC credentials are available.
- Each package trusts `release.yml` on npm.
- Each package repository URL is `git+https://github.com/thedonmon/natsail.git`. `pnpm release:check` enforces this before publication.
- The publisher rejects `NPM_TOKEN` or `NODE_AUTH_TOKEN`, local runs, and runs without the Changesets action output file.

If a publication stops after some packages succeed, do not change those versions. Correct the failure and rerun the same workflow attempt. The publisher skips versions that reached npm and can restore a missing tag during the rerun.

The publisher skips versions that already exist in the registry. npm never permits reuse of a published name and version pair.

## Adding a package

A new package needs one manual bootstrap publication before npm exposes package settings. After that publication, configure the same `release.yml` trusted publisher used by the existing package set, require two-factor authentication, and disallow tokens. Later versions use the routine OIDC workflow.

Keep a new package at `0.0.0` in its implementation pull request and add a minor Changeset. After merging the implementation, manually publish that `0.0.0` bootstrap from `main`, configure its trusted publisher, and then merge the version pull request. The version pull request produces `0.1.0`, which the routine OIDC workflow publishes with provenance.

To publish the bootstrap, check out the merged `main` commit with a clean worktree, authenticate the npm CLI as an `@natsail` owner with two-factor authentication, and run:

```sh
pnpm install --frozen-lockfile
pnpm release:check

bootstrap_dir=$(mktemp -d "${TMPDIR:-/tmp}/natsail-bootstrap.XXXXXX")
pnpm --filter @natsail/<package> pack --pack-destination "$bootstrap_dir"
npm publish "$bootstrap_dir/natsail-<package>-0.0.0.tgz" --access public
```

Do not run `pnpm release:publish` locally. That command intentionally accepts only the trusted GitHub Actions environment on `main`. After the bootstrap publication, configure the package's trusted publisher and security settings before merging the Changesets version pull request.

Also add the package to the hand-maintained lists: `releasePackages` in `scripts/publish-packages.mjs`, `scripts/verify-package-tarballs.mjs`, the bundle budgets in `scripts/verify-consumer-bundles.mjs`, the aliases in `vitest.config.ts`, and the paths in `tsconfig.tests.json`.
