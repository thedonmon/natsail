# Release NATSail packages

NATSail uses Changesets for versions and changelogs. The release workflow publishes through npm trusted publishing. Version `0.1.0` was the one manual bootstrap release.

The workflow does not use an npm token. npm creates provenance automatically for trusted publication from this public repository.

NATSail used the manual bootstrap path for version `0.1.0`. Trusted publishing is active for later releases.

## Completed bootstrap

The following setup is complete. Keep this section as a record of the package ownership and trusted-publisher settings.

1. Create the npm organization named `natsail` while signed in as `0xdon0`. This organization owns the `@natsail` scope.
2. Keep `0xdon0` as an organization owner for the first publication.
3. Version `0.1.0` was published manually. It does not include provenance.
4. Each package has a GitHub Actions trusted publisher with these exact values:
   - Organization or user: `thedonmon`
   - Repository: `natsail`
   - Workflow filename: `release.yml`
   - Environment name: leave blank
   - Allowed action: `npm publish`

5. The GitHub repository variable `NPM_RELEASES_ENABLED` is `true`.

   ```sh
   gh variable set NPM_RELEASES_ENABLED --body true --repo thedonmon/natsail
   ```

6. Each npm package requires two-factor authentication and disallows tokens.

Read the [npm trusted-publishing guide](https://docs.npmjs.com/trusted-publishers/) before you configure the package settings.

## Routine release

1. Run `pnpm changeset` in each pull request that changes published behavior.
2. Select all affected packages and the correct semantic-version change.
3. Describe the consumer-visible result and migration in the changeset.
4. Merge the pull request after CI passes.
5. Review the Changesets version pull request.
6. Merge the version pull request after its package and changelog changes are correct.

The release workflow builds and packs all six packages. It installs every tarball together before publication.

The NATSail publisher compares each local version with npm. It packs missing versions with pnpm so no `workspace:` dependency reaches npm. It then publishes each tarball with the npm CLI and GitHub Actions OIDC. The Changesets action creates the package tags and GitHub releases. npm attaches provenance to each trusted publication.

An ordinary push with no new package version is a successful no-op. You can inspect the same plan locally without publishing anything:

```sh
pnpm release:plan
```

Read the [Changesets guide](https://github.com/changesets/changesets/blob/main/docs/intro-to-using-changesets.md) for version and publication semantics.

## Safety gates

`NPM_RELEASES_ENABLED` is false or absent during bootstrap. In this mode, the workflow creates only the version pull request.

The publish step requires all of these conditions:

- The repository variable is `true`.
- The workflow runs from `main` on a GitHub-hosted runner.
- The workflow has `id-token: write` permission.
- Each package trusts `release.yml` on npm.
- Each package repository URL is `git+https://github.com/thedonmon/natsail.git`.
- The publisher rejects npm tokens and local publication attempts.

If a publication stops after some packages succeed, do not change those versions. Correct the failure and rerun the same workflow attempt. The publisher skips versions that reached npm and can restore a missing tag during the rerun.

The publisher skips versions that already exist in the registry. npm never permits reuse of a published name and version pair.
