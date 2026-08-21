# Releasing NATSail packages

NATSail uses Changesets for versions and changelogs. The release workflow publishes through npm trusted publishing after one manual bootstrap release.

The workflow does not use an npm token. npm creates provenance automatically for trusted publication from this public repository.

## One-time bootstrap

1. Create the npm organization named `natsail` while signed in as `0xdon0`. This organization owns the `@natsail` scope.
2. Keep `0xdon0` as an organization owner for the first publication.
3. Refresh the npm login for the owner account.

   ```sh
   npm login --auth-type=web
   npm whoami
   npm org ls natsail 0xdon0
   ```

4. Mark the Changesets version pull request as ready, and merge it after its checks pass.
5. Use a clean checkout of the new `main` commit.
6. Install the exact workspace dependencies.

   ```sh
   pnpm install --frozen-lockfile
   ```

7. Run the complete release test.

   ```sh
   pnpm release:check
   ```

8. Publish the first package versions with an npm account that uses two-factor authentication.

   ```sh
   npm whoami
   pnpm release:publish
   git push --follow-tags
   ```

   The manual bootstrap publication does not include provenance. Trusted publications add provenance automatically.

9. Open the settings for each published package on npm.
10. Add a GitHub Actions trusted publisher with these exact values:
    - Organization or user: `thedonmon`
    - Repository: `natsail`
    - Workflow filename: `release.yml`
    - Environment name: leave blank
    - Allowed action: `npm publish`

11. Enable automated publication in the GitHub repository.

    ```sh
    gh variable set NPM_RELEASES_ENABLED --body true --repo thedonmon/natsail
    ```

12. Set each npm package to require two-factor authentication and disallow tokens.

Read the [npm trusted-publishing guide](https://docs.npmjs.com/trusted-publishers/) before you configure the package settings.

## Routine release

1. Run `pnpm changeset` in each pull request that changes published behavior.
2. Select all affected packages and the correct semantic-version change.
3. Describe the consumer-visible result and migration in the changeset.
4. Merge the pull request after CI passes.
5. Review the Changesets version pull request.
6. Merge the version pull request after its package and changelog changes are correct.

The release workflow builds and packs all six packages. It installs every tarball together before publication.

Then Changesets publishes each new version, creates package tags, and creates GitHub releases. npm attaches provenance to each package.

Read the [Changesets guide](https://github.com/changesets/changesets/blob/main/docs/intro-to-using-changesets.md) for version and publication semantics.

## Safety gates

`NPM_RELEASES_ENABLED` is false or absent during bootstrap. In this mode, the workflow creates only the version pull request.

The publish step requires all of these conditions:

- The repository variable is `true`.
- The workflow runs from `main` on a GitHub-hosted runner.
- The workflow has `id-token: write` permission.
- Each package trusts `release.yml` on npm.
- Each package repository URL is `git+https://github.com/thedonmon/natsail.git`.

If a publication stops after some packages succeed, do not change those versions. Correct the failure and run the workflow again.

Changesets skips versions that already exist in the registry. npm never permits reuse of a published name and version pair.
