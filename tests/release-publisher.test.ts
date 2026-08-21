import { describe, expect, it } from 'vitest'

import {
  createPublishPlan,
  createTagEvent,
  normalizePublishedVersions,
  validateTrustedPublishingEnvironment,
} from '../scripts/publish-packages.mjs'

const localPackage = {
  directory: 'core',
  name: '@natsail/core',
  version: '0.1.0',
}

describe('release publisher', () => {
  it('reads versions from the npm registry document', () => {
    expect(normalizePublishedVersions({ versions: { '0.1.0': {}, '0.2.0': {} } })).toEqual(
      new Set(['0.1.0', '0.2.0'])
    )
  })

  it('accepts the one-item array returned by newer npm info commands', () => {
    expect(normalizePublishedVersions([{ versions: ['0.1.0', '0.2.0'] }])).toEqual(
      new Set(['0.1.0', '0.2.0'])
    )
  })

  it('skips a package version that already has its release tag', () => {
    expect(
      createPublishPlan(
        [localPackage],
        new Map([[localPackage.name, new Set([localPackage.version])]]),
        new Set([`${localPackage.name}@${localPackage.version}`])
      )
    ).toEqual([{ ...localPackage, action: 'skip', tag: '@natsail/core@0.1.0' }])
  })

  it('publishes a version that is missing from npm', () => {
    expect(
      createPublishPlan([localPackage], new Map([[localPackage.name, new Set()]]), new Set())
    ).toEqual([{ ...localPackage, action: 'publish', tag: '@natsail/core@0.1.0' }])
  })

  it('recovers a missing tag only during a workflow rerun', () => {
    const versions = new Map([[localPackage.name, new Set([localPackage.version])]])
    const firstAttempt = createPublishPlan([localPackage], versions, new Set())
    const rerun = createPublishPlan([localPackage], versions, new Set(), {
      recoverMissingTags: true,
    })

    expect(firstAttempt[0]).toMatchObject({ action: 'skip', warning: expect.any(String) })
    expect(rerun[0]).toMatchObject({ action: 'recover-tag', warning: expect.any(String) })
  })

  it('emits the Changesets action tag contract', () => {
    expect(createTagEvent({ ...localPackage, tag: '@natsail/core@0.1.0' })).toEqual({
      type: 'git-tag',
      tag: '@natsail/core@0.1.0',
      packageName: '@natsail/core',
    })
  })

  it('requires main-branch OIDC and rejects npm tokens', () => {
    const trustedEnvironment = {
      GITHUB_ACTIONS: 'true',
      GITHUB_REF: 'refs/heads/main',
      GITHUB_REPOSITORY: 'thedonmon/natsail',
      CHANGESETS_OUTPUT: '/tmp/changesets-output.ndjson',
      ACTIONS_ID_TOKEN_REQUEST_URL: 'https://example.test/oidc',
      ACTIONS_ID_TOKEN_REQUEST_TOKEN: 'redacted',
    }

    expect(() => validateTrustedPublishingEnvironment(trustedEnvironment)).not.toThrow()
    expect(() =>
      validateTrustedPublishingEnvironment({ ...trustedEnvironment, NPM_TOKEN: 'redacted' })
    ).toThrow(/requires trusted publishing/)
    expect(() =>
      validateTrustedPublishingEnvironment({
        ...trustedEnvironment,
        GITHUB_REF: 'refs/heads/feature',
      })
    ).toThrow(/GITHUB_REF/)
  })
})
