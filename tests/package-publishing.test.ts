import { readFile } from 'node:fs/promises'

import { describe, expect, it } from 'vitest'

const packageDirectories = [
  'checkpoints',
  'core',
  'jetstream',
  'opentelemetry',
  'session',
  'effect',
  'react',
  'rxjs',
]

describe('package publishing', () => {
  it.each(packageDirectories)('gives %s complete public npm metadata', async (directory) => {
    const packageRoot = new URL(`../packages/${directory}/`, import.meta.url)
    const manifest = JSON.parse(await readFile(new URL('package.json', packageRoot), 'utf8')) as {
      bugs?: { url?: string }
      description?: string
      files?: string[]
      homepage?: string
      license?: string
      main?: string
      module?: string
      name?: string
      private?: boolean
      publishConfig?: {
        access?: string
        provenance?: boolean
        registry?: string
        tag?: string
      }
      repository?: { directory?: string; type?: string; url?: string }
      scripts?: { prepack?: string }
      types?: string
    }

    expect(manifest.private).not.toBe(true)
    expect(manifest.description).toBeTruthy()
    expect(manifest.license).toBe('Apache-2.0')
    expect(manifest.files).toEqual(['dist'])
    expect(manifest.main).toBe('./dist/index.js')
    expect(manifest.module).toBe('./dist/index.js')
    expect(manifest.types).toBe('./dist/index.d.ts')
    expect(manifest.repository).toEqual({
      type: 'git',
      url: 'git+https://github.com/thedonmon/natsail.git',
      directory: `packages/${directory}`,
    })
    expect(manifest.homepage).toContain('https://github.com/thedonmon/natsail/')
    expect(manifest.bugs?.url).toBe('https://github.com/thedonmon/natsail/issues')
    expect(manifest.publishConfig).toEqual({
      access: 'public',
      registry: 'https://registry.npmjs.org/',
      ...(directory === 'effect' ? { tag: 'next' } : {}),
    })
    expect(manifest.scripts?.prepack).toBe('pnpm build')
    await expect(readFile(new URL('README.md', packageRoot), 'utf8')).resolves.toContain(
      `# ${manifest.name}`
    )
  })

  it('keeps examples private', async () => {
    for (const directory of [
      'ai-transport',
      'chat-ui',
      'effect-chat',
      'gateway-chat',
      'react-chat',
      'rxjs-chat',
    ]) {
      const manifest = JSON.parse(
        await readFile(new URL(`../examples/${directory}/package.json`, import.meta.url), 'utf8')
      ) as { private?: boolean }
      expect(manifest.private).toBe(true)
    }
  })

  it('uses gated trusted-publishing automation', async () => {
    const ciWorkflow = await readFile(
      new URL('../.github/workflows/ci.yml', import.meta.url),
      'utf8'
    )
    const workflow = await readFile(
      new URL('../.github/workflows/release.yml', import.meta.url),
      'utf8'
    )
    const changesets = JSON.parse(
      await readFile(new URL('../.changeset/config.json', import.meta.url), 'utf8')
    ) as { access?: string; baseBranch?: string }

    expect(changesets).toMatchObject({ access: 'public', baseBranch: 'main' })
    expect(ciWorkflow).toContain('ready_for_review')
    expect(workflow).toContain('id-token: write')
    expect(workflow).toContain("vars.NPM_RELEASES_ENABLED == 'true'")
    expect(workflow).toContain('pr-draft: create')
    expect(workflow).toContain('publish-script: pnpm release:publish')
    expect(workflow).not.toContain('NPM_TOKEN')

    const workspace = JSON.parse(
      await readFile(new URL('../package.json', import.meta.url), 'utf8')
    ) as { scripts?: Record<string, string> }
    expect(workspace.scripts?.['release:publish']).toBe('node scripts/publish-packages.mjs')
  })
})
