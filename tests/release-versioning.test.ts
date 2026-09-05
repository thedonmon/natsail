import { spawnSync } from 'node:child_process'
import { copyFile, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { expect, it } from 'vitest'

const repositoryRoot = fileURLToPath(new URL('..', import.meta.url))

it('generates versions and formats changelogs with the installed release tools', async () => {
  const fixture = await mkdtemp(join(tmpdir(), 'natsail-version-check-'))
  try {
    await mkdir(join(fixture, '.changeset'))
    await mkdir(join(fixture, 'packages/core'), { recursive: true })
    for (const path of [
      'package.json',
      'pnpm-workspace.yaml',
      '.oxfmtrc.json',
      '.changeset/config.json',
    ]) {
      await copyFile(join(repositoryRoot, path), join(fixture, path))
    }
    await symlink(join(repositoryRoot, 'node_modules'), join(fixture, 'node_modules'), 'junction')
    await writeFile(
      join(fixture, 'packages/core/package.json'),
      JSON.stringify({ name: '@natsail/core', version: '1.2.3' })
    )
    await writeFile(
      join(fixture, 'packages/core/CHANGELOG.md'),
      '# @natsail/core\n\n## 1.2.3\n\n-   Existing change\n'
    )
    await writeFile(
      join(fixture, '.changeset/release-smoke.md'),
      '---\n"@natsail/core": patch\n---\n\nVersion-generation smoke test.\n'
    )

    const versioning = spawnSync('pnpm', ['exec', 'changeset', 'version'], {
      cwd: fixture,
      encoding: 'utf8',
      timeout: 20_000,
      stdio: 'pipe',
    })
    expect(versioning.error).toBeUndefined()
    expect(versioning.status, `${versioning.stdout}\n${versioning.stderr}`).toBe(0)

    const manifest = JSON.parse(await readFile(join(fixture, 'packages/core/package.json'), 'utf8'))
    const changelog = await readFile(join(fixture, 'packages/core/CHANGELOG.md'), 'utf8')
    expect(manifest.version).toBe('1.2.4')
    expect(changelog).toContain('## 1.2.4')
    expect(changelog).toContain('Version-generation smoke test.')
    expect(changelog).toContain('\n- Existing change\n')
  } finally {
    await rm(fixture, { recursive: true, force: true })
  }
}, 30_000)
