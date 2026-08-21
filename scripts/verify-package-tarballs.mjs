import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdtemp, mkdir, readdir, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const repositoryRoot = fileURLToPath(new URL('..', import.meta.url))
const packages = [
  { directory: 'checkpoints', name: '@natsail/checkpoints' },
  { directory: 'core', name: '@natsail/core' },
  { directory: 'jetstream', name: '@natsail/jetstream' },
  { directory: 'session', name: '@natsail/session' },
  { directory: 'react', name: '@natsail/react' },
  { directory: 'rxjs', name: '@natsail/rxjs' },
]

function run(command, args, cwd = repositoryRoot) {
  try {
    return execFileSync(command, args, {
      cwd,
      encoding: 'utf8',
      env: { ...process.env, FORCE_COLOR: '0' },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
  } catch (error) {
    if (error && typeof error === 'object') {
      const output = [error.stdout, error.stderr].filter(Boolean).join('\n')
      if (output) process.stderr.write(output)
    }
    throw error
  }
}

const temporaryRoot = await mkdtemp(join(tmpdir(), 'natsail-package-check-'))

try {
  const tarballs = new Map()

  for (const packageInfo of packages) {
    const destination = join(temporaryRoot, packageInfo.directory)
    await mkdir(destination, { recursive: true })

    run('pnpm', ['--filter', packageInfo.name, 'pack', '--pack-destination', destination])

    const archiveNames = (await readdir(destination)).filter((name) => name.endsWith('.tgz'))
    assert.equal(archiveNames.length, 1, `${packageInfo.name} must create one tarball`)

    const archivePath = join(destination, archiveNames[0])
    const entries = run('tar', ['-tzf', archivePath]).trim().split('\n')
    const manifest = JSON.parse(run('tar', ['-xOf', archivePath, 'package/package.json']))

    for (const required of [
      'package/README.md',
      'package/dist/index.d.ts',
      'package/dist/index.js',
      'package/package.json',
    ]) {
      assert(entries.includes(required), `${packageInfo.name} tarball is missing ${required}`)
    }

    assert(!entries.some((entry) => entry.startsWith('package/src/')))
    assert.equal(manifest.name, packageInfo.name)
    assert.notEqual(manifest.private, true)
    assert.equal(manifest.publishConfig?.access, 'public')
    assert.equal(manifest.publishConfig?.provenance, true)

    for (const dependencyGroup of ['dependencies', 'optionalDependencies', 'peerDependencies']) {
      for (const version of Object.values(manifest[dependencyGroup] ?? {})) {
        assert(!String(version).startsWith('workspace:'), `${packageInfo.name} leaked ${version}`)
      }
    }

    tarballs.set(packageInfo.name, archivePath)
    const archiveSize = (await stat(archivePath)).size
    console.log(`Verified ${packageInfo.name} (${archiveSize} bytes).`)
  }

  const consumerRoot = join(temporaryRoot, 'consumer')
  await mkdir(consumerRoot)

  const localPackages = Object.fromEntries(
    [...tarballs].map(([name, archivePath]) => [name, `file:${archivePath}`])
  )
  await writeFile(
    join(consumerRoot, 'package.json'),
    `${JSON.stringify(
      {
        name: 'natsail-package-smoke-test',
        private: true,
        type: 'module',
        dependencies: {
          ...localPackages,
          react: '^19.0.0',
          rxjs: '^7.8.0',
        },
        pnpm: { overrides: localPackages },
      },
      null,
      2
    )}\n`
  )
  await writeFile(
    join(consumerRoot, 'smoke.mjs'),
    `${packages
      .map(
        ({ directory, name }) =>
          `const ${directory.replace('-', '_')} = await import('${name}')\nif (Object.keys(${directory.replace('-', '_')}).length === 0) throw new Error('${name} has no exports')`
      )
      .join('\n')}\nconsole.log('Imported all NATSail package tarballs.')\n`
  )

  run('pnpm', ['install', '--ignore-scripts'], consumerRoot)
  process.stdout.write(run('node', ['smoke.mjs'], consumerRoot))
} finally {
  await rm(temporaryRoot, { recursive: true, force: true })
}
