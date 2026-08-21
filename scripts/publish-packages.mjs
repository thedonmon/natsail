import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { appendFile, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const registryUrl = 'https://registry.npmjs.org/'
const expectedRepository = 'thedonmon/natsail'
const repositoryRoot = fileURLToPath(new URL('..', import.meta.url))

export const releasePackages = [
  { directory: 'checkpoints', name: '@natsail/checkpoints' },
  { directory: 'core', name: '@natsail/core' },
  { directory: 'jetstream', name: '@natsail/jetstream' },
  { directory: 'session', name: '@natsail/session' },
  { directory: 'react', name: '@natsail/react' },
  { directory: 'rxjs', name: '@natsail/rxjs' },
]

export function normalizePublishedVersions(value) {
  if (Array.isArray(value)) {
    if (value.every((version) => typeof version === 'string')) return new Set(value)
    if (value.length === 1) return normalizePublishedVersions(value[0])
  }

  if (value && typeof value === 'object') {
    if (Array.isArray(value.versions)) return new Set(value.versions)
    if (value.versions && typeof value.versions === 'object') {
      return new Set(Object.keys(value.versions))
    }
  }

  throw new TypeError('The npm registry returned an unsupported package document.')
}

export function createPublishPlan(
  localPackages,
  publishedVersionsByName,
  existingTags,
  { recoverMissingTags = false } = {}
) {
  return localPackages.map((packageInfo) => {
    const tag = `${packageInfo.name}@${packageInfo.version}`
    const publishedVersions = publishedVersionsByName.get(packageInfo.name)
    assert(publishedVersions, `Missing registry result for ${packageInfo.name}`)

    if (!publishedVersions.has(packageInfo.version)) {
      return { ...packageInfo, action: 'publish', tag }
    }

    if (!existingTags.has(tag)) {
      return {
        ...packageInfo,
        action: recoverMissingTags ? 'recover-tag' : 'skip',
        tag,
        warning: 'The package exists in npm, but its GitHub tag is missing.',
      }
    }

    return { ...packageInfo, action: 'skip', tag }
  })
}

function run(command, args, { capture = false, cwd = repositoryRoot } = {}) {
  try {
    return execFileSync(command, args, {
      cwd,
      encoding: 'utf8',
      env: { ...process.env, FORCE_COLOR: '0' },
      stdio: capture ? ['ignore', 'pipe', 'pipe'] : 'inherit',
    })
  } catch (error) {
    if (capture && error && typeof error === 'object') {
      const output = [error.stdout, error.stderr].filter(Boolean).join('\n')
      if (output) process.stderr.write(output)
    }
    throw error
  }
}

async function requestJson(url, options = {}) {
  const { headers = {}, notFoundValue } = options
  const response = await fetch(url, {
    headers,
    signal: AbortSignal.timeout(15_000),
  })

  if (response.status === 404 && Object.hasOwn(options, 'notFoundValue')) {
    return notFoundValue
  }
  if (!response.ok) {
    throw new Error(`Request failed with ${response.status} ${response.statusText}: ${url}`)
  }
  return response.json()
}

async function readLocalPackages() {
  return Promise.all(
    releasePackages.map(async (packageInfo) => {
      const manifestPath = join(repositoryRoot, 'packages', packageInfo.directory, 'package.json')
      const manifest = JSON.parse(await readFile(manifestPath, 'utf8'))

      assert.equal(manifest.name, packageInfo.name)
      assert.equal(typeof manifest.version, 'string')
      assert(manifest.version.length > 0, `${manifest.name} must have a version`)

      return { ...packageInfo, version: manifest.version }
    })
  )
}

async function readPublishedVersions(packageName) {
  const document = await requestJson(`${registryUrl}${encodeURIComponent(packageName)}`, {
    notFoundValue: { versions: {} },
  })
  return normalizePublishedVersions(document)
}

async function githubTagExists(tag, repository, token) {
  const headers = {
    Accept: 'application/vnd.github+json',
    'User-Agent': 'natsail-release',
    'X-GitHub-Api-Version': '2022-11-28',
  }
  if (token) headers.Authorization = `Bearer ${token}`

  return requestJson(`https://api.github.com/repos/${repository}/git/ref/tags/${encodeURI(tag)}`, {
    headers,
    notFoundValue: undefined,
  }).then((value) => value !== undefined)
}

async function buildPublishPlan({ recoverMissingTags = false } = {}) {
  const localPackages = await readLocalPackages()
  const publishedVersions = await Promise.all(
    localPackages.map(({ name }) => readPublishedVersions(name))
  )
  const publishedVersionsByName = new Map(
    localPackages.map(({ name }, index) => [name, publishedVersions[index]])
  )
  const repository = process.env.GITHUB_REPOSITORY || expectedRepository
  const existingTagValues = await Promise.all(
    localPackages.map(async (packageInfo) => {
      const versions = publishedVersionsByName.get(packageInfo.name)
      if (!versions.has(packageInfo.version)) return undefined
      const tag = `${packageInfo.name}@${packageInfo.version}`
      return (await githubTagExists(tag, repository, process.env.GITHUB_TOKEN)) ? tag : undefined
    })
  )

  return createPublishPlan(
    localPackages,
    publishedVersionsByName,
    new Set(existingTagValues.filter(Boolean)),
    { recoverMissingTags }
  )
}

function printPlan(plan) {
  console.log('NATSail package release plan:')
  for (const item of plan) {
    const descriptions = {
      publish: 'publish with npm trusted publishing',
      'recover-tag': 'restore the missing GitHub tag',
      skip: 'already published',
    }
    console.log(`- ${item.tag}: ${descriptions[item.action]}`)
    if (item.warning && item.action === 'skip') console.warn(`  Warning: ${item.warning}`)
  }
}

export function validateTrustedPublishingEnvironment(environment = process.env) {
  const requirements = [
    ['GITHUB_ACTIONS', 'true'],
    ['GITHUB_REF', 'refs/heads/main'],
    ['GITHUB_REPOSITORY', expectedRepository],
  ]

  for (const [name, expected] of requirements) {
    if (environment[name] !== expected) {
      throw new Error(`Refusing to publish: ${name} must equal ${expected}.`)
    }
  }
  if (!environment.CHANGESETS_OUTPUT) {
    throw new Error('Refusing to publish without the Changesets action output file.')
  }
  if (!environment.ACTIONS_ID_TOKEN_REQUEST_URL || !environment.ACTIONS_ID_TOKEN_REQUEST_TOKEN) {
    throw new Error('Refusing to publish without GitHub Actions OIDC credentials.')
  }
  if (environment.NPM_TOKEN || environment.NODE_AUTH_TOKEN) {
    throw new Error('Refusing to publish with an npm token. NATSail requires trusted publishing.')
  }
}

export function createTagEvent(packageInfo) {
  return {
    type: 'git-tag',
    tag: packageInfo.tag,
    packageName: packageInfo.name,
  }
}

async function emitTagEvent(outputPath, packageInfo) {
  await appendFile(outputPath, `${JSON.stringify(createTagEvent(packageInfo))}\n`)
}

async function packPackage(packageInfo, temporaryRoot) {
  const destination = join(temporaryRoot, packageInfo.directory)
  await mkdir(destination, { recursive: true })
  run('pnpm', ['--filter', packageInfo.name, 'pack', '--pack-destination', destination])

  const archives = (await readdir(destination)).filter((entry) => entry.endsWith('.tgz'))
  assert.equal(archives.length, 1, `${packageInfo.name} must create one tarball`)
  const archivePath = join(destination, archives[0])
  const manifest = JSON.parse(
    run('tar', ['-xOf', archivePath, 'package/package.json'], { capture: true })
  )

  assert.equal(manifest.name, packageInfo.name)
  assert.equal(manifest.version, packageInfo.version)
  for (const dependencyGroup of ['dependencies', 'optionalDependencies', 'peerDependencies']) {
    for (const version of Object.values(manifest[dependencyGroup] ?? {})) {
      assert(!String(version).startsWith('workspace:'), `${packageInfo.name} leaked ${version}`)
    }
  }

  return archivePath
}

async function publishPlan(plan, outputPath) {
  await writeFile(outputPath, '', { flag: 'a' })
  const temporaryRoot = await mkdtemp(join(tmpdir(), 'natsail-release-'))

  try {
    for (const packageInfo of plan) {
      if (packageInfo.action === 'skip') continue
      if (packageInfo.action === 'recover-tag') {
        await emitTagEvent(outputPath, packageInfo)
        continue
      }

      const archivePath = await packPackage(packageInfo, temporaryRoot)
      run('npm', ['publish', archivePath, '--access', 'public', '--registry', registryUrl])
      await emitTagEvent(outputPath, packageInfo)
    }
  } finally {
    await rm(temporaryRoot, { recursive: true, force: true })
  }
}

export async function main(args = process.argv.slice(2)) {
  const dryRun = args.includes('--dry-run')
  if (!dryRun) validateTrustedPublishingEnvironment()

  const recoverMissingTags = Number(process.env.GITHUB_RUN_ATTEMPT ?? '1') > 1
  const plan = await buildPublishPlan({ recoverMissingTags })
  printPlan(plan)

  if (dryRun) return
  await publishPlan(plan, process.env.CHANGESETS_OUTPUT)
}

const entryPoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : undefined
if (entryPoint === import.meta.url) {
  main().catch((error) => {
    console.error(error)
    process.exitCode = 1
  })
}
