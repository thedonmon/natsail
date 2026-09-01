import { readFile } from 'node:fs/promises'

import { describe, expect, it } from 'vitest'

describe('package boundaries', () => {
  it('keeps optional libraries out of the Core package', async () => {
    const packageText = await readFile(
      new URL('../packages/core/package.json', import.meta.url),
      'utf8'
    )
    const source = await readFile(new URL('../packages/core/src/index.ts', import.meta.url), 'utf8')
    const manifest = JSON.parse(packageText) as {
      dependencies: Record<string, string>
      sideEffects: boolean
    }

    expect(Object.keys(manifest.dependencies)).toEqual(['@nats-io/nats-core'])
    expect(manifest.sideEffects).toBe(false)
    expect(source).not.toMatch(/@nats-io\/jetstream|from ['"]effect['"]|react|rxjs/i)
  })

  it('keeps Effect, React, and RxJS in separate adapter packages', async () => {
    const effectManifest = JSON.parse(
      await readFile(new URL('../packages/effect/package.json', import.meta.url), 'utf8')
    ) as {
      dependencies: Record<string, string>
      peerDependencies: Record<string, string>
    }
    const reactManifest = JSON.parse(
      await readFile(new URL('../packages/react/package.json', import.meta.url), 'utf8')
    ) as {
      dependencies: Record<string, string>
      peerDependencies: Record<string, string>
    }
    const rxjsManifest = JSON.parse(
      await readFile(new URL('../packages/rxjs/package.json', import.meta.url), 'utf8')
    ) as {
      dependencies: Record<string, string>
      peerDependencies: Record<string, string>
    }

    expect(Object.keys(effectManifest.dependencies)).toEqual([
      '@natsail/core',
      '@natsail/jetstream',
      '@natsail/session',
    ])
    expect(Object.keys(effectManifest.peerDependencies)).toEqual(['effect'])
    expect(Object.keys(reactManifest.dependencies)).toEqual([
      '@natsail/core',
      '@natsail/jetstream',
      '@natsail/session',
    ])
    expect(Object.keys(reactManifest.peerDependencies)).toEqual(['react'])
    expect(Object.keys(rxjsManifest.dependencies)).toEqual([
      '@natsail/core',
      '@natsail/jetstream',
      '@natsail/session',
    ])
    expect(Object.keys(rxjsManifest.peerDependencies)).toEqual(['rxjs'])
  })

  it('keeps checkpoint storage independent from NATS and frameworks', async () => {
    const packageText = await readFile(
      new URL('../packages/checkpoints/package.json', import.meta.url),
      'utf8'
    )
    const source = await readFile(
      new URL('../packages/checkpoints/src/index.ts', import.meta.url),
      'utf8'
    )
    const manifest = JSON.parse(packageText) as {
      dependencies?: Record<string, string>
      sideEffects: boolean
    }

    expect(manifest.dependencies).toBeUndefined()
    expect(manifest.sideEffects).toBe(false)
    expect(source).not.toMatch(/@nats-io|from ['"]effect['"]|react|rxjs/i)
  })
})
