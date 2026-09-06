import { fileURLToPath } from 'node:url'
import { gzipSync } from 'node:zlib'

import { expect, test } from '@playwright/test'
import { build } from 'vite'

import type * as acceptance from './fixture'

declare global {
  interface Window {
    NatsailRxjsAcceptance: typeof acceptance
    originalObservable: typeof Observable | undefined
  }
}

let bundle: string

test.beforeAll(async () => {
  const result = await build({
    configFile: false,
    logLevel: 'silent',
    build: {
      write: false,
      minify: 'esbuild',
      lib: {
        entry: fileURLToPath(new URL('./fixture.ts', import.meta.url)),
        name: 'NatsailRxjsAcceptance',
        formats: ['iife'],
      },
      rollupOptions: { output: { inlineDynamicImports: true } },
    },
  })
  const outputs = (Array.isArray(result) ? result : [result]).flatMap((item) => {
    if (!('output' in item)) throw new Error('Expected a completed browser fixture build')
    return item.output
  })
  const chunks = outputs.filter((item) => item.type === 'chunk')
  expect(chunks).toHaveLength(1)
  expect(chunks[0]!.imports).toEqual([])
  bundle = chunks[0]!.code
  console.log(
    `RxJS browser fixture, including RxJS/polyfill: ${gzipSync(bundle).byteLength} gzip bytes`
  )
})

for (const realm of ['native', 'polyfill'] as const) {
  test(`${realm}: built adapter preserves lifecycle, cancellation, and timer batching`, async ({
    page,
  }) => {
    const pageErrors: string[] = []
    page.on('pageerror', (error) => pageErrors.push(error.message))
    await page.evaluate((fallback) => {
      window.originalObservable = globalThis.Observable
      if (fallback) {
        delete (globalThis as { Observable?: unknown }).Observable
        delete (globalThis as { Subscriber?: unknown }).Subscriber
      }
    }, realm === 'polyfill')
    const before = await page.evaluate(() => typeof globalThis.Observable)
    expect(before).toBe(realm === 'native' ? 'function' : 'undefined')
    await page.addScriptTag({ content: bundle })
    expect(await page.evaluate(() => typeof globalThis.Observable)).toBe('function')
    expect(await page.evaluate(() => globalThis.Observable === window.originalObservable)).toBe(
      realm === 'native'
    )

    expect(await page.evaluate(() => window.NatsailRxjsAcceptance.lifecycle())).toEqual({
      isPlatformObservable: true,
      subscribeReturnsUndefined: true,
      joined: { first: [10], second: [10], references: 2, starts: 1 },
      remaining: { first: [10], second: [10, 20], references: 1, closes: 0 },
      released: { activeSessions: 0, closes: 1 },
      beforeRestartDelivery: [],
      restarted: [30],
      starts: 3,
      errors: ['Error: source failed'],
      activeSessions: 0,
    })
    expect(await page.evaluate(() => window.NatsailRxjsAcceptance.batching())).toEqual({
      immediate: [1],
      afterFlush: [1, 3],
      afterAbort: [1, 3],
      activeSessions: 0,
    })
    expect(pageErrors).toEqual([])
  })
}
