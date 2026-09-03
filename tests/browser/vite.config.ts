import { fileURLToPath } from 'node:url'

import { defineConfig } from 'vite'

const fromRoot = (path: string) => fileURLToPath(new URL(`../../${path}`, import.meta.url))
const browserTestPort = Number(process.env.NATSAIL_BROWSER_TEST_PORT ?? 4173)

if (!Number.isSafeInteger(browserTestPort) || browserTestPort <= 0 || browserTestPort > 65_535) {
  throw new RangeError('NATSAIL_BROWSER_TEST_PORT must be a valid TCP port')
}

export default defineConfig({
  root: fileURLToPath(new URL('.', import.meta.url)),
  build: {
    rollupOptions: {
      input: {
        browser: fileURLToPath(new URL('./index.html', import.meta.url)),
        sharedWorker: fileURLToPath(new URL('./shared-worker.html', import.meta.url)),
      },
    },
  },
  define: {
    __NATS_WS_URL__: JSON.stringify(process.env.NATS_WS_URL ?? 'ws://127.0.0.1:9223'),
  },
  resolve: {
    alias: {
      '@natsail/browser-broker': fromRoot('packages/browser-broker/src/index.ts'),
      '@natsail/checkpoints': fromRoot('packages/checkpoints/src/index.ts'),
      '@natsail/core': fromRoot('packages/core/src/index.ts'),
      '@natsail/jetstream': fromRoot('packages/jetstream/src/index.ts'),
      '@natsail/session': fromRoot('packages/session/src/index.ts'),
    },
  },
  server: {
    host: '127.0.0.1',
    port: browserTestPort,
    strictPort: true,
  },
})
