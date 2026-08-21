import { fileURLToPath } from 'node:url'

import { defineConfig } from 'vite'

const fromRoot = (path: string) => fileURLToPath(new URL(`../../${path}`, import.meta.url))

export default defineConfig({
  root: fileURLToPath(new URL('.', import.meta.url)),
  resolve: {
    alias: {
      '@natsail/checkpoints': fromRoot('packages/checkpoints/src/index.ts'),
      '@natsail/core': fromRoot('packages/core/src/index.ts'),
      '@natsail/jetstream': fromRoot('packages/jetstream/src/index.ts'),
    },
  },
  server: {
    host: '127.0.0.1',
    port: 4173,
    strictPort: true,
  },
})
