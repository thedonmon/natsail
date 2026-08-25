import { fileURLToPath } from 'node:url'

import { defineConfig } from 'vitest/config'

const fromRoot = (path: string) => fileURLToPath(new URL(path, import.meta.url))

export default defineConfig({
  resolve: {
    alias: {
      '@natsail/checkpoints': fromRoot('./packages/checkpoints/src/index.ts'),
      '@natsail/core': fromRoot('./packages/core/src/index.ts'),
      '@natsail/effect': fromRoot('./packages/effect/src/index.ts'),
      '@natsail/jetstream': fromRoot('./packages/jetstream/src/index.ts'),
      '@natsail/react': fromRoot('./packages/react/src/index.ts'),
      '@natsail/rxjs': fromRoot('./packages/rxjs/src/index.ts'),
      '@natsail/session': fromRoot('./packages/session/src/index.ts'),
    },
  },
  test: {
    include: ['tests/**/*.test.ts'],
    testTimeout: 10_000,
  },
})
