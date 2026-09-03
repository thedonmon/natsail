import { defineConfig, devices } from '@playwright/test'

const port = Number(process.env.NATSAIL_BROWSER_TEST_PORT ?? 4173)

if (!Number.isSafeInteger(port) || port <= 0 || port > 65_535) {
  throw new RangeError('NATSAIL_BROWSER_TEST_PORT must be a valid TCP port')
}

export default defineConfig({
  testDir: './tests/browser',
  testMatch: 'shared-worker.spec.ts',
  fullyParallel: false,
  reporter: process.env.CI ? 'github' : 'list',
  use: {
    baseURL: `http://127.0.0.1:${port}`,
    trace: 'retain-on-failure',
  },
  webServer: {
    command: 'pnpm exec vite --config tests/browser/vite.config.ts',
    url: `http://127.0.0.1:${port}`,
    reuseExistingServer: false,
    timeout: 30_000,
  },
  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        channel: process.env.CI ? undefined : 'chrome',
      },
    },
  ],
})
