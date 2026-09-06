import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './tests/rxjs-browser',
  fullyParallel: true,
  reporter: process.env.CI ? 'github' : 'list',
  use: { trace: 'retain-on-failure' },
  projects: [{ name: 'chromium', use: { browserName: 'chromium' } }],
})
