import { expect, test } from '@playwright/test'

interface BrowserLoadResult {
  connectionRequests: number
  consumerCount: number
  receivedCount: number
  userAgent: string
}

test('runs 64 bounded consumers over one WebSocket in a real browser', async ({ page }) => {
  await page.goto('/')
  await expect(page.locator('html')).toHaveAttribute('data-natsail-ready', 'true')

  const result = await page.evaluate<BrowserLoadResult>(async () => {
    return window.runNatsailBrowserLoad()
  })

  expect(result.connectionRequests).toBe(1)
  expect(result.consumerCount).toBe(64)
  expect(result.receivedCount).toBe(64)
  expect(result.userAgent).toContain('Chrome')
})

declare global {
  interface Window {
    runNatsailBrowserLoad: () => Promise<BrowserLoadResult>
  }
}
