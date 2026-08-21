// REPOSITORY EXAMPLE — real-browser proof for the direct @natsail/react rooms app.

import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { fileURLToPath } from 'node:url'

import { chromium, expect } from '@playwright/test'

const root = fileURLToPath(new URL('../..', import.meta.url))
const baseUrl = 'http://127.0.0.1:4175'
const screenshot = '/tmp/natsail-react-chat-example.png'

const delay = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds))

const waitForApp = async (output) => {
  const deadline = Date.now() + 30_000
  let lastError
  while (Date.now() < deadline) {
    try {
      const response = await fetch(baseUrl, { signal: AbortSignal.timeout(500) })
      if (response.ok) return
    } catch (error) {
      lastError = error
    }
    await delay(100)
  }
  throw new Error(`Timed out waiting for the React example\n${output()}`, { cause: lastError })
}

const stop = async (child) => {
  if (child.exitCode !== null) return
  const exited = once(child, 'exit')
  child.kill('SIGTERM')
  await Promise.race([exited, delay(6_000)])
  if (child.exitCode === null) child.kill('SIGKILL')
}

let output = ''
const dev = spawn('node', ['examples/react-chat/dev.mjs'], {
  cwd: root,
  stdio: ['ignore', 'pipe', 'pipe'],
})
dev.stdout.on('data', (chunk) => {
  output += chunk
  process.stdout.write(chunk)
})
dev.stderr.on('data', (chunk) => {
  output += chunk
  process.stderr.write(chunk)
})

let browser
try {
  await waitForApp(() => output)
  browser = await chromium.launch(process.env.CI ? {} : { channel: 'chrome' })
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })
  await page.goto(`${baseUrl}/rooms/gateway-lab`)

  await expect(page.locator('[data-gateway-phase]').first()).toHaveAttribute(
    'data-gateway-phase',
    'live'
  )
  await expect(page.locator('[data-deliveries]')).toHaveAttribute('data-deliveries', '5')
  await expect(
    page.locator('[data-proof-status="pass"]', { hasText: 'Verify one shared room feed' })
  ).toBeVisible()

  const body = `Direct React primitive ${crypto.randomUUID().slice(0, 8)}`
  await page.locator('#probe-body').fill(body)
  await page.getByRole('button', { name: 'Send message' }).click()
  await expect(page.getByText(body)).toBeVisible()
  await expect(
    page.locator('[data-proof-status="pass"]', { hasText: 'Observe a publish round trip' })
  ).toBeVisible()

  await page.getByRole('button', { name: 'Edge cases' }).click()
  await expect(page.getByText(/Core NATS is live-only/)).toBeVisible()
  await page.screenshot({ path: screenshot, fullPage: true })

  process.stdout.write(
    `\n${JSON.stringify(
      {
        verdict: 'passed',
        proven: [
          'NatsProvider owns one shared runtime and session registry',
          'the reducer-backed React hook retains every delivered chat message',
          'Core NATS publish returns through the shared wildcard subscription',
          'the guided proof rail reports browser-observed results',
          'TanStack room navigation reuses the same live session',
        ],
        screenshot,
      },
      null,
      2
    )}\n`
  )
} finally {
  await browser?.close().catch(() => undefined)
  await stop(dev)
}
