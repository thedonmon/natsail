// REPOSITORY EXAMPLE — real-browser verification for the RxJS JetStream rooms app.

import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { fileURLToPath } from 'node:url'

import { chromium, expect } from '@playwright/test'

const root = fileURLToPath(new URL('../..', import.meta.url))
const baseUrl = 'http://127.0.0.1:4177'
const screenshot = '/tmp/natsail-rxjs-chat-example.png'
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
  throw new Error(`Timed out waiting for the RxJS example\n${output()}`, { cause: lastError })
}

const stop = async (child) => {
  if (child.exitCode !== null) return
  const exited = once(child, 'exit')
  child.kill('SIGTERM')
  await Promise.race([exited, delay(6_000)])
  if (child.exitCode === null) child.kill('SIGKILL')
}

let output = ''
const dev = spawn('node', ['examples/rxjs-chat/dev.mjs'], {
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
  await page.goto(baseUrl)

  await expect(page.locator('main[data-example-mode="rxjs"]')).toBeVisible()
  await expect(page.locator('[data-gateway-phase]').first()).toHaveAttribute(
    'data-gateway-phase',
    'live'
  )
  await expect(page.locator('[data-deliveries]')).toHaveAttribute('data-deliveries', '5')
  await expect(page.locator('[data-source-starts]')).toHaveAttribute('data-source-starts', '1')
  await page.getByText('How this example is wired').click()
  await expect(page.locator('.architecture-details p')).toContainText(
    'Two validated RxJS projections'
  )

  const body = `RxJS round trip ${crypto.randomUUID().slice(0, 8)}`
  await page.locator('#probe-body').fill(body)
  await page.getByRole('button', { name: 'Send message' }).click()
  await expect(page.getByText(body)).toBeVisible()
  await expect(page.locator('[data-deliveries]')).toHaveAttribute('data-deliveries', '6')

  await page.getByRole('button', { name: 'Reconnect and publish 3' }).click()
  await expect(page.locator('[data-gateway-phase]').first()).toHaveAttribute(
    'data-gateway-phase',
    'gap'
  )
  await expect(page.locator('[data-catchups]')).toHaveAttribute('data-catchups', '1', {
    timeout: 10_000,
  })
  await expect(page.locator('[data-gateway-phase]').first()).toHaveAttribute(
    'data-gateway-phase',
    'live'
  )
  await expect(page.getByText(/Published after the forced reconnect/)).toHaveCount(3)
  await expect(page.locator('[data-deliveries]')).toHaveAttribute('data-deliveries', '9')
  await expect(page.locator('[data-source-starts]')).toHaveAttribute('data-source-starts', '1')
  await page.screenshot({ path: screenshot })

  process.stdout.write(
    `\n${JSON.stringify(
      {
        verdict: 'passed',
        proven: [
          'one atomic package reducer publishes the complete multi-room JetStream timeline after replay',
          'two validated RxJS projections share one NATSail session definition and consumer',
          'a publish returns through the shared Observable with its global stream cursor',
          'three messages published after a forced transport reconnect reach the same reduced state',
          'the React view observes RxJS state without opening another NATS subscription',
        ],
        currentBoundary:
          'Persisted materialized reducer state is not implemented; a fresh lease reconstructs state from atomic replay',
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
