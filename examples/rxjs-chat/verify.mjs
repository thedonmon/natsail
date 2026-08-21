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
  await expect(page.getByText(/Two RxJS projections share one keyed NATSail session/)).toBeVisible()

  const body = `RxJS round trip ${crypto.randomUUID().slice(0, 8)}`
  await page.locator('#probe-body').fill(body)
  await page.getByRole('button', { name: 'Send message' }).click()
  await expect(page.getByText(body)).toBeVisible()
  await expect(page.locator('[data-deliveries]')).toHaveAttribute('data-deliveries', '6')

  await page.getByRole('button', { name: 'Pause stream and publish 3' }).click()
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
  await expect(page.getByText(/Published while the RxJS consumer was paused/)).toHaveCount(3)
  await expect(page.locator('[data-deliveries]')).toHaveAttribute('data-deliveries', '9')
  await expect(page.locator('[data-source-starts]')).toHaveAttribute('data-source-starts', '1')
  await page.screenshot({ path: screenshot })

  process.stdout.write(
    `\n${JSON.stringify(
      {
        verdict: 'passed',
        proven: [
          'scan preserves the complete multi-room JetStream timeline',
          'two RxJS projections share one NATSail session source',
          'a publish returns through the shared Observable with its global stream cursor',
          'three messages published while the ordered consumer is closed replay after its checkpoint',
          'the React view observes RxJS state without opening another NATS subscription',
        ],
        currentBoundary:
          'JetStream composition uses the generic SessionSource seam; a direct RxJS JetStream helper is not yet published',
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
