// PROTOTYPE — browser proof for room fan-out and retained-log catch-up.

import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { fileURLToPath } from 'node:url'

import { chromium, expect } from '@playwright/test'

const root = fileURLToPath(new URL('../..', import.meta.url))
const baseUrl = 'http://127.0.0.1:4174'
const screenshot = '/tmp/natsail-gateway-chat-proof.png'

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
  throw new Error(`Timed out waiting for the prototype app\n${output()}`, { cause: lastError })
}

const stop = async (child) => {
  if (child.exitCode !== null) return
  const exited = once(child, 'exit')
  child.kill('SIGTERM')
  await Promise.race([exited, delay(6_000)])
  if (child.exitCode === null) child.kill('SIGKILL')
}

let output = ''
const dev = spawn('node', ['examples/gateway-chat/dev.mjs'], {
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
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } })
  const firstTab = await context.newPage()

  await firstTab.goto(`${baseUrl}/rooms/gateway-lab`)
  await expect(firstTab.locator('[data-gateway-phase]').first()).toHaveAttribute(
    'data-gateway-phase',
    'live'
  )
  await expect(firstTab.locator('[data-deliveries]')).toHaveAttribute('data-deliveries', '5')

  const secondTab = await context.newPage()
  await secondTab.goto(`${baseUrl}/rooms/gateway-lab`)
  await expect(secondTab.locator('[data-gateway-phase]').first()).toHaveAttribute(
    'data-gateway-phase',
    'live'
  )

  await firstTab.getByRole('button', { name: 'Simulate gap' }).click()
  await expect(firstTab.locator('[data-gateway-phase]').first()).toHaveAttribute(
    'data-gateway-phase',
    'offline'
  )
  await delay(200)

  const missedBody = `Catch-up proof ${crypto.randomUUID().slice(0, 8)}`
  await secondTab.locator('#probe-body').fill(missedBody)
  await secondTab.getByRole('button', { name: 'Send test message' }).click()
  await expect(secondTab.getByText(missedBody)).toBeVisible()

  await firstTab.getByRole('button', { name: 'Reconnect' }).click()
  await expect(firstTab.getByText(missedBody)).toBeVisible()
  await expect(firstTab.locator('[data-gateway-phase]').first()).toHaveAttribute(
    'data-gateway-phase',
    'live'
  )
  await expect(firstTab.locator('[data-catchups]')).toHaveAttribute('data-catchups', '1')
  await expect(
    firstTab.locator('[data-proof-status="pass"]', { hasText: 'Recover a missed delivery' })
  ).toBeVisible()
  await firstTab.screenshot({ path: screenshot, fullPage: true })

  process.stdout.write(
    `\n${JSON.stringify(
      {
        verdict: 'valuable',
        proven: [
          'the guided test bench exposes the exact two-tab recovery sequence',
          'two browser tabs receive the same room delivery',
          'a disconnected tab catches up from the bounded Durable Object retained log',
          'the tab returns to the shared live feed after applying the missing sequence',
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
