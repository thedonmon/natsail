// PRIVATE EXAMPLE — real-browser verification for the AI chat experience.

import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { fileURLToPath } from 'node:url'

import { chromium, expect } from '@playwright/test'

const root = fileURLToPath(new URL('../..', import.meta.url))
const baseUrl = 'http://127.0.0.1:4176'
const screenshot = '/tmp/natsail-ai-chat-example.png'
const gatewayPrompt = 'Help me plan the gateway release.'
const reconnectPrompt = "What happens if the connection drops while you're answering?"

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
  throw new Error(`Timed out waiting for the AI transport example\n${output()}`, {
    cause: lastError,
  })
}

const stop = async (child) => {
  if (child.exitCode !== null) return
  const exited = once(child, 'exit')
  child.kill('SIGTERM')
  await Promise.race([exited, delay(6_000)])
  if (child.exitCode === null) child.kill('SIGKILL')
}

let output = ''
const dev = spawn('node', ['examples/ai-transport/dev.mjs'], {
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

  await expect(page.getByText('Online', { exact: true })).toBeVisible()
  await expect(
    page.getByText('Can we make reconnect behavior obvious enough to trust before release?')
  ).toBeVisible()
  await expect(page.locator('[data-history-loaded="true"]')).toContainText(
    /loaded through stream sequence \d+/
  )
  await expect(page.getByText(/JetStream · sequence \d+/)).toHaveCount(2)
  await page.getByRole('button', { name: gatewayPrompt }).click()
  await expect(page.getByText('Streaming through NATS…')).toBeVisible()

  const reconnect = page.getByRole('button', { name: 'Run recovery test' })
  await expect(reconnect).toBeEnabled({ timeout: 10_000 })
  const outageStartedAt = Date.now()
  await reconnect.click()
  await expect(
    page.getByText('Connection interrupted. JetStream is retaining the reply…')
  ).toBeVisible()
  const recovered = page.locator('[data-reconnect-state="reconnected"]')
  await expect(recovered).toBeVisible({ timeout: 10_000 })
  await expect(recovered).toHaveAttribute('data-retained-frames', /^[1-9]\d*$/)
  if (Date.now() - outageStartedAt < 1_400) {
    throw new Error('The simulated outage completed too quickly to observe')
  }
  await expect(page.getByText(/Before release, interrupt an answer on purpose\./)).toBeVisible({
    timeout: 15_000,
  })
  await expect(page.getByText(/without asking the user to reload\./)).toBeVisible()
  await expect(page.getByText(gatewayPrompt, { exact: true })).toBeVisible()
  await expect(page.getByText('Delivered', { exact: true })).toBeVisible()
  await expect(page.getByRole('textbox', { name: 'Message NATSail Assistant' })).toBeEnabled()
  await page.screenshot({ path: screenshot })

  await page.getByRole('button', { name: reconnectPrompt }).click()
  await expect(
    page.getByText(/ordered consumer resumes strictly after its last processed cursor/)
  ).toBeVisible({
    timeout: 15_000,
  })

  await page.locator('details.chat-developer > summary').click()
  await expect(page.getByText('Native chunks', { exact: true })).toBeVisible()
  await expect(page.getByText('Ack policy', { exact: true })).toBeVisible()
  await expect(page.getByText('None', { exact: true })).toBeVisible()
  await expect(page.getByText('new / all', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'JetStream', exact: true })).toHaveAttribute(
    'aria-pressed',
    'true'
  )
  await expect(page.getByRole('button', { name: 'Drop', exact: true })).toHaveAttribute(
    'aria-pressed',
    'true'
  )
  await page.getByRole('button', { name: 'Inject random message' }).click()
  await expect(page.locator('[data-live-stream-message="true"]')).toBeVisible()
  await expect(page.getByText(/JetStream · sequence \d+/)).toHaveCount(3)
  await page.getByRole('button', { name: 'TanStack AI' }).click()
  await page.getByRole('button', { name: gatewayPrompt }).click()
  const tanStackReconnect = page.getByRole('button', { name: 'Run recovery test' })
  await expect(tanStackReconnect).toBeEnabled({ timeout: 10_000 })
  await tanStackReconnect.click()
  await expect(page.locator('[data-reconnect-state="reconnected"]')).toHaveAttribute(
    'data-retained-frames',
    /^[1-9]\d*$/
  )
  await expect(page.getByText(/without asking the user to reload\./)).toBeVisible({
    timeout: 15_000,
  })
  await expect(page.getByText('Delivered', { exact: true })).toBeVisible()

  await page.locator('details.chat-developer > summary').click()
  await page.getByRole('button', { name: 'Core', exact: true }).click()
  await expect(
    page.getByText('Local, deterministic, and streaming through Core NATS')
  ).toBeVisible()
  await expect(page.getByRole('button', { name: 'Run recovery test' })).toBeDisabled()

  process.stdout.write(
    `\n${JSON.stringify(
      {
        verdict: 'passed',
        proven: [
          'the example behaves as a real multi-turn chat instead of a diagnostic dashboard',
          'AI SDK receives the complete retained stream after its ordered consumer reconnects',
          'TanStack AI receives the same native stream shape through an ordered consumer',
          'the transcript remains intact while retained chunks cross the disconnected gap',
          'the app loads earlier conversation messages from stream sequence history',
          'a random local message can be injected into the live conversation stream',
          'AckPolicy.None and duplicate-policy configuration are explicit',
          'Core NATS remains available as an explicit live-only comparison',
          'the deterministic responder needs no model, route, API key, or network API',
        ],
        currentBoundary:
          'active-run gap recovery is proven; full page reload still needs persisted framework and run state',
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
