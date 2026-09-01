import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { fileURLToPath } from 'node:url'

import { chromium, expect } from '@playwright/test'

const root = fileURLToPath(new URL('..', import.meta.url))
const delay = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds))

const waitForApp = async (baseUrl, output) => {
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
  throw new Error(`Timed out waiting for the chat lab\n${output()}`, { cause: lastError })
}

const stop = async (child) => {
  if (child.exitCode !== null) return
  const exited = once(child, 'exit')
  child.kill('SIGTERM')
  await Promise.race([exited, delay(6_000)])
  if (child.exitCode === null) child.kill('SIGKILL')
}

const numericMetric = async (page, name) =>
  Number(await page.locator(`[data-metric="${name}"]`).textContent())

export async function verifyChatLab({ adapter, baseUrl, devScript, screenshot }) {
  let output = ''
  const dev = spawn('node', [devScript], {
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
    await waitForApp(baseUrl, () => output)
    browser = await chromium.launch(process.env.CI ? {} : { channel: 'chrome' })
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } })
    const primary = await context.newPage()
    await primary.goto(baseUrl)

    await expect(primary.locator(`main[data-example-mode="${adapter}"]`)).toBeVisible()
    await expect(primary.locator('main')).toHaveAttribute('data-chat-phase', 'live', {
      timeout: 15_000,
    })
    await expect(primary.locator('.product-message')).toHaveCount(240)
    await expect(primary.locator('[data-metric="history-events"]')).toHaveText('240')
    await expect(primary.locator('[data-metric="state-updates"]')).toHaveText('1')

    await primary.getByRole('button', { name: /Tiny follow-up/ }).click()
    await expect(primary.locator('main')).toHaveAttribute('data-chat-phase', 'live')
    await expect(primary.locator('.product-message')).toHaveCount(8)
    await expect(primary.locator('[data-metric="history-events"]')).toHaveText('8')

    await primary.getByRole('button', { name: /Launch readiness review/ }).click()
    await expect(primary.locator('main')).toHaveAttribute('data-chat-phase', 'live')
    await expect(primary.locator('.product-message')).toHaveCount(96)

    const second = await context.newPage()
    await second.goto(primary.url())
    await expect(second.locator('main')).toHaveAttribute('data-chat-phase', 'live')
    await expect(primary.getByText('2 tabs')).toBeVisible({ timeout: 6_000 })

    const body = `${adapter} update from another tab ${crypto.randomUUID().slice(0, 8)}`
    await second.locator('.product-composer textarea').fill(body)
    await second.getByRole('button', { name: 'Send message' }).click()
    const transcript = primary.locator('.product-transcript')
    await expect(transcript.getByText(body)).toBeVisible()
    await expect(primary.getByText('Another tab sent a message')).toBeVisible()
    await expect(transcript.getByText('I’m on it.')).toBeVisible()
    await expect(transcript.getByText(/I replayed the conversation context/)).toBeVisible()

    const beforeBurst = await numericMetric(primary, 'state-updates')
    await primary.getByRole('button', { name: 'Simulate busy room' }).click()
    await expect(primary.locator('.product-message')).toHaveCount(139, { timeout: 10_000 })
    await expect.poll(() => numericMetric(primary, 'largest-ui-batch')).toBeGreaterThan(1)
    const afterBurst = await numericMetric(primary, 'state-updates')
    const result = {
      adapter,
      historyEvents: await numericMetric(primary, 'history-events'),
      historyReady: await primary.locator('[data-metric="history-ready"]').textContent(),
      liveStateUpdatesForBurst: afterBurst - beforeBurst,
      largestUiBatch: await numericMetric(primary, 'largest-ui-batch'),
      reactCommits: await numericMetric(primary, 'react-commits'),
    }

    await second.close()
    await primary.screenshot({ path: screenshot, fullPage: true })
    process.stdout.write(
      `\n${JSON.stringify(
        {
          verdict: 'passed',
          result,
          proven: [
            '240 retained user and assistant messages render from one atomic replay state',
            'switching conversations rebuilds exact 8-message and 96-message histories',
            'a second tab publishes through NATS and the first tab receives a visible update notification',
            'the deterministic assistant publishes two live replies',
            'forty compact live updates reach React in fewer presentation updates than messages',
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
}
