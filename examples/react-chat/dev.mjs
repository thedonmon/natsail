// REPOSITORY EXAMPLE — one-command local runner for the direct React primitives app.

import { execFileSync, spawn } from 'node:child_process'
import { once } from 'node:events'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../..', import.meta.url))
const appDirectory = fileURLToPath(new URL('.', import.meta.url))
const viteBin = fileURLToPath(new URL('../../node_modules/vite/bin/vite.js', import.meta.url))
const viteConfig = fileURLToPath(new URL('./vite.config.ts', import.meta.url))
const appUrl = 'http://127.0.0.1:4175'

let vite
let startedFixtures = false
let stopping = false

const delay = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds))

const waitFor = async (operation, description, timeoutMs = 15_000) => {
  const deadline = Date.now() + timeoutMs
  let lastError
  while (Date.now() < deadline) {
    try {
      const result = await operation()
      if (result !== undefined && result !== false) return result
    } catch (error) {
      lastError = error
    }
    await delay(60)
  }
  throw new Error(`Timed out waiting for ${description}`, { cause: lastError })
}

const runningServices = () => {
  try {
    return execFileSync('docker', ['compose', 'ps', '--services', '--filter', 'status=running'], {
      cwd: root,
      encoding: 'utf8',
    })
      .trim()
      .split('\n')
      .filter(Boolean)
  } catch {
    return []
  }
}

const stopChild = async (child) => {
  if (!child || child.exitCode !== null) return
  const exited = once(child, 'exit')
  child.kill('SIGTERM')
  await Promise.race([exited, delay(3_000)])
  if (child.exitCode === null) child.kill('SIGKILL')
}

const cleanup = async () => {
  if (stopping) return
  stopping = true
  await stopChild(vite).catch(() => undefined)
  if (startedFixtures) {
    execFileSync('pnpm', ['nats:down'], { cwd: root, stdio: 'inherit' })
  }
}

try {
  process.stdout.write('\nNATSail React chat example — direct browser-to-NATS primitives\n')
  const services = runningServices()
  startedFixtures = !services.includes('nats')
  if (startedFixtures) {
    execFileSync('pnpm', ['nats:up'], { cwd: root, stdio: 'inherit' })
  }

  await waitFor(async () => {
    const response = await fetch('http://127.0.0.1:8223/healthz', {
      signal: AbortSignal.timeout(500),
    })
    return response.ok
  }, 'the local NATS fixture')

  vite = spawn(
    process.execPath,
    [viteBin, appDirectory, '--config', viteConfig, '--host', '127.0.0.1'],
    {
      cwd: root,
      stdio: ['ignore', 'pipe', 'pipe'],
    }
  )
  vite.stdout.on('data', (chunk) => process.stdout.write(`[app] ${chunk}`))
  vite.stderr.on('data', (chunk) => process.stderr.write(`[app] ${chunk}`))

  await waitFor(async () => {
    if (vite.exitCode !== null) throw new Error('Vite exited during startup')
    const response = await fetch(appUrl, { signal: AbortSignal.timeout(500) })
    return response.ok
  }, 'the React chat example')

  process.stdout.write(`\nREADY ${appUrl}/rooms/gateway-lab\n`)
  process.stdout.write('The browser connects directly to ws://127.0.0.1:9223.\n')
  process.stdout.write('Follow the three checks in the left test rail.\n')
  process.stdout.write('Press Ctrl-C to stop.\n\n')

  await new Promise((resolve, reject) => {
    const finish = () => resolve()
    process.once('SIGINT', finish)
    process.once('SIGTERM', finish)
    vite.once('exit', (code) => {
      if (!stopping) reject(new Error(`App process exited unexpectedly with ${code}`))
    })
  })
} finally {
  await cleanup()
}
