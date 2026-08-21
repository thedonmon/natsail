// PROTOTYPE — one-command local bench for the TanStack rooms hypothesis.

import { execFileSync, spawn } from 'node:child_process'
import { once } from 'node:events'
import { rm } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'

import { jetstreamManager, StorageType } from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'

const root = fileURLToPath(new URL('../..', import.meta.url))
const appDirectory = fileURLToPath(new URL('.', import.meta.url))
const viteBin = fileURLToPath(new URL('../../node_modules/vite/bin/vite.js', import.meta.url))
const viteConfig = fileURLToPath(new URL('./vite.config.ts', import.meta.url))
const gatewayConfig = fileURLToPath(
  new URL('../../prototypes/cloudflare-durable-object-gateway/wrangler.jsonc', import.meta.url)
)
const persistTo = fileURLToPath(
  new URL('../../.wrangler/PROTOTYPE_WIPE_ME_tanstack_chat', import.meta.url)
)
const appUrl = 'http://127.0.0.1:4174'
const gatewayUrl = 'http://127.0.0.1:8791'
const stream = 'NATSAIL_GATEWAY_PROTOTYPE'
const subjectPrefix = 'prototype.gateway.>'

let admin
let manager
let wrangler
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

const spawnLogged = (label, command, args) => {
  const child = spawn(command, args, { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] })
  child.stdout.on('data', (chunk) => process.stdout.write(`[${label}] ${chunk}`))
  child.stderr.on('data', (chunk) => process.stderr.write(`[${label}] ${chunk}`))
  return child
}

const stopChild = async (child) => {
  if (!child || child.exitCode !== null) return
  const exited = once(child, 'exit')
  child.kill('SIGTERM')
  await Promise.race([exited, delay(3_000)])
  if (child.exitCode === null) child.kill('SIGKILL')
}

const startGateway = async () => {
  const child = spawnLogged('gateway', 'pnpm', [
    'exec',
    'wrangler',
    'dev',
    '--config',
    gatewayConfig,
    '--persist-to',
    persistTo,
    '--log-level',
    'warn',
  ])
  await waitFor(async () => {
    if (child.exitCode !== null) throw new Error('Wrangler exited during startup')
    const response = await fetch(`${gatewayUrl}/health`, { signal: AbortSignal.timeout(500) })
    return response.ok
  }, 'the Durable Object gateway')
  return child
}

const openSeedClient = async () => {
  const url = new URL('ws://127.0.0.1:8791/gateway/demo/socket')
  url.searchParams.set('token', 'prototype-only')
  url.searchParams.set('client', 'seed-fixture')
  const socket = new WebSocket(url)
  const frames = []
  const waiters = []

  socket.addEventListener('message', (event) => {
    const frame = JSON.parse(String(event.data))
    const index = waiters.findIndex((waiter) => waiter.predicate(frame))
    if (index >= 0) {
      const [waiter] = waiters.splice(index, 1)
      clearTimeout(waiter.timer)
      waiter.resolve(frame)
    } else {
      frames.push(frame)
    }
  })

  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Seed client failed to open')), 10_000)
    socket.addEventListener(
      'open',
      () => {
        clearTimeout(timer)
        resolve()
      },
      { once: true }
    )
    socket.addEventListener('error', () => reject(new Error('Seed client socket error')), {
      once: true,
    })
  })

  return {
    socket,
    next(predicate, description) {
      const index = frames.findIndex(predicate)
      if (index >= 0) return Promise.resolve(frames.splice(index, 1)[0])
      return new Promise((resolve, reject) => {
        const waiter = {
          predicate,
          resolve,
          timer: setTimeout(() => {
            const active = waiters.indexOf(waiter)
            if (active >= 0) waiters.splice(active, 1)
            reject(new Error(`Timed out waiting for seed client ${description}`))
          }, 10_000),
        }
        waiters.push(waiter)
      })
    },
  }
}

const seedRooms = async () => {
  const seed = await openSeedClient()
  await seed.next((frame) => frame.type === 'ready', 'ready frame')

  const messages = [
    [
      'general',
      'Mika / systems',
      'One tenant object is carrying every room on one upstream connection.',
    ],
    ['gateway-lab', 'Noor / edge', 'Durable storage now retains the last 128 deliveries.'],
    [
      'gateway-lab',
      'Avery / app',
      'A reconnecting tab applies its gap before it rejoins live fan-out.',
    ],
    [
      'edge-cases',
      'Sol / runtime',
      'The shared cursor and each tab applied cursor are deliberately separate.',
    ],
    [
      'release',
      'Tess / release',
      'Promotion depends on replay clarity, storage cost, and slow-client policy.',
    ],
  ]

  for (const [roomId, author, body] of messages) {
    const message = {
      id: crypto.randomUUID(),
      roomId,
      author,
      body,
      sentAt: new Date().toISOString(),
      clientId: 'seed-fixture',
    }
    const value = JSON.stringify(message)
    seed.socket.send(JSON.stringify({ type: 'publish', value }))
    await seed.next(
      (frame) => frame.type === 'data' && frame.value === value,
      `delivery for ${roomId}`
    )
  }

  seed.socket.close(1000, 'Prototype seed complete')
}

const cleanup = async () => {
  if (stopping) return
  stopping = true
  await stopChild(vite).catch(() => undefined)
  await stopChild(wrangler).catch(() => undefined)
  await rm(persistTo, { recursive: true, force: true }).catch(() => undefined)
  if (manager) await manager.streams.delete(stream).catch(() => undefined)
  if (admin && !admin.isClosed()) await admin.drain().catch(() => undefined)
  if (startedFixtures) {
    execFileSync('pnpm', ['nats:down'], { cwd: root, stdio: 'inherit' })
  }
}

try {
  process.stdout.write('\nNATSail gateway chat example — local hypothesis bench\n')
  const services = runningServices()
  startedFixtures = !services.includes('nats')
  if (startedFixtures) {
    execFileSync('pnpm', ['nats:up'], { cwd: root, stdio: 'inherit' })
  }

  admin = await waitFor(
    () => connect({ servers: 'nats://127.0.0.1:4223', timeout: 500 }),
    'the local NATS fixture'
  )
  manager = await jetstreamManager(admin)
  await manager.streams.delete(stream).catch(() => undefined)
  await manager.streams.add({
    name: stream,
    subjects: [subjectPrefix],
    storage: StorageType.Memory,
  })
  await rm(persistTo, { recursive: true, force: true })

  wrangler = await startGateway()
  await seedRooms()
  vite = spawnLogged('app', process.execPath, [
    viteBin,
    appDirectory,
    '--config',
    viteConfig,
    '--host',
    '127.0.0.1',
  ])
  await waitFor(async () => {
    if (vite.exitCode !== null) throw new Error('Vite exited during startup')
    const response = await fetch(appUrl, { signal: AbortSignal.timeout(500) })
    return response.ok
  }, 'the TanStack chat application')

  process.stdout.write(`\nREADY ${appUrl}/rooms/gateway-lab\n`)
  process.stdout.write('Follow the three checks in the left test rail.\n')
  process.stdout.write('Press Ctrl-C to stop and remove the scratch stream.\n\n')

  await new Promise((resolve, reject) => {
    const finish = () => resolve()
    process.once('SIGINT', finish)
    process.once('SIGTERM', finish)
    wrangler.once('exit', (code) => {
      if (!stopping) reject(new Error(`Gateway process exited unexpectedly with ${code}`))
    })
    vite.once('exit', (code) => {
      if (!stopping) reject(new Error(`App process exited unexpectedly with ${code}`))
    })
  })
} finally {
  await cleanup()
}
