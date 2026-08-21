// REPOSITORY EXAMPLE — one-command RxJS rooms app over a resumable JetStream feed.

import { execFileSync, spawn } from 'node:child_process'
import { once } from 'node:events'
import { fileURLToPath } from 'node:url'

import { jetstream, jetstreamManager, StorageType } from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'

const root = fileURLToPath(new URL('../..', import.meta.url))
const appDirectory = fileURLToPath(new URL('.', import.meta.url))
const viteBin = fileURLToPath(new URL('../../node_modules/vite/bin/vite.js', import.meta.url))
const viteConfig = fileURLToPath(new URL('./vite.config.ts', import.meta.url))
const appUrl = 'http://127.0.0.1:4177'
const stream = 'NATSAIL_RXJS_CHAT'
const subjectPrefix = 'natsail.examples.rxjs.chat'
const streamSubjects = `${subjectPrefix}.>`
const encoder = new TextEncoder()

let vite
let connection
let manager
let createdStream = false
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
  if (createdStream) await manager?.streams.delete(stream).catch(() => undefined)
  await connection?.drain().catch(() => undefined)
  if (startedFixtures) execFileSync('pnpm', ['nats:down'], { cwd: root, stdio: 'inherit' })
}

const seedMessages = async (client) => {
  const values = [
    ['general', 'Mika / systems', 'This retained room feed enters the app as one RxJS Observable.'],
    [
      'gateway-lab',
      'Noor / frontend',
      'Two RxJS projections share one keyed NATSail session and one ordered consumer.',
    ],
    [
      'gateway-lab',
      'Avery / app',
      'The transcript uses scan while the room evidence uses a separate projection.',
    ],
    [
      'edge-cases',
      'Sol / runtime',
      'Pause the consumer, publish into the gap, and resume after the processed checkpoint.',
    ],
    [
      'release',
      'Tess / release',
      'This public repository example is workspace-only and is not an npm package.',
    ],
  ]

  for (const [roomId, author, body] of values) {
    await client.publish(
      `${subjectPrefix}.${roomId}`,
      encoder.encode(
        JSON.stringify({
          id: `rxjs-seed-${roomId}-${author}`,
          roomId,
          author,
          body,
          sentAt: new Date().toISOString(),
          clientId: 'rxjs-example-seed',
        })
      )
    )
  }
}

try {
  process.stdout.write('\nNATSail RxJS chat example — shared projections over JetStream\n')
  const services = runningServices()
  startedFixtures = !services.includes('nats')
  if (startedFixtures) execFileSync('pnpm', ['nats:up'], { cwd: root, stdio: 'inherit' })

  await waitFor(async () => {
    const response = await fetch('http://127.0.0.1:8223/healthz', {
      signal: AbortSignal.timeout(500),
    })
    return response.ok
  }, 'the local NATS fixture')

  connection = await connect({ servers: 'nats://127.0.0.1:4223' })
  manager = await jetstreamManager(connection)
  let streamInfo
  try {
    streamInfo = await manager.streams.info(stream)
  } catch {
    await manager.streams.add({
      name: stream,
      subjects: [streamSubjects],
      storage: StorageType.Memory,
    })
    createdStream = true
    streamInfo = await manager.streams.info(stream)
  }
  if (streamInfo.state.messages === 0) await seedMessages(jetstream(connection))

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
  }, 'the RxJS chat example')

  process.stdout.write(`\nREADY ${appUrl}\n`)
  process.stdout.write('The browser connects directly to ws://127.0.0.1:9223.\n')
  process.stdout.write('Send a message or run the visible retained-recovery action.\n')
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
