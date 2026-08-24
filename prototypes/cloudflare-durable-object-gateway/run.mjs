// PROTOTYPE — one-command scenario runner, intentionally not a reusable test harness.

import { execFileSync, spawn } from 'node:child_process'
import { once } from 'node:events'
import { rm } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'

import { jetstream, jetstreamManager, StorageType } from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'

const root = fileURLToPath(new URL('../..', import.meta.url))
const config = fileURLToPath(new URL('./wrangler.jsonc', import.meta.url))
const persistTo = fileURLToPath(
  new URL('../../.wrangler/PROTOTYPE_WIPE_ME_cloudflare_gateway', import.meta.url)
)
const baseUrl = 'http://127.0.0.1:8791'
const websocketUrl = 'ws://127.0.0.1:8791'
const token = 'prototype-only'
const stream = 'NATSAIL_GATEWAY_PROTOTYPE'
const subjectPrefix = 'prototype.gateway.>'
let admin
let manager
let wrangler
let startedFixtures = false

const print = (label, value) => {
  process.stdout.write(`\n${label}\n${JSON.stringify(value, null, 2)}\n`)
}

const assert = (condition, message) => {
  if (!condition) throw new Error(message)
}

const delay = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds))

const waitFor = async (operation, description, timeoutMs = 10_000) => {
  const deadline = Date.now() + timeoutMs
  let lastError
  while (Date.now() < deadline) {
    try {
      const result = await operation()
      if (result !== undefined && result !== false) return result
    } catch (error) {
      lastError = error
    }
    await delay(50)
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

const connectToNats = () =>
  waitFor(
    async () => connect({ servers: 'nats://127.0.0.1:4223', timeout: 500 }),
    'the local NATS fixture'
  )

const startWrangler = async () => {
  let output = ''
  const child = spawn(
    'pnpm',
    [
      'exec',
      'wrangler',
      'dev',
      '--config',
      config,
      '--persist-to',
      persistTo,
      '--log-level',
      'warn',
    ],
    { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] }
  )
  child.stdout.on('data', (chunk) => {
    output += chunk
    process.stdout.write(`[wrangler] ${chunk}`)
  })
  child.stderr.on('data', (chunk) => {
    output += chunk
    process.stderr.write(`[wrangler] ${chunk}`)
  })

  try {
    await waitFor(async () => {
      if (child.exitCode !== null) {
        throw new Error(`Wrangler stopped during startup\n${output}`)
      }
      const response = await fetch(`${baseUrl}/health`, { signal: AbortSignal.timeout(500) })
      return response.ok
    }, 'the prototype workerd server')
    return child
  } catch (error) {
    child.kill('SIGTERM')
    throw error
  }
}

const stopWrangler = async () => {
  if (!wrangler || wrangler.exitCode !== null) return
  const exited = once(wrangler, 'exit')
  wrangler.kill('SIGTERM')
  await Promise.race([exited, delay(3_000)])
  if (wrangler.exitCode === null) wrangler.kill('SIGKILL')
  wrangler = undefined
}

const gatewayHttp = (operation) =>
  `${baseUrl}/gateway/demo/${operation}?token=${encodeURIComponent(token)}`

const gatewayWs = (client, cursor) => {
  const url = new URL(`${websocketUrl}/gateway/demo/socket`)
  url.searchParams.set('token', token)
  url.searchParams.set('client', client)
  if (cursor !== undefined) url.searchParams.set('cursor', String(cursor))
  return url
}

const gatewayState = async (label) => {
  const response = await fetch(gatewayHttp('state'))
  assert(response.ok, `Gateway state failed with ${response.status}`)
  const state = await response.json()
  print(label, state)
  return state
}

const gatewayHistory = async (after, label) => {
  const response = await fetch(`${gatewayHttp('history')}&after=${after}`)
  assert(response.ok, `Gateway history failed with ${response.status}`)
  const history = await response.json()
  print(label, history)
  return history
}

const waitForState = (predicate, label) =>
  waitFor(async () => {
    const response = await fetch(gatewayHttp('state'))
    if (!response.ok) return false
    const state = await response.json()
    return predicate(state) ? state : false
  }, label)

const openClient = async (name, cursor) => {
  const socket = new WebSocket(gatewayWs(name, cursor))
  const frames = []
  const waiters = []

  socket.addEventListener('message', (event) => {
    const frame = JSON.parse(event.data)
    process.stdout.write(`[client ${name}] <- ${JSON.stringify(frame)}\n`)
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
    const timer = setTimeout(() => reject(new Error(`Timed out opening client ${name}`)), 10_000)
    socket.addEventListener('open', resolve, { once: true })
    socket.addEventListener('error', () => reject(new Error(`Client ${name} failed to open`)), {
      once: true,
    })
    socket.addEventListener('open', () => clearTimeout(timer), { once: true })
  })

  return {
    name,
    socket,
    send(frame) {
      process.stdout.write(`[client ${name}] -> ${JSON.stringify(frame)}\n`)
      socket.send(JSON.stringify(frame))
    },
    next(predicate, description, timeoutMs = 10_000) {
      const index = frames.findIndex(predicate)
      if (index >= 0) return Promise.resolve(frames.splice(index, 1)[0])

      return new Promise((resolve, reject) => {
        const waiter = {
          predicate,
          resolve,
          timer: setTimeout(() => {
            const active = waiters.indexOf(waiter)
            if (active >= 0) waiters.splice(active, 1)
            reject(new Error(`Timed out waiting for client ${name} ${description}`))
          }, timeoutMs),
        }
        waiters.push(waiter)
      })
    },
    close() {
      socket.close(1000, 'prototype scenario complete')
    },
  }
}

try {
  print('PROTOTYPE QUESTION', {
    question:
      'Can a tenant Durable Object share one NATSail upstream, persist its checkpoint, and recover without hiding downstream cursor gaps?',
    production: false,
  })

  const services = runningServices()
  startedFixtures = !services.includes('nats')
  if (startedFixtures) {
    execFileSync('pnpm', ['nats:up'], { cwd: root, stdio: 'inherit' })
  }

  admin = await connectToNats()
  manager = await jetstreamManager(admin)
  await manager.streams.delete(stream).catch(() => undefined)
  await manager.streams.add({
    name: stream,
    subjects: [subjectPrefix],
    storage: StorageType.Memory,
  })
  const js = jetstream(admin)
  await rm(persistTo, { recursive: true, force: true })

  wrangler = await startWrangler()
  const firstA = await openClient('A')
  const firstB = await openClient('B')
  await Promise.all([
    firstA.next((frame) => frame.type === 'ready', 'ready frame'),
    firstB.next((frame) => frame.type === 'ready', 'ready frame'),
  ])

  const connected = await gatewayState('STATE AFTER TWO CLIENTS CONNECT')
  assert(connected.clients.length === 2, 'Expected two clients in one Durable Object')
  assert(connected.upstreamConnectionAttempts === 1, 'Expected one shared NATS connection')

  const firstDataA = firstA.next(
    (frame) => frame.type === 'data' && frame.value === 'before-restart',
    'first data frame'
  )
  const firstDataB = firstB.next(
    (frame) => frame.type === 'data' && frame.value === 'before-restart',
    'first data frame'
  )
  firstA.send({ type: 'publish', value: 'before-restart' })
  const [deliveredA, deliveredB] = await Promise.all([firstDataA, firstDataB])
  firstA.send({ type: 'checkpoint', cursor: deliveredA.cursor })
  firstB.send({ type: 'checkpoint', cursor: deliveredB.cursor })
  await Promise.all([
    firstA.next((frame) => frame.type === 'checkpointed', 'checkpoint acknowledgement'),
    firstB.next((frame) => frame.type === 'checkpointed', 'checkpoint acknowledgement'),
  ])
  const beforeRestart = await waitForState(
    (state) => state.checkpoint === deliveredA.cursor,
    'the first Durable Object checkpoint'
  )
  print('STATE AFTER SHARED DELIVERY', beforeRestart)

  firstA.close()
  firstB.close()
  await stopWrangler()
  print('ACTION: LOCAL WORKERD PROCESS STOPPED', {
    clientCursorA: deliveredA.cursor,
    clientCursorB: deliveredB.cursor,
    persistedGatewayCursor: beforeRestart.checkpoint,
  })

  const missed = await js.publish(beforeRestart.subject, 'stored-while-object-was-down')
  print('ACTION: JETSTREAM STORED A MESSAGE WHILE THE OBJECT WAS DOWN', {
    streamSequence: missed.seq,
  })

  wrangler = await startWrangler()
  const recoveredA = await openClient('A-reconnected', deliveredA.cursor)
  const replayedA = await recoveredA.next(
    (frame) => frame.type === 'data' && frame.value === 'stored-while-object-was-down',
    'replayed data frame'
  )
  recoveredA.send({ type: 'checkpoint', cursor: replayedA.cursor })
  await recoveredA.next((frame) => frame.type === 'checkpointed', 'replay checkpoint')

  const recovered = await waitForState(
    (state) => state.checkpoint === replayedA.cursor && state.instanceStarts === 2,
    'the reconstructed Durable Object checkpoint'
  )
  print('STATE AFTER WORKERD RESTART AND JETSTREAM REPLAY', recovered)
  assert(recovered.upstreamConnectionAttempts === 2, 'Expected a new upstream after reconstruction')

  const behindB = await openClient('B-behind', deliveredB.cursor)
  const resumeRequired = await behindB.next(
    (frame) => frame.type === 'resume-required',
    'explicit downstream resume gap'
  )
  print('STATE MODEL RESULT: LATE CLIENT GAP IS EXPLICIT', resumeRequired)

  const catchUp = await gatewayHistory(
    deliveredB.cursor,
    'BOUNDED RETAINED LOG CATCHES THE LATE CLIENT UP'
  )
  assert(catchUp.complete, 'Expected the late cursor to remain inside the retained window')
  assert(catchUp.frames.length === 1, 'Expected exactly one retained catch-up delivery')
  assert(
    catchUp.frames[0].value === 'stored-while-object-was-down',
    'Expected the retained catch-up delivery to contain the missed value'
  )

  const currentB = await openClient('B-current', catchUp.retainedThrough)
  await currentB.next((frame) => frame.type === 'ready', 'ready frame')
  const liveA = recoveredA.next(
    (frame) => frame.type === 'data' && frame.value === 'after-recovery',
    'live post-recovery data'
  )
  const liveB = currentB.next(
    (frame) => frame.type === 'data' && frame.value === 'after-recovery',
    'live post-recovery data'
  )
  recoveredA.send({ type: 'publish', value: 'after-recovery' })
  const [liveDeliveryA, liveDeliveryB] = await Promise.all([liveA, liveB])
  assert(liveDeliveryA.cursor === liveDeliveryB.cursor, 'Expected one shared live stream cursor')

  const finalState = await waitForState(
    (state) => state.checkpoint === liveDeliveryA.cursor,
    'the final shared checkpoint'
  )
  print('FINAL PROTOTYPE STATE', finalState)
  assert(finalState.clients.length === 2, 'Expected two current clients after recovery')
  assert(finalState.upstreamConnectionAttempts === 2, 'Expected one connection per object instance')

  print('PROTOTYPE VERDICT', {
    valuable: true,
    proven: [
      'one tenant Durable Object multiplexes two clients over one NATS connection',
      'Durable Object storage satisfies the NATSail checkpoint interface',
      'a reconstructed runtime replays JetStream data stored while workerd was down',
      'a late client behind the shared gateway cursor receives an explicit resume-required result',
      'a late client inside the bounded Durable Object retention window can fetch its missing delivery',
    ],
    unresolved: [
      'retained-log storage cost and expiry behavior need comparison with temporary catch-up consumers',
      'outbound NATS sockets prevent cost-saving hibernation while active',
      'remote deployment, authentication, byte backpressure, and forced eviction remain',
    ],
  })

  recoveredA.close()
  currentB.close()
  await fetch(gatewayHttp('shutdown'), { method: 'POST' }).catch(() => undefined)
} finally {
  await stopWrangler().catch(() => undefined)
  await rm(persistTo, { recursive: true, force: true }).catch(() => undefined)
  if (manager) await manager.streams.delete(stream).catch(() => undefined)
  if (admin && !admin.isClosed()) await admin.drain().catch(() => undefined)
  if (startedFixtures) {
    execFileSync('pnpm', ['nats:down'], { cwd: root, stdio: 'inherit' })
  }
}
