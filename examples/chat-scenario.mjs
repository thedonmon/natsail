import { execFileSync, spawn } from 'node:child_process'
import { once } from 'node:events'
import { fileURLToPath } from 'node:url'

import { jetstream, jetstreamManager, StorageType } from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'

const root = fileURLToPath(new URL('..', import.meta.url))
const viteBin = fileURLToPath(new URL('../node_modules/vite/bin/vite.js', import.meta.url))

export const demoScenario = [
  {
    id: 'durable-chat',
    title: 'Designing durable chat history',
    assistant: 'Nora',
    count: 240,
    updatedAt: '2026-09-01T15:42:00.000Z',
    user: [
      'Could we keep the replay boundary invisible to the chat surface?',
      'I want switching back to this conversation to feel instant, but ordering still has to be exact.',
      'What happens when a second tab adds a message while this one is focused elsewhere?',
      'Can the package own the awkward lifecycle details without owning our conversation model?',
      'The short version: do we still process every event?',
      'Let’s make sure a long history does not paint one half-built turn at a time.',
    ],
    assistantMessages: [
      'Yes. Replay is reduced privately, then the complete conversation is published at the caught-up boundary. The UI never needs to render an intermediate historical state.',
      'The ordered consumer remains the source of truth. Presentation batching changes when React sees cumulative state; it does not change event order or skip reducer input.',
      'A lightweight live subject can notify every tab. The tab that is not viewing this conversation marks it unread, while reopening the JetStream feed reconstructs the durable history.',
      'That is the intended seam. NATSail owns connection, consumer, recovery, and cancellation. The application still owns message shape, grouping, and retention.',
      'Every event. In sequence.',
      'The load path emits one assembled transcript. Live traffic then lands in small frame-sized batches, so the browser has room to paint and accept input.',
    ],
  },
  {
    id: 'launch-readiness',
    title: 'Launch readiness review',
    assistant: 'Mara',
    count: 96,
    updatedAt: '2026-09-01T14:18:00.000Z',
    user: [
      'Are the package boundaries clear enough for release?',
      'Give me the remaining checks without turning this into a giant checklist.',
      'What should the example prove before we call the adapter ready?',
      'Can advanced consumers still reach the underlying runtime?',
    ],
    assistantMessages: [
      'They are. Core owns the connection, JetStream owns durable delivery, and each framework package adapts those contracts without opening another transport.',
      'Build the workspace, run unit and browser suites, inspect the packed declarations, and exercise reconnect plus replay against a real NATS server.',
      'A realistic chat should load retained history atomically, accept a live burst, switch conversations, and remain consistent across two browser tabs.',
      'Yes. The runtime and session registry remain explicit escape hatches for request/reply, manager operations, and application-specific consumers.',
    ],
  },
  {
    id: 'agent-handoff',
    title: 'Agent handoff notes',
    assistant: 'Sol',
    count: 48,
    updatedAt: '2026-09-01T11:06:00.000Z',
    user: [
      'Where did the investigation land?',
      'Anything surprising in the stream?',
      'Can you leave the next person a concise handoff?',
      'Perfect. Keep going.',
    ],
    assistantMessages: [
      'The transport was healthy. The visible regression came from rebuilding conversation turns incrementally in the view instead of publishing one ordered materialized state.',
      'Only that tiny timestamp inversions are normal enough that stream sequence—not wall-clock time—must remain the ordering authority.',
      'Use one conversation-scoped materializer, hide replay until catch-up, batch cumulative live state at the rendering boundary, and keep raw delivery available for processors.',
      'On it.',
    ],
  },
  {
    id: 'tiny-follow-up',
    title: 'Tiny follow-up',
    assistant: 'June',
    count: 8,
    updatedAt: '2026-08-31T22:40:00.000Z',
    user: ['Does this still work for a tiny conversation?', 'And cross-tab updates?'],
    assistantMessages: [
      'Yep—same path, almost no history.',
      'Open another tab and send a message.',
    ],
  },
]

const messageAt = (conversation, index) => {
  const end = new Date(conversation.updatedAt).getTime()
  return new Date(end - (conversation.count - 1 - index) * 42_000).toISOString()
}

const userContext = [
  'I’m checking this from the conversation switch path.',
  'The important part for me is that the transcript never scrambles while it loads.',
  'I’m comparing this with the smaller thread before we settle on defaults.',
  'This came up while another tab was still connected.',
  'I care more about a responsive browser than shaving a tiny amount off the reducer.',
  'The stream cursor should stay authoritative even when timestamps are close.',
  'Assume this conversation keeps growing for the rest of the afternoon.',
  'I’m deliberately keeping the question short this time.',
  'This is the same workflow a real chat route will use after a reload.',
  'Let’s keep the application model separate from the transport mechanics.',
  'I want the failure mode to be visible without turning the chat into a dashboard.',
  'We should be able to explain this behavior to a package consumer in one paragraph.',
]

const assistantContext = [
  'The cursor shown beside the message comes from the ordered stream, not a client timestamp.',
  'Cancellation also releases the conversation-scoped consumer when you switch away.',
  'A second tab gets the lightweight notification immediately and still replays durable state on demand.',
  'The performance rail counts presentation updates separately from the number of events reduced.',
  'That keeps the rendering optimization outside the domain reducer.',
  'The same fixture is used by the Effect and RxJS examples, so the comparison stays honest.',
  'The live path remains bounded even if a background agent publishes a compact burst.',
  'No optimistic message is inserted here; the transcript shows the event only after its NATS round trip.',
  'The short-history conversation takes this exact path with less replay work.',
  'If the source fails, the loading surface becomes an explicit error state instead of partial history.',
  'React receives immutable cumulative state and stable message keys.',
  'The assistant response is another ordinary event on the same conversation subject.',
]

export const createDemoMessages = (namespace) =>
  demoScenario.flatMap((conversation, conversationIndex) =>
    Array.from({ length: conversation.count }, (_, index) => {
      const position = index % 5
      const role = position === 0 || position === 3 ? 'user' : 'assistant'
      const source = role === 'user' ? conversation.user : conversation.assistantMessages
      const base = source[Math.floor(index / 2) % source.length]
      const context = role === 'user' ? userContext : assistantContext
      const detail = context[(index * 5 + conversationIndex * 3) % context.length]
      const body = base.length < 28 || index % 7 === 0 ? base : `${base}\n\n${detail}`
      return {
        id: `${namespace}-seed-${conversation.id}-${String(index + 1).padStart(3, '0')}`,
        conversationId: conversation.id,
        role,
        author: role === 'user' ? 'You' : conversation.assistant,
        body,
        sentAt: messageAt(conversation, index),
        clientId: `${namespace}-fixture`,
      }
    })
  )

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

const seedMessages = async (client, namespace, subjectPrefix) => {
  for (const message of createDemoMessages(namespace)) {
    await client.publish(`${subjectPrefix}.${message.conversationId}`, JSON.stringify(message))
  }
}

const startAssistantResponder = (connection, client, namespace, subjectPrefix) => {
  const subscription = connection.subscribe(`${subjectPrefix}.>`)
  const task = (async () => {
    for await (const message of subscription) {
      let value
      try {
        value = JSON.parse(message.string())
      } catch {
        continue
      }
      if (
        value?.role !== 'user' ||
        typeof value.conversationId !== 'string' ||
        typeof value.id !== 'string' ||
        String(value.clientId).endsWith('-fixture')
      ) {
        continue
      }
      const conversation = demoScenario.find((candidate) => candidate.id === value.conversationId)
      if (!conversation) continue

      const publishReply = async (body, suffix) => {
        await client.publish(
          `${subjectPrefix}.${conversation.id}`,
          JSON.stringify({
            id: `${namespace}-reply-${value.id}-${suffix}`,
            conversationId: conversation.id,
            role: 'assistant',
            author: conversation.assistant,
            body,
            sentAt: new Date().toISOString(),
            clientId: `${namespace}-assistant`,
          })
        )
      }

      await delay(180)
      await publishReply('I’m on it.', 'ack')
      await delay(420)
      await publishReply(
        `I replayed the conversation context and picked up your message: “${String(value.body).slice(0, 96)}${String(value.body).length > 96 ? '…' : ''}” The durable feed and the live notification path agree.`,
        'answer'
      )
    }
  })()
  return { subscription, task }
}

export async function runChatExample({
  appDirectory,
  appUrl,
  label,
  namespace,
  stream,
  subjectPrefix,
  viteConfig,
}) {
  const streamSubjects = `${subjectPrefix}.>`
  let vite
  let connection
  let manager
  let responder
  let createdStream = false
  let startedFixtures = false
  let stopping = false

  const cleanup = async () => {
    if (stopping) return
    stopping = true
    responder?.subscription.unsubscribe()
    await stopChild(vite).catch(() => undefined)
    if (createdStream) await manager?.streams.delete(stream).catch(() => undefined)
    await connection?.drain().catch(() => undefined)
    if (startedFixtures) execFileSync('pnpm', ['nats:down'], { cwd: root, stdio: 'inherit' })
  }

  try {
    process.stdout.write(`\nNATSail ${label} chat lab — realistic replay and live rendering\n`)
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
    if (streamInfo.state.messages === 0)
      await seedMessages(jetstream(connection), namespace, subjectPrefix)
    responder = startAssistantResponder(connection, jetstream(connection), namespace, subjectPrefix)

    vite = spawn(
      process.execPath,
      [viteBin, appDirectory, '--config', viteConfig, '--host', '127.0.0.1'],
      { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] }
    )
    vite.stdout.on('data', (chunk) => process.stdout.write(`[app] ${chunk}`))
    vite.stderr.on('data', (chunk) => process.stderr.write(`[app] ${chunk}`))

    await waitFor(async () => {
      if (vite.exitCode !== null) throw new Error('Vite exited during startup')
      const response = await fetch(appUrl, { signal: AbortSignal.timeout(500) })
      return response.ok
    }, `the ${label} chat example`)

    process.stdout.write(`\nREADY ${appUrl}\n`)
    process.stdout.write('Switch conversations to replay different history sizes.\n')
    process.stdout.write(
      'Open another tab for live update notifications, or run the busy-room burst.\n'
    )
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
    await responder?.task.catch(() => undefined)
  }
}
