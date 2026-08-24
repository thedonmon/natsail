// REPOSITORY EXAMPLE — one-command AI SDK and TanStack AI chat experience.

import { execFileSync, spawn } from 'node:child_process'
import { once } from 'node:events'
import { fileURLToPath } from 'node:url'

import { connect } from '@nats-io/transport-node'
import { jetstream, jetstreamManager, StorageType } from '@nats-io/jetstream'
import { createChat as createAiSdkChat } from '@shadcn/helpers/ai-sdk'
import { createChat as createTanStackChat } from '@shadcn/helpers/tanstack-ai'
import { natsCodecs } from '@natsail/core'

const root = fileURLToPath(new URL('../..', import.meta.url))
const appDirectory = fileURLToPath(new URL('.', import.meta.url))
const viteBin = fileURLToPath(new URL('../../node_modules/vite/bin/vite.js', import.meta.url))
const viteConfig = fileURLToPath(new URL('./vite.config.ts', import.meta.url))
const appUrl = 'http://127.0.0.1:4176'
const requestSubject = 'natsail.examples.ai.requests'
const responseStream = 'NATSAIL_AI_RESPONSES'
const responseStreamSubjects = 'natsail.examples.ai.responses.jetstream.>'
const conversationStream = 'NATSAIL_AI_CONVERSATIONS'
const conversationSubject = 'natsail.examples.ai.conversations.release-room'
const jsonCodec = natsCodecs.json()
const gatewayPrompt = 'Help me plan the gateway release.'
const reconnectPrompt = "What happens if the connection drops while you're answering?"

const createScriptedChat = (createChat, delivery) =>
  createChat()
    .user(gatewayPrompt)
    .assistant(({ writer }) => {
      writer.reasoning(
        'I should turn the transport guarantees into a release plan that describes what a person will actually experience.'
      )
      writer.text(
        'Start with the path users feel: open a conversation, send a message, and watch the answer stream without the interface jumping. Keep the NATS connection, framework adapter, and cursor mechanics behind that experience.\n\n'
      )
      writer.sleep(240)
      writer.text(
        'Before release, interrupt an answer on purpose. The transcript should stay stable, the connection state should change in place, and the composer should recover without asking the user to reload.'
      )
    })
    .user(reconnectPrompt)
    .assistant(({ writer }) => {
      if (delivery === 'jetstream') {
        writer.reasoning(
          'JetStream is retaining the native stream, so I should explain cursor-based gap recovery without claiming page-reload state restoration.'
        )
        writer.text(
          'While this answer streams, JetStream retains every native frame on the reply subject. The browser can disconnect without turning those in-flight deltas into a permanent hole.\n\n'
        )
        writer.sleep(240)
        writer.text(
          'After the WebSocket reconnects, the ordered consumer resumes strictly after its last processed cursor. The active transcript continues in place; restoring the same chat after a full page reload would also require persisted framework and run state.'
        )
      } else {
        writer.reasoning(
          'Core NATS reconnects live subscriptions, so I need to distinguish that from replaying chunks missed during the gap.'
        )
        writer.text(
          'The live connection can rejoin while this answer is still streaming. Messages already rendered remain in the transcript and new live chunks continue after reconnect.\n\n'
        )
        writer.sleep(240)
        writer.text(
          'The honest limitation is the gap itself: Core NATS does not replay deltas published while the browser was disconnected. Durable resume needs a stable run cursor plus JetStream or the gateway retained log.'
        )
      }
    })

const transportOptions = {
  delayMs: 55,
  fallback:
    'This deterministic conversation has reached its final scripted turn. Start a fresh framework session to replay it.',
}
const aiSdkTransports = {
  core: createScriptedChat(createAiSdkChat, 'core').transport(transportOptions),
  jetstream: createScriptedChat(createAiSdkChat, 'jetstream').transport(transportOptions),
}
const tanStackTransports = {
  core: createScriptedChat(createTanStackChat, 'core').transport(transportOptions),
  jetstream: createScriptedChat(createTanStackChat, 'jetstream').transport(transportOptions),
}

let vite
let responder
let responderJetStream
let responderManager
let responderSubscription
let createdResponseStream = false
let createdConversationStream = false
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

const publishFrame = async (subject, frame, delivery) => {
  if (stopping) return
  const data = jsonCodec.encode({ ...frame, publishedAt: Date.now() })
  if (delivery === 'jetstream') {
    await responderJetStream.publish(subject, data)
  } else {
    responder.publish(subject, data)
  }
}

const handleRequest = async (message) => {
  let request
  try {
    request = jsonCodec.decode(message.data)
    if (!request || typeof request.replySubject !== 'string') {
      throw new Error('Request is missing a reply subject')
    }
    if (request.delivery !== 'core' && request.delivery !== 'jetstream') {
      throw new Error(`Unknown delivery mode ${String(request.delivery)}`)
    }

    if (request.framework === 'ai-sdk') {
      const stream = await aiSdkTransports[request.delivery].sendMessages({
        ...request.payload,
        abortSignal: undefined,
      })
      for await (const chunk of stream) {
        await publishFrame(request.replySubject, { type: 'chunk', chunk }, request.delivery)
      }
    } else if (request.framework === 'tanstack-ai') {
      const stream = tanStackTransports[request.delivery].connect(
        request.payload.messages,
        request.payload.data,
        undefined,
        request.payload.runContext
      )
      for await (const chunk of stream) {
        await publishFrame(request.replySubject, { type: 'chunk', chunk }, request.delivery)
      }
    } else {
      throw new Error(`Unknown framework ${String(request.framework)}`)
    }

    await publishFrame(request.replySubject, { type: 'end' }, request.delivery)
    if (!stopping) await responder.flush()
  } catch (error) {
    if (stopping) return
    if (request?.replySubject) {
      try {
        await publishFrame(
          request.replySubject,
          {
            type: 'error',
            message: error instanceof Error ? error.message : String(error),
          },
          request.delivery === 'jetstream' ? 'jetstream' : 'core'
        )
        await responder.flush()
      } catch (publishError) {
        if (!stopping) {
          process.stderr.write(
            `[responder] ${publishError instanceof Error ? publishError.stack : String(publishError)}\n`
          )
        }
      }
    } else {
      process.stderr.write(`[responder] ${error instanceof Error ? error.stack : String(error)}\n`)
    }
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
  responderSubscription?.unsubscribe()
  if (createdResponseStream) {
    await responderManager?.streams.delete(responseStream).catch(() => undefined)
  }
  if (createdConversationStream) {
    await responderManager?.streams.delete(conversationStream).catch(() => undefined)
  }
  await responder?.drain().catch(() => undefined)
  if (startedFixtures) {
    execFileSync('pnpm', ['nats:down'], { cwd: root, stdio: 'inherit' })
  }
}

try {
  process.stdout.write(
    '\nNATSail AI chat example — AI SDK and TanStack AI over Core NATS or JetStream\n'
  )
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

  responder = await connect({ servers: 'nats://127.0.0.1:4223' })
  responderManager = await jetstreamManager(responder)
  try {
    await responderManager.streams.info(responseStream)
  } catch {
    await responderManager.streams.add({
      name: responseStream,
      subjects: [responseStreamSubjects],
      storage: StorageType.Memory,
    })
    createdResponseStream = true
  }
  let conversationInfo
  try {
    conversationInfo = await responderManager.streams.info(conversationStream)
  } catch {
    await responderManager.streams.add({
      name: conversationStream,
      subjects: [conversationSubject],
      storage: StorageType.Memory,
    })
    createdConversationStream = true
    conversationInfo = await responderManager.streams.info(conversationStream)
  }
  responderJetStream = jetstream(responder)
  if (conversationInfo.state.messages === 0) {
    const history = [
      {
        type: 'message',
        id: 'release-room-1',
        phase: 'history',
        role: 'user',
        author: 'You',
        text: 'Can we make reconnect behavior obvious enough to trust before release?',
      },
      {
        type: 'message',
        id: 'release-room-2',
        phase: 'history',
        role: 'assistant',
        author: 'NATSail Assistant',
        text: 'Yes. Load this conversation from JetStream, keep its stream sequence visible, and publish while the browser is offline.',
      },
      {
        type: 'history-ready',
        id: 'release-room-ready',
      },
    ]
    for (const event of history) {
      await responderJetStream.publish(conversationSubject, jsonCodec.encode(event))
    }
  }
  responderSubscription = responder.subscribe(requestSubject, {
    callback: (error, message) => {
      if (error) process.stderr.write(`[responder] ${error.message}\n`)
      else void handleRequest(message)
    },
  })
  await responder.flush()

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
  }, 'the AI transport example')

  process.stdout.write(`\nREADY ${appUrl}\n`)
  process.stdout.write(
    'No API key or model is required; shadcn helpers generate both native streams.\n'
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
}
