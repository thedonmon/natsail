// REPOSITORY EXAMPLE — one-command Effect chat lab over conversation-scoped JetStream feeds.

import { fileURLToPath } from 'node:url'

import { runChatExample } from '../chat-scenario.mjs'

await runChatExample({
  appDirectory: fileURLToPath(new URL('.', import.meta.url)),
  appUrl: 'http://127.0.0.1:4178',
  label: 'Effect',
  namespace: 'effect',
  stream: 'NATSAIL_EFFECT_CHAT',
  subjectPrefix: 'natsail.examples.effect.chat',
  viteConfig: fileURLToPath(new URL('./vite.config.ts', import.meta.url)),
})
