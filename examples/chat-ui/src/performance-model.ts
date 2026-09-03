export type DemoAdapter = 'effect' | 'rxjs'

export type DemoConversationTone = 'amber' | 'blue' | 'green' | 'rose'

export interface DemoConversation {
  readonly id: string
  readonly title: string
  readonly summary: string
  readonly assistant: string
  readonly initials: string
  readonly tone: DemoConversationTone
  readonly seededMessages: number
  readonly updatedAt: string
}

export interface DemoChatMessage {
  readonly id: string
  readonly conversationId: string
  readonly role: 'assistant' | 'user'
  readonly author: string
  readonly body: string
  readonly sentAt: string
  readonly clientId: string
}

export interface DemoChatEntry {
  readonly message: DemoChatMessage
  readonly cursor: number
}

export interface DemoConversationActivity {
  readonly preview: string
  readonly updatedAt: string
  readonly unread: number
}

export interface DemoPerformanceMetrics {
  readonly historyEvents: number
  readonly historyReadyMs?: number
  readonly historyRenderedMs?: number
  readonly stateUpdates: number
  readonly reactCommits: number
  readonly lastBatchSize: number
  readonly largestBatchSize: number
  readonly lastCommitMs?: number
  readonly largestCommitMs?: number
  readonly telemetryMeasurements: number
  readonly replayTelemetryMs?: number
  readonly lastHandlerTelemetryMs?: number
  readonly averagePublishTelemetryMs?: number
  readonly bufferSignals: number
}

export interface DemoUpdateNotice {
  readonly id: string
  readonly conversationId: string
  readonly title: string
  readonly body: string
}

export type DemoConversationPhase = 'connecting' | 'error' | 'live' | 'replaying'

export const demoConversations: readonly DemoConversation[] = [
  {
    id: 'durable-chat',
    title: 'Designing durable chat history',
    summary: 'Replay boundaries, message ordering, and a very long working session',
    assistant: 'Nora',
    initials: 'DH',
    tone: 'amber',
    seededMessages: 240,
    updatedAt: '2026-09-01T15:42:00.000Z',
  },
  {
    id: 'launch-readiness',
    title: 'Launch readiness review',
    summary: 'Release notes, package checks, and the last few rough edges',
    assistant: 'Mara',
    initials: 'LR',
    tone: 'blue',
    seededMessages: 96,
    updatedAt: '2026-09-01T14:18:00.000Z',
  },
  {
    id: 'agent-handoff',
    title: 'Agent handoff notes',
    summary: 'A medium thread with delegated work and concise progress updates',
    assistant: 'Sol',
    initials: 'AH',
    tone: 'green',
    seededMessages: 48,
    updatedAt: '2026-09-01T11:06:00.000Z',
  },
  {
    id: 'tiny-follow-up',
    title: 'Tiny follow-up',
    summary: 'A short conversation for the small-history baseline',
    assistant: 'June',
    initials: 'TF',
    tone: 'rose',
    seededMessages: 8,
    updatedAt: '2026-08-31T22:40:00.000Z',
  },
  {
    id: 'research-archive',
    title: 'Research synthesis archive',
    summary: 'One thousand messages from a long-running research and planning thread',
    assistant: 'Iris',
    initials: '1K',
    tone: 'blue',
    seededMessages: 1_000,
    updatedAt: '2026-08-31T19:24:00.000Z',
  },
  {
    id: 'incident-archive',
    title: 'Full incident archive',
    summary: 'Five thousand messages for the large-history browser benchmark',
    assistant: 'Vale',
    initials: '5K',
    tone: 'rose',
    seededMessages: 5_000,
    updatedAt: '2026-08-30T21:12:00.000Z',
  },
]

export const isDemoChatMessage = (value: unknown): value is DemoChatMessage => {
  if (!value || typeof value !== 'object') return false
  const message = value as Partial<DemoChatMessage>
  return (
    typeof message.id === 'string' &&
    typeof message.conversationId === 'string' &&
    (message.role === 'assistant' || message.role === 'user') &&
    typeof message.author === 'string' &&
    typeof message.body === 'string' &&
    typeof message.sentAt === 'string' &&
    typeof message.clientId === 'string'
  )
}
