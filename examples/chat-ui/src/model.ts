export type ConnectionPhase =
  | 'booting'
  | 'catching-up'
  | 'connecting'
  | 'error'
  | 'gap'
  | 'live'
  | 'offline'

export interface Room {
  id: string
  label: string
  purpose: string
  signal: string
}

export interface ChatMessage {
  id: string
  roomId: string
  author: string
  body: string
  sentAt: string
  clientId: string
}

export interface TimelineEntry {
  message: ChatMessage
  cursor?: number
  delivery: 'applied' | 'failed' | 'sending'
}

export interface TimelineState {
  messages: TimelineEntry[]
  cursor: number
  phase: ConnectionPhase
  catchUpCount: number
  gatewayCursor?: number
  retainedFrom?: number
  retainedThrough?: number
  instanceId?: string
  diagnostic?: string
}

export const rooms: Room[] = [
  {
    id: 'general',
    label: 'General signal',
    purpose: 'Cross-team notes and pulse checks',
    signal: 'GEN',
  },
  {
    id: 'gateway-lab',
    label: 'Gateway lab',
    purpose: 'Durable Object and replay experiments',
    signal: 'GWY',
  },
  {
    id: 'edge-cases',
    label: 'Edge cases',
    purpose: 'Failure modes, races, and odd packets',
    signal: '404',
  },
  {
    id: 'release',
    label: 'Release line',
    purpose: 'What is proven enough to promote',
    signal: 'REL',
  },
]

export const isChatMessage = (value: unknown): value is ChatMessage => {
  if (!value || typeof value !== 'object') return false
  const message = value as Partial<ChatMessage>
  return (
    typeof message.id === 'string' &&
    typeof message.roomId === 'string' &&
    typeof message.author === 'string' &&
    typeof message.body === 'string' &&
    typeof message.sentAt === 'string' &&
    typeof message.clientId === 'string'
  )
}

export const phaseLabel = (phase: ConnectionPhase): string => phase.replace('-', ' ').toUpperCase()
