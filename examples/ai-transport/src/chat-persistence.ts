import type { UIMessage as AiSdkMessage } from 'ai'
import type { ChatClientPersistence, UIMessage as TanStackMessage } from '@tanstack/ai-client'

import type { DeliveryKind, FrameworkKind } from './transports'

const storagePrefix = 'natsail:ai-example:v1'
const activeRunMaxAgeMs = 5 * 60_000

export const aiSdkChatId = 'natsail-ai-sdk-chat'
export const tanStackChatId = 'natsail-tanstack-ai-chat'

export interface ActiveChatRun {
  framework: FrameworkKind
  delivery: DeliveryKind
  chatId: string
  replySubject: string
  startedAt: number
}

export interface ChatPreferences {
  framework: FrameworkKind
  delivery: DeliveryKind
  duplicatePolicy: 'drop' | 'deliver' | 'error'
}

const localStorageValue = (key: string): string | null => {
  try {
    return window.localStorage.getItem(`${storagePrefix}:${key}`)
  } catch {
    return null
  }
}

const setLocalStorageValue = (key: string, value: unknown): void => {
  try {
    window.localStorage.setItem(`${storagePrefix}:${key}`, JSON.stringify(value))
  } catch {
    // Persistence is a recovery enhancement. The chat still works when storage is unavailable.
  }
}

const removeLocalStorageValue = (key: string): void => {
  try {
    window.localStorage.removeItem(`${storagePrefix}:${key}`)
  } catch {
    // See setLocalStorageValue.
  }
}

const readJson = <T>(key: string): T | undefined => {
  const stored = localStorageValue(key)
  if (!stored) return undefined
  try {
    return JSON.parse(stored) as T
  } catch {
    removeLocalStorageValue(key)
    return undefined
  }
}

const messagesKey = (framework: FrameworkKind, delivery: DeliveryKind): string =>
  `messages:${framework}:${delivery}`

const activeRunKey = (framework: FrameworkKind, delivery: DeliveryKind): string =>
  `active-run:${framework}:${delivery}`

export const getOrCreateClientId = (): string => {
  const existing = readJson<unknown>('client-id')
  if (typeof existing === 'string' && existing.length > 0) return existing
  const clientId = `ai-chat-${crypto.randomUUID().slice(0, 8)}`
  setLocalStorageValue('client-id', clientId)
  return clientId
}

export const loadPreferences = (): ChatPreferences => {
  const value = readJson<Partial<ChatPreferences>>('preferences')
  return {
    framework:
      value?.framework === 'tanstack-ai' || value?.framework === 'ai-sdk'
        ? value.framework
        : 'ai-sdk',
    delivery:
      value?.delivery === 'core' || value?.delivery === 'jetstream' ? value.delivery : 'jetstream',
    duplicatePolicy:
      value?.duplicatePolicy === 'deliver' || value?.duplicatePolicy === 'error'
        ? value.duplicatePolicy
        : 'drop',
  }
}

export const savePreferences = (preferences: ChatPreferences): void => {
  setLocalStorageValue('preferences', preferences)
}

export const loadAiSdkMessages = (delivery: DeliveryKind): AiSdkMessage[] => {
  const messages = readJson<unknown>(messagesKey('ai-sdk', delivery))
  return Array.isArray(messages) ? (messages as AiSdkMessage[]) : []
}

export const saveAiSdkMessages = (delivery: DeliveryKind, messages: AiSdkMessage[]): void => {
  setLocalStorageValue(messagesKey('ai-sdk', delivery), messages)
}

export const tanStackPersistence = (delivery: DeliveryKind): ChatClientPersistence => ({
  getItem: () => {
    const messages = readJson<unknown>(messagesKey('tanstack-ai', delivery))
    if (!Array.isArray(messages)) return []
    return (messages as TanStackMessage[]).map((message) => ({
      ...message,
      ...(typeof message.createdAt === 'string' ? { createdAt: new Date(message.createdAt) } : {}),
    }))
  },
  setItem: (_id, messages) => {
    setLocalStorageValue(messagesKey('tanstack-ai', delivery), messages)
  },
  removeItem: () => {
    removeLocalStorageValue(messagesKey('tanstack-ai', delivery))
  },
})

export const saveActiveRun = (run: ActiveChatRun): void => {
  setLocalStorageValue(activeRunKey(run.framework, run.delivery), run)
}

export const loadActiveRun = (
  framework: FrameworkKind,
  delivery: DeliveryKind
): ActiveChatRun | undefined => {
  const key = activeRunKey(framework, delivery)
  const value = readJson<Partial<ActiveChatRun>>(key)
  const valid =
    value?.framework === framework &&
    value.delivery === delivery &&
    typeof value.chatId === 'string' &&
    typeof value.replySubject === 'string' &&
    typeof value.startedAt === 'number'
  if (!valid || Date.now() - value.startedAt! > activeRunMaxAgeMs) {
    removeLocalStorageValue(key)
    return undefined
  }
  return value as ActiveChatRun
}

export const clearActiveRun = (framework: FrameworkKind, delivery: DeliveryKind): void => {
  removeLocalStorageValue(activeRunKey(framework, delivery))
}
