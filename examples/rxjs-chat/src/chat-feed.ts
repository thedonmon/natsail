import { type Subscription } from 'rxjs'

import { natsCodecs, type NatsPayloadCodec, type NatsRuntime } from '@natsail/core'
import {
  demoConversations,
  isDemoChatMessage,
  type DemoChatEntry,
  type DemoChatMessage,
  type DemoConversationActivity,
  type DemoConversationPhase,
  type DemoPerformanceMetrics,
  type DemoUpdateNotice,
} from '@natsail/example-chat-ui'
import { defineReducingJetStreamSession, type JetStreamDelivery } from '@natsail/jetstream'
import { observeNatsCoreSubscription, observeNatsJetStreamState } from '@natsail/rxjs'
import type { SessionRegistry } from '@natsail/session'

import { readPerformanceTelemetry, resetPerformanceTelemetry } from './runtime'

export const chatStream = 'NATSAIL_RXJS_CHAT'
export const chatSubjectPrefix = 'natsail.examples.rxjs.chat'

const jsonCodec = natsCodecs.json<unknown>()
const chatCodec: NatsPayloadCodec<DemoChatMessage> = {
  encode: (message) => jsonCodec.encode(message),
  decode: (data) => {
    const value = jsonCodec.decode(data)
    if (!isDemoChatMessage(value)) throw new Error('Received an invalid RxJS chat message')
    return value
  },
}

interface ConversationModel {
  readonly entries: readonly DemoChatEntry[]
}

export interface RxjsChatState {
  readonly activeConversationId: string
  readonly activity: Readonly<Record<string, DemoConversationActivity>>
  readonly entries: readonly DemoChatEntry[]
  readonly phase: DemoConversationPhase
  readonly metrics: DemoPerformanceMetrics
  readonly revision: number
  readonly notice: DemoUpdateNotice | undefined
}

const initialMetrics = (): DemoPerformanceMetrics => ({
  historyEvents: 0,
  stateUpdates: 0,
  reactCommits: 0,
  lastBatchSize: 0,
  largestBatchSize: 0,
  telemetryMeasurements: 0,
  bufferSignals: 0,
})

const initialActivity = (): Record<string, DemoConversationActivity> =>
  Object.fromEntries(
    demoConversations.map((conversation) => [
      conversation.id,
      {
        preview: conversation.summary,
        updatedAt: conversation.updatedAt,
        unread: 0,
      },
    ])
  )

const selectedFromUrl = (): string => {
  const candidate = new URL(window.location.href).searchParams.get('conversation')
  return demoConversations.some((conversation) => conversation.id === candidate)
    ? (candidate as string)
    : demoConversations[0]!.id
}

const reduceConversation = (
  state: ConversationModel,
  delivery: JetStreamDelivery<DemoChatMessage>
): ConversationModel => ({
  entries: [...state.entries, { message: delivery.value, cursor: delivery.cursor.sequence }].slice(
    -10_000
  ),
})

export class RxjsChatController {
  private readonly listeners = new Set<() => void>()
  private activeSubscription?: Subscription
  private notificationSubscription?: Subscription
  private loadStartedAt = performance.now()
  private previousEntryCount = 0
  private closed = false
  private lastCommittedRevision = -1
  private state: RxjsChatState = {
    activeConversationId: selectedFromUrl(),
    activity: initialActivity(),
    entries: [],
    phase: 'connecting',
    metrics: initialMetrics(),
    revision: 0,
    notice: undefined,
  }

  constructor(
    private readonly runtime: NatsRuntime,
    private readonly sessions: SessionRegistry,
    readonly clientId: string
  ) {
    this.startNotifications()
    this.selectConversation(this.state.activeConversationId, false)
  }

  readonly getSnapshot = (): RxjsChatState => this.state

  readonly subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  selectConversation = (conversationId: string, updateUrl = true): void => {
    if (!demoConversations.some((conversation) => conversation.id === conversationId)) return
    this.activeSubscription?.unsubscribe()
    resetPerformanceTelemetry()
    this.loadStartedAt = performance.now()
    this.previousEntryCount = 0
    this.patch({
      activeConversationId: conversationId,
      entries: [],
      phase: 'replaying',
      metrics: initialMetrics(),
      activity: {
        ...this.state.activity,
        [conversationId]: { ...this.state.activity[conversationId]!, unread: 0 },
      },
    })

    if (updateUrl) {
      const url = new URL(window.location.href)
      url.searchParams.set('conversation', conversationId)
      window.history.replaceState(null, '', url)
    }

    const definition = defineReducingJetStreamSession(
      this.runtime,
      `rxjs-performance-chat:${conversationId}`,
      {
        stream: chatStream,
        filter: `${chatSubjectPrefix}.${conversationId}`,
        start: 'all',
        maxBufferedMessages: 64,
        duplicateDeliveryPolicy: 'drop',
        recovery: { delayMs: 250 },
        codec: chatCodec,
      },
      {
        scope: 'performance-chat:v1',
        initial: () => ({ entries: [] }),
        reduce: reduceConversation,
      }
    )

    this.activeSubscription = observeNatsJetStreamState(this.sessions, definition, {
      liveBatchMs: 16,
    }).subscribe({
      next: (snapshot) => {
        if (snapshot.phase !== 'live') return
        const batchSize = Math.max(0, snapshot.data.entries.length - this.previousEntryCount)
        this.previousEntryCount = snapshot.data.entries.length
        const firstLive = this.state.phase !== 'live'
        const last = snapshot.data.entries.at(-1)?.message
        this.patch({
          entries: snapshot.data.entries,
          phase: 'live',
          metrics: {
            ...this.state.metrics,
            historyEvents: snapshot.replay.delivered,
            ...(firstLive ? { historyReadyMs: performance.now() - this.loadStartedAt } : {}),
            stateUpdates: this.state.metrics.stateUpdates + 1,
            lastBatchSize: batchSize,
            largestBatchSize: firstLive
              ? 0
              : Math.max(this.state.metrics.largestBatchSize, batchSize),
            ...readPerformanceTelemetry(),
          },
          ...(last
            ? {
                activity: {
                  ...this.state.activity,
                  [conversationId]: {
                    preview: last.body,
                    updatedAt: last.sentAt,
                    unread: 0,
                  },
                },
              }
            : {}),
        })
      },
      error: () => this.patch({ phase: 'error' }),
    })
  }

  send = async (body: string): Promise<void> => {
    const message: DemoChatMessage = {
      id: `rxjs-user-${crypto.randomUUID()}`,
      conversationId: this.state.activeConversationId,
      role: 'user',
      author: 'You',
      body,
      sentAt: new Date().toISOString(),
      clientId: this.clientId,
    }
    await this.runtime.publish(
      `${chatSubjectPrefix}.${message.conversationId}`,
      chatCodec.encode(message)
    )
    this.patch({
      metrics: { ...this.state.metrics, ...readPerformanceTelemetry() },
    })
  }

  busyBurst = async (count: number): Promise<void> => {
    const conversation = this.state.activeConversationId
    const startedAt = Date.now()
    await Promise.all(
      Array.from({ length: count }, (_, index) => {
        const message: DemoChatMessage = {
          id: `rxjs-burst-${crypto.randomUUID()}`,
          conversationId: conversation,
          role: 'assistant',
          author: 'Background agents',
          body: `Background task ${String(index + 1).padStart(4, '0')} finished and published its compact progress update.`,
          sentAt: new Date(startedAt + index).toISOString(),
          clientId: 'rxjs-busy-room',
        }
        return this.runtime.publish(
          `${chatSubjectPrefix}.${conversation}`,
          chatCodec.encode(message)
        )
      })
    )
    this.patch({
      metrics: { ...this.state.metrics, ...readPerformanceTelemetry() },
    })
  }

  roomUpdate = async (): Promise<void> => {
    const conversation = demoConversations.find(
      (candidate) => candidate.id !== this.state.activeConversationId
    )!
    const message: DemoChatMessage = {
      id: `rxjs-room-update-${crypto.randomUUID()}`,
      conversationId: conversation.id,
      role: 'assistant',
      author: conversation.assistant,
      body: 'A new room update arrived through the shared NATS subscription.',
      sentAt: new Date().toISOString(),
      clientId: 'rxjs-room-notification-test',
    }
    await this.runtime.publish(`${chatSubjectPrefix}.${conversation.id}`, chatCodec.encode(message))
    this.patch({
      metrics: { ...this.state.metrics, ...readPerformanceTelemetry() },
    })
  }

  dismissNotice = (): void => this.patch({ notice: undefined })

  recordReactCommit = (revision: number, duration?: number): void => {
    if (revision === this.lastCommittedRevision) return
    this.lastCommittedRevision = revision
    const firstLiveRender =
      this.state.phase === 'live' && this.state.metrics.historyRenderedMs === undefined
    this.patch(
      {
        metrics: {
          ...this.state.metrics,
          ...(firstLiveRender ? { historyRenderedMs: performance.now() - this.loadStartedAt } : {}),
          reactCommits: this.state.metrics.reactCommits + 1,
          ...(duration === undefined
            ? {}
            : {
                lastCommitMs: duration,
                largestCommitMs: Math.max(this.state.metrics.largestCommitMs ?? 0, duration),
              }),
        },
      },
      false
    )
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    this.activeSubscription?.unsubscribe()
    this.notificationSubscription?.unsubscribe()
  }

  private startNotifications(): void {
    this.notificationSubscription = observeNatsCoreSubscription(
      this.sessions,
      this.runtime,
      'rxjs-performance-chat:notifications',
      {
        subject: `${chatSubjectPrefix}.>`,
        codec: chatCodec,
      }
    ).subscribe({ next: (message) => this.receiveNotification(message) })
  }

  private receiveNotification(message: DemoChatMessage): void {
    const previous = this.state.activity[message.conversationId]
    if (!previous) return
    const inactive = message.conversationId !== this.state.activeConversationId
    const fromAnotherTab = message.role === 'user' && message.clientId !== this.clientId
    const arrivedWhileAway = document.hidden && message.clientId !== this.clientId
    if (!inactive && !fromAnotherTab && !arrivedWhileAway) return
    const shouldNotify = inactive || arrivedWhileAway || fromAnotherTab
    const conversation = demoConversations.find((item) => item.id === message.conversationId)!

    this.patch({
      activity: {
        ...this.state.activity,
        [message.conversationId]: {
          preview: message.body,
          updatedAt: message.sentAt,
          unread: inactive ? previous.unread + 1 : 0,
        },
      },
      ...(shouldNotify
        ? {
            notice: {
              id: message.id,
              conversationId: message.conversationId,
              title: inactive ? `${conversation.title} was updated` : 'Another tab sent a message',
              body: message.body,
            },
          }
        : {}),
    })
  }

  private patch(patch: Partial<RxjsChatState>, incrementRevision = true): void {
    if (this.closed) return
    this.state = {
      ...this.state,
      ...patch,
      revision: incrementRevision ? this.state.revision + 1 : this.state.revision,
    }
    for (const listener of this.listeners) listener()
  }
}
