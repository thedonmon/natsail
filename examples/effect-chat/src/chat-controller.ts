import { Effect, Stream } from 'effect'

import { natsCodecs, type NatsPayloadCodec } from '@natsail/core'
import type { NatsailService } from '@natsail/effect'
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
import type { JetStreamDelivery } from '@natsail/jetstream'

export const chatStream = 'NATSAIL_EFFECT_CHAT'
export const chatSubjectPrefix = 'natsail.examples.effect.chat'

const jsonCodec = natsCodecs.json<unknown>()
const chatCodec: NatsPayloadCodec<DemoChatMessage> = {
  encode: (message) => jsonCodec.encode(message),
  decode: (data) => {
    const value = jsonCodec.decode(data)
    if (!isDemoChatMessage(value)) throw new Error('Received an invalid Effect chat message')
    return value
  },
}

interface ConversationModel {
  readonly entries: readonly DemoChatEntry[]
}

export interface EffectChatState {
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

const reduceConversationBatch = (
  state: ConversationModel,
  deliveries: readonly JetStreamDelivery<DemoChatMessage>[]
): ConversationModel => ({
  entries: [
    ...state.entries,
    ...deliveries.map((delivery) => ({
      message: delivery.value,
      cursor: delivery.cursor.sequence,
    })),
  ].slice(-600),
})

export class EffectChatController {
  private readonly listeners = new Set<() => void>()
  private activeAbort?: AbortController
  private readonly notificationAbort = new AbortController()
  private loadStartedAt = performance.now()
  private previousEntryCount = 0
  private closed = false
  private lastCommittedRevision = -1
  private state: EffectChatState = {
    activeConversationId: selectedFromUrl(),
    activity: initialActivity(),
    entries: [],
    phase: 'connecting',
    metrics: initialMetrics(),
    revision: 0,
    notice: undefined,
  }

  constructor(
    private readonly natsail: NatsailService,
    readonly clientId: string
  ) {
    this.startNotifications()
    this.selectConversation(this.state.activeConversationId, false)
  }

  readonly getSnapshot = (): EffectChatState => this.state

  readonly subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  selectConversation = (conversationId: string, updateUrl = true): void => {
    if (!demoConversations.some((conversation) => conversation.id === conversationId)) return
    this.activeAbort?.abort()
    const abort = new AbortController()
    this.activeAbort = abort
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

    const snapshots = this.natsail.materializeJetStream(
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
        initial: (): ConversationModel => ({ entries: [] }),
        reduceBatch: (state, deliveries) =>
          Effect.sync(() => reduceConversationBatch(state, deliveries)),
      },
      {
        bufferSize: 256,
        batchSize: 256,
        batchWithin: '16 millis',
      }
    )

    void Effect.runPromise(
      snapshots.pipe(
        Stream.runForEach((snapshot) =>
          Effect.sync(() => {
            if (snapshot.phase !== 'live' || abort.signal.aborted) return
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
                largestBatchSize: Math.max(this.state.metrics.largestBatchSize, batchSize),
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
          })
        )
      ),
      { signal: abort.signal }
    ).catch(() => {
      if (!abort.signal.aborted) this.patch({ phase: 'error' })
    })
  }

  send = async (body: string): Promise<void> => {
    const message: DemoChatMessage = {
      id: `effect-user-${crypto.randomUUID()}`,
      conversationId: this.state.activeConversationId,
      role: 'user',
      author: 'You',
      body,
      sentAt: new Date().toISOString(),
      clientId: this.clientId,
    }
    await Effect.runPromise(
      this.natsail.publish(
        `${chatSubjectPrefix}.${message.conversationId}`,
        chatCodec.encode(message)
      )
    )
  }

  busyBurst = async (): Promise<void> => {
    const conversation = this.state.activeConversationId
    const startedAt = Date.now()
    const effects = Array.from({ length: 40 }, (_, index) => {
      const message: DemoChatMessage = {
        id: `effect-burst-${crypto.randomUUID()}`,
        conversationId: conversation,
        role: 'assistant',
        author: 'Background agents',
        body: `Background task ${String(index + 1).padStart(2, '0')} finished and published its compact progress update.`,
        sentAt: new Date(startedAt + index).toISOString(),
        clientId: 'effect-busy-room',
      }
      return this.natsail.publish(`${chatSubjectPrefix}.${conversation}`, chatCodec.encode(message))
    })
    await Effect.runPromise(Effect.all(effects, { concurrency: 'unbounded', discard: true }))
  }

  dismissNotice = (): void => this.patch({ notice: undefined })

  recordReactCommit = (revision: number, _duration: number): void => {
    if (revision === this.lastCommittedRevision) return
    this.lastCommittedRevision = revision
    this.patch(
      {
        metrics: {
          ...this.state.metrics,
          reactCommits: this.state.metrics.reactCommits + 1,
        },
      },
      false
    )
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    this.activeAbort?.abort()
    this.notificationAbort.abort()
  }

  private startNotifications(): void {
    const notifications = this.natsail.subscribe(
      {
        subject: `${chatSubjectPrefix}.>`,
        codec: chatCodec,
      },
      { bufferSize: 256, overflowStrategy: 'suspend' }
    )
    void Effect.runPromise(
      notifications.pipe(
        Stream.runForEach((message) => Effect.sync(() => this.receiveNotification(message)))
      ),
      { signal: this.notificationAbort.signal }
    ).catch(() => undefined)
  }

  private receiveNotification(message: DemoChatMessage): void {
    const previous = this.state.activity[message.conversationId]
    if (!previous) return
    const inactive = message.conversationId !== this.state.activeConversationId
    const fromAnotherTab = message.role === 'user' && message.clientId !== this.clientId
    const shouldNotify =
      inactive || (document.hidden && message.clientId !== this.clientId) || fromAnotherTab
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

  private patch(patch: Partial<EffectChatState>, incrementRevision = true): void {
    if (this.closed) return
    this.state = {
      ...this.state,
      ...patch,
      revision: incrementRevision ? this.state.revision + 1 : this.state.revision,
    }
    for (const listener of this.listeners) listener()
  }
}
