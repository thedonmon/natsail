import { BehaviorSubject } from 'rxjs'

import { natsCodecs, type NatsPayloadCodec, type NatsRuntime } from '@natsail/core'
import { isChatMessage, type ChatMessage, type TimelineEntry } from '@natsail/example-chat-ui'
import {
  defineReducingJetStreamSession,
  type JetStreamDelivery,
  type JetStreamStateSnapshot,
} from '@natsail/jetstream'
import type { SessionDefinition } from '@natsail/session'

export const chatStream = 'NATSAIL_RXJS_CHAT'
export const chatSubjectPrefix = 'natsail.examples.rxjs.chat'
export const chatStreamSubjects = `${chatSubjectPrefix}.>`

export interface ChatFeedModel {
  readonly entries: readonly TimelineEntry[]
  readonly roomIds: readonly string[]
  readonly retainedFrom?: number
  readonly retainedThrough?: number
}

type FeedPhase = 'connecting' | 'gap' | 'catching-up' | 'live' | 'offline' | 'error'

export interface ChatFeedState {
  readonly phase: FeedPhase
  readonly catchUpCount: number
  readonly retainedFrom?: number
  readonly retainedThrough?: number
  readonly diagnostic?: string
}

const jsonCodec = natsCodecs.json<unknown>()
const chatCodec: NatsPayloadCodec<ChatMessage> = {
  encode: (message) => jsonCodec.encode(message),
  decode: (data) => {
    const value = jsonCodec.decode(data)
    if (!isChatMessage(value)) throw new Error('Received an invalid RxJS chat message')
    return value
  },
}

const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => window.setTimeout(resolve, milliseconds))

const initialFeed = (): ChatFeedModel => ({ entries: [], roomIds: [] })

const reduceFeed = (
  state: ChatFeedModel,
  delivery: JetStreamDelivery<ChatMessage>
): ChatFeedModel => {
  if (state.entries.some((entry) => entry.message.id === delivery.value.id)) return state

  const entries = [
    ...state.entries,
    {
      message: delivery.value,
      cursor: delivery.cursor.sequence,
      delivery: 'applied' as const,
    },
  ].slice(-256)
  const roomIds = state.roomIds.includes(delivery.value.roomId)
    ? state.roomIds
    : [...state.roomIds, delivery.value.roomId]

  return {
    entries,
    roomIds,
    ...(delivery.replay === 'initial'
      ? {
          retainedFrom: Math.min(
            state.retainedFrom ?? delivery.cursor.sequence,
            delivery.cursor.sequence
          ),
          retainedThrough: Math.max(
            state.retainedThrough ?? delivery.cursor.sequence,
            delivery.cursor.sequence
          ),
        }
      : state.retainedFrom === undefined
        ? {}
        : {
            retainedFrom: state.retainedFrom,
            retainedThrough: state.retainedThrough,
          }),
  }
}

type ChatFeedPatch = {
  [Key in keyof ChatFeedState]?: ChatFeedState[Key] | undefined
}

/** Example UI controller; delivery, replay, reduction, and recovery stay package-owned. */
export class ChatFeed {
  readonly definition: SessionDefinition<JetStreamStateSnapshot<ChatFeedModel>>
  readonly state$ = new BehaviorSubject<ChatFeedState>({
    phase: 'connecting',
    catchUpCount: 0,
  })

  constructor(private readonly runtime: NatsRuntime) {
    this.definition = defineReducingJetStreamSession(
      runtime,
      'rxjs-chat:all-rooms',
      {
        stream: chatStream,
        filter: chatStreamSubjects,
        start: 'all',
        maxBufferedBytes: 1024 * 1024,
        duplicateDeliveryPolicy: 'drop',
        recovery: { delayMs: 250 },
        codec: chatCodec,
      },
      {
        scope: 'chat-timeline:v2',
        initial: initialFeed,
        reduce: reduceFeed,
      }
    )
  }

  async publish(message: ChatMessage): Promise<void> {
    await this.runtime.publish(`${chatSubjectPrefix}.${message.roomId}`, chatCodec.encode(message))
  }

  /** Forces a visible transport reconnect and verifies three publishes afterward. */
  async recover(roomId: string): Promise<void> {
    if (this.state$.value.phase !== 'live') {
      throw new Error('The RxJS feed must be live before a recovery run')
    }

    const connection = await this.runtime.connection()
    const disconnected = (async () => {
      for await (const status of connection.status()) {
        if (status.type === 'disconnect') return
      }
    })()
    const reconnecting = this.runtime.reconnect({ reason: 'rxjs-chat recovery example' })

    this.patch({ phase: 'gap', retainedFrom: undefined, retainedThrough: undefined })
    await disconnected
    await delay(1_000)
    await reconnecting
    this.patch({ phase: 'catching-up' })
    const messages = Array.from(
      { length: 3 },
      (_, index): ChatMessage => ({
        id: `rxjs-recovery-${crypto.randomUUID()}`,
        roomId,
        author: 'JetStream recovery bot',
        body: `Published after the forced reconnect · ${index + 1}/3`,
        sentAt: new Date().toISOString(),
        clientId: 'rxjs-recovery-bot',
      })
    )
    for (const message of messages) await this.publish(message)
    await delay(750)
    this.patch({
      phase: 'live',
      catchUpCount: this.state$.value.catchUpCount + 1,
    })
  }

  follow(snapshot: JetStreamStateSnapshot<ChatFeedModel> | undefined, error?: unknown): void {
    if (error !== undefined) {
      this.patch({
        phase: 'error',
        diagnostic: error instanceof Error ? error.message : String(error),
      })
      return
    }
    if (
      !snapshot ||
      this.state$.value.phase === 'gap' ||
      this.state$.value.phase === 'catching-up'
    ) {
      return
    }
    this.patch({
      phase: snapshot.phase === 'live' ? 'live' : 'connecting',
      retainedFrom: snapshot.data.retainedFrom,
      retainedThrough: snapshot.data.retainedThrough,
      diagnostic: undefined,
    })
  }

  private patch(patch: ChatFeedPatch): void {
    const next = { ...this.state$.value } as Record<keyof ChatFeedState, unknown>
    for (const [key, value] of Object.entries(patch) as Array<
      [keyof ChatFeedState, ChatFeedState[keyof ChatFeedState] | undefined]
    >) {
      if (value === undefined) delete next[key]
      else next[key] = value
    }
    this.state$.next(next as unknown as ChatFeedState)
  }
}
