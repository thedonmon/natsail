import { BehaviorSubject } from 'rxjs'

import { createMemoryCheckpointStore } from '@natsail/checkpoints'
import {
  natsCodecs,
  type NatsPayloadCodec,
  type NatsRuntime,
  type SubscriptionLease,
} from '@natsail/core'
import { isChatMessage, type ChatMessage } from '@natsail/example-chat-ui'
import { createJetStreamSessionSource, type JetStreamDelivery } from '@natsail/jetstream'
import type { SessionSource } from '@natsail/session'

export const chatStream = 'NATSAIL_RXJS_CHAT'
export const chatSubjectPrefix = 'natsail.examples.rxjs.chat'
export const chatStreamSubjects = `${chatSubjectPrefix}.>`

const checkpoints = createMemoryCheckpointStore()
const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => window.setTimeout(resolve, milliseconds))

type FeedPhase = 'connecting' | 'gap' | 'catching-up' | 'live' | 'offline' | 'error'

export interface ChatFeedState {
  phase: FeedPhase
  catchUpCount: number
  sourceStarts: number
  retainedFrom?: number
  retainedThrough?: number
  diagnostic?: string
}

type ChatFeedPatch = {
  [Key in keyof ChatFeedState]?: ChatFeedState[Key] | undefined
}

type Deferred<T> = {
  promise: Promise<T>
  resolve: (value: T | PromiseLike<T>) => void
  reject: (reason?: unknown) => void
}

const deferred = <T>(): Deferred<T> => {
  let resolve!: Deferred<T>['resolve']
  let reject!: Deferred<T>['reject']
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, resolve, reject }
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

export class RecoverableChatFeed {
  readonly state$ = new BehaviorSubject<ChatFeedState>({
    phase: 'connecting',
    catchUpCount: 0,
    sourceStarts: 0,
  })

  readonly source: SessionSource<JetStreamDelivery<ChatMessage>> = (accept) => {
    if (this.accept) throw new Error('The RxJS chat feed already has an active session')
    this.accept = accept
    this.closedState = deferred<void>()
    this.stopped = false
    this.patch({
      phase: 'connecting',
      sourceStarts: this.state$.value.sourceStarts + 1,
    })
    const ready = this.open()

    return {
      ready,
      closed: this.closedState.promise,
      close: async () => {
        if (this.stopped) return this.closedState!.promise
        this.stopped = true
        const lease = this.activeLease
        this.activeLease = undefined
        await lease?.close()
        this.patch({ phase: 'offline' })
        this.closedState?.resolve()
        this.accept = undefined
        return this.closedState!.promise
      },
    }
  }

  private accept: ((value: JetStreamDelivery<ChatMessage>) => Promise<void>) | undefined
  private activeLease: SubscriptionLease | undefined
  private closedState: Deferred<void> | undefined
  private stopped = false
  private pendingRecoveryIds = new Set<string>()
  private recoveryComplete: Deferred<void> | undefined

  constructor(private readonly runtime: NatsRuntime) {}

  async publish(message: ChatMessage): Promise<void> {
    await this.runtime.publish(`${chatSubjectPrefix}.${message.roomId}`, chatCodec.encode(message))
  }

  async recover(roomId: string): Promise<void> {
    if (this.state$.value.phase !== 'live' || !this.activeLease) {
      throw new Error('The RxJS feed must be live before a recovery run')
    }

    const lease = this.activeLease
    this.activeLease = undefined
    this.patch({ phase: 'gap', retainedFrom: undefined, retainedThrough: undefined })
    await lease.close()

    const messages = Array.from(
      { length: 3 },
      (_, index): ChatMessage => ({
        id: `rxjs-recovery-${crypto.randomUUID()}`,
        roomId,
        author: 'JetStream recovery bot',
        body: `Published while the RxJS consumer was paused · ${index + 1}/3`,
        sentAt: new Date().toISOString(),
        clientId: 'rxjs-recovery-bot',
      })
    )
    this.pendingRecoveryIds = new Set(messages.map((message) => message.id))
    this.recoveryComplete = deferred<void>()
    for (const message of messages) await this.publish(message)

    await delay(1_500)
    this.patch({ phase: 'catching-up' })
    await this.open()
    await this.recoveryComplete.promise
  }

  private async open(): Promise<void> {
    if (!this.accept || this.stopped) return
    const source = createJetStreamSessionSource(this.runtime, {
      stream: chatStream,
      filter: chatStreamSubjects,
      start: 'all',
      maxBufferedBytes: 1024 * 1024,
      duplicateDeliveryPolicy: 'drop',
      resume: {
        key: 'rxjs-chat:all-rooms',
        store: checkpoints,
        scope: 'chat-message:v1',
      },
      codec: chatCodec,
    })
    const lease = source(async (delivery) => {
      await this.accept?.(delivery)
      if (!this.pendingRecoveryIds.delete(delivery.value.id)) return

      const retainedFrom = Math.min(
        this.state$.value.retainedFrom ?? delivery.cursor.sequence,
        delivery.cursor.sequence
      )
      const retainedThrough = Math.max(
        this.state$.value.retainedThrough ?? delivery.cursor.sequence,
        delivery.cursor.sequence
      )
      this.patch({ retainedFrom, retainedThrough })
      if (this.pendingRecoveryIds.size === 0) {
        this.patch({
          phase: 'live',
          catchUpCount: this.state$.value.catchUpCount + 1,
        })
        this.recoveryComplete?.resolve()
        this.recoveryComplete = undefined
      }
    })
    this.activeLease = lease
    void lease.closed.then(
      () => {
        if (!this.stopped && this.activeLease === lease) {
          this.fail(new Error('The ordered RxJS chat consumer stopped unexpectedly'))
        }
      },
      (error: unknown) => {
        if (!this.stopped && this.activeLease === lease) this.fail(error)
      }
    )
    try {
      await lease.ready
      if (this.pendingRecoveryIds.size === 0) this.patch({ phase: 'live' })
    } catch (error) {
      this.fail(error)
      throw error
    }
  }

  private fail(cause: unknown): void {
    const error = cause instanceof Error ? cause : new Error(String(cause))
    this.patch({ phase: 'error', diagnostic: error.message })
    this.recoveryComplete?.reject(error)
    this.recoveryComplete = undefined
    this.closedState?.reject(error)
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
