import type { ChatTransport, UIMessage, UIMessageChunk } from 'ai'
import type { ModelMessage, StreamChunk } from '@tanstack/ai'
import type { UIMessage as TanStackUIMessage } from '@tanstack/ai-client'
import type { RunAgentInputContext, SubscribeConnectionAdapter } from '@tanstack/ai-react'

import { createIndexedDbCheckpointStore } from '@natsail/checkpoints'
import {
  natsCodecs,
  type NatsPayloadCodec,
  type NatsRuntime,
  type SubscriptionLease,
} from '@natsail/core'
import { consumeJetStream, type JetStreamDuplicateDeliveryPolicy } from '@natsail/jetstream'
import { clearActiveRun, loadActiveRun, saveActiveRun } from './chat-persistence'

const requestSubject = 'natsail.examples.ai.requests'
const checkpoints = createIndexedDbCheckpointStore({ databaseName: 'natsail-ai-example' })

export const aiResponseStream = 'NATSAIL_AI_RESPONSES'
export const aiResponseStreamSubjects = 'natsail.examples.ai.responses.jetstream.>'

export type FrameworkKind = 'ai-sdk' | 'tanstack-ai'
export type DeliveryKind = 'core' | 'jetstream'

export interface TransportReceipt {
  framework: FrameworkKind
  delivery: DeliveryKind
  direction: 'publish' | 'receive' | 'complete'
  event: string
  subject: string
  sequence?: number
  duplicate?: boolean
  publishedAt?: number
}

export type PageRecoveryStrategy = 'full-run-replay' | 'checkpoint-continuation'

export interface PageRecoveryState {
  phase: 'idle' | 'restoring' | 'restored'
  strategy?: PageRecoveryStrategy
  checkpointSequence?: number
  firstRecoveredSequence?: number
}

export type PageRecoveryListener = (state: PageRecoveryState) => void

interface WireChunkFrame {
  type: 'chunk'
  chunk: unknown
  publishedAt?: number
}

interface WireEndFrame {
  type: 'end'
  publishedAt?: number
}

interface WireErrorFrame {
  type: 'error'
  message: string
  publishedAt?: number
}

type WireFrame = WireChunkFrame | WireEndFrame | WireErrorFrame
const jsonCodec = natsCodecs.json<unknown>()

const decodeWireFrame = (value: unknown): WireFrame => {
  if (!value || typeof value !== 'object') throw new Error('Invalid AI transport frame')
  const frame = value as Partial<WireFrame>
  if (frame.type === 'chunk' && 'chunk' in frame) return frame as WireChunkFrame
  if (frame.type === 'end') return frame as WireEndFrame
  if (frame.type === 'error' && typeof frame.message === 'string') return frame as WireErrorFrame
  throw new Error('Unknown AI transport frame')
}

const wireFrameCodec: NatsPayloadCodec<WireFrame> = {
  encode: (frame) => jsonCodec.encode(frame),
  decode: (data) => decodeWireFrame(jsonCodec.decode(data)),
}

const eventType = (chunk: unknown): string => {
  if (chunk && typeof chunk === 'object' && 'type' in chunk) {
    const type = (chunk as { type?: unknown }).type
    if (typeof type === 'string') return type
  }
  return 'chunk'
}

const abortError = (): DOMException => new DOMException('The transport was aborted', 'AbortError')
const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => window.setTimeout(resolve, milliseconds))

interface DeliveryMetadata {
  sequence?: number
  duplicate?: boolean
}

export interface JetStreamTransportOptions {
  duplicateDeliveryPolicy?: JetStreamDuplicateDeliveryPolicy
  maxBufferedMessages?: number
}

type JetStreamSubscriptionMode = 'checkpoint' | 'full-run-replay'

const checkpointKey = (replySubject: string): string => `ai-chat:${replySubject}`

const subscribeToFrames = (
  runtime: NatsRuntime,
  delivery: DeliveryKind,
  replySubject: string,
  jetStreamOptions: JetStreamTransportOptions,
  handler: (frame: WireFrame, metadata: DeliveryMetadata) => void | Promise<void>,
  mode: JetStreamSubscriptionMode = 'checkpoint'
): SubscriptionLease => {
  if (delivery === 'core') {
    return runtime.subscribe<WireFrame>(
      {
        subject: replySubject,
        codec: wireFrameCodec,
      },
      (frame) => handler(frame, {})
    )
  }

  return consumeJetStream(
    runtime,
    {
      stream: aiResponseStream,
      filter: replySubject,
      start: mode === 'full-run-replay' ? 'all' : 'new',
      maxBufferedMessages: jetStreamOptions.maxBufferedMessages ?? 128,
      duplicateDeliveryPolicy: jetStreamOptions.duplicateDeliveryPolicy ?? 'drop',
      ...(mode === 'checkpoint'
        ? {
            resume: {
              key: checkpointKey(replySubject),
              store: checkpoints,
            },
          }
        : {}),
      codec: wireFrameCodec,
    },
    (frame) =>
      handler(frame.value, {
        sequence: frame.cursor.sequence,
        duplicate: frame.duplicate,
      })
  )
}

type AiSdkSendOptions<UI_MESSAGE extends UIMessage> = Parameters<
  ChatTransport<UI_MESSAGE>['sendMessages']
>[0]

export class NatsAiSdkChatTransport<UI_MESSAGE extends UIMessage = UIMessage>
  implements ChatTransport<UI_MESSAGE>
{
  private interruptStream: ((durationMs: number) => Promise<void>) | undefined

  constructor(
    private readonly runtime: NatsRuntime,
    private readonly clientId: string,
    private readonly delivery: DeliveryKind,
    private readonly jetStreamOptions: JetStreamTransportOptions = {},
    private readonly onReceipt?: (receipt: TransportReceipt) => void,
    private readonly onPageRecovery?: PageRecoveryListener
  ) {}

  sendMessages = async (
    options: AiSdkSendOptions<UI_MESSAGE>
  ): Promise<ReadableStream<UIMessageChunk>> => {
    const requestId = crypto.randomUUID()
    const replySubject = `natsail.examples.ai.responses.${this.delivery}.ai-sdk.${this.clientId}.${requestId}`
    let lease: SubscriptionLease | undefined
    let finished = false
    let abortListener: (() => void) | undefined

    const release = async () => {
      if (finished) return
      finished = true
      this.interruptStream = undefined
      if (abortListener) options.abortSignal?.removeEventListener('abort', abortListener)
      await lease?.close()
    }

    return new ReadableStream<UIMessageChunk>({
      start: async (controller) => {
        const handleFrame = async (frame: WireFrame, metadata: DeliveryMetadata) => {
          if (frame.type === 'chunk') {
            const chunk = frame.chunk as UIMessageChunk
            this.onReceipt?.({
              framework: 'ai-sdk',
              delivery: this.delivery,
              direction: 'receive',
              event: eventType(chunk),
              subject: replySubject,
              ...metadata,
              ...(frame.publishedAt === undefined ? {} : { publishedAt: frame.publishedAt }),
            })
            controller.enqueue(chunk)
            return
          }
          if (frame.type === 'error') {
            clearActiveRun('ai-sdk', this.delivery)
            controller.error(new Error(frame.message))
            void release().catch(() => undefined)
            return
          }
          this.onReceipt?.({
            framework: 'ai-sdk',
            delivery: this.delivery,
            direction: 'complete',
            event: 'stream complete',
            subject: replySubject,
            ...metadata,
            ...(frame.publishedAt === undefined ? {} : { publishedAt: frame.publishedAt }),
          })
          clearActiveRun('ai-sdk', this.delivery)
          controller.close()
          void release().catch(() => undefined)
        }
        const openSubscription = async () => {
          lease = subscribeToFrames(
            this.runtime,
            this.delivery,
            replySubject,
            this.jetStreamOptions,
            handleFrame
          )
          await lease.ready
        }

        try {
          await openSubscription()
          if (this.delivery === 'jetstream') {
            this.interruptStream = async (durationMs) => {
              await lease?.close()
              await delay(durationMs)
              if (!finished) await openSubscription()
            }
          }
          if (options.abortSignal?.aborted) throw abortError()
          abortListener = () => {
            if (finished) return
            controller.error(abortError())
            void release().catch(() => undefined)
          }
          options.abortSignal?.addEventListener('abort', abortListener, { once: true })
          this.onReceipt?.({
            framework: 'ai-sdk',
            delivery: this.delivery,
            direction: 'publish',
            event: options.trigger,
            subject: requestSubject,
          })
          if (this.delivery === 'jetstream') {
            saveActiveRun({
              framework: 'ai-sdk',
              delivery: this.delivery,
              chatId: options.chatId,
              replySubject,
              startedAt: Date.now(),
            })
          }
          await this.runtime.publish(
            requestSubject,
            jsonCodec.encode({
              framework: 'ai-sdk',
              delivery: this.delivery,
              replySubject,
              payload: {
                trigger: options.trigger,
                chatId: options.chatId,
                messageId: options.messageId,
                messages: options.messages,
              },
            })
          )
        } catch (error) {
          clearActiveRun('ai-sdk', this.delivery)
          controller.error(error)
          void release().catch(() => undefined)
        }
      },
      cancel: release,
    })
  }

  interruptActiveStream = async (durationMs: number): Promise<void> => {
    if (!this.interruptStream) throw new Error('No active JetStream reply can be interrupted')
    await this.interruptStream(durationMs)
  }

  reconnectToStream = async ({
    chatId,
    abortSignal,
  }: {
    chatId: string
    abortSignal?: AbortSignal
  }): Promise<ReadableStream<UIMessageChunk> | null> => {
    if (this.delivery !== 'jetstream') return null
    const activeRun = loadActiveRun('ai-sdk', this.delivery)
    if (!activeRun || activeRun.chatId !== chatId) return null

    const checkpoint = await checkpoints.load(checkpointKey(activeRun.replySubject))
    let firstRecoveredSequence: number | undefined
    this.onPageRecovery?.({
      phase: 'restoring',
      strategy: 'full-run-replay',
      ...(checkpoint ? { checkpointSequence: checkpoint.sequence } : {}),
    })

    return new ReadableStream<UIMessageChunk>({
      start: async (controller) => {
        let lease: SubscriptionLease | undefined
        let finished = false
        let abortListener: (() => void) | undefined
        const release = async () => {
          if (finished) return
          finished = true
          if (abortListener) abortSignal?.removeEventListener('abort', abortListener)
          await lease?.close()
        }
        const finishRecovery = () => {
          clearActiveRun('ai-sdk', this.delivery)
          this.onPageRecovery?.({
            phase: 'restored',
            strategy: 'full-run-replay',
            ...(checkpoint ? { checkpointSequence: checkpoint.sequence } : {}),
            ...(firstRecoveredSequence === undefined ? {} : { firstRecoveredSequence }),
          })
        }

        try {
          lease = subscribeToFrames(
            this.runtime,
            this.delivery,
            activeRun.replySubject,
            this.jetStreamOptions,
            (frame, metadata) => {
              if (firstRecoveredSequence === undefined && metadata.sequence !== undefined) {
                firstRecoveredSequence = metadata.sequence
                this.onPageRecovery?.({
                  phase: 'restoring',
                  strategy: 'full-run-replay',
                  ...(checkpoint ? { checkpointSequence: checkpoint.sequence } : {}),
                  firstRecoveredSequence,
                })
              }
              if (frame.type === 'chunk') {
                const chunk = frame.chunk as UIMessageChunk
                this.onReceipt?.({
                  framework: 'ai-sdk',
                  delivery: this.delivery,
                  direction: 'receive',
                  event: eventType(chunk),
                  subject: activeRun.replySubject,
                  ...metadata,
                  ...(frame.publishedAt === undefined ? {} : { publishedAt: frame.publishedAt }),
                })
                controller.enqueue(chunk)
              } else if (frame.type === 'error') {
                clearActiveRun('ai-sdk', this.delivery)
                controller.error(new Error(frame.message))
                void release().catch(() => undefined)
              } else {
                this.onReceipt?.({
                  framework: 'ai-sdk',
                  delivery: this.delivery,
                  direction: 'complete',
                  event: 'page recovery complete',
                  subject: activeRun.replySubject,
                  ...metadata,
                  ...(frame.publishedAt === undefined ? {} : { publishedAt: frame.publishedAt }),
                })
                finishRecovery()
                controller.close()
                void release().catch(() => undefined)
              }
            },
            'full-run-replay'
          )
          await lease.ready
          if (abortSignal?.aborted) throw abortError()
          abortListener = () => {
            controller.error(abortError())
            void release().catch(() => undefined)
          }
          abortSignal?.addEventListener('abort', abortListener, { once: true })
        } catch (error) {
          controller.error(error)
          void release().catch(() => undefined)
        }
      },
    })
  }
}

class AsyncEventQueue<T> {
  private readonly values: T[] = []
  private readonly waiters: Array<{
    resolve: (value: T) => void
    reject: (error: unknown) => void
  }> = []

  push(value: T): void {
    const waiter = this.waiters.shift()
    if (waiter) waiter.resolve(value)
    else this.values.push(value)
  }

  fail(error: unknown): void {
    for (const waiter of this.waiters.splice(0)) waiter.reject(error)
  }

  async take(signal?: AbortSignal): Promise<T> {
    const value = this.values.shift()
    if (value !== undefined) return value
    if (signal?.aborted) throw abortError()
    return new Promise<T>((resolve, reject) => {
      const waiter = { resolve, reject }
      this.waiters.push(waiter)
      signal?.addEventListener(
        'abort',
        () => {
          const index = this.waiters.indexOf(waiter)
          if (index >= 0) this.waiters.splice(index, 1)
          reject(abortError())
        },
        { once: true }
      )
    })
  }
}

export class NatsTanStackConnection implements SubscribeConnectionAdapter {
  private readonly queue = new AsyncEventQueue<StreamChunk>()
  private readonly replySubject: string
  private lease: SubscriptionLease | undefined
  private ready: Promise<void> | undefined
  private pageRecoveryActive: boolean
  private recoveryCheckpointSequence: number | undefined
  private firstRecoveredSequence: number | undefined

  constructor(
    private readonly runtime: NatsRuntime,
    clientId: string,
    private readonly chatId: string,
    private readonly delivery: DeliveryKind,
    private readonly jetStreamOptions: JetStreamTransportOptions = {},
    private readonly onReceipt?: (receipt: TransportReceipt) => void,
    private readonly onPageRecovery?: PageRecoveryListener
  ) {
    this.replySubject = `natsail.examples.ai.responses.${delivery}.tanstack-ai.${clientId}`
    const activeRun = loadActiveRun('tanstack-ai', delivery)
    this.pageRecoveryActive =
      delivery === 'jetstream' &&
      activeRun?.chatId === chatId &&
      activeRun.replySubject === this.replySubject
  }

  private ensureSubscription(): Promise<void> {
    if (this.ready) return this.ready
    this.ready = this.openSubscription()
    return this.ready
  }

  private async openSubscription(): Promise<void> {
    if (this.pageRecoveryActive) {
      const checkpoint = await checkpoints.load(checkpointKey(this.replySubject))
      this.recoveryCheckpointSequence = checkpoint?.sequence
      this.onPageRecovery?.({
        phase: 'restoring',
        strategy: 'checkpoint-continuation',
        ...(checkpoint ? { checkpointSequence: checkpoint.sequence } : {}),
      })
    }
    this.lease = subscribeToFrames(
      this.runtime,
      this.delivery,
      this.replySubject,
      this.jetStreamOptions,
      (frame, metadata) => {
        if (
          this.pageRecoveryActive &&
          this.firstRecoveredSequence === undefined &&
          metadata.sequence !== undefined
        ) {
          this.firstRecoveredSequence = metadata.sequence
          this.onPageRecovery?.({
            phase: 'restoring',
            strategy: 'checkpoint-continuation',
            ...(this.recoveryCheckpointSequence === undefined
              ? {}
              : { checkpointSequence: this.recoveryCheckpointSequence }),
            firstRecoveredSequence: this.firstRecoveredSequence,
          })
        }
        if (frame.type === 'chunk') {
          const chunk = frame.chunk as StreamChunk
          this.onReceipt?.({
            framework: 'tanstack-ai',
            delivery: this.delivery,
            direction: 'receive',
            event: eventType(chunk),
            subject: this.replySubject,
            ...metadata,
            ...(frame.publishedAt === undefined ? {} : { publishedAt: frame.publishedAt }),
          })
          this.queue.push(chunk)
        } else if (frame.type === 'error') {
          clearActiveRun('tanstack-ai', this.delivery)
          this.queue.fail(new Error(frame.message))
        } else {
          this.onReceipt?.({
            framework: 'tanstack-ai',
            delivery: this.delivery,
            direction: 'complete',
            event: 'response complete',
            subject: this.replySubject,
            ...metadata,
            ...(frame.publishedAt === undefined ? {} : { publishedAt: frame.publishedAt }),
          })
          clearActiveRun('tanstack-ai', this.delivery)
          if (this.pageRecoveryActive) {
            this.pageRecoveryActive = false
            this.onPageRecovery?.({
              phase: 'restored',
              strategy: 'checkpoint-continuation',
              ...(this.recoveryCheckpointSequence === undefined
                ? {}
                : { checkpointSequence: this.recoveryCheckpointSequence }),
              ...(this.firstRecoveredSequence === undefined
                ? {}
                : { firstRecoveredSequence: this.firstRecoveredSequence }),
            })
          }
        }
      }
    )
    this.lease.closed.catch((error) => this.queue.fail(error))
    await this.lease.ready
  }

  interruptActiveStream = async (durationMs: number): Promise<void> => {
    if (this.delivery !== 'jetstream' || !this.lease) {
      throw new Error('No active JetStream reply can be interrupted')
    }
    await this.lease.close()
    this.lease = undefined
    this.ready = undefined
    await delay(durationMs)
    await this.ensureSubscription()
  }

  subscribe = (abortSignal?: AbortSignal): AsyncIterable<StreamChunk> => {
    void this.ensureSubscription()
    const queue = this.queue
    return {
      async *[Symbol.asyncIterator]() {
        while (!abortSignal?.aborted) {
          try {
            yield await queue.take(abortSignal)
          } catch (error) {
            if (abortSignal?.aborted) return
            throw error
          }
        }
      },
    }
  }

  send = async (
    messages: Array<TanStackUIMessage> | Array<ModelMessage>,
    data?: Record<string, unknown>,
    abortSignal?: AbortSignal,
    runContext?: RunAgentInputContext
  ): Promise<void> => {
    await this.ensureSubscription()
    if (abortSignal?.aborted) throw abortError()
    this.onReceipt?.({
      framework: 'tanstack-ai',
      delivery: this.delivery,
      direction: 'publish',
      event: 'send',
      subject: requestSubject,
    })
    if (this.delivery === 'jetstream') {
      saveActiveRun({
        framework: 'tanstack-ai',
        delivery: this.delivery,
        chatId: this.chatId,
        replySubject: this.replySubject,
        startedAt: Date.now(),
      })
    }
    try {
      await this.runtime.publish(
        requestSubject,
        jsonCodec.encode({
          framework: 'tanstack-ai',
          delivery: this.delivery,
          replySubject: this.replySubject,
          payload: {
            messages,
            data,
            runContext,
          },
        })
      )
    } catch (error) {
      clearActiveRun('tanstack-ai', this.delivery)
      throw error
    }
  }

  close = async (): Promise<void> => {
    await this.lease?.close()
  }
}
