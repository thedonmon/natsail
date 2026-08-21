import type { ChatTransport, UIMessage, UIMessageChunk } from 'ai'
import type { ModelMessage, StreamChunk } from '@tanstack/ai'
import type { UIMessage as TanStackUIMessage } from '@tanstack/ai-client'
import type { RunAgentInputContext, SubscribeConnectionAdapter } from '@tanstack/ai-react'

import { createMemoryCheckpointStore } from '@natsail/checkpoints'
import type { NatsRuntime, SubscriptionLease } from '@natsail/core'
import { consumeJetStream, type JetStreamDuplicateDeliveryPolicy } from '@natsail/jetstream'

const encoder = new TextEncoder()
const decoder = new TextDecoder()
const requestSubject = 'natsail.examples.ai.requests'
const checkpoints = createMemoryCheckpointStore()

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

const decodeWireFrame = (data: Uint8Array): WireFrame => {
  const value: unknown = JSON.parse(decoder.decode(data))
  if (!value || typeof value !== 'object') throw new Error('Invalid AI transport frame')
  const frame = value as Partial<WireFrame>
  if (frame.type === 'chunk' && 'chunk' in frame) return frame as WireChunkFrame
  if (frame.type === 'end') return frame as WireEndFrame
  if (frame.type === 'error' && typeof frame.message === 'string') return frame as WireErrorFrame
  throw new Error('Unknown AI transport frame')
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

const subscribeToFrames = (
  runtime: NatsRuntime,
  delivery: DeliveryKind,
  replySubject: string,
  jetStreamOptions: JetStreamTransportOptions,
  handler: (frame: WireFrame, metadata: DeliveryMetadata) => void | Promise<void>
): SubscriptionLease => {
  if (delivery === 'core') {
    return runtime.subscribe<WireFrame>(
      {
        subject: replySubject,
        decode: (message) => decodeWireFrame(message.data),
      },
      (frame) => handler(frame, {})
    )
  }

  return consumeJetStream(
    runtime,
    {
      stream: aiResponseStream,
      filter: replySubject,
      start: 'new',
      maxBufferedMessages: jetStreamOptions.maxBufferedMessages ?? 128,
      duplicateDeliveryPolicy: jetStreamOptions.duplicateDeliveryPolicy ?? 'drop',
      resume: {
        key: `ai-chat:${replySubject}`,
        store: checkpoints,
      },
      decode: (message) => decodeWireFrame(message.data),
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
    private readonly onReceipt?: (receipt: TransportReceipt) => void
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
          await this.runtime.publish(
            requestSubject,
            encoder.encode(
              JSON.stringify({
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
          )
        } catch (error) {
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

  reconnectToStream = async (): Promise<ReadableStream<UIMessageChunk> | null> => null
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

  constructor(
    private readonly runtime: NatsRuntime,
    clientId: string,
    private readonly delivery: DeliveryKind,
    private readonly jetStreamOptions: JetStreamTransportOptions = {},
    private readonly onReceipt?: (receipt: TransportReceipt) => void
  ) {
    this.replySubject = `natsail.examples.ai.responses.${delivery}.tanstack-ai.${clientId}`
  }

  private ensureSubscription(): Promise<void> {
    if (this.ready) return this.ready
    this.lease = subscribeToFrames(
      this.runtime,
      this.delivery,
      this.replySubject,
      this.jetStreamOptions,
      (frame, metadata) => {
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
        }
      }
    )
    this.lease.closed.catch((error) => this.queue.fail(error))
    this.ready = this.lease.ready
    return this.ready
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
    await this.runtime.publish(
      requestSubject,
      encoder.encode(
        JSON.stringify({
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
    )
  }

  close = async (): Promise<void> => {
    await this.lease?.close()
  }
}
