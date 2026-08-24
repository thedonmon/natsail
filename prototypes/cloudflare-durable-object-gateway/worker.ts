// PROTOTYPE — throwaway Durable Object gateway used to validate the package seam.

import { DurableObject } from 'cloudflare:workers'
import { wsconnect } from '@nats-io/nats-core'

import type { StreamCheckpoint } from '@natsail/checkpoints'
import { createNatsRuntime, natsCodecs, type NatsRuntime } from '@natsail/core'
import { consumeJetStream, type JetStreamDelivery } from '@natsail/jetstream'

const CHECKPOINT_KEY = 'shared-upstream'
const RETAINED_PREFIX = 'retained-delivery:'
const RETAINED_LIMIT = 128

interface Env {
  NATS_GATEWAYS: DurableObjectNamespace<NatsGatewayPrototype>
  NATS_WEBSOCKET_URL: string
  PROTOTYPE_STREAM: string
  PROTOTYPE_TOKEN: string
}

interface ClientAttachment {
  clientId: string
  cursor?: number
}

interface ClientFrame {
  type: 'checkpoint' | 'pause-upstream' | 'publish' | 'state'
  cursor?: number
  value?: string
}

interface RetainedDelivery {
  type: 'data'
  value: string
  cursor: number
  duplicate: boolean
}

class DurableObjectCheckpointStore {
  constructor(private readonly storage: DurableObjectStorage) {}

  async load(key: string): Promise<StreamCheckpoint | undefined> {
    return this.storage.get<StreamCheckpoint>(`checkpoint:${key}`)
  }

  async save(key: string, checkpoint: StreamCheckpoint): Promise<void> {
    const stored = await this.load(key)
    if (
      stored &&
      stored.stream === checkpoint.stream &&
      stored.epoch === checkpoint.epoch &&
      checkpoint.sequence < stored.sequence
    ) {
      throw new Error(
        `Prototype checkpoint regression from ${stored.sequence} to ${checkpoint.sequence}`
      )
    }
    await this.storage.put(`checkpoint:${key}`, checkpoint)
  }

  async clear(key: string): Promise<void> {
    await this.storage.delete(`checkpoint:${key}`)
  }
}

export class NatsGatewayPrototype extends DurableObject<Env> {
  private readonly instanceId = crypto.randomUUID()
  private readonly checkpoints = new DurableObjectCheckpointStore(this.ctx.storage)
  private runtime: NatsRuntime | undefined
  private upstreamPromise: Promise<void> | undefined
  private upstreamState: 'idle' | 'connecting' | 'ready' | 'paused' | 'error' = 'idle'
  private lastDiagnostic: string | undefined

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    ctx.blockConcurrencyWhile(async () => {
      await this.increment('instanceStarts')
    })
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    if (url.pathname.endsWith('/socket')) {
      return this.openSocket(url)
    }
    if (url.pathname.endsWith('/state')) {
      return Response.json(await this.snapshot())
    }
    if (url.pathname.endsWith('/history')) {
      return Response.json(await this.history(this.parseCursor(url.searchParams.get('after')) ?? 0))
    }
    if (url.pathname.endsWith('/shutdown')) {
      await this.pauseUpstream()
      return Response.json(await this.snapshot())
    }

    return new Response('Not found', { status: 404 })
  }

  async webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    try {
      const frame = JSON.parse(
        typeof message === 'string' ? message : natsCodecs.text.decode(new Uint8Array(message))
      ) as ClientFrame

      switch (frame.type) {
        case 'publish': {
          if (typeof frame.value !== 'string' || frame.value.length > 1_024) {
            throw new Error('Prototype publish values must be strings of at most 1,024 characters')
          }
          await this.ensureUpstream()
          await this.runtime!.publish(this.subject(), frame.value)
          this.send(socket, { type: 'published', value: frame.value })
          break
        }
        case 'checkpoint': {
          if (!Number.isSafeInteger(frame.cursor) || frame.cursor! < 0) {
            throw new Error('Prototype client checkpoints must be non-negative integers')
          }
          const [upstream, retained] = await Promise.all([
            this.checkpoints.load(CHECKPOINT_KEY),
            this.ctx.storage.list<RetainedDelivery>({ prefix: RETAINED_PREFIX }),
          ])
          const retainedThrough = [...retained.values()].at(-1)?.cursor ?? 0
          const deliveredThrough = Math.max(upstream?.sequence ?? 0, retainedThrough)
          if (frame.cursor! > deliveredThrough) {
            throw new Error('A client cannot checkpoint beyond the shared delivered cursor')
          }
          const attachment = this.attachment(socket)
          socket.serializeAttachment({ ...attachment, cursor: frame.cursor })
          this.send(socket, { type: 'checkpointed', cursor: frame.cursor })
          break
        }
        case 'pause-upstream': {
          await this.pauseUpstream()
          this.broadcast({ type: 'upstream-paused' })
          break
        }
        case 'state': {
          this.send(socket, { type: 'state', state: await this.snapshot() })
          break
        }
        default:
          throw new Error('Unknown prototype client frame')
      }
    } catch (error) {
      this.send(socket, {
        type: 'error',
        message: error instanceof Error ? error.message : String(error),
      })
    }
  }

  webSocketClose(): void {}

  webSocketError(_socket: WebSocket, error: unknown): void {
    this.lastDiagnostic = error instanceof Error ? error.message : String(error)
  }

  private async openSocket(url: URL): Promise<Response> {
    const pair = new WebSocketPair()
    const [client, server] = Object.values(pair)
    const requestedCursor = this.parseCursor(url.searchParams.get('cursor'))
    const clientId = url.searchParams.get('client') ?? crypto.randomUUID()
    this.ctx.acceptWebSocket(server)
    server.serializeAttachment({
      clientId,
      ...(requestedCursor === undefined ? {} : { cursor: requestedCursor }),
    } satisfies ClientAttachment)

    const checkpoint = await this.checkpoints.load(CHECKPOINT_KEY)
    if (
      requestedCursor !== undefined &&
      checkpoint !== undefined &&
      requestedCursor < checkpoint.sequence
    ) {
      this.send(server, {
        type: 'resume-required',
        requestedCursor,
        gatewayCursor: checkpoint.sequence,
      })
      server.close(4009, 'Client cursor is behind the shared gateway cursor')
      return new Response(null, { status: 101, webSocket: client })
    }

    this.send(server, {
      type: 'connecting',
      clientId,
      instanceId: this.instanceId,
      checkpoint: checkpoint?.sequence,
    })
    this.ctx.waitUntil(
      this.ensureUpstream().then(
        async () => {
          this.send(server, {
            type: 'ready',
            clientId,
            instanceId: this.instanceId,
            state: await this.snapshot(),
          })
        },
        (error: unknown) => {
          this.send(server, {
            type: 'error',
            message: error instanceof Error ? error.message : String(error),
          })
          server.close(1011, 'Cannot connect the prototype gateway upstream')
        }
      )
    )

    return new Response(null, { status: 101, webSocket: client })
  }

  private ensureUpstream(): Promise<void> {
    if (this.upstreamPromise) return this.upstreamPromise

    this.upstreamPromise = this.startUpstream().catch((error: unknown) => {
      this.upstreamPromise = undefined
      this.upstreamState = 'error'
      this.lastDiagnostic = error instanceof Error ? error.message : String(error)
      throw error
    })
    return this.upstreamPromise
  }

  private async startUpstream(): Promise<void> {
    this.upstreamState = 'connecting'
    const runtime = createNatsRuntime({
      connect: async () => {
        await this.increment('upstreamConnectionAttempts')
        return wsconnect({ servers: this.env.NATS_WEBSOCKET_URL, timeout: 2_000 })
      },
      initialConnectRetry: { maxAttempts: 3, delayMs: 100 },
      limits: { maxJetStreamConsumers: 1, maxBufferedMessages: 16 },
    })
    this.runtime = runtime
    void this.observeRuntime(runtime)
    await runtime.connection()
    this.broadcast({ type: 'upstream-stage', stage: 'nats-connected' })

    const lease = consumeJetStream(
      runtime,
      {
        stream: this.env.PROTOTYPE_STREAM,
        filter: this.subject(),
        start: 'all',
        maxBufferedMessages: 16,
        duplicateDeliveryPolicy: 'drop',
        resume: {
          key: CHECKPOINT_KEY,
          store: this.checkpoints,
        },
        codec: natsCodecs.text,
      },
      async (delivery) => this.deliver(delivery)
    )

    await Promise.race([
      lease.ready,
      new Promise<never>((_resolve, reject) => {
        setTimeout(
          () => reject(new Error('Timed out opening the prototype JetStream consumer')),
          8_000
        )
      }),
    ])
    await this.increment('upstreamGenerations')
    this.upstreamState = 'ready'
    this.broadcast({ type: 'upstream-ready', state: await this.snapshot() })
  }

  private async pauseUpstream(): Promise<void> {
    const runtime = this.runtime
    this.runtime = undefined
    this.upstreamPromise = undefined
    this.upstreamState = 'paused'
    if (runtime) {
      await runtime.close()
    }
  }

  private async deliver(delivery: JetStreamDelivery<string>): Promise<void> {
    const frame: RetainedDelivery = {
      type: 'data',
      value: delivery.value,
      cursor: delivery.cursor.sequence,
      duplicate: delivery.duplicate,
    }
    await this.retain(frame)
    this.broadcast(frame)
    await this.increment('deliveries')
  }

  private async retain(delivery: RetainedDelivery): Promise<void> {
    await this.ctx.storage.put(this.retainedKey(delivery.cursor), delivery)
    const retained = await this.ctx.storage.list<RetainedDelivery>({ prefix: RETAINED_PREFIX })
    const overflow = retained.size - RETAINED_LIMIT
    if (overflow > 0) {
      await this.ctx.storage.delete([...retained.keys()].slice(0, overflow))
    }
  }

  private async history(after: number): Promise<Record<string, unknown>> {
    const retained = [
      ...(await this.ctx.storage.list<RetainedDelivery>({ prefix: RETAINED_PREFIX })).values(),
    ]
    const frames = retained.filter((delivery) => delivery.cursor > after)
    const first = retained[0]?.cursor
    const last = retained.at(-1)?.cursor

    return {
      prototype: true,
      after,
      retainedLimit: RETAINED_LIMIT,
      retainedFrom: first,
      retainedThrough: last,
      complete: first === undefined || after >= first - 1,
      frames,
    }
  }

  private async observeRuntime(runtime: NatsRuntime): Promise<void> {
    for await (const event of runtime.events) {
      if (event.type === 'status') {
        if (event.state === 'connected') this.upstreamState = 'ready'
        if (event.state === 'connecting') this.upstreamState = 'connecting'
      } else {
        this.lastDiagnostic = `${event.source}:${event.code}`
      }
    }
  }

  private async snapshot(): Promise<Record<string, unknown>> {
    const [
      checkpoint,
      instanceStarts,
      upstreamConnectionAttempts,
      upstreamGenerations,
      deliveries,
      retained,
    ] = await Promise.all([
      this.checkpoints.load(CHECKPOINT_KEY),
      this.number('instanceStarts'),
      this.number('upstreamConnectionAttempts'),
      this.number('upstreamGenerations'),
      this.number('deliveries'),
      this.ctx.storage.list<RetainedDelivery>({ prefix: RETAINED_PREFIX }),
    ])

    return {
      prototype: true,
      instanceId: this.instanceId,
      instanceStarts,
      upstreamConnectionAttempts,
      upstreamGenerations,
      upstreamState: this.upstreamState,
      deliveries,
      checkpoint: checkpoint?.sequence,
      retainedDeliveries: retained.size,
      retainedFrom: [...retained.values()][0]?.cursor,
      retainedThrough: [...retained.values()].at(-1)?.cursor,
      subject: this.subject(),
      clients: this.ctx.getWebSockets().map((socket) => this.attachment(socket)),
      ...(this.lastDiagnostic === undefined ? {} : { lastDiagnostic: this.lastDiagnostic }),
    }
  }

  private subject(): string {
    return `prototype.gateway.${this.ctx.id.toString()}.events`
  }

  private retainedKey(cursor: number): string {
    return `${RETAINED_PREFIX}${cursor.toString().padStart(16, '0')}`
  }

  private attachment(socket: WebSocket): ClientAttachment {
    return (socket.deserializeAttachment() ?? { clientId: 'unknown' }) as ClientAttachment
  }

  private broadcast(frame: Record<string, unknown>): void {
    for (const socket of this.ctx.getWebSockets()) {
      this.send(socket, frame)
    }
  }

  private send(socket: WebSocket, frame: Record<string, unknown>): void {
    try {
      socket.send(JSON.stringify(frame))
    } catch (error) {
      this.lastDiagnostic = error instanceof Error ? error.message : String(error)
    }
  }

  private parseCursor(value: string | null): number | undefined {
    if (value === null) return undefined
    const cursor = Number(value)
    if (!Number.isSafeInteger(cursor) || cursor < 0) {
      throw new Error('The prototype cursor must be a non-negative integer')
    }
    return cursor
  }

  private async number(key: string): Promise<number> {
    return (await this.ctx.storage.get<number>(key)) ?? 0
  }

  private async increment(key: string): Promise<number> {
    const next = (await this.number(key)) + 1
    await this.ctx.storage.put(key, next)
    return next
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === '/health') {
      return new Response('ok')
    }

    const route = url.pathname.match(/^\/gateway\/([a-z0-9-]+)\/(socket|state|history|shutdown)$/)
    if (!route) {
      return new Response('Not found', { status: 404 })
    }
    if (url.searchParams.get('token') !== env.PROTOTYPE_TOKEN) {
      return new Response('Unauthorized', { status: 401 })
    }

    const id = env.NATS_GATEWAYS.idFromName(route[1]!)
    return env.NATS_GATEWAYS.get(id).fetch(request)
  },
}
