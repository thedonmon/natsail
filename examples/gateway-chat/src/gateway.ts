import type { QueryClient } from '@tanstack/react-query'

import type { ChatMessage, TimelineEntry, TimelineState } from '@natsail/example-chat-ui'
import { decodeFrame, fetchHistory, timelineKey, type HistoryResponse } from './queries'

interface GatewayFrame {
  type: string
  clientId?: string
  instanceId?: string
  checkpoint?: number
  cursor?: number
  gatewayCursor?: number
  requestedCursor?: number
  value?: string
  duplicate?: boolean
  message?: string
  state?: {
    checkpoint?: number
    retainedFrom?: number
    retainedThrough?: number
  }
}

const clients = new WeakMap<QueryClient, GatewayClient>()

type TimelinePatch = Partial<
  Pick<TimelineState, 'gatewayCursor' | 'instanceId' | 'phase' | 'retainedFrom' | 'retainedThrough'>
> & { diagnostic?: string | undefined }

const updateMessages = (current: TimelineEntry[], additions: TimelineEntry[]): TimelineEntry[] => {
  const next = [...current]
  for (const addition of additions) {
    const index = next.findIndex(
      (entry) =>
        entry.message.id === addition.message.id ||
        (entry.cursor !== undefined && entry.cursor === addition.cursor)
    )
    if (index >= 0) next[index] = addition
    else next.push(addition)
  }
  return next.slice(-256)
}

export class GatewayClient {
  private socket: WebSocket | undefined
  private manualClose = false
  private resumeInFlight = false
  private retryTimer: number | undefined
  private readonly pendingPublishes = new Map<
    string,
    { resolve: () => void; reject: (error: Error) => void; timer: number }
  >()

  constructor(
    private readonly queryClient: QueryClient,
    private readonly tenant: string,
    readonly clientId: string
  ) {}

  connect(): void {
    if (
      this.socket?.readyState === WebSocket.CONNECTING ||
      this.socket?.readyState === WebSocket.OPEN
    ) {
      return
    }
    this.manualClose = false
    this.patch({ phase: 'connecting', diagnostic: undefined })

    const timeline = this.timeline()
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const url = new URL(`${protocol}//${window.location.host}/gateway/${this.tenant}/socket`)
    url.searchParams.set('token', 'prototype-only')
    url.searchParams.set('client', this.clientId)
    if (timeline.cursor > 0) url.searchParams.set('cursor', String(timeline.cursor))

    const socket = new WebSocket(url)
    this.socket = socket
    socket.addEventListener('message', (event) => {
      void this.onFrame(JSON.parse(String(event.data)) as GatewayFrame)
    })
    socket.addEventListener('close', () => this.onClose(socket))
    socket.addEventListener('error', () => {
      this.patch({ phase: 'error', diagnostic: 'The browser WebSocket reported an error.' })
    })
  }

  disconnect(): void {
    this.manualClose = true
    if (this.retryTimer !== undefined) window.clearTimeout(this.retryTimer)
    this.retryTimer = undefined
    this.socket?.close(1000, 'Prototype disconnect control')
    this.socket = undefined
    this.patch({ phase: 'offline', diagnostic: 'Disconnected locally to exercise catch-up.' })
  }

  async publish(message: ChatMessage): Promise<void> {
    const socket = this.socket
    if (!socket || socket.readyState !== WebSocket.OPEN || this.timeline().phase !== 'live') {
      throw new Error('The gateway must be live before publishing.')
    }

    const value = JSON.stringify(message)
    await new Promise<void>((resolve, reject) => {
      const timer = window.setTimeout(() => {
        this.pendingPublishes.delete(value)
        reject(new Error('The gateway did not acknowledge the publish in time.'))
      }, 4_000)
      this.pendingPublishes.set(value, { resolve, reject, timer })
      socket.send(JSON.stringify({ type: 'publish', value }))
    })
  }

  addOptimistic(message: ChatMessage): void {
    const current = this.timeline()
    this.setTimeline({
      ...current,
      messages: updateMessages(current.messages, [{ message, delivery: 'sending' }]),
    })
  }

  failOptimistic(messageId: string, error: Error): void {
    const current = this.timeline()
    this.setTimeline({
      ...current,
      messages: current.messages.map((entry) =>
        entry.message.id === messageId ? { ...entry, delivery: 'failed' } : entry
      ),
      diagnostic: error.message,
    })
  }

  private async onFrame(frame: GatewayFrame): Promise<void> {
    switch (frame.type) {
      case 'connecting':
        this.patch({
          phase: 'connecting',
          ...(frame.instanceId === undefined ? {} : { instanceId: frame.instanceId }),
          ...(frame.checkpoint === undefined ? {} : { gatewayCursor: frame.checkpoint }),
        })
        break
      case 'ready':
        this.patch({
          phase: 'live',
          diagnostic: undefined,
          ...(frame.instanceId === undefined ? {} : { instanceId: frame.instanceId }),
          ...(frame.state?.checkpoint === undefined
            ? {}
            : { gatewayCursor: frame.state.checkpoint }),
          ...(frame.state?.retainedFrom === undefined
            ? {}
            : { retainedFrom: frame.state.retainedFrom }),
          ...(frame.state?.retainedThrough === undefined
            ? {}
            : { retainedThrough: frame.state.retainedThrough }),
        })
        break
      case 'data':
        this.applyData(frame)
        break
      case 'published':
        if (frame.value !== undefined) {
          const pending = this.pendingPublishes.get(frame.value)
          if (pending) {
            window.clearTimeout(pending.timer)
            this.pendingPublishes.delete(frame.value)
            pending.resolve()
          }
        }
        break
      case 'resume-required':
        if (frame.gatewayCursor !== undefined) {
          await this.catchUp(frame.gatewayCursor)
        }
        break
      case 'error':
        this.patch({ phase: 'error', diagnostic: frame.message ?? 'Unknown gateway error.' })
        break
    }
  }

  private applyData(frame: GatewayFrame): void {
    if (frame.value === undefined || frame.cursor === undefined || frame.duplicate === undefined) {
      return
    }
    const entry = decodeFrame({
      type: 'data',
      value: frame.value,
      cursor: frame.cursor,
      duplicate: frame.duplicate,
    })
    if (!entry) return

    const current = this.timeline()
    this.setTimeline({
      ...current,
      messages: updateMessages(current.messages, [entry]),
      cursor: Math.max(current.cursor, frame.cursor),
      gatewayCursor: Math.max(current.gatewayCursor ?? 0, frame.cursor),
      retainedThrough: Math.max(current.retainedThrough ?? 0, frame.cursor),
    })
    this.socket?.send(JSON.stringify({ type: 'checkpoint', cursor: frame.cursor }))
  }

  private async catchUp(gatewayCursor: number): Promise<void> {
    if (this.resumeInFlight) return
    this.resumeInFlight = true
    const current = this.timeline()
    this.patch({
      phase: 'catching-up',
      gatewayCursor,
      diagnostic: `Applying deliveries ${current.cursor + 1} through ${gatewayCursor}.`,
    })

    try {
      const history = await fetchHistory(this.tenant, current.cursor)
      this.applyHistory(history)
      if (!history.complete) {
        this.patch({
          phase: 'gap',
          diagnostic: 'This client fell behind the Durable Object retention window.',
        })
        return
      }
      this.patch({ phase: 'connecting', diagnostic: undefined })
      window.setTimeout(() => this.connect(), 80)
    } catch (error) {
      this.patch({
        phase: 'error',
        diagnostic: error instanceof Error ? error.message : String(error),
      })
    } finally {
      this.resumeInFlight = false
    }
  }

  private applyHistory(history: HistoryResponse): void {
    const current = this.timeline()
    const additions = history.frames.flatMap((frame) => {
      const entry = decodeFrame(frame)
      return entry ? [entry] : []
    })
    this.setTimeline({
      ...current,
      messages: updateMessages(current.messages, additions),
      cursor: Math.max(current.cursor, history.retainedThrough ?? current.cursor),
      catchUpCount: current.catchUpCount + 1,
      ...(history.retainedFrom === undefined ? {} : { retainedFrom: history.retainedFrom }),
      ...(history.retainedThrough === undefined
        ? {}
        : { retainedThrough: history.retainedThrough }),
    })
  }

  private onClose(socket: WebSocket): void {
    if (this.socket !== socket) return
    this.socket = undefined
    if (this.resumeInFlight) return
    if (this.manualClose) {
      this.patch({ phase: 'offline' })
      return
    }
    this.patch({ phase: 'offline', diagnostic: 'Gateway socket closed; retrying.' })
    this.retryTimer = window.setTimeout(() => this.connect(), 750)
  }

  private patch(patch: TimelinePatch): void {
    const current = this.timeline()
    const { diagnostic, ...statePatch } = patch
    const next: TimelineState = { ...current, ...statePatch }
    if ('diagnostic' in patch) {
      if (diagnostic === undefined) delete next.diagnostic
      else next.diagnostic = diagnostic
    }
    this.setTimeline(next)
  }

  private timeline(): TimelineState {
    const value = this.queryClient.getQueryData<TimelineState>(timelineKey(this.tenant))
    if (!value) throw new Error('The timeline query must be loaded before opening the gateway.')
    return value
  }

  private setTimeline(timeline: TimelineState): void {
    this.queryClient.setQueryData(timelineKey(this.tenant), timeline)
  }
}

export const gatewayFor = (queryClient: QueryClient, tenant: string): GatewayClient => {
  const existing = clients.get(queryClient)
  if (existing) return existing

  const clientId = `tab-${crypto.randomUUID().slice(0, 8)}`
  const client = new GatewayClient(queryClient, tenant, clientId)
  clients.set(queryClient, client)
  return client
}
