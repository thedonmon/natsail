import { describe, expect, it, vi } from 'vitest'

import type { NatsRuntime, NatsRuntimeEvent, SubscriptionLease } from '@natsail/core'
import {
  observeNatsCoreSubscription,
  observeNatsRuntimeEvents,
  observeNatsRuntimeStatus,
  observeNatsSession,
  observeNatsSessionValues,
} from '@natsail/rxjs'
import { createSessionRegistry, type SessionSource } from '@natsail/session'

function controllableSource<T>(): {
  source: SessionSource<T>
  deliver(value: T): Promise<void>
  close: ReturnType<typeof vi.fn<() => Promise<void>>>
  starts: ReturnType<typeof vi.fn<SessionSource<T>>>
} {
  let accept!: (value: T) => Promise<void>
  let closeSession!: () => void
  const closed = new Promise<void>((resolve) => {
    closeSession = resolve
  })
  const close = vi.fn(async () => closeSession())
  const lease: SubscriptionLease = {
    ready: Promise.resolve(),
    closed,
    close,
  }
  const starts = vi.fn<SessionSource<T>>((next) => {
    accept = next
    return lease
  })

  return {
    source: starts,
    deliver: (value) => accept(value),
    close,
    starts,
  }
}

describe('RxJS session adapter', () => {
  it('opens one registry-shared Core NATS subscription for multiple subscribers', async () => {
    let accept!: (value: string) => Promise<void>
    let closeSession!: () => void
    const closed = new Promise<void>((resolve) => {
      closeSession = resolve
    })
    const close = vi.fn(async () => closeSession())
    const subscribe = vi.fn(
      (_options: unknown, handler: (value: string) => Promise<void>): SubscriptionLease => {
        accept = handler
        return { ready: Promise.resolve(), closed, close }
      }
    )
    const runtime = { subscribe } as unknown as NatsRuntime
    const registry = createSessionRegistry()
    const values = observeNatsCoreSubscription(registry, runtime, 'orders', {
      subject: 'events.orders',
      decode: () => 'decoded',
    })
    const first: string[] = []
    const second: string[] = []

    const firstSubscription = values.subscribe((value) => first.push(value))
    const secondSubscription = values.subscribe((value) => second.push(value))
    await Promise.resolve()
    expect(subscribe).toHaveBeenCalledOnce()

    await accept('created')
    expect(first).toEqual(['created'])
    expect(second).toEqual(['created'])

    firstSubscription.unsubscribe()
    expect(close).not.toHaveBeenCalled()
    secondSubscription.unsubscribe()
    await Promise.resolve()
    expect(close).toHaveBeenCalledOnce()
  })

  it('shares a session across Observable subscribers', async () => {
    const registry = createSessionRegistry()
    const controlled = controllableSource<string>()
    const snapshots = observeNatsSession(registry, 'conversation:rxjs', controlled.source)
    const first: string[] = []
    const second: string[] = []

    const firstSubscription = snapshots.subscribe((snapshot) => {
      first.push(`${snapshot.phase}:${snapshot.value ?? ''}`)
    })
    const secondSubscription = snapshots.subscribe((snapshot) => {
      second.push(`${snapshot.phase}:${snapshot.value ?? ''}`)
    })
    await Promise.resolve()

    expect(controlled.starts).toHaveBeenCalledOnce()
    await controlled.deliver('hello')
    expect(first.at(-1)).toBe('live:hello')
    expect(second.at(-1)).toBe('live:hello')

    firstSubscription.unsubscribe()
    expect(controlled.close).not.toHaveBeenCalled()

    secondSubscription.unsubscribe()
    await Promise.resolve()
    expect(controlled.close).toHaveBeenCalledOnce()
  })

  it('emits each delivered value once and completes with the session', async () => {
    const registry = createSessionRegistry()
    const controlled = controllableSource<string>()
    const values: string[] = []
    let completions = 0

    observeNatsSessionValues(registry, 'conversation:values', controlled.source).subscribe({
      next: (value) => values.push(value),
      complete: () => {
        completions += 1
      },
    })
    await Promise.resolve()

    await controlled.deliver('same')
    await controlled.deliver('same')
    await controlled.close()
    await Promise.resolve()

    expect(values).toEqual(['same', 'same'])
    expect(completions).toBe(1)
  })

  it('converts runtime events to a cancellable Observable and distinct status stream', async () => {
    const events = controllableEvents()
    const runtime = { events: events.iterable } as NatsRuntime
    const allEvents: NatsRuntimeEvent[] = []
    const states: string[] = []
    const eventSubscription = observeNatsRuntimeEvents(runtime).subscribe((event) => {
      allEvents.push(event)
    })
    const statusSubscription = observeNatsRuntimeStatus(runtime).subscribe((status) => {
      states.push(status.state)
    })

    events.push({ type: 'status', state: 'reconnecting', at: 1 })
    events.push({
      type: 'diagnostic',
      source: 'connection',
      code: 'retrying',
      level: 'info',
      message: 'Retrying',
      at: 2,
    })
    events.push({ type: 'status', state: 'reconnecting', at: 3 })
    events.push({ type: 'status', state: 'connected', at: 4 })
    await vi.waitFor(() => expect(allEvents).toHaveLength(4))
    await vi.waitFor(() => expect(states).toEqual(['reconnecting', 'connected']))

    eventSubscription.unsubscribe()
    statusSubscription.unsubscribe()
    await vi.waitFor(() => expect(events.activeIterators()).toBe(0))
  })
})

function controllableEvents(): {
  iterable: AsyncIterable<NatsRuntimeEvent>
  push(event: NatsRuntimeEvent): void
  activeIterators(): number
} {
  const subscribers = new Set<{
    queue: NatsRuntimeEvent[]
    resume?: () => void
    closed: boolean
  }>()

  return {
    iterable: {
      [Symbol.asyncIterator]() {
        const subscriber = { queue: [] as NatsRuntimeEvent[], closed: false }
        subscribers.add(subscriber)

        return {
          async next(): Promise<IteratorResult<NatsRuntimeEvent>> {
            while (subscriber.queue.length === 0 && !subscriber.closed) {
              await new Promise<void>((resolve) => {
                subscriber.resume = resolve
              })
            }
            if (subscriber.closed) {
              return { done: true, value: undefined }
            }
            return { done: false, value: subscriber.queue.shift()! }
          },
          async return(): Promise<IteratorResult<NatsRuntimeEvent>> {
            subscriber.closed = true
            subscribers.delete(subscriber)
            subscriber.resume?.()
            return { done: true, value: undefined }
          },
        }
      },
    },
    push(event) {
      for (const subscriber of subscribers) {
        subscriber.queue.push(event)
        subscriber.resume?.()
        subscriber.resume = undefined
      }
    },
    activeIterators: () => subscribers.size,
  }
}
