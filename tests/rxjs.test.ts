import { describe, expect, it, vi } from 'vitest'

import type { NatsRuntime, NatsRuntimeEvent, SubscriptionLease } from '@natsail/core'
import {
  observeNatsCoreSubscription,
  observeNatsJetStreamReducer,
  observeNatsJetStreamState,
  observeNatsRuntimeEvents,
  observeNatsRuntimeStatus,
  observeNatsSession,
  observeNatsSessionEvents,
  observeNatsSessionValues,
} from '@natsail/rxjs'
import { createSessionRegistry, defineSession, type SessionSource } from '@natsail/session'
import type { JetStreamStateSnapshot } from '@natsail/jetstream'

function controllableSource<T>(): {
  source: SessionSource<T>
  deliver(value: T): Promise<void>
  fail(error: unknown): void
  close: ReturnType<typeof vi.fn<() => Promise<void>>>
  starts: ReturnType<typeof vi.fn<SessionSource<T>>>
} {
  let accept!: (value: T) => Promise<void>
  let closeSession!: () => void
  let failSession!: (error: unknown) => void
  const closed = new Promise<void>((resolve, reject) => {
    closeSession = resolve
    failSession = reject
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
    fail: (error) => failSession(error),
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

  it('converts registry lifecycle diagnostics to a cancellable Observable', async () => {
    const registry = createSessionRegistry()
    const controlled = controllableSource<string>()
    const types: string[] = []
    const subscription = observeNatsSessionEvents(registry).subscribe((event) => {
      types.push(event.type)
    })

    const handle = registry.acquire('conversation:events', controlled.source)
    await handle.ready
    await handle.release()
    await Promise.resolve()

    expect(types).toEqual(expect.arrayContaining(['opened', 'retained', 'released', 'closed']))
    subscription.unsubscribe()
    await registry.close()
  })

  it('observes the same validated reduced JetStream state definition as React', async () => {
    const registry = createSessionRegistry()
    const controlled = controllableSource<JetStreamStateSnapshot<string[]>>()
    const definition = defineSession({
      key: 'conversation:rxjs-reducer',
      contract: 'conversation:v1',
      source: controlled.source,
    })
    const phases: string[] = []
    const values: string[][] = []
    const subscription = observeNatsJetStreamReducer(registry, definition).subscribe((snapshot) => {
      if (!snapshot.value) return
      phases.push(snapshot.value.phase)
      values.push(snapshot.value.data)
    })
    await Promise.resolve()

    await controlled.deliver({
      phase: 'replaying',
      data: [],
      restarts: 0,
      replay: { delivered: 0, remaining: 1 },
    })
    await controlled.deliver({
      phase: 'live',
      data: ['one', 'two'],
      restarts: 0,
      replay: { delivered: 2, remaining: 0 },
    })

    expect(phases).toEqual(['replaying', 'live'])
    expect(values).toEqual([[], ['one', 'two']])
    expect(registry.inspect().activeSessions).toBe(1)

    subscription.unsubscribe()
    await registry.close()
  })

  it('emits each reduced JetStream value once without session lifecycle duplicates', async () => {
    const registry = createSessionRegistry()
    const controlled = controllableSource<JetStreamStateSnapshot<string[]>>()
    const definition = defineSession({
      key: 'conversation:rxjs-state-values',
      contract: 'conversation:v1',
      source: controlled.source,
    })
    const states: Array<JetStreamStateSnapshot<string[]>> = []
    let completions = 0
    observeNatsJetStreamState(registry, definition, { liveBatchMs: 0 }).subscribe({
      next: (state) => states.push(state),
      complete: () => {
        completions += 1
      },
    })
    await Promise.resolve()

    await controlled.deliver({
      phase: 'replaying',
      data: [],
      restarts: 0,
      replay: { delivered: 0, remaining: 2 },
    })
    await controlled.deliver({
      phase: 'live',
      data: ['one', 'two'],
      restarts: 0,
      replay: { delivered: 2, remaining: 0 },
    })
    await controlled.close()
    await vi.waitFor(() => expect(completions).toBe(1))

    expect(states.map(({ phase, data }) => ({ phase, data }))).toEqual([
      { phase: 'replaying', data: [] },
      { phase: 'live', data: ['one', 'two'] },
    ])
    await registry.close()
  })

  it('emits hydration immediately and coalesces cumulative live state to one render window', async () => {
    vi.useFakeTimers()
    try {
      const registry = createSessionRegistry()
      const controlled = controllableSource<JetStreamStateSnapshot<number>>()
      const definition = defineSession({
        key: 'conversation:rxjs-state-batches',
        contract: 'conversation:v1',
        source: controlled.source,
      })
      const states: Array<JetStreamStateSnapshot<number>> = []
      let completions = 0
      observeNatsJetStreamState(registry, definition, { liveBatchMs: 16 }).subscribe({
        next: (state) => states.push(state),
        complete: () => {
          completions += 1
        },
      })
      await Promise.resolve()

      await controlled.deliver({
        phase: 'replaying',
        data: 0,
        restarts: 0,
        replay: { delivered: 0, remaining: 3 },
      })
      await controlled.deliver({
        phase: 'live',
        data: 3,
        restarts: 0,
        replay: { delivered: 3, remaining: 0 },
      })
      await controlled.deliver({
        phase: 'live',
        data: 4,
        restarts: 0,
        replay: { delivered: 3, remaining: 0 },
      })
      await controlled.deliver({
        phase: 'live',
        data: 5,
        restarts: 0,
        replay: { delivered: 3, remaining: 0 },
      })

      expect(states.map(({ phase, data }) => ({ phase, data }))).toEqual([
        { phase: 'replaying', data: 0 },
        { phase: 'live', data: 3 },
      ])
      await vi.advanceTimersByTimeAsync(15)
      expect(states.at(-1)?.data).toBe(3)
      await vi.advanceTimersByTimeAsync(1)
      expect(states.at(-1)?.data).toBe(5)

      await controlled.deliver({
        phase: 'live',
        data: 6,
        restarts: 0,
        replay: { delivered: 3, remaining: 0 },
      })
      await controlled.close()
      await Promise.resolve()

      expect(states.at(-1)?.data).toBe(6)
      expect(completions).toBe(1)
      await registry.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('flushes pending live state before an immediate reconnect phase', async () => {
    vi.useFakeTimers()
    try {
      const registry = createSessionRegistry()
      const controlled = controllableSource<JetStreamStateSnapshot<number>>()
      const definition = defineSession({
        key: 'conversation:rxjs-state-reconnect',
        contract: 'conversation:v1',
        source: controlled.source,
      })
      const states: Array<{ phase: string; data: number }> = []
      const subscription = observeNatsJetStreamState(registry, definition, {
        liveBatchMs: 16,
      }).subscribe((state) => states.push({ phase: state.phase, data: state.data }))
      await Promise.resolve()

      await controlled.deliver({
        phase: 'live',
        data: 1,
        restarts: 0,
        replay: { delivered: 1, remaining: 0 },
      })
      await controlled.deliver({
        phase: 'live',
        data: 2,
        restarts: 0,
        replay: { delivered: 1, remaining: 0 },
      })
      await controlled.deliver({
        phase: 'reconnecting',
        data: 2,
        restarts: 1,
        replay: { delivered: 1 },
      })
      await controlled.deliver({
        phase: 'live',
        data: 3,
        restarts: 1,
        replay: { delivered: 1, remaining: 0 },
      })

      expect(states).toEqual([
        { phase: 'live', data: 1 },
        { phase: 'live', data: 2 },
        { phase: 'reconnecting', data: 2 },
        { phase: 'live', data: 3 },
      ])
      subscription.unsubscribe()
      await Promise.resolve()
      expect(controlled.close).toHaveBeenCalledOnce()
      await registry.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('turns a large cumulative live burst into one presentation update', async () => {
    vi.useFakeTimers()
    try {
      const registry = createSessionRegistry()
      const controlled = controllableSource<JetStreamStateSnapshot<number>>()
      const definition = defineSession({
        key: 'conversation:rxjs-state-burst',
        contract: 'conversation:v1',
        source: controlled.source,
      })
      const rendered: number[] = []
      const subscription = observeNatsJetStreamState(registry, definition, {
        liveBatchMs: 16,
      }).subscribe((state) => rendered.push(state.data))
      await Promise.resolve()

      await controlled.deliver({
        phase: 'replaying',
        data: 0,
        restarts: 0,
        replay: { delivered: 0, remaining: 217 },
      })
      await controlled.deliver({
        phase: 'live',
        data: 217,
        restarts: 0,
        replay: { delivered: 217, remaining: 0 },
      })
      for (let data = 218; data <= 434; data += 1) {
        await controlled.deliver({
          phase: 'live',
          data,
          restarts: 0,
          replay: { delivered: 217, remaining: 0 },
        })
      }

      expect(rendered).toEqual([0, 217])
      await vi.advanceTimersByTimeAsync(16)
      expect(rendered).toEqual([0, 217, 434])

      subscription.unsubscribe()
      await registry.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('applies shared count and byte bounds to cumulative live presentation', async () => {
    const registry = createSessionRegistry()
    const controlled = controllableSource<JetStreamStateSnapshot<number>>()
    const definition = defineSession({
      key: 'conversation:rxjs-shared-policy',
      contract: 'conversation:v1',
      source: controlled.source,
    })
    const states: number[] = []
    const subscription = observeNatsJetStreamState(registry, definition, {
      batchPolicy: { maxItems: 2, maxBytes: 2, sizeOf: () => 1 },
    }).subscribe((state) => states.push(state.data))
    await Promise.resolve()

    for (const data of [1, 2, 3]) {
      await controlled.deliver({
        phase: 'live',
        data,
        restarts: 0,
        replay: { delivered: 0, remaining: 0 },
      })
    }

    expect(states).toEqual([1, 3])
    subscription.unsubscribe()
    await registry.close()
  })

  it('discards a pending live partial when the source fails', async () => {
    vi.useFakeTimers()
    try {
      const registry = createSessionRegistry()
      const controlled = controllableSource<JetStreamStateSnapshot<number>>()
      const definition = defineSession({
        key: 'conversation:rxjs-abnormal-batch',
        contract: 'conversation:v1',
        source: controlled.source,
      })
      const states: number[] = []
      let failure: unknown
      observeNatsJetStreamState(registry, definition, { liveBatchMs: 16 }).subscribe({
        next: (state) => states.push(state.data),
        error: (error) => (failure = error),
      })
      await Promise.resolve()
      await controlled.deliver({
        phase: 'live',
        data: 1,
        restarts: 0,
        replay: { delivered: 0, remaining: 0 },
      })
      await controlled.deliver({
        phase: 'live',
        data: 2,
        restarts: 0,
        replay: { delivered: 0, remaining: 0 },
      })
      controlled.fail(new Error('source failed'))
      await Promise.resolve()

      expect(states).toEqual([1])
      expect(failure).toMatchObject({ message: 'source failed' })
      await registry.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('rejects invalid live state batch windows', () => {
    const registry = createSessionRegistry()
    const controlled = controllableSource<JetStreamStateSnapshot<number>>()
    const definition = defineSession({
      key: 'conversation:rxjs-state-invalid',
      contract: 'conversation:v1',
      source: controlled.source,
    })

    expect(() => observeNatsJetStreamState(registry, definition, { liveBatchMs: -1 })).toThrow(
      'liveBatchMs'
    )
    expect(() =>
      observeNatsJetStreamState(registry, definition, { liveBatchMs: Number.NaN })
    ).toThrow('liveBatchMs')
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
