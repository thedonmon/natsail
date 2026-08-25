/** @vitest-environment jsdom */

import { act, createElement, Fragment, StrictMode } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { describe, expect, it, vi } from 'vitest'

import type { SubscriptionLease } from '@natsail/core'
import {
  NatsProvider,
  NatsManagedProvider,
  useNatsConnection,
  useNatsCoreSubscriptionReducer,
  useNatsCoreSubscriptionSelector,
  useNatsJetStreamReducer,
  useNatsRuntime,
  useNatsRuntimeStatus,
  useNatsSession,
  useNatsSessionSelector,
} from '@natsail/react'
import type { NatsRuntime, NatsRuntimeEvent } from '@natsail/core'
import {
  createSessionRegistry,
  defineSession,
  type SessionRegistry,
  type SessionSource,
} from '@natsail/session'
import type { JetStreamStateSnapshot } from '@natsail/jetstream'

function controllableSource<T>(): {
  source: SessionSource<T>
  deliver(value: T): Promise<void>
  close: ReturnType<typeof vi.fn<() => Promise<void>>>
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

  return {
    source: (next) => {
      accept = next
      return lease
    },
    deliver: (value) => accept(value),
    close,
  }
}

function Probe<T>({
  label,
  registry,
  source,
}: {
  label: string
  registry: SessionRegistry
  source: SessionSource<T>
}) {
  const snapshot = useNatsSession(registry, 'conversation:react', source)
  return createElement('output', null, `${label}:${snapshot.phase}:${String(snapshot.value ?? '')}`)
}

describe('React session adapter', () => {
  it('shares a session across hooks and releases it after the final unmount', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
    const registry = createSessionRegistry({ idleCloseMs: 10 })
    const controlled = controllableSource<string>()
    const container = document.createElement('div')
    let root!: Root

    await act(async () => {
      root = createRoot(container)
      root.render(
        createElement(
          Fragment,
          null,
          createElement(Probe, {
            key: 'first',
            label: 'first',
            registry,
            source: controlled.source,
          }),
          createElement(Probe, {
            key: 'second',
            label: 'second',
            registry,
            source: controlled.source,
          })
        )
      )
    })

    await act(async () => controlled.deliver('hello'))
    expect(container.textContent).toBe('first:live:hellosecond:live:hello')

    await act(async () => {
      root.render(
        createElement(Probe, {
          label: 'first',
          registry,
          source: controlled.source,
        })
      )
    })
    expect(controlled.close).not.toHaveBeenCalled()

    await act(async () => root.unmount())
    await new Promise((resolve) => setTimeout(resolve, 15))
    expect(controlled.close).toHaveBeenCalledOnce()
  })

  it('provides the runtime and registry once at the application root', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
    const registry = createSessionRegistry()
    const controlled = controllableSource<string>()
    const runtime = { events: emptyEvents() } as NatsRuntime
    const container = document.createElement('div')
    let root!: Root

    function ContextProbe() {
      const activeRuntime = useNatsRuntime()
      const snapshot = useNatsSession('context-session', controlled.source)
      return createElement(
        'output',
        null,
        `${String(activeRuntime === runtime)}:${snapshot.phase}:${snapshot.value ?? ''}`
      )
    }

    await act(async () => {
      root = createRoot(container)
      root.render(
        createElement(NatsProvider, { runtime, sessions: registry }, createElement(ContextProbe))
      )
    })

    await act(async () => controlled.deliver('provided'))
    expect(container.textContent).toBe('true:live:provided')

    await act(async () => root.unmount())
    await registry.close()
  })

  it('re-renders a selector only when its selected value changes', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
    const registry = createSessionRegistry()
    const controlled = controllableSource<string>()
    const runtime = { events: emptyEvents() } as NatsRuntime
    const container = document.createElement('div')
    let renders = 0
    let root!: Root

    function SelectorProbe() {
      renders += 1
      const value = useNatsSessionSelector(
        'selector-session',
        controlled.source,
        (snapshot) => snapshot.value
      )
      return createElement('output', null, value ?? '')
    }

    await act(async () => {
      root = createRoot(container)
      root.render(
        createElement(NatsProvider, { runtime, sessions: registry }, createElement(SelectorProbe))
      )
    })

    await act(async () => controlled.deliver('same'))
    const rendersAfterFirstValue = renders
    await act(async () => controlled.deliver('same'))

    expect(container.textContent).toBe('same')
    expect(renders).toBe(rendersAfterFirstValue)

    await act(async () => root.unmount())
    await registry.close()
  })

  it('exposes runtime connection state without duplicating connection ownership', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
    const events = controllableEvents()
    const runtime = { events: events.iterable } as NatsRuntime
    const registry = createSessionRegistry()
    const container = document.createElement('div')
    let root!: Root

    function StatusProbe() {
      const status = useNatsRuntimeStatus()
      return createElement('output', null, status.state)
    }

    await act(async () => {
      root = createRoot(container)
      root.render(
        createElement(NatsProvider, { runtime, sessions: registry }, createElement(StatusProbe))
      )
    })
    expect(container.textContent).toBe('idle')

    await act(async () => {
      events.push({ type: 'status', state: 'reconnecting', at: 1 })
      await Promise.resolve()
    })
    expect(container.textContent).toBe('reconnecting')

    await act(async () => root.unmount())
    await registry.close()
  })

  it('resolves the runtime-owned connection without application effects', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
    const events = controllableEvents()
    const connection = { getServer: () => 'nats://test' }
    const connect = vi.fn().mockResolvedValue(connection)
    const runtime = { events: events.iterable, connection: connect } as unknown as NatsRuntime
    const registry = createSessionRegistry()
    const container = document.createElement('div')
    let root!: Root

    function ConnectionProbe() {
      const snapshot = useNatsConnection()
      return createElement(
        'output',
        null,
        `${snapshot.status.state}:${snapshot.connection?.getServer() ?? 'none'}`
      )
    }

    await act(async () => {
      root = createRoot(container)
      root.render(
        createElement(NatsProvider, { runtime, sessions: registry }, createElement(ConnectionProbe))
      )
      await Promise.resolve()
    })

    expect(container.textContent).toBe('idle:nats://test')
    expect(connect).toHaveBeenCalledOnce()

    await act(async () => root.unmount())
    await registry.close()
  })

  it('opens a shared Core NATS subscription without application source glue', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
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
    const runtime = { events: emptyEvents(), subscribe } as unknown as NatsRuntime
    const registry = createSessionRegistry()
    const container = document.createElement('div')
    let root!: Root

    function CoreProbe() {
      const value = useNatsCoreSubscriptionSelector(
        'orders',
        { subject: 'events.orders', decode: () => 'decoded' },
        (snapshot) => snapshot.value
      )
      return createElement('output', null, value ?? '')
    }

    await act(async () => {
      root = createRoot(container)
      root.render(
        createElement(NatsProvider, { runtime, sessions: registry }, createElement(CoreProbe))
      )
    })
    expect(subscribe).toHaveBeenCalledOnce()
    expect(subscribe.mock.calls[0]?.[0]).toMatchObject({ subject: 'events.orders' })

    await act(async () => accept('created'))
    expect(container.textContent).toBe('created')

    await act(async () => root.unmount())
    expect(close).toHaveBeenCalledOnce()
  })

  it('reduces every Core NATS delivery before React renders the collection', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
    let accept!: (value: string) => Promise<void>
    const sourceLease = controllableSource<string>()
    const subscribe = vi.fn(
      (_options: unknown, handler: (value: string) => Promise<void>): SubscriptionLease => {
        accept = handler
        return {
          ready: Promise.resolve(),
          closed: new Promise<void>(() => undefined),
          close: async () => undefined,
        }
      }
    )
    const runtime = { events: emptyEvents(), subscribe } as unknown as NatsRuntime
    const registry = createSessionRegistry()
    const container = document.createElement('div')
    let root!: Root

    function ReducedProbe() {
      const snapshot = useNatsCoreSubscriptionReducer(
        'events:reduced',
        { subject: 'events.reduced', decode: () => 'decoded' },
        () => [] as string[],
        (values, value) => [...values, value]
      )
      return createElement('output', null, snapshot.value?.join(',') ?? '')
    }

    await act(async () => {
      root = createRoot(container)
      root.render(
        createElement(NatsProvider, { runtime, sessions: registry }, createElement(ReducedProbe))
      )
    })

    await act(async () => {
      await Promise.all([accept('one'), accept('two'), accept('three')])
    })
    expect(container.textContent).toBe('one,two,three')
    expect(subscribe).toHaveBeenCalledOnce()

    await act(async () => root.unmount())
    await registry.close()
    await sourceLease.close()
  })

  it('owns provider cleanup without closing the resource during Strict Mode effect replay', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
    const sessions = createSessionRegistry()
    const runtime = {
      events: emptyEvents(),
      close: vi.fn(async () => undefined),
    } as unknown as NatsRuntime
    const close = vi.fn(async () => {
      await sessions.close()
      await runtime.close()
    })
    const create = vi.fn(() => ({ runtime, sessions, close }))
    const container = document.createElement('div')
    let root!: Root

    function ManagedProbe() {
      return createElement('output', null, useNatsRuntime() === runtime ? 'managed' : 'wrong')
    }

    await act(async () => {
      root = createRoot(container)
      root.render(
        createElement(
          StrictMode,
          null,
          createElement(
            NatsManagedProvider,
            { identity: 'primary', create, fallback: createElement('output', null, 'loading') },
            createElement(ManagedProbe)
          )
        )
      )
      await Promise.resolve()
    })

    expect(container.textContent).toBe('managed')
    expect(create).toHaveBeenCalledOnce()
    expect(close).not.toHaveBeenCalled()

    await act(async () => root.unmount())
    await Promise.resolve()
    expect(close).toHaveBeenCalledOnce()
  })

  it('coalesces reduced JetStream renders while preserving every accepted state', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
    vi.useFakeTimers()
    try {
      const registry = createSessionRegistry()
      const controlled = controllableSource<JetStreamStateSnapshot<string[]>>()
      const definition = defineSession({
        key: 'conversation:coalesced',
        contract: 'conversation:v1',
        source: controlled.source,
      })
      const runtime = { events: emptyEvents() } as NatsRuntime
      const container = document.createElement('div')
      let renders = 0
      let root!: Root

      function ReducedJetStreamProbe() {
        renders += 1
        const snapshot = useNatsJetStreamReducer(definition)
        return createElement('output', null, snapshot.value?.data.join(',') ?? '')
      }

      await act(async () => {
        root = createRoot(container)
        root.render(
          createElement(
            NatsProvider,
            { runtime, sessions: registry },
            createElement(ReducedJetStreamProbe)
          )
        )
      })
      const rendersBeforeBurst = renders

      await act(async () => {
        await controlled.deliver({
          phase: 'live',
          data: ['one'],
          restarts: 0,
          replay: { delivered: 1, remaining: 0 },
        })
        await controlled.deliver({
          phase: 'live',
          data: ['one', 'two'],
          restarts: 0,
          replay: { delivered: 1, remaining: 0 },
        })
        await controlled.deliver({
          phase: 'live',
          data: ['one', 'two', 'three'],
          restarts: 0,
          replay: { delivered: 1, remaining: 0 },
        })
      })

      expect(renders).toBe(rendersBeforeBurst)
      await act(async () => vi.advanceTimersByTimeAsync(16))
      expect(container.textContent).toBe('one,two,three')
      expect(renders).toBe(rendersBeforeBurst + 1)

      await act(async () => root.unmount())
      await registry.close()
    } finally {
      vi.useRealTimers()
    }
  })
})

function emptyEvents(): AsyncIterable<NatsRuntimeEvent> {
  return {
    async *[Symbol.asyncIterator]() {},
  }
}

function controllableEvents(): {
  iterable: AsyncIterable<NatsRuntimeEvent>
  push(event: NatsRuntimeEvent): void
} {
  const queue: NatsRuntimeEvent[] = []
  let resume: (() => void) | undefined
  let closed = false

  return {
    iterable: {
      async *[Symbol.asyncIterator]() {
        try {
          while (!closed) {
            if (queue.length === 0) {
              await new Promise<void>((resolve) => {
                resume = resolve
              })
            }
            while (queue.length > 0) {
              yield queue.shift()!
            }
          }
        } finally {
          closed = true
          resume?.()
        }
      },
    },
    push(event) {
      queue.push(event)
      resume?.()
      resume = undefined
    },
  }
}
