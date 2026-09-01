/** @vitest-environment jsdom */

import { Effect, Fiber, Stream } from 'effect'
import { act, createElement } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { describe, expect, it, vi } from 'vitest'

import type { NatsRuntime, NatsRuntimeEvent, SubscriptionLease } from '@natsail/core'
import { makeNatsail } from '@natsail/effect'
import { NatsProvider, useNatsSession } from '@natsail/react'
import { observeNatsSessionValues } from '@natsail/rxjs'
import { createCoreSessionSource, createSessionRegistry, defineSession } from '@natsail/session'

describe('Effect, React, and RxJS adapter composition', () => {
  it('shares one validated source across a Stream, hook, and Observable', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
    let accept!: (value: string) => Promise<void>
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
    const subscribe = vi.fn(
      (_options: unknown, handler: (value: string) => Promise<void>): SubscriptionLease => {
        accept = handler
        return lease
      }
    )
    const runtime = { events: emptyEvents(), subscribe } as unknown as NatsRuntime
    const registry = createSessionRegistry()
    const rxValues: string[] = []
    const options = { subject: 'events.shared', decode: () => 'decoded' }
    const definition = defineSession({
      key: 'conversation:shared-adapters',
      contract: 'subject=events.shared;decoder=test',
      source: createCoreSessionSource(runtime, options),
    })
    const rxSubscription = observeNatsSessionValues(registry, definition).subscribe((value) =>
      rxValues.push(value)
    )
    const effectFiber = Effect.runFork(
      makeNatsail({ runtime, sessions: registry })
        .sessionValues(definition)
        .pipe(Stream.take(1), Stream.runCollect)
    )
    const container = document.createElement('div')
    let root!: Root

    function Probe() {
      const snapshot = useNatsSession(definition)
      return createElement('output', null, snapshot.value ?? '')
    }

    await act(async () => {
      root = createRoot(container)
      root.render(
        createElement(NatsProvider, { runtime, sessions: registry }, createElement(Probe))
      )
    })
    await vi.waitFor(() => expect(registry.inspect().sessions[0]?.references).toBe(3))
    expect(subscribe).toHaveBeenCalledOnce()

    await act(async () => accept('hello'))
    expect(container.textContent).toBe('hello')
    expect(rxValues).toEqual(['hello'])
    expect(await Effect.runPromise(Fiber.join(effectFiber))).toEqual(['hello'])

    rxSubscription.unsubscribe()
    expect(close).not.toHaveBeenCalled()

    await act(async () => root.unmount())
    await Promise.resolve()
    expect(close).toHaveBeenCalledOnce()
  })
})

function emptyEvents(): AsyncIterable<NatsRuntimeEvent> {
  return {
    async *[Symbol.asyncIterator]() {},
  }
}
