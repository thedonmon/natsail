/** @vitest-environment jsdom */

import { act, createElement } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { describe, expect, it, vi } from 'vitest'

import type { NatsRuntime, NatsRuntimeEvent, SubscriptionLease } from '@natsail/core'
import { NatsProvider, useNatsCoreSubscriptionSelector } from '@natsail/react'
import { observeNatsCoreSubscription } from '@natsail/rxjs'
import { createSessionRegistry } from '@natsail/session'

describe('React and RxJS adapter composition', () => {
  it('shares one source between a hook and an Observable', async () => {
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
    const rxSubscription = observeNatsCoreSubscription(
      registry,
      runtime,
      'conversation:shared-adapters',
      options
    ).subscribe((value) => rxValues.push(value))
    const container = document.createElement('div')
    let root!: Root

    function Probe() {
      const value = useNatsCoreSubscriptionSelector(
        'conversation:shared-adapters',
        options,
        (snapshot) => snapshot.value
      )
      return createElement('output', null, value ?? '')
    }

    await act(async () => {
      root = createRoot(container)
      root.render(
        createElement(NatsProvider, { runtime, sessions: registry }, createElement(Probe))
      )
    })
    expect(subscribe).toHaveBeenCalledOnce()

    await act(async () => accept('hello'))
    expect(container.textContent).toBe('hello')
    expect(rxValues).toEqual(['hello'])

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
