/** @vitest-environment jsdom */

import { act, createElement } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { NatsRuntime, SubscriptionLease } from '@natsail/core'
import type { JetStreamProcessingDelivery } from '@natsail/jetstream'
import { NatsProvider, useNatsJetStreamProcessor } from '@natsail/react'
import { createSessionRegistry } from '@natsail/session'

const mocks = vi.hoisted(() => ({
  processJetStream: vi.fn(),
}))

vi.mock('@natsail/jetstream', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@natsail/jetstream')>()),
  processJetStream: mocks.processJetStream,
}))

describe('React explicit-ack processor adapter', () => {
  beforeEach(() => mocks.processJetStream.mockReset())

  it('stays closed while options are disabled and starts when options become available', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
    const close = vi.fn(async () => undefined)
    mocks.processJetStream.mockReturnValue({
      ready: Promise.resolve(),
      closed: new Promise(() => undefined),
      close,
    })
    const runtime = { events: emptyEvents() } as NatsRuntime
    const sessions = createSessionRegistry()
    const container = document.createElement('div')
    let root!: Root

    function ProcessorProbe({ enabled }: { enabled: boolean }) {
      const snapshot = useNatsJetStreamProcessor(
        'optional-processor',
        enabled
          ? {
              stream: 'EVENTS',
              consumer: { mode: 'owned', name: 'optional_processor' },
              filter: 'events.>',
              start: 'all',
              decode: () => 'decoded',
            }
          : null,
        async () => undefined
      )
      return createElement('output', null, snapshot.phase)
    }

    await act(async () => {
      root = createRoot(container)
      root.render(
        createElement(
          NatsProvider,
          { runtime, sessions },
          createElement(ProcessorProbe, { enabled: false })
        )
      )
      await Promise.resolve()
    })
    expect(mocks.processJetStream).not.toHaveBeenCalled()
    expect(container.textContent).toBe('closed')

    await act(async () => {
      root.render(
        createElement(
          NatsProvider,
          { runtime, sessions },
          createElement(ProcessorProbe, { enabled: true })
        )
      )
      await Promise.resolve()
    })
    expect(mocks.processJetStream).toHaveBeenCalledOnce()
    expect(container.textContent).toBe('live')

    await act(async () => root.unmount())
    expect(close).toHaveBeenCalledOnce()
    await sessions.close()
  })

  it('owns one processor lease and closes it after unmount', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
    let deliver!: (delivery: JetStreamProcessingDelivery<string>) => Promise<void>
    let closeLease!: () => void
    const closed = new Promise<void>((resolve) => {
      closeLease = resolve
    })
    const close = vi.fn(async () => closeLease())
    mocks.processJetStream.mockImplementation((_runtime, _options, handler): SubscriptionLease => {
      deliver = handler
      return { ready: Promise.resolve(), closed, close }
    })
    const runtime = { events: emptyEvents() } as NatsRuntime
    const sessions = createSessionRegistry()
    const received: string[] = []
    const container = document.createElement('div')
    let root!: Root

    function ProcessorProbe() {
      const snapshot = useNatsJetStreamProcessor(
        'react-processor',
        {
          stream: 'EVENTS',
          consumer: { mode: 'owned', name: 'react_processor' },
          filter: 'events.>',
          start: 'all',
          decode: () => 'decoded',
        },
        async (delivery) => {
          received.push(delivery.value)
        }
      )
      return createElement('output', null, snapshot.phase)
    }

    await act(async () => {
      root = createRoot(container)
      root.render(createElement(NatsProvider, { runtime, sessions }, createElement(ProcessorProbe)))
      await Promise.resolve()
    })

    expect(mocks.processJetStream).toHaveBeenCalledOnce()
    expect(container.textContent).toBe('live')
    await act(async () => {
      await deliver({
        value: 'processed',
        cursor: { stream: 'EVENTS', sequence: 1 },
        redelivered: false,
        deliveryAttempt: 1,
      })
    })
    expect(received).toEqual(['processed'])

    await act(async () => root.unmount())
    expect(close).toHaveBeenCalledOnce()
    await sessions.close()
  })
})

function emptyEvents(): AsyncIterable<never> {
  return {
    async *[Symbol.asyncIterator]() {},
  }
}
