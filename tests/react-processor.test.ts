/** @vitest-environment jsdom */

import { act, createElement } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { NatsRuntime } from '@natsail/core'
import type {
  JetStreamProcessingDelivery,
  JetStreamProcessorInspection,
  JetStreamProcessorLease,
} from '@natsail/jetstream'
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
    mocks.processJetStream.mockReturnValue(processorLease(close))
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
    mocks.processJetStream.mockImplementation(
      (_runtime, _options, handler): JetStreamProcessorLease => {
        deliver = handler
        return processorLease(close, closed)
      }
    )
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
        subject: 'events.processed',
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

  it('waits for the previous lease to close before replacing its key', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
    let finishFirstClose!: () => void
    const firstClose = new Promise<void>((resolve) => {
      finishFirstClose = resolve
    })
    const closeFirst = vi.fn(() => firstClose)
    const closeSecond = vi.fn(async () => undefined)
    mocks.processJetStream
      .mockReturnValueOnce(processorLease(closeFirst))
      .mockReturnValueOnce(processorLease(closeSecond))
    const runtime = { events: emptyEvents() } as NatsRuntime
    const sessions = createSessionRegistry()
    const container = document.createElement('div')
    let root!: Root

    function ProcessorProbe({ processorKey }: { processorKey: string }) {
      const snapshot = useNatsJetStreamProcessor(
        processorKey,
        {
          stream: 'EVENTS',
          consumer: { mode: 'owned', name: 'replaceable_processor' },
          filter: 'events.>',
          start: 'all',
          decode: () => 'decoded',
        },
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
          createElement(ProcessorProbe, { processorKey: 'first' })
        )
      )
      await Promise.resolve()
    })
    expect(mocks.processJetStream).toHaveBeenCalledOnce()

    await act(async () => {
      root.render(
        createElement(
          NatsProvider,
          { runtime, sessions },
          createElement(ProcessorProbe, { processorKey: 'second' })
        )
      )
      await Promise.resolve()
    })
    expect(closeFirst).toHaveBeenCalledOnce()
    expect(mocks.processJetStream).toHaveBeenCalledOnce()
    expect(container.textContent).toBe('connecting')

    await act(async () => {
      finishFirstClose()
      await firstClose
      await Promise.resolve()
    })
    expect(mocks.processJetStream).toHaveBeenCalledTimes(2)
    expect(container.textContent).toBe('live')

    await act(async () => root.unmount())
    expect(closeSecond).toHaveBeenCalledOnce()
    await sessions.close()
  })

  it('reports processor recovery without remounting the hook', async () => {
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
    let inspection = processorInspection()
    const listeners = new Set<() => void>()
    const close = vi.fn(async () => undefined)
    mocks.processJetStream.mockReturnValue({
      ready: Promise.resolve(),
      closed: new Promise(() => undefined),
      close,
      inspect: () => inspection,
      subscribe: (listener: () => void) => {
        listeners.add(listener)
        return () => listeners.delete(listener)
      },
    })
    const runtime = { events: emptyEvents() } as NatsRuntime
    const sessions = createSessionRegistry()
    const container = document.createElement('div')
    let root!: Root

    function ProcessorProbe() {
      const snapshot = useNatsJetStreamProcessor(
        'recovering-processor',
        {
          stream: 'EVENTS',
          consumer: { mode: 'owned', name: 'recovering_processor' },
          filter: 'events.>',
          start: 'all',
          recovery: {},
          decode: () => 'decoded',
        },
        async () => undefined
      )
      return createElement('output', null, `${snapshot.phase}:${snapshot.restarts}`)
    }

    await act(async () => {
      root = createRoot(container)
      root.render(createElement(NatsProvider, { runtime, sessions }, createElement(ProcessorProbe)))
      await Promise.resolve()
    })
    expect(container.textContent).toBe('live:0')

    await act(async () => {
      inspection = processorInspection({
        phase: 'reconnecting',
        restarts: 1,
        error: new Error('connection lost'),
      })
      for (const listener of listeners) listener()
    })
    expect(container.textContent).toBe('reconnecting:1')

    await act(async () => root.unmount())
    expect(close).toHaveBeenCalledOnce()
    await sessions.close()
  })
})

function processorLease(
  close: () => Promise<void>,
  closed: Promise<void> = new Promise(() => undefined)
): JetStreamProcessorLease {
  return {
    ready: Promise.resolve(),
    closed,
    close,
    inspect: () => processorInspection(),
    subscribe: () => () => undefined,
  }
}

function processorInspection(
  patch: Partial<JetStreamProcessorInspection> = {}
): JetStreamProcessorInspection {
  return {
    phase: 'live',
    restarts: 0,
    stream: 'EVENTS',
    consumer: { name: 'processor', mode: 'owned', owned: true },
    pendingAcknowledgements: 0,
    pendingMessages: 0,
    delivered: { consumer: 0, stream: 0 },
    acknowledged: { consumer: 0, stream: 0 },
    redeliveries: 0,
    paused: false,
    desired: {
      durableName: 'processor',
      deliveryKind: 'pull',
      ackPolicy: 'explicit',
      deliverPolicy: 'all',
      replayPolicy: 'instant',
      filters: ['events.>'],
    },
    ...patch,
  }
}

function emptyEvents(): AsyncIterable<never> {
  return {
    async *[Symbol.asyncIterator]() {},
  }
}
