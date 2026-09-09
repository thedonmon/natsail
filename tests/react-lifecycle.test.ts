/** @vitest-environment jsdom */

import { act, createElement, StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { afterEach, expect, it, vi } from 'vitest'
import type { NatsConnection } from '@nats-io/nats-core'
import { createNatsRuntime, natsCodecs } from '@natsail/core'
import {
  NatsManagedProvider,
  useNatsCoreSubscription,
  type NatsManagedResource,
} from '@natsail/react'
import { closeNatsResources, createSessionRegistry } from '@natsail/session'
import { deferred, transport } from './fixtures/lifecycle'

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
afterEach(() => vi.useRealTimers())

function PendingSession() {
  useNatsCoreSubscription('work', { subject: 'work', codec: natsCodecs.text })
  return null
}

it.each(['default', 'custom'] as const)(
  'starts the runtime deadline with %s cleanup while a real session awaits connection',
  async (cleanup) => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    let rejectFactory!: (error: Error) => void
    const factory = new Promise<NatsConnection>((_, reject) => {
      rejectFactory = reject
    })
    const runtime = createNatsRuntime({ connect: () => factory, shutdownTimeoutMs: 10 })
    const sessions = createSessionRegistry({ idleCloseMs: 1_000 })
    const onCloseError = vi.fn()
    const root = createRoot(document.createElement('div'))
    await act(async () =>
      root.render(
        createElement(NatsManagedProvider, {
          identity: 'first',
          create: () => ({
            runtime,
            sessions,
            ...(cleanup === 'custom'
              ? { close: () => closeNatsResources({ runtime, sessions }) }
              : {}),
          }),
          onCloseError,
          children: createElement(PendingSession),
        })
      )
    )
    expect(runtime.inspect().activeResources).toBe(1)
    await act(async () => root.unmount())
    try {
      await vi.advanceTimersByTimeAsync(10)
      expect(runtime.inspect().connection.state).toBe('closed')
      expect(runtime.inspect().activeResources).toBe(0)
      expect(onCloseError).toHaveBeenCalledWith(
        expect.objectContaining({
          name: 'NatsRuntimeShutdownTimeoutError',
        })
      )
    } finally {
      rejectFactory(new Error('test factory stopped'))
      await runtime.close().catch(() => undefined)
    }
  }
)

it('keeps StrictMode replay safe and allows replacements while old pending factories shut down', async () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
  const resources: (NatsManagedResource & {
    factory: ReturnType<typeof deferred<NatsConnection>>
    signal: AbortSignal | undefined
  })[] = []
  const create = vi.fn(() => {
    const factory = deferred<NatsConnection>()
    let signal: AbortSignal | undefined
    const runtime = createNatsRuntime({
      connect: {
        create: (context) => {
          signal = context.signal
          return factory.promise
        },
      },
      shutdownTimeoutMs: 10,
    })
    const resource = {
      runtime,
      sessions: createSessionRegistry({ idleCloseMs: 1_000 }),
      factory,
      get signal() {
        return signal
      },
    }
    resources.push(resource)
    return resource
  })
  const onCloseError = vi.fn()
  const root = createRoot(document.createElement('div'))
  const render = async (identity: string) =>
    act(async () =>
      root.render(
        createElement(
          StrictMode,
          null,
          createElement(NatsManagedProvider, {
            identity,
            create,
            onCloseError,
            children: createElement(PendingSession),
          })
        )
      )
    )
  await render('first')
  expect(create).toHaveBeenCalledOnce()
  expect(resources[0]?.signal?.aborted).toBe(false)
  await render('second')
  await render('third')
  expect(create).toHaveBeenCalledTimes(3)
  expect(resources.map((resource) => resource.signal?.aborted)).toEqual([true, true, false])
  expect(resources[2]?.runtime.inspect().connection.state).toBe('connecting')
  await vi.advanceTimersByTimeAsync(10)
  expect(onCloseError).toHaveBeenCalledTimes(2)
  for (const resource of resources.slice(0, 2)) {
    const network = transport()
    resource.factory.resolve(network.connection)
    await vi.advanceTimersByTimeAsync(0)
    expect(network.close).toHaveBeenCalledOnce()
    expect(resource.runtime.inspect()).toMatchObject({
      connectionGeneration: 0,
      activeResources: 0,
    })
  }
  const current = resources[2]!
  const network = transport()
  await act(async () => current.factory.resolve(network.connection))
  expect(current.runtime.inspect().connectionGeneration).toBe(1)
  await act(async () => root.unmount())
  await current.runtime.close()
  expect(network.drain).toHaveBeenCalledOnce()
  expect(network.close).not.toHaveBeenCalled()
  expect(onCloseError).toHaveBeenCalledTimes(2)
})

it('bounds provider cleanup while a real runtime handler is stalled', async () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
  const network = transport()
  const runtime = createNatsRuntime({
    connect: async () => network.connection,
    shutdownTimeoutMs: 10,
  })
  const sessions = createSessionRegistry({ idleCloseMs: 1_000 })
  const started = deferred<void>()
  const release = deferred<void>()
  let signal: AbortSignal | undefined
  const handle = sessions.acquire('stalled', (accept) =>
    runtime.subscribe(
      { subject: 'work', codec: natsCodecs.text },
      async (value, _message, context) => {
        signal = context?.signal
        started.resolve()
        await release.promise
        await accept(value)
      }
    )
  )
  const root = createRoot(document.createElement('div'))
  const onCloseError = vi.fn()
  await act(async () =>
    root.render(
      createElement(NatsManagedProvider, {
        identity: 'work',
        create: () => ({ runtime, sessions }),
        onCloseError,
      })
    )
  )
  await started.promise
  await act(async () => root.unmount())
  expect(signal?.aborted).toBe(false)
  await vi.advanceTimersByTimeAsync(10)
  expect(signal?.aborted).toBe(true)
  expect(onCloseError).toHaveBeenCalledWith(
    expect.objectContaining({ name: 'NatsRuntimeShutdownTimeoutError' })
  )
  expect(network.close).toHaveBeenCalledOnce()
  release.resolve()
  await sessions.close()
  expect(handle.getSnapshot().phase).toBe('closed')
  expect(runtime.inspect().activeResources).toBe(0)
})

it('finishes pending provider cleanup normally when its factory cooperates with disposal', async () => {
  const runtime = createNatsRuntime({
    connect: {
      create: ({ signal }) =>
        new Promise<NatsConnection>((_, reject) => {
          signal.addEventListener('abort', () => reject(signal.reason), { once: true })
        }),
    },
  })
  const sessions = createSessionRegistry({ idleCloseMs: 1_000 })
  const onCloseError = vi.fn()
  const root = createRoot(document.createElement('div'))
  await act(async () =>
    root.render(
      createElement(NatsManagedProvider, {
        identity: 'work',
        create: () => ({ runtime, sessions }),
        onCloseError,
        children: createElement(PendingSession),
      })
    )
  )
  await act(async () => root.unmount())
  await expect(runtime.close()).resolves.toBeUndefined()
  await sessions.close()
  expect(onCloseError).not.toHaveBeenCalled()
  expect(runtime.inspect().activeResources).toBe(0)
})
