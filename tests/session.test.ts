import { describe, expect, it, vi } from 'vitest'

import type { NatsRuntime, NatsailScheduler, SubscriptionLease } from '@natsail/core'
import {
  createCoreSessionSource,
  createReducingSessionSource,
  createSessionRegistry,
  defineSession,
  SessionContractMismatchError,
} from '@natsail/session'

function controllableLease(): {
  lease: SubscriptionLease
  close: ReturnType<typeof vi.fn<() => Promise<void>>>
} {
  let closeSession!: () => void
  const closed = new Promise<void>((resolve) => {
    closeSession = resolve
  })
  const close = vi.fn(async () => {
    closeSession()
  })

  return {
    lease: {
      ready: Promise.resolve(),
      closed,
      close,
    },
    close,
  }
}

describe('session registry', () => {
  it('adapts a Core NATS subscription into a session source', () => {
    const sourceLease = controllableLease()
    const subscribe = vi.fn(() => sourceLease.lease)
    const runtime = { subscribe } as unknown as NatsRuntime
    const options = {
      subject: 'events.orders',
      decode: () => 'decoded',
    }
    const accept = vi.fn(async (_value: string) => undefined)

    const source = createCoreSessionSource(runtime, options)

    expect(source(accept)).toBe(sourceLease.lease)
    expect(subscribe).toHaveBeenCalledWith(options, accept)
  })

  it('preserves a value delivered before the source becomes ready', async () => {
    let resolveReady!: () => void
    const ready = new Promise<void>((resolve) => {
      resolveReady = resolve
    })
    let accept!: (value: string) => Promise<void>
    const sourceLease = controllableLease()
    const registry = createSessionRegistry()
    const handle = registry.acquire('early-value', (next) => {
      accept = next
      return { ...sourceLease.lease, ready }
    })

    await accept('arrived-early')
    expect(handle.getSnapshot()).toEqual({
      phase: 'connecting',
      revision: 1,
      valueRevision: 1,
      value: 'arrived-early',
    })

    resolveReady()
    await handle.ready
    expect(handle.getSnapshot()).toEqual({
      phase: 'live',
      revision: 2,
      valueRevision: 1,
      value: 'arrived-early',
    })

    await handle.release()
  })

  it('reduces concurrent source deliveries serially without skipping values', async () => {
    const sourceLease = controllableLease()
    let deliver!: (value: string) => Promise<void>
    const snapshots: string[][] = []
    const source = (accept: (value: string) => Promise<void>) => {
      deliver = accept
      return sourceLease.lease
    }
    const reduced = createReducingSessionSource<string, string[]>(
      source,
      () => [] as string[],
      async (values, value) => {
        await Promise.resolve()
        return [...values, value]
      }
    )

    reduced(async (value) => {
      snapshots.push(value)
    })
    await Promise.all([deliver('one'), deliver('two'), deliver('three')])

    expect(snapshots).toEqual([['one'], ['one', 'two'], ['one', 'two', 'three']])
  })

  it('keeps reducers serial across cooperative yields and does not publish failed work', async () => {
    const sourceLease = controllableLease()
    let deliver!: (value: number) => Promise<void>
    let now = 0
    let active = 0
    let maxActive = 0
    let yields = 0
    const scheduler: NatsailScheduler = {
      now: () => now,
      schedule: () => ({ cancel: () => undefined }),
      yield: async () => {
        yields += 1
        await Promise.resolve()
      },
    }
    const published: number[] = []
    const reduced = createReducingSessionSource<number, number>(
      (accept: (value: number) => Promise<void>) => {
        deliver = accept
        return sourceLease.lease
      },
      () => 0,
      async (state, value) => {
        active += 1
        maxActive = Math.max(maxActive, active)
        await Promise.resolve()
        active -= 1
        now += 3
        if (value === 2) throw new Error('reducer failed')
        return state + value
      },
      { workBudget: { yieldAfterMs: 3, scheduler } }
    )
    reduced(async (state) => {
      published.push(state)
    })

    const first = deliver(1)
    const failed = deliver(2)
    const third = deliver(3)
    await expect(failed).rejects.toThrow('reducer failed')
    await Promise.all([first, third])

    expect(maxActive).toBe(1)
    expect(yields).toBe(2)
    expect(published).toEqual([1, 4])
  })

  it('shares one source until the final caller releases it', async () => {
    const sourceLease = controllableLease()
    let deliver!: (value: string) => Promise<void>
    const start = vi.fn((accept: (value: string) => Promise<void>) => {
      deliver = accept
      return sourceLease.lease
    })
    const registry = createSessionRegistry()

    const first = registry.acquire('conversation:one', start)
    const second = registry.acquire('conversation:one', start)
    await Promise.all([first.ready, second.ready])

    expect(start).toHaveBeenCalledOnce()
    expect(first.getSnapshot()).toEqual({ phase: 'live', revision: 1, valueRevision: 0 })
    expect(second.getSnapshot()).toBe(first.getSnapshot())

    await deliver('hello')
    expect(first.getSnapshot()).toEqual({
      phase: 'live',
      revision: 2,
      valueRevision: 1,
      value: 'hello',
    })

    await first.release()
    expect(sourceLease.close).not.toHaveBeenCalled()

    await second.release()
    expect(sourceLease.close).toHaveBeenCalledOnce()
    expect(second.getSnapshot()).toEqual({
      phase: 'closed',
      revision: 3,
      valueRevision: 1,
      value: 'hello',
    })
  })

  it('reuses an idle session acquired again within the cleanup grace period', async () => {
    vi.useFakeTimers()
    try {
      const sourceLease = controllableLease()
      const start = vi.fn(() => sourceLease.lease)
      const registry = createSessionRegistry({ idleCloseMs: 50 })

      const first = registry.acquire('conversation:strict-mode', start)
      await first.ready
      await first.release()

      const second = registry.acquire('conversation:strict-mode', start)
      await second.ready
      expect(start).toHaveBeenCalledOnce()
      expect(sourceLease.close).not.toHaveBeenCalled()

      await second.release()
      await vi.advanceTimersByTimeAsync(50)
      expect(sourceLease.close).toHaveBeenCalledOnce()
    } finally {
      vi.useRealTimers()
    }
  })

  it('restarts one shared logical session while preserving its handle and latest value', async () => {
    const leases = [controllableLease(), controllableLease(), controllableLease()]
    const accepts: Array<(value: string) => Promise<void>> = []
    const source = vi.fn((accept: (value: string) => Promise<void>) => {
      accepts.push(accept)
      return leases[accepts.length - 1]!.lease
    })
    const registry = createSessionRegistry()
    const first = registry.acquire('conversation:restart', source)
    const second = registry.acquire('conversation:restart', source)

    await first.ready
    await accepts[0]!('before')
    expect(second.getSnapshot()).toEqual({
      phase: 'live',
      revision: 2,
      valueRevision: 1,
      value: 'before',
    })

    await first.restart()
    expect(source).toHaveBeenCalledTimes(2)
    expect(leases[0]!.close).toHaveBeenCalledOnce()
    expect(second.getSnapshot()).toEqual({
      phase: 'live',
      revision: 4,
      valueRevision: 1,
      value: 'before',
    })

    await accepts[0]!('stale')
    await accepts[1]!('after')
    expect(first.getSnapshot()).toEqual({
      phase: 'live',
      revision: 5,
      valueRevision: 2,
      value: 'after',
    })

    await registry.restart('conversation:restart')
    expect(source).toHaveBeenCalledTimes(3)

    await first.release()
    await second.release()
  })

  it('moves to the error phase when startup fails and rejects ready', async () => {
    const failure = new Error('source failed to start')
    const registry = createSessionRegistry()
    const handle = registry.acquire('startup-failure', () => ({
      ready: Promise.reject(failure),
      closed: new Promise<void>(() => undefined),
      close: async () => undefined,
    }))

    await expect(handle.ready).rejects.toBe(failure)

    expect(handle.getSnapshot()).toMatchObject({ phase: 'error', error: failure })
    await handle.release()
  })

  it.each([
    { mode: 'rejects', phase: 'error' },
    { mode: 'resolves', phase: 'closed' },
  ] as const)(
    'ends in the $phase phase when a live lease closed promise $mode',
    async ({ mode, phase }) => {
      const failure = new Error('source died')
      let settle!: { resolve(): void; reject(error: Error): void }
      const closed = new Promise<void>((resolve, reject) => (settle = { resolve, reject }))
      let accept!: (value: string) => Promise<void>
      const registry = createSessionRegistry()
      const handle = registry.acquire('live-failure', (next) => {
        accept = next
        return { ready: Promise.resolve(), closed, close: async () => undefined }
      })
      await handle.ready
      await accept('before')

      if (mode === 'rejects') settle.reject(failure)
      else settle.resolve()
      await vi.waitFor(() => expect(handle.getSnapshot().phase).toBe(phase))
      await accept('ignored')

      expect(handle.getSnapshot()).toMatchObject({
        phase,
        value: 'before',
        ...(mode === 'rejects' ? { error: failure } : {}),
      })
      await handle.release()
    }
  )

  it('keeps the error phase when ready resolves after the lease already failed', async () => {
    const failure = new Error('source died during startup')
    let ready!: () => void
    const registry = createSessionRegistry()
    const handle = registry.acquire('failed-before-ready', () => ({
      ready: new Promise<void>((resolve) => (ready = resolve)),
      closed: Promise.reject(failure),
      close: async () => undefined,
    }))
    await vi.waitFor(() => expect(handle.getSnapshot().phase).toBe('error'))

    ready()
    await handle.ready

    expect(handle.getSnapshot()).toMatchObject({ phase: 'error', error: failure })
    await handle.release()
  })

  it('recovers an errored session through restart and clears the error', async () => {
    const failure = new Error('source died')
    let failFirst!: (error: Error) => void
    const first = {
      ready: Promise.resolve(),
      closed: new Promise<void>((_resolve, reject) => (failFirst = reject)),
      close: async () => undefined,
    }
    const leases = [first, controllableLease().lease]
    let starts = 0
    const registry = createSessionRegistry()
    const handle = registry.acquire('error-restart', () => leases[starts++]!)
    await handle.ready
    failFirst(failure)
    await vi.waitFor(() => expect(handle.getSnapshot().phase).toBe('error'))

    await handle.restart()

    expect(handle.getSnapshot().phase).toBe('live')
    expect(handle.getSnapshot()).not.toHaveProperty('error')
    await handle.release()
  })

  it('rejects a validated key reused with different delivery semantics', async () => {
    const registry = createSessionRegistry()
    const firstSource = controllableLease()
    const secondSource = controllableLease()
    const first = defineSession({
      key: 'conversation:contract',
      contract: 'stream=events;start=all',
      source: () => firstSource.lease,
    })
    const conflicting = defineSession({
      key: 'conversation:contract',
      contract: 'stream=events;start=new',
      source: () => secondSource.lease,
    })

    const handle = registry.acquire(first)
    expect(() => registry.acquire(conflicting)).toThrow(SessionContractMismatchError)
    expect(registry.inspect()).toMatchObject({
      closed: false,
      activeSessions: 1,
      sessions: [
        {
          key: 'conversation:contract',
          contract: 'stream=events;start=all',
          references: 1,
          idle: false,
        },
      ],
    })

    await handle.release()
  })

  it('does not mix validated and unvalidated acquisitions for one key', async () => {
    const registry = createSessionRegistry()
    const sourceLease = controllableLease()
    const source = () => sourceLease.lease
    const definition = defineSession({
      key: 'conversation:mixed-contract',
      contract: 'events:v1',
      source,
    })

    const handle = registry.acquire(definition)
    expect(() => registry.acquire(definition.key, source)).toThrow(SessionContractMismatchError)

    await handle.release()
  })

  it('reports session reference and lifecycle events for resource diagnostics', async () => {
    const sourceLease = controllableLease()
    const registry = createSessionRegistry({ idleCloseMs: 25 })
    const iterator = registry.events[Symbol.asyncIterator]()
    const definition = defineSession({
      key: 'conversation:diagnostics',
      contract: 'events:v1',
      source: () => sourceLease.lease,
    })

    const handle = registry.acquire(definition)
    expect((await iterator.next()).value).toMatchObject({
      type: 'opened',
      key: definition.key,
      contract: definition.contract,
      references: 0,
    })
    expect((await iterator.next()).value).toMatchObject({
      type: 'retained',
      key: definition.key,
      references: 1,
    })

    await handle.ready
    await handle.release()
    expect(registry.inspect().sessions[0]).toMatchObject({
      key: definition.key,
      references: 0,
      idle: true,
    })

    await iterator.return?.()
    await registry.close()
  })
})
