import { describe, expect, it, vi } from 'vitest'

import type { NatsRuntime, SubscriptionLease } from '@natsail/core'
import {
  createCoreSessionSource,
  createReducingSessionSource,
  createSessionRegistry,
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
    const reduced = createReducingSessionSource(
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
})
