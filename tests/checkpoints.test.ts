import { IDBFactory } from 'fake-indexeddb'
import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  CheckpointConflictError,
  CheckpointValidationError,
  createIndexedDbCheckpointStore,
  createMemoryCheckpointStore,
} from '@natsail/checkpoints'

describe('checkpoint stores', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('stores and clears independent stream checkpoints by key', async () => {
    const store = createMemoryCheckpointStore()

    await store.save('conversation:one', {
      stream: 'CONVERSATIONS',
      epoch: '2026-08-21T00:00:00.000Z',
      sequence: 42,
    })

    expect(await store.load('conversation:one')).toEqual({
      stream: 'CONVERSATIONS',
      epoch: '2026-08-21T00:00:00.000Z',
      sequence: 42,
    })
    expect(await store.load('conversation:two')).toBeUndefined()

    await store.clear('conversation:one')
    expect(await store.load('conversation:one')).toBeUndefined()
  })

  it('persists checkpoints across IndexedDB store instances', async () => {
    vi.stubGlobal('indexedDB', new IDBFactory())
    const databaseName = `natsail-${crypto.randomUUID()}`
    const first = createIndexedDbCheckpointStore({ databaseName })

    await first.save('conversation:one', {
      stream: 'CONVERSATIONS',
      epoch: '2026-08-21T00:00:00.000Z',
      sequence: 42,
    })

    const second = createIndexedDbCheckpointStore({ databaseName })
    expect(await second.load('conversation:one')).toEqual({
      stream: 'CONVERSATIONS',
      epoch: '2026-08-21T00:00:00.000Z',
      sequence: 42,
    })

    await second.clear('conversation:one')
    expect(await first.load('conversation:one')).toBeUndefined()
  })

  it('rejects an invalid stream sequence', async () => {
    const store = createMemoryCheckpointStore()

    await expect(
      store.save('conversation:invalid', {
        stream: 'CONVERSATIONS',
        epoch: '2026-08-21T00:00:00.000Z',
        sequence: -1,
      })
    ).rejects.toBeInstanceOf(CheckpointValidationError)
  })

  it('prevents a stale writer from moving a checkpoint backward', async () => {
    vi.stubGlobal('indexedDB', new IDBFactory())
    const stores = [
      createMemoryCheckpointStore(),
      createIndexedDbCheckpointStore({ databaseName: `natsail-${crypto.randomUUID()}` }),
    ]

    for (const store of stores) {
      await store.save('conversation:shared', {
        stream: 'CONVERSATIONS',
        epoch: '2026-08-21T00:00:00.000Z',
        sequence: 42,
      })

      await expect(
        store.save('conversation:shared', {
          stream: 'CONVERSATIONS',
          epoch: '2026-08-21T00:00:00.000Z',
          sequence: 41,
        })
      ).rejects.toBeInstanceOf(CheckpointConflictError)
      expect((await store.load('conversation:shared'))?.sequence).toBe(42)
    }
  })
})
