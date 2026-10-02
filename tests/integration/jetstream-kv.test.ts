import { jetstream, jetstreamManager, StorageType, type JsMsg } from '@nats-io/jetstream'
import { Kvm } from '@nats-io/kv'
import { afterEach, describe, expect, it } from 'vitest'

import { createNatsRuntime } from '@natsail/core'
import {
  createReducingJetStreamSessionSource,
  type JetStreamStateSnapshot,
} from '@natsail/jetstream'

import { connectToTestNats } from './helpers.js'

type KvChange = { key: string; operation: string; value: string }

describe('KV bucket watched through a reducing JetStream session', () => {
  const cleanups: Array<() => Promise<void>> = []

  afterEach(async () => {
    await Promise.allSettled(cleanups.splice(0).map((cleanup) => cleanup()))
  })

  it('replays current entries, then applies puts, deletes, and purges live', async () => {
    const admin = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const bucket = `cfg_${crypto.randomUUID().replaceAll('-', '_')}`
    const kv = await new Kvm(jetstream(admin)).create(bucket, { storage: StorageType.Memory })
    const manager = await jetstreamManager(admin)
    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    cleanups.push(async () => {
      await runtime.close()
      await manager.streams.delete(`KV_${bucket}`)
      await admin.drain()
    })
    await kv.put('a', '1')
    await kv.put('b', '2')

    let latest: JetStreamStateSnapshot<Map<string, string>> | undefined
    const lease = createReducingJetStreamSessionSource(
      runtime,
      {
        stream: `KV_${bucket}`,
        filter: `$KV.${bucket}.>`,
        start: 'all',
        decode: (message: JsMsg): KvChange => ({
          key: message.subject.slice(`$KV.${bucket}.`.length),
          operation: message.headers?.get('KV-Operation') || 'PUT',
          value: new TextDecoder().decode(message.data),
        }),
      },
      {
        scope: 'kv-map:v1',
        initial: () => new Map<string, string>(),
        reduce: (state, { value: change }) => {
          const next = new Map(state)
          if (change.operation === 'PUT') next.set(change.key, change.value)
          else next.delete(change.key)
          return next
        },
      }
    )(async (snapshot) => {
      latest = snapshot
    })
    cleanups.push(() => lease.close())

    await expect
      .poll(() => latest && { phase: latest.phase, data: Object.fromEntries(latest.data) })
      .toEqual({ phase: 'live', data: { a: '1', b: '2' } })

    await kv.put('a', '3')
    await kv.delete('b')
    await kv.put('c', '4')
    await kv.purge('c')

    await expect.poll(() => latest && Object.fromEntries(latest.data)).toEqual({ a: '3' })
  })
})
