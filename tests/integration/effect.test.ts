import { Chunk, Effect, Fiber, Stream } from 'effect'
import { describe, expect, it } from 'vitest'

import { createNatsRuntime, natsCodecs, type NatsRuntime } from '@natsail/core'
import { makeNatsailScopedLayer, Natsail } from '@natsail/effect'
import {
  createCoreSessionSource,
  createSessionRegistry,
  defineSession,
  type SessionRegistry,
} from '@natsail/session'

import { connectToTestNats, uniqueSubject } from './helpers.js'

describe('Effect adapter with Core NATS', () => {
  it('shares one scoped subscription and releases every resource after the program', async () => {
    let runtime!: NatsRuntime
    let sessions!: SessionRegistry
    const subject = uniqueSubject('effect')
    const layer = makeNatsailScopedLayer(
      Effect.sync(() => {
        runtime = createNatsRuntime({ connect: connectToTestNats })
        sessions = createSessionRegistry()
        return { runtime, sessions }
      })
    )

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const nats = yield* Natsail
        const definition = defineSession({
          key: `core:${subject}`,
          contract: `core-subject=${subject};codec=text`,
          source: createCoreSessionSource(nats.runtime, {
            subject,
            codec: natsCodecs.text,
          }),
        })
        const first = yield* Effect.fork(
          nats.sessionValues(definition).pipe(Stream.take(1), Stream.runCollect)
        )
        const second = yield* Effect.fork(
          nats.sessionValues(definition).pipe(Stream.take(1), Stream.runCollect)
        )

        yield* Effect.promise(async () => {
          await expect
            .poll(() => sessions.inspect().sessions[0])
            .toMatchObject({ phase: 'live', references: 2 })
        })
        yield* nats.publish(subject, natsCodecs.text.encode('hello from Effect'))

        const values = yield* Effect.all([Fiber.join(first), Fiber.join(second)], {
          concurrency: 'unbounded',
        })
        return values.map(Chunk.toReadonlyArray)
      }).pipe(Effect.provide(layer))
    )

    expect(result).toEqual([['hello from Effect'], ['hello from Effect']])
    expect(sessions.inspect()).toMatchObject({ closed: true, activeSessions: 0 })
    expect(runtime.inspect().connection.state).toBe('closed')
  })
})
