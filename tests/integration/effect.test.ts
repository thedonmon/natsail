import { jetstream, jetstreamManager, StorageType } from '@nats-io/jetstream'
import { Effect, Fiber, Stream } from 'effect'
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

describe('Effect adapter with NATS', () => {
  it('streams wildcard Core subjects directly and scopes the subscription', async () => {
    let runtime!: NatsRuntime
    const subjectRoot = uniqueSubject('effect-direct')
    const layer = makeNatsailScopedLayer(
      Effect.sync(() => {
        runtime = createNatsRuntime({ connect: connectToTestNats })
        return { runtime, sessions: createSessionRegistry() }
      })
    )

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const nats = yield* Natsail
        const fiber = yield* Effect.forkChild(
          nats
            .subscribe(
              {
                subject: `${subjectRoot}.*`,
                codec: natsCodecs.text,
              },
              { bufferSize: 1, overflowStrategy: 'suspend' }
            )
            .pipe(Stream.take(2), Stream.runCollect)
        )

        yield* Effect.promise(() => expect.poll(() => runtime.inspect().activeResources).toBe(1))
        yield* nats.publish(`${subjectRoot}.first`, natsCodecs.text.encode('first'))
        yield* nats.publish(`${subjectRoot}.second`, natsCodecs.text.encode('second'))

        return yield* Fiber.join(fiber)
      }).pipe(Effect.provide(layer))
    )

    expect(result).toEqual(['first', 'second'])
    expect(runtime.inspect().connection.state).toBe('closed')
  })

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
        const first = yield* Effect.forkChild(
          nats.sessionValues(definition).pipe(Stream.take(1), Stream.runCollect)
        )
        const second = yield* Effect.forkChild(
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
        return values
      }).pipe(Effect.provide(layer))
    )

    expect(result).toEqual([['hello from Effect'], ['hello from Effect']])
    expect(sessions.inspect()).toMatchObject({ closed: true, activeSessions: 0 })
    expect(runtime.inspect().connection.state).toBe('closed')
  })

  it('materializes JetStream replay and acknowledges only after an Effect handler succeeds', async () => {
    const adminConnection = await connectToTestNats()
    const manager = await jetstreamManager(adminConnection)
    const client = jetstream(adminConnection)
    const stream = `EFFECT_${crypto.randomUUID().replaceAll('-', '_').toUpperCase()}`
    const eventSubject = uniqueSubject('effect-jetstream-events')
    const jobSubject = uniqueSubject('effect-jetstream-jobs')
    const consumerName = `effect_${crypto.randomUUID().replaceAll('-', '_')}`
    let runtime!: NatsRuntime

    try {
      await manager.streams.add({
        name: stream,
        subjects: [eventSubject, jobSubject],
        storage: StorageType.Memory,
      })
      await client.publish(eventSubject, '1')
      await client.publish(eventSubject, '2')

      const layer = makeNatsailScopedLayer(
        Effect.sync(() => {
          runtime = createNatsRuntime({ connect: connectToTestNats })
          return { runtime, sessions: createSessionRegistry() }
        })
      )
      let markHydrated!: () => void
      const hydrated = new Promise<void>((resolve) => {
        markHydrated = resolve
      })
      let markProcessorEntered!: () => void
      const processorEntered = new Promise<void>((resolve) => {
        markProcessorEntered = resolve
      })
      let releaseProcessor!: () => void
      const processorMayFinish = new Promise<void>((resolve) => {
        releaseProcessor = resolve
      })

      const snapshots = await Effect.runPromise(
        Effect.gen(function* () {
          const nats = yield* Natsail
          const materialized = yield* Effect.forkChild(
            nats
              .materializeJetStream(
                {
                  stream,
                  filter: eventSubject,
                  start: 'all',
                  codec: natsCodecs.text,
                  maxBufferedMessages: 2,
                  recovery: { delayMs: 10 },
                },
                {
                  initial: () => 0,
                  reduceBatch: (state, deliveries) =>
                    Effect.succeed(
                      state + deliveries.reduce((sum, delivery) => sum + Number(delivery.value), 0)
                    ),
                },
                { bufferSize: 2, batchSize: 2, batchWithin: '5 millis' }
              )
              .pipe(
                Stream.tap((snapshot) =>
                  Effect.sync(() => {
                    if (snapshot.phase === 'live' && snapshot.data === 3) markHydrated()
                  })
                ),
                Stream.take(3),
                Stream.runCollect
              )
          )

          yield* Effect.promise(() => hydrated)
          yield* nats.publish(eventSubject, natsCodecs.text.encode('3'))
          const result = yield* Fiber.join(materialized)

          const processor = yield* Effect.forkChild(
            nats.runJetStreamProcessor(
              {
                stream,
                consumer: { mode: 'ensure', name: consumerName },
                filter: jobSubject,
                start: 'all',
                codec: natsCodecs.text,
              },
              () =>
                Effect.promise(async () => {
                  markProcessorEntered()
                  await processorMayFinish
                })
            )
          )
          yield* nats.publish(jobSubject, natsCodecs.text.encode('job'))
          yield* Effect.promise(() => processorEntered)
          yield* Effect.promise(async () => {
            const before = await manager.consumers.info(stream, consumerName)
            expect(before.ack_floor.stream_seq).toBe(0)
          })
          releaseProcessor()
          yield* Effect.promise(() =>
            expect
              .poll(
                async () =>
                  (await manager.consumers.info(stream, consumerName)).ack_floor.stream_seq
              )
              .toBe(4)
          )
          yield* Fiber.interrupt(processor)

          return result
        }).pipe(Effect.provide(layer))
      )

      expect(snapshots.map(({ phase, data }) => ({ phase, data }))).toEqual([
        { phase: 'replaying', data: 0 },
        { phase: 'live', data: 3 },
        { phase: 'live', data: 6 },
      ])
      expect(runtime.inspect().connection.state).toBe('closed')
    } finally {
      await manager.streams.delete(stream).catch(() => undefined)
      await adminConnection.drain()
    }
  })
})
