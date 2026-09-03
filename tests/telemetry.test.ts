import type { Msg, NatsConnection, Status } from '@nats-io/nats-core'
import { describe, expect, it, vi } from 'vitest'

import {
  createNatsRuntime,
  NATS_RUNTIME_ADAPTER,
  natsCodecs,
  type NatsailTelemetryEvent,
  type RuntimeResource,
} from '@natsail/core'
import { createSessionRegistry } from '@natsail/session'

function controllableConnection(clock: { value: number }) {
  let closed = false
  let resolveClosed!: () => void
  let waiting: ((value: IteratorResult<Status>) => void) | undefined
  const queued: Status[] = []
  const closedPromise = new Promise<void>((resolve) => {
    resolveClosed = resolve
  })
  const publish = vi.fn(() => {
    clock.value += 2
  })
  const request = vi.fn(async () => {
    clock.value += 3
    return { data: natsCodecs.text.encode('reply') } as Msg
  })
  const connection = {
    closed: () => closedPromise,
    drain: vi.fn(async () => {
      closed = true
      resolveClosed()
      waiting?.({ done: true, value: undefined })
    }),
    getServer: () => 'nats://private.example',
    isClosed: () => closed,
    publish,
    request,
    status: () => ({
      [Symbol.asyncIterator]() {
        return {
          next: () => {
            const status = queued.shift()
            if (status) return Promise.resolve({ done: false as const, value: status })
            if (closed) return Promise.resolve({ done: true as const, value: undefined })
            return new Promise<IteratorResult<Status>>((resolve) => {
              waiting = resolve
            })
          },
        }
      },
    }),
  } as unknown as NatsConnection

  return {
    connection,
    emit(status: Status) {
      if (waiting) {
        const resolve = waiting
        waiting = undefined
        resolve({ done: false, value: status })
      } else {
        queued.push(status)
      }
    },
  }
}

describe('NATSail telemetry', () => {
  it('records deterministic operation, recovery, resource, and capacity measurements', async () => {
    const clock = { value: 10 }
    const events: NatsailTelemetryEvent[] = []
    const controlled = controllableConnection(clock)
    const runtime = createNatsRuntime({
      connect: async () => {
        clock.value += 5
        return controlled.connection
      },
      limits: { maxJetStreamConsumers: 4, maxBufferedMessages: 64 },
      telemetry: { record: (event) => events.push(event) },
      telemetryAttributes: { deployment: 'test', outcome: 'caller-value', host: 'browser-broker' },
      telemetryClock: { now: () => clock.value },
    })
    const runtimeEvents = runtime.events[Symbol.asyncIterator]()

    await runtime.connection()
    await runtime.publish('private.subject', new Uint8Array([1, 2]))
    await runtime.request({ subject: 'private.request', codec: natsCodecs.text })

    let finishResource!: () => void
    const resourceClosed = new Promise<void>((resolve) => {
      finishResource = resolve
    })
    const resource = runtime[NATS_RUNTIME_ADAPTER].manage(
      () =>
        ({
          closed: resourceClosed,
          close: async () => finishResource(),
        }) satisfies RuntimeResource,
      { jetStreamConsumers: 1, bufferedMessages: 8 }
    )
    await resource.close()
    await resource.closed
    await vi.waitFor(() =>
      expect(
        events.some(
          (event) =>
            event.type === 'gauge' &&
            event.name === 'natsail.runtime.resources.active' &&
            event.value === 0
        )
      ).toBe(true)
    )

    controlled.emit({ type: 'disconnect', server: 'nats://private.example' })
    await vi.waitFor(() =>
      expect(events).toContainEqual(
        expect.objectContaining({
          name: 'natsail.connection.transitions',
          attributes: expect.objectContaining({ state: 'disconnected' }),
        })
      )
    )
    clock.value += 7
    controlled.emit({ type: 'reconnect', server: 'nats://private.example' })
    controlled.emit({ type: 'slowConsumer', pending: 12 })
    await vi.waitFor(() =>
      expect(events).toContainEqual(
        expect.objectContaining({
          type: 'duration',
          name: 'natsail.connection.recovery.duration',
          durationMs: 7,
        })
      )
    )

    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'duration',
        name: 'natsail.connection.attempt.duration',
        durationMs: 5,
        attributes: expect.objectContaining({
          deployment: 'test',
          host: 'browser-broker',
          outcome: 'success',
          source: 'runtime',
        }),
      })
    )
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'duration',
        name: 'natsail.core.publish.duration',
        durationMs: 2,
      })
    )
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'duration',
        name: 'natsail.core.request.duration',
        durationMs: 3,
      })
    )
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'gauge',
        name: 'natsail.runtime.capacity.limit',
        value: 64,
        attributes: expect.objectContaining({ resource: 'buffered-messages' }),
      })
    )
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'counter',
        name: 'natsail.buffer.signals',
        attributes: expect.objectContaining({ signal: 'slow-consumer' }),
      })
    )
    expect(JSON.stringify(events)).not.toContain('private.subject')
    expect(JSON.stringify(events)).not.toContain('private.request')
    expect(JSON.stringify(events)).not.toContain('private.example')

    const lowFrequencyEvents = await Promise.all([
      runtimeEvents.next(),
      runtimeEvents.next(),
      runtimeEvents.next(),
    ])
    expect(lowFrequencyEvents.map((entry) => entry.value?.type)).toEqual([
      'status',
      'status',
      'status',
    ])

    await runtimeEvents.return?.()
    await runtime.close()
  })

  it('never lets a throwing sink change runtime or session behavior', async () => {
    const sink = {
      record: () => {
        throw new Error('telemetry backend unavailable')
      },
    }
    const controlled = controllableConnection({ value: 0 })
    const runtime = createNatsRuntime({
      connect: async () => controlled.connection,
      telemetry: sink,
    })

    await expect(runtime.publish('safe.operation')).resolves.toBeUndefined()

    let finish!: () => void
    const closed = new Promise<void>((resolve) => {
      finish = resolve
    })
    const sessions = createSessionRegistry({ telemetry: sink })
    const handle = sessions.acquire('secret-session-key', () => ({
      ready: Promise.resolve(),
      closed,
      close: async () => finish(),
    }))
    await expect(handle.ready).resolves.toBeUndefined()
    await expect(handle.release()).resolves.toBeUndefined()

    await sessions.close()
    await runtime.close()
  })

  it('records failed connection, publish, and request outcomes with an injected clock', async () => {
    const clock = { value: 20 }
    const events: NatsailTelemetryEvent[] = []
    const runtime = createNatsRuntime({
      connect: async () => {
        clock.value += 4
        throw new Error('connection unavailable')
      },
      telemetry: { record: (event) => events.push(event) },
      telemetryClock: { now: () => clock.value },
    })

    await expect(runtime.publish('private.publish')).rejects.toThrow('connection unavailable')
    await expect(
      runtime.request({ subject: 'private.request', codec: natsCodecs.text })
    ).rejects.toThrow('connection unavailable')

    expect(events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: 'duration',
          name: 'natsail.connection.attempt.duration',
          durationMs: 4,
          attributes: expect.objectContaining({ outcome: 'failure' }),
        }),
        expect.objectContaining({
          type: 'duration',
          name: 'natsail.core.publish.duration',
          durationMs: 4,
          attributes: expect.objectContaining({ outcome: 'failure' }),
        }),
        expect.objectContaining({
          type: 'duration',
          name: 'natsail.core.request.duration',
          durationMs: 4,
          attributes: expect.objectContaining({ outcome: 'failure' }),
        }),
      ])
    )
    expect(JSON.stringify(events)).not.toContain('private.publish')
    expect(JSON.stringify(events)).not.toContain('private.request')
    await runtime.close()
  })

  it('reports session reference counts without session identifiers', async () => {
    const clock = { value: 100 }
    const events: NatsailTelemetryEvent[] = []
    let finish!: () => void
    const closed = new Promise<void>((resolve) => {
      finish = resolve
    })
    const registry = createSessionRegistry({
      telemetry: { record: (event) => events.push(event) },
      telemetryClock: { now: () => clock.value },
      telemetryAttributes: { application: 'test' },
    })
    const source = () => ({
      ready: Promise.resolve(),
      closed,
      close: async () => finish(),
    })

    const first = registry.acquire('secret-session-key', source)
    const second = registry.acquire('secret-session-key', source)
    await Promise.all([first.ready, second.ready])
    await first.release()
    await second.release()

    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'gauge',
        name: 'natsail.session.references.active',
        value: 2,
        attributes: expect.objectContaining({ application: 'test', source: 'session' }),
      })
    )
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'counter',
        name: 'natsail.session.lifecycle',
        attributes: expect.objectContaining({ action: 'closed', phase: 'closed' }),
      })
    )
    expect(JSON.stringify(events)).not.toContain('secret-session-key')
    await registry.close()
  })
})
