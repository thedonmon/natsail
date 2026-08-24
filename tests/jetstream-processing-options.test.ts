import { describe, expect, it, vi } from 'vitest'

import { createNatsRuntime, natsCodecs } from '@natsail/core'
import { processJetStream, type JetStreamProcessorOptions } from '@natsail/jetstream'

describe('JetStream processor option validation', () => {
  it.each([
    ['stream', { stream: '' }, 'stream'],
    ['consumer name', { consumer: { mode: 'owned', name: '' } }, 'consumer name'],
    ['filter', { filter: '' }, 'filter'],
    ['filter list', { filter: [] }, 'filter'],
    ['start sequence', { start: { after: -1 } }, 'start.after'],
    ['ack wait', { ackWaitMs: 0 }, 'ackWaitMs'],
    ['maximum deliveries', { maxDeliver: 0 }, 'maxDeliver'],
    ['maximum pending acknowledgements', { maxAckPending: 0 }, 'maxAckPending'],
    ['message buffer', { maxBufferedMessages: 0 }, 'maxBufferedMessages'],
    ['byte buffer', { maxBufferedMessages: undefined, maxBufferedBytes: 0 }, 'maxBufferedBytes'],
  ] as const)('rejects an invalid %s before acquiring a connection', (_name, patch, message) => {
    const connect = vi.fn()
    const runtime = createNatsRuntime({ connect })
    const options = {
      stream: 'EVENTS',
      consumer: { mode: 'owned', name: 'processor' },
      filter: 'events.>',
      start: 'all',
      decode: () => 'decoded',
      ...patch,
    } as JetStreamProcessorOptions<string>

    expect(() => processJetStream(runtime, options, async () => undefined)).toThrow(message)
    expect(connect).not.toHaveBeenCalled()
  })

  it('allows maxDeliver -1 for unlimited redelivery', () => {
    const runtime = createNatsRuntime({
      connect: vi.fn().mockRejectedValue(new Error('stop after validation')),
    })

    const lease = processJetStream(
      runtime,
      {
        stream: 'EVENTS',
        consumer: { mode: 'owned', name: 'processor' },
        filter: 'events.>',
        start: 'all',
        maxDeliver: -1,
        decode: () => 'decoded',
      },
      async () => undefined
    )

    expect(lease).toBeDefined()
    void lease.ready.catch(() => undefined)
    void lease.closed.catch(() => undefined)
  })

  it('requires exactly one codec or raw-message decoder before connecting', () => {
    const connect = vi.fn()
    const runtime = createNatsRuntime({ connect })
    const base = {
      stream: 'EVENTS',
      consumer: { mode: 'owned', name: 'processor' },
      filter: 'events.>',
      start: 'all',
    } as const

    expect(() => processJetStream(runtime, base as never, async () => undefined)).toThrow(
      'exactly one codec or decode function'
    )
    expect(() =>
      processJetStream(
        runtime,
        { ...base, codec: natsCodecs.text, decode: () => 'ambiguous' } as never,
        async () => undefined
      )
    ).toThrow('exactly one codec or decode function')
    expect(connect).not.toHaveBeenCalled()
  })
})
