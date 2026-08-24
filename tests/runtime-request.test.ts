import { describe, expect, it, vi } from 'vitest'

import type { Msg, NatsConnection } from '@nats-io/nats-core'
import { createNatsRuntime, natsCodecs, NatsRuntimeRequestAbortedError } from '@natsail/core'

describe('runtime request/reply', () => {
  it('uses the shared connection once and decodes the response', async () => {
    const response = { data: natsCodecs.text.encode('{"ok":true}') } as Msg
    const request = vi.fn().mockResolvedValue(response)
    const connection = fakeConnection(request)
    const connect = vi.fn().mockResolvedValue(connection)
    const runtime = createNatsRuntime({ connect })

    await expect(
      runtime.request({
        subject: 'operations.check',
        data: new Uint8Array([1, 2, 3]),
        timeoutMs: 2_000,
        codec: natsCodecs.json<{ ok: boolean }>(),
      })
    ).resolves.toEqual({ ok: true })

    expect(connect).toHaveBeenCalledOnce()
    expect(request).toHaveBeenCalledOnce()
    expect(request).toHaveBeenCalledWith('operations.check', new Uint8Array([1, 2, 3]), {
      timeout: 2_000,
    })

    await runtime.close()
  })

  it('does not connect or send when the request is already aborted', async () => {
    const request = vi.fn()
    const connect = vi.fn().mockResolvedValue(fakeConnection(request))
    const runtime = createNatsRuntime({ connect })
    const controller = new AbortController()
    controller.abort()

    await expect(
      runtime.request({
        subject: 'operations.cancelled',
        signal: controller.signal,
        decode: () => 'unreachable',
      })
    ).rejects.toBeInstanceOf(NatsRuntimeRequestAbortedError)

    expect(connect).not.toHaveBeenCalled()
    expect(request).not.toHaveBeenCalled()
    await runtime.close()
  })

  it('stops waiting when an in-flight request is aborted', async () => {
    const request = vi.fn().mockReturnValue(new Promise<Msg>(() => undefined))
    const runtime = createNatsRuntime({ connect: async () => fakeConnection(request) })
    const controller = new AbortController()

    const result = runtime.request({
      subject: 'operations.slow',
      signal: controller.signal,
      decode: () => 'unreachable',
    })
    await vi.waitFor(() => expect(request).toHaveBeenCalledOnce())
    controller.abort()

    await expect(result).rejects.toBeInstanceOf(NatsRuntimeRequestAbortedError)
    expect(request).toHaveBeenCalledOnce()
    await runtime.close()
  })

  it('rejects invalid subjects and timeouts before connecting', async () => {
    const connect = vi.fn().mockResolvedValue(fakeConnection(vi.fn()))
    const runtime = createNatsRuntime({ connect })

    await expect(runtime.request({ subject: '', decode: () => undefined })).rejects.toThrow(
      'subject'
    )
    await expect(
      runtime.request({ subject: 'operations.check', timeoutMs: 0, decode: () => undefined })
    ).rejects.toThrow('timeoutMs')

    expect(connect).not.toHaveBeenCalled()
    await runtime.close()
  })

  it('requires exactly one codec or raw-message decoder before connecting', async () => {
    const connect = vi.fn().mockResolvedValue(fakeConnection(vi.fn()))
    const runtime = createNatsRuntime({ connect })

    await expect(runtime.request({ subject: 'operations.missing' } as never)).rejects.toThrow(
      'exactly one codec or decode function'
    )
    await expect(
      runtime.request({
        subject: 'operations.ambiguous',
        codec: natsCodecs.text,
        decode: () => 'ambiguous',
      } as never)
    ).rejects.toThrow('exactly one codec or decode function')

    expect(() =>
      runtime.subscribe({ subject: 'events.missing' } as never, async () => undefined)
    ).toThrow('exactly one codec or decode function')
    expect(() =>
      runtime.subscribe(
        {
          subject: 'events.ambiguous',
          codec: natsCodecs.text,
          decode: () => 'ambiguous',
        } as never,
        async () => undefined
      )
    ).toThrow('exactly one codec or decode function')
    expect(connect).not.toHaveBeenCalled()
    await runtime.close()
  })

  it('stops every in-flight request when the runtime closes', async () => {
    const request = vi.fn().mockReturnValue(new Promise<Msg>(() => undefined))
    const runtime = createNatsRuntime({ connect: async () => fakeConnection(request) })

    const result = runtime.request({
      subject: 'operations.shutdown',
      decode: () => 'unreachable',
    })
    await vi.waitFor(() => expect(request).toHaveBeenCalledOnce())
    await runtime.close()

    await expect(result).rejects.toBeInstanceOf(NatsRuntimeRequestAbortedError)
  })
})

function fakeConnection(request: ReturnType<typeof vi.fn>): NatsConnection {
  let closed = false
  let resolveClosed!: () => void
  const closedPromise = new Promise<void>((resolve) => {
    resolveClosed = resolve
  })

  return {
    getServer: () => 'nats://test',
    status: () => ({
      async *[Symbol.asyncIterator]() {},
    }),
    isClosed: () => closed,
    closed: () => closedPromise,
    request,
    drain: async () => {
      closed = true
      resolveClosed()
    },
  } as NatsConnection
}
