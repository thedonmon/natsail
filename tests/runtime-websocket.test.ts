import { afterEach, expect, it, vi } from 'vitest'
import { wsconnect } from '@nats-io/nats-core'
import { createNatsRuntime, type NatsRuntimeEvent } from '@natsail/core'

class FakeSocket {
  binaryType = 'arraybuffer'
  bufferedAmount = 0
  onopen: (() => void) | null = null
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null
  onerror: ((event: { message: string }) => void) | null = null
  onclose: ((event: { wasClean: boolean; reason: string }) => void) | null = null
  closed = false
  autoPong = false
  publishes = 0
  send(data: Uint8Array) {
    // Deliberately do not retain or log CONNECT frames.
    const frame = new TextDecoder().decode(data)
    if (frame.includes('PUB work ')) this.publishes += 1
    if (this.autoPong && frame.includes('PING\r\n')) {
      queueMicrotask(() => this.receive('PONG\r\n'))
    }
  }
  close() {
    if (this.closed) return
    this.closed = true
    this.onclose?.({ wasClean: true, reason: '' })
  }
  receive(frame: string) {
    this.onmessage?.({ data: new TextEncoder().encode(frame).buffer })
  }
  info() {
    this.receive(
      'INFO {"server_id":"test","version":"2.11.0","proto":1,"host":"unit.invalid","port":4222,"headers":true,"max_payload":1048576}\r\n'
    )
  }
  disconnect() {
    this.closed = true
    this.onclose?.({ wasClean: false, reason: 'offline' })
  }
}

function setup() {
  const socket = new FakeSocket()
  const connect = vi.fn(() =>
    wsconnect({
      servers: 'ws://unit.invalid:4222',
      timeout: 20,
      reconnect: false,
      wsFactory: async () => ({ socket: socket as unknown as WebSocket, encrypted: false }),
    })
  )
  const runtime = createNatsRuntime({ connect, shutdownTimeoutMs: 10 })
  return { socket, connect, runtime }
}

afterEach(() => vi.useRealTimers())

it.each([false, true])(
  'accepts a handshake before timeout processing (wall clock advanced: %s)',
  async (advanceClock) => {
    vi.useFakeTimers()
    const { socket, connect, runtime } = setup()
    const connecting = runtime.connection()
    await vi.advanceTimersByTimeAsync(0)
    socket.info()
    await vi.advanceTimersByTimeAsync(19)
    if (advanceClock) vi.setSystemTime(Date.now() + 60_000)
    socket.autoPong = true
    socket.receive('PONG\r\n')
    const connection = await connecting
    await vi.advanceTimersByTimeAsync(1)
    expect(connection.isClosed()).toBe(false)
    expect(runtime.inspect().connectionGeneration).toBe(1)
    expect(connect).toHaveBeenCalledOnce()
    await runtime.close()
    expect(socket.closed).toBe(true)
  }
)

it('lets the transport timeout win, and ignores a queued late handshake callback', async () => {
  vi.useFakeTimers()
  const { socket, connect, runtime } = setup()
  const connecting = runtime.connection().catch((error: unknown) => error)
  await vi.advanceTimersByTimeAsync(0)
  socket.info()
  await vi.advanceTimersByTimeAsync(0)
  const queuedMessage = socket.onmessage!
  await vi.advanceTimersByTimeAsync(20)
  expect(await connecting).toBeInstanceOf(Error)
  queuedMessage({ data: new TextEncoder().encode('PONG\r\n').buffer })
  await vi.advanceTimersByTimeAsync(0)
  expect(socket.closed).toBe(true)
  expect(runtime.inspect().connectionGeneration).toBe(0)
  expect(connect).toHaveBeenCalledOnce()
  await runtime.close()
})

it.each([0, 10])(
  'discards a successful transport handshake after cleanup starts (elapsed shutdown: %s)',
  async (elapsed) => {
    vi.useFakeTimers()
    const { socket, runtime } = setup()
    const events: NatsRuntimeEvent[] = []
    const watching = (async () => {
      for await (const event of runtime.events) events.push(event)
    })()
    const connecting = runtime.connection().catch((error: unknown) => error)
    await vi.advanceTimersByTimeAsync(0)
    socket.info()
    await vi.advanceTimersByTimeAsync(0)
    const closing = runtime.close().catch((error: unknown) => error)
    await vi.advanceTimersByTimeAsync(elapsed)
    socket.autoPong = true
    socket.receive('PONG\r\n')
    expect(await connecting).toBeInstanceOf(Error)
    const outcome = await closing
    if (elapsed) expect(outcome).toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError' })
    else expect(outcome).toBeUndefined()
    await watching
    expect(socket.closed).toBe(true)
    expect(runtime.inspect().connectionGeneration).toBe(0)
    expect(events.some((event) => event.type === 'status' && event.state === 'connected')).toBe(
      false
    )
  }
)

it.each([true, false])(
  'closes while disconnected with queued outbound data (recovers within grace: %s)',
  async (recovers) => {
    vi.useFakeTimers()
    const sockets: FakeSocket[] = []
    const connect = vi.fn(() =>
      wsconnect({
        servers: 'ws://unit.invalid:4222',
        timeout: 20,
        reconnect: true,
        reconnectTimeWait: 1,
        reconnectJitter: 0,
        reconnectJitterTLS: 0,
        wsFactory: async () => {
          const socket = new FakeSocket()
          sockets.push(socket)
          return { socket: socket as unknown as WebSocket, encrypted: false }
        },
      })
    )
    const runtime = createNatsRuntime({ connect, shutdownTimeoutMs: 10 })
    const connecting = runtime.connection()
    await vi.advanceTimersByTimeAsync(0)
    const first = sockets[0]!
    first.autoPong = true
    first.info()
    await connecting
    first.disconnect()
    await vi.advanceTimersByTimeAsync(1)
    await runtime.publish('work', 'queued')
    expect(first.publishes).toBe(0)
    const closing = runtime.close().catch((error: unknown) => error)
    await vi.advanceTimersByTimeAsync(0)
    const second = sockets[1]!
    expect(second).toBeDefined()
    if (recovers) {
      second.autoPong = true
      second.info()
      await expect(closing).resolves.toBeUndefined()
      expect(second.publishes).toBe(1)
    } else {
      await vi.advanceTimersByTimeAsync(10)
      expect(await closing).toMatchObject({ name: 'NatsRuntimeShutdownTimeoutError' })
      expect(second.publishes).toBe(0)
    }
    expect(second.closed).toBe(true)
    expect(connect).toHaveBeenCalledOnce()
    expect(runtime.inspect().connection.state).toBe('closed')
    await vi.advanceTimersByTimeAsync(100)
    expect(sockets).toHaveLength(2)
  }
)
