import { vi } from 'vitest'
import type { Msg, NatsConnection } from '@nats-io/nats-core'
import { SubscriptionImpl, Subscriptions, type ProtocolHandler } from '@nats-io/nats-core/internal'
import { natsCodecs } from '@natsail/core'

export function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((done, fail) => {
    resolve = done
    reject = fail
  })
  return { promise, resolve, reject }
}

/** Real nats.js subscription buffering/drain, with only the network round trip controlled. */
export function transport(values: readonly string[] = ['work']) {
  const disconnected = deferred<void>()
  let stopped = false
  const subscriptions = new Subscriptions()
  const flush = vi.fn<() => Promise<void>>(async () => undefined)
  const protocol = {
    options: {},
    isClosed: () => stopped,
    subscriptions,
    unsub: vi.fn(),
    unsubscribe: (subscription: SubscriptionImpl) => subscriptions.cancel(subscription),
    flush,
  } as unknown as ProtocolHandler
  const subscription = subscriptions.add(new SubscriptionImpl(protocol, 'work'))
  const deliver = (value: string) => {
    if (subscriptions.get(subscription.sid)) {
      subscription.callback(null, { subject: 'work', data: natsCodecs.text.encode(value) } as Msg)
    }
  }
  const disconnect = () => {
    stopped = true
    subscriptions.close()
    disconnected.resolve()
  }
  const close = vi.fn(async () => disconnect())
  const drain = vi.fn(async () => {
    await Promise.all(subscriptions.all().map((active) => active.drain()))
    disconnect()
  })
  const connection = {
    subscribe: () => {
      values.forEach(deliver)
      return subscription
    },
    getServer: () => 'mock:4222',
    status: async function* () {},
    isClosed: () => stopped,
    closed: () => disconnected.promise,
    close,
    drain,
  } as unknown as NatsConnection
  return { connection, close, drain, flush, deliver }
}
