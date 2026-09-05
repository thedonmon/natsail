import {
  createBrowserBrokerClient,
  type BrowserBrokerClient,
  type BrowserBrokerStats,
} from '@natsail/browser-broker'
import type { SubscriptionLease } from '@natsail/core'

interface DeliveryWaiter {
  resolve(value: string): void
  timeout: ReturnType<typeof setTimeout>
}

const queuedDeliveries = new Map<string, string[]>()
const waitingDeliveries = new Map<string, DeliveryWaiter[]>()
const leases = new Map<string, SubscriptionLease>()
const encoder = new TextEncoder()
const decoder = new TextDecoder()
let client: BrowserBrokerClient

const acceptDelivery = (subject: string, value: string): void => {
  const waiters = waitingDeliveries.get(subject)
  const waiter = waiters?.shift()
  if (waiter) {
    clearTimeout(waiter.timeout)
    waiter.resolve(value)
    return
  }
  const queue = queuedDeliveries.get(subject) ?? []
  queue.push(value)
  queuedDeliveries.set(subject, queue)
}

const nextMessage = (subject: string): Promise<string> => {
  const queue = queuedDeliveries.get(subject)
  const value = queue?.shift()
  if (value !== undefined) return Promise.resolve(value)
  return new Promise((resolve, reject) => {
    const waiters = waitingDeliveries.get(subject) ?? []
    const waiter: DeliveryWaiter = {
      resolve,
      timeout: setTimeout(() => {
        const index = waiters.indexOf(waiter)
        if (index >= 0) waiters.splice(index, 1)
        reject(new Error(`Timed out waiting for ${subject}`))
      }, 5_000),
    }
    waiters.push(waiter)
    waitingDeliveries.set(subject, waiters)
  })
}

void (async () => {
  client = await createBrowserBrokerClient({
    identity: { tenant: 'browser-acceptance', authenticationContext: 'anonymous-v1' },
    credentials: () => ({ revision: 1, bytes: new Uint8Array(0) }),
    connect: () =>
      new SharedWorker(new URL('./nats-shared-worker.ts', import.meta.url), {
        name: 'natsail-browser-broker-acceptance',
        type: 'module',
      }).port,
    strict: true,
    heartbeatIntervalMs: 250,
    requestTimeoutMs: 2_000,
  })

  window.natsailBrowserBroker = {
    close: async () => {
      await Promise.all([...leases.values()].map((lease) => lease.close()))
      leases.clear()
      await client.close()
    },
    nextMessage,
    reconnect: () => client.reconnect(),
    publish: (subject, value) => client.publish(subject, encoder.encode(value)),
    request: async (subject, value) =>
      decoder.decode(await client.request(subject, encoder.encode(value))),
    stats: () => client.stats(),
    subscribe: async (subject) => {
      const lease = client.createSource({ key: subject, contract: 'core-text:v1' })(
        async (delivery) => acceptDelivery(subject, decoder.decode(delivery.data))
      )
      leases.set(subject, lease)
      await lease.ready
    },
  }
  document.documentElement.dataset.natsailBrowserBrokerReady = 'true'
})()

declare global {
  interface Window {
    natsailBrowserBroker: {
      close(): Promise<void>
      nextMessage(subject: string): Promise<string>
      reconnect(): Promise<void>
      publish(subject: string, value: string): Promise<void>
      request(subject: string, value: string): Promise<string>
      stats(): Promise<BrowserBrokerStats>
      subscribe(subject: string): Promise<void>
    }
  }
}
