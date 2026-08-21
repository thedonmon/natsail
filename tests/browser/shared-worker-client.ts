interface SharedWorkerStats {
  clientCount: number
  connectionRequests: number
  subscriptionCount: number
}

interface WorkerResult {
  error?: string
  id: number
  result?: unknown
  type: 'result'
}

interface WorkerDelivery {
  subject: string
  type: 'delivery'
  value: string
}

interface PendingRequest {
  reject(error: Error): void
  resolve(value: unknown): void
}

interface DeliveryWaiter {
  reject(error: Error): void
  resolve(value: string): void
  timeout: ReturnType<typeof setTimeout>
}

const worker = new SharedWorker(new URL('./nats-shared-worker.ts', import.meta.url), {
  name: 'natsail-browser-test',
  type: 'module',
})
const pending = new Map<number, PendingRequest>()
const queuedDeliveries = new Map<string, string[]>()
const waitingDeliveries = new Map<string, DeliveryWaiter[]>()
let nextRequestId = 1

const request = <T>(action: string, details: Record<string, unknown> = {}): Promise<T> => {
  const id = nextRequestId
  nextRequestId += 1

  return new Promise<T>((resolve, reject) => {
    pending.set(id, {
      reject,
      resolve: (value) => resolve(value as T),
    })
    worker.port.postMessage({ action, id, ...details })
  })
}

const acceptDelivery = (delivery: WorkerDelivery): void => {
  const waiters = waitingDeliveries.get(delivery.subject)
  const waiter = waiters?.shift()

  if (waiter) {
    clearTimeout(waiter.timeout)
    waiter.resolve(delivery.value)
    return
  }

  const queue = queuedDeliveries.get(delivery.subject) ?? []
  queue.push(delivery.value)
  queuedDeliveries.set(delivery.subject, queue)
}

worker.port.onmessage = (event: MessageEvent<WorkerDelivery | WorkerResult>) => {
  if (event.data.type === 'delivery') {
    acceptDelivery(event.data)
    return
  }

  const active = pending.get(event.data.id)
  if (!active) {
    return
  }

  pending.delete(event.data.id)
  if (event.data.error) {
    active.reject(new Error(event.data.error))
  } else {
    active.resolve(event.data.result)
  }
}
worker.port.start()

const nextMessage = (subject: string): Promise<string> => {
  const queue = queuedDeliveries.get(subject)
  const value = queue?.shift()
  if (value !== undefined) {
    return Promise.resolve(value)
  }

  return new Promise((resolve, reject) => {
    const waiters = waitingDeliveries.get(subject) ?? []
    const waiter: DeliveryWaiter = {
      reject,
      resolve,
      timeout: setTimeout(() => {
        const index = waiters.indexOf(waiter)
        if (index >= 0) {
          waiters.splice(index, 1)
        }
        reject(new Error(`Timed out waiting for ${subject}`))
      }, 5_000),
    }
    waiters.push(waiter)
    waitingDeliveries.set(subject, waiters)
  })
}

window.natsailSharedWorker = {
  close: async () => {
    await request<void>('close')
    worker.port.close()
  },
  nextMessage,
  publish: (subject, value) => request<void>('publish', { subject, value }),
  stats: () => request<SharedWorkerStats>('stats'),
  subscribe: (subject) => request<void>('subscribe', { subject }),
}

document.documentElement.dataset.natsailSharedWorkerReady = 'true'

declare global {
  interface Window {
    natsailSharedWorker: {
      close(): Promise<void>
      nextMessage(subject: string): Promise<string>
      publish(subject: string, value: string): Promise<void>
      stats(): Promise<SharedWorkerStats>
      subscribe(subject: string): Promise<void>
    }
  }
}
