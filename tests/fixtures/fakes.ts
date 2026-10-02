import type { ConsumerMessages } from '@nats-io/jetstream'
import type { NatsConnection } from '@nats-io/nats-core'
import {
  createNatsailTelemetryReporter,
  createNatsRuntime,
  NATS_RUNTIME_ADAPTER,
  type NatsailScheduledTask,
  type NatsailScheduler,
  type NatsRuntime,
  type NatsRuntimeEvent,
  type NatsRuntimeOptions,
  type RuntimeResource,
  type SubscriptionLease,
} from '@natsail/core'
import type { SessionSource } from '@natsail/session'
import { vi } from 'vitest'

export function emptyEvents<T = never>(): AsyncIterable<T> {
  return {
    async *[Symbol.asyncIterator]() {},
  }
}

/** Virtual-time scheduler; `advance` runs due tasks and `yields` counts cooperative yields. */
export class ManualScheduler implements NatsailScheduler {
  time = 0
  yields = 0
  private tasks: Array<{ at: number; cancelled: boolean; task: () => void }> = []

  now(): number {
    return this.time
  }

  schedule(task: () => void, delayMs: number): NatsailScheduledTask {
    const scheduled = { at: this.time + delayMs, cancelled: false, task }
    this.tasks.push(scheduled)
    return { cancel: () => (scheduled.cancelled = true) }
  }

  async yield(): Promise<void> {
    this.yields += 1
  }

  advance(ms: number): void {
    this.time += ms
    for (const scheduled of this.tasks.splice(0)) {
      if (!scheduled.cancelled && scheduled.at <= this.time) scheduled.task()
      else this.tasks.push(scheduled)
    }
  }
}

/** A real runtime over a fake connection that closes when drained or closed. */
export function fakeConnectionRuntime(
  options: Omit<NatsRuntimeOptions, 'connect'> & { server?: string } = {}
) {
  const { server = 'mock:4222', ...runtimeOptions } = options
  let closeConnection!: () => void
  const closed = new Promise<void>((resolve) => {
    closeConnection = resolve
  })
  const connection = {
    closed: () => closed,
    drain: vi.fn(async () => closeConnection()),
    close: vi.fn(async () => closeConnection()),
    getServer: vi.fn(() => server),
    isClosed: vi.fn(() => false),
    status: async function* () {},
  } as unknown as NatsConnection
  return createNatsRuntime({ ...runtimeOptions, connect: async () => connection })
}

/** A finite or held-open JetStream message iterator with observable close. */
export function messageSource(
  deliveries: readonly unknown[],
  closedError?: Error,
  stayOpen = false
): ConsumerMessages {
  let closeRequested = false
  let finish!: () => void
  const closeSignal = new Promise<void>((resolve) => {
    finish = resolve
  })

  return {
    async *[Symbol.asyncIterator]() {
      for (const delivery of deliveries) {
        if (closeRequested) break
        yield delivery
      }
      if (stayOpen && !closeRequested) await closeSignal
    },
    close: vi.fn(async () => {
      closeRequested = true
      finish()
    }),
    closed: vi.fn(async () => closedError),
    status: async function* () {
      await closeSignal
    },
  } as unknown as ConsumerMessages
}

/** A runtime double whose methods are inert unless a test overrides them. */
export function runtimeStub(overrides: Partial<NatsRuntime> = {}): NatsRuntime {
  return {
    [NATS_RUNTIME_ADAPTER]: {
      manage: <T extends RuntimeResource>(create: () => T) => create(),
      reportDiagnostic: () => undefined,
      telemetry: createNatsailTelemetryReporter(),
    },
    events: emptyEvents<NatsRuntimeEvent>(),
    connection: vi.fn(async () => ({}) as never),
    reconnect: vi.fn(async () => ({}) as never),
    publish: vi.fn(async () => undefined),
    request: vi.fn(async () => undefined as never),
    subscribe: vi.fn(() => {
      throw new Error('Not implemented by this test runtime')
    }),
    inspect: vi.fn(() => ({}) as never),
    close: vi.fn(async () => undefined),
    ...overrides,
  } as unknown as NatsRuntime
}

/** A session source whose lease the test delivers to, finishes, fails or closes. */
export function controllableSource<T>() {
  let accept!: (value: T) => Promise<void>
  let finish!: () => void
  let fail!: (error: unknown) => void
  const closed = new Promise<void>((resolve, reject) => {
    finish = resolve
    fail = reject
  })
  const close = vi.fn(async () => finish())
  const lease: SubscriptionLease = { ready: Promise.resolve(), closed, close }
  const starts = vi.fn<SessionSource<T>>((next) => {
    accept = next
    return lease
  })
  return { source: starts, starts, close, deliver: (value: T) => accept(value), finish, fail }
}

/** Multicast runtime events; every iterator sees every pushed event. */
export function controllableEvents(): {
  iterable: AsyncIterable<NatsRuntimeEvent>
  push(event: NatsRuntimeEvent): void
  activeIterators(): number
} {
  const subscribers = new Set<{
    queue: NatsRuntimeEvent[]
    resume?: () => void
    closed: boolean
  }>()

  return {
    iterable: {
      [Symbol.asyncIterator]() {
        const subscriber: {
          queue: NatsRuntimeEvent[]
          resume?: () => void
          closed: boolean
        } = { queue: [], closed: false }
        subscribers.add(subscriber)

        return {
          async next(): Promise<IteratorResult<NatsRuntimeEvent>> {
            while (subscriber.queue.length === 0 && !subscriber.closed) {
              await new Promise<void>((resolve) => {
                subscriber.resume = resolve
              })
            }
            if (subscriber.closed) return { done: true, value: undefined }
            return { done: false, value: subscriber.queue.shift()! }
          },
          async return(): Promise<IteratorResult<NatsRuntimeEvent>> {
            subscriber.closed = true
            subscribers.delete(subscriber)
            subscriber.resume?.()
            return { done: true, value: undefined }
          },
        }
      },
    },
    push(event) {
      for (const subscriber of subscribers) {
        subscriber.queue.push(event)
        subscriber.resume?.()
        delete subscriber.resume
      }
    },
    activeIterators: () => subscribers.size,
  }
}
