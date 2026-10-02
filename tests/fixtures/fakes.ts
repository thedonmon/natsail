import type { ConsumerMessages } from '@nats-io/jetstream'
import type { NatsConnection } from '@nats-io/nats-core'
import {
  createNatsRuntime,
  type NatsailScheduledTask,
  type NatsailScheduler,
  type NatsRuntimeOptions,
} from '@natsail/core'
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
