import { describe, expect, it, vi } from 'vitest'

import {
  createNatsailBatcher,
  createNatsailTelemetryReporter,
  createNatsailWorkController,
  defineNatsailBatchPolicy,
  type NatsailScheduledTask,
  type NatsailScheduler,
  type NatsailTelemetryEvent,
} from '@natsail/core'

class ManualScheduler implements NatsailScheduler {
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

describe('shared batching and cooperative work', () => {
  it('flushes by count and preserves serial application order', async () => {
    const applied: number[][] = []
    const batcher = createNatsailBatcher<number>({ maxItems: 2 }, async (values) => {
      applied.push([...values])
    })
    const first = batcher.add(1)
    const second = batcher.add(2)
    const third = batcher.add(3)
    await Promise.all([first, second])
    await batcher.complete()
    await third
    expect(applied).toEqual([[1, 2], [3]])
  })

  it('flushes by bytes and rejects invalid or oversized measurements', async () => {
    const applied: string[][] = []
    const batcher = createNatsailBatcher(
      { maxBytes: 4, sizeOf: (value: string) => value.length },
      (values) => {
        applied.push([...values])
      }
    )
    const first = batcher.add('aa')
    const second = batcher.add('bb')
    await Promise.all([first, second])
    expect(applied).toEqual([['aa', 'bb']])
    await expect(batcher.add('12345')).rejects.toMatchObject({
      name: 'NatsailBatchItemTooLargeError',
    })

    const invalid = createNatsailBatcher({ maxBytes: 4, sizeOf: () => Number.NaN }, () => undefined)
    await expect(invalid.add('x')).rejects.toThrow('finite non-negative')
    const thrown = createNatsailBatcher(
      {
        maxBytes: 4,
        sizeOf: () => {
          throw new Error('measure failed')
        },
      },
      () => undefined
    )
    await expect(thrown.add('x')).rejects.toThrow('measure failed')
  })

  it('flushes by time and completion without racing the cancelled timer', async () => {
    const scheduler = new ManualScheduler()
    const applied: number[][] = []
    const batcher = createNatsailBatcher<number>(
      { maxItems: 10, maxWaitMs: 5 },
      (values) => {
        applied.push([...values])
      },
      { scheduler }
    )
    const first = batcher.add(1)
    scheduler.advance(5)
    await first
    const second = batcher.add(2)
    await batcher.complete()
    scheduler.advance(5)
    await second
    expect(applied).toEqual([[1], [2]])
  })

  it('drops a pending partial batch on cancellation', async () => {
    const apply = vi.fn()
    const batcher = createNatsailBatcher({ maxItems: 2 }, apply)
    const pending = batcher.add(1)
    batcher.cancel()
    await expect(pending).rejects.toMatchObject({ name: 'NatsailBatchCancelledError' })
    expect(apply).not.toHaveBeenCalled()
  })

  it('validates bounded policies and yields only after the configured time slice', async () => {
    expect(() => defineNatsailBatchPolicy({})).toThrow('at least one bound')
    expect(() => defineNatsailBatchPolicy({ maxBytes: 10 })).toThrow('requires sizeOf')
    expect(() => defineNatsailBatchPolicy({ maxItems: 0 })).toThrow('positive safe integer')

    const scheduler = new ManualScheduler()
    const work = createNatsailWorkController({ yieldAfterMs: 4, scheduler })
    scheduler.advance(100)
    work.reset()
    await work.checkpoint()
    expect(scheduler.yields).toBe(0)
    scheduler.advance(4)
    await work.checkpoint()
    await work.checkpoint()
    expect(scheduler.yields).toBe(1)
    scheduler.advance(4)
    await work.checkpoint()
    expect(scheduler.yields).toBe(2)
  })

  it('reports closed low-cardinality flush and yield signals', async () => {
    const events: NatsailTelemetryEvent[] = []
    const telemetry = createNatsailTelemetryReporter({
      sink: { record: (event) => events.push(event) },
    })
    const scheduler = new ManualScheduler()
    const batcher = createNatsailBatcher({ maxItems: 1 }, () => undefined, {
      scheduler,
      telemetry,
      source: 'session',
    })
    await batcher.add('private-payload')
    const work = createNatsailWorkController({ yieldAfterMs: 1, scheduler }, telemetry, 'session')
    scheduler.advance(1)
    await work.checkpoint()

    expect(events.map((event) => event.name)).toEqual([
      'natsail.batch.flushes',
      'natsail.work.yields',
    ])
    expect(JSON.stringify(events)).not.toContain('private-payload')
  })
})
