import { map } from 'rxjs/map'

import { observeNatsJetStreamState, observeNatsSessionValues } from '@natsail/rxjs'
import { createSessionRegistry, defineSession, type SessionSource } from '@natsail/session'
import type { JetStreamStateSnapshot } from '@natsail/jetstream'

function controlledSource<T>() {
  const runs: Array<{
    deliver(value: T): Promise<void>
    finish(): void
    fail(error: unknown): void
    closed: Promise<void>
    closes: number
  }> = []
  const source: SessionSource<T> = (deliver) => {
    let finish!: () => void
    let fail!: (error: unknown) => void
    const closed = new Promise<void>((resolve, reject) => {
      finish = resolve
      fail = reject
    })
    const run = { deliver, finish, fail, closed, closes: 0 }
    runs.push(run)
    return {
      ready: Promise.resolve(),
      closed,
      close: async () => {
        run.closes += 1
        finish()
      },
    }
  }
  return { source, runs }
}

export async function lifecycle() {
  const registry = createSessionRegistry()
  const controlled = controlledSource<number>()
  const values = observeNatsSessionValues(registry, 'browser-lifecycle', controlled.source)[map](
    (value) => value * 10
  )
  const controllers = Array.from({ length: 4 }, () => new AbortController())
  const first: number[] = []
  const second: number[] = []
  const restarted: number[] = []
  const errors: string[] = []
  const onError = (error: unknown) => errors.push(String(error))

  try {
    const result = values.subscribe(
      { next: (value) => first.push(value), error: onError },
      {
        signal: controllers[0]!.signal,
      }
    )
    await Promise.resolve()
    await controlled.runs[0]!.deliver(1)
    values.subscribe(
      { next: (value) => second.push(value), error: onError },
      {
        signal: controllers[1]!.signal,
      }
    )
    const joined = {
      first: [...first],
      second: [...second],
      references: registry.inspect().sessions[0]!.references,
      starts: controlled.runs.length,
    }

    controllers[0]!.abort()
    await controlled.runs[0]!.deliver(2)
    const remaining = {
      first: [...first],
      second: [...second],
      references: registry.inspect().sessions[0]!.references,
      closes: controlled.runs[0]!.closes,
    }
    controllers[1]!.abort()
    await controlled.runs[0]!.closed
    const released = {
      activeSessions: registry.inspect().activeSessions,
      closes: controlled.runs[0]!.closes,
    }

    let complete!: () => void
    const completed = new Promise<void>((resolve) => {
      complete = resolve
    })
    values.subscribe(
      { next: (value) => restarted.push(value), error: onError, complete },
      {
        signal: controllers[2]!.signal,
      }
    )
    await Promise.resolve()
    const beforeRestartDelivery = [...restarted]
    await controlled.runs[1]!.deliver(3)
    controlled.runs[1]!.finish()
    await completed

    let failed!: () => void
    const failure = new Promise<void>((resolve) => {
      failed = resolve
    })
    values.subscribe(
      {
        error: (error) => {
          onError(error)
          failed()
        },
      },
      {
        signal: controllers[3]!.signal,
      }
    )
    await Promise.resolve()
    controlled.runs[2]!.fail(new Error('source failed'))
    await failure

    return {
      isPlatformObservable: values instanceof Observable,
      subscribeReturnsUndefined: result === undefined,
      joined,
      remaining,
      released,
      beforeRestartDelivery,
      restarted,
      starts: controlled.runs.length,
      errors,
      activeSessions: registry.inspect().activeSessions,
    }
  } finally {
    for (const controller of controllers) controller.abort()
    await registry.close()
  }
}

export async function batching() {
  const registry = createSessionRegistry()
  const controlled = controlledSource<JetStreamStateSnapshot<number>>()
  const definition = defineSession({
    key: 'browser-batching',
    contract: 'state',
    source: controlled.source,
  })
  const controller = new AbortController()
  const states: number[] = []
  let flushed!: () => void
  let failed!: (error: unknown) => void
  const flush = new Promise<void>((resolve, reject) => {
    flushed = resolve
    failed = reject
  })
  try {
    observeNatsJetStreamState(registry, definition, { liveBatchMs: 16 }).subscribe(
      {
        next: (snapshot) => {
          states.push(snapshot.data)
          if (snapshot.data === 3) flushed()
        },
        error: failed,
      },
      { signal: controller.signal }
    )
    await Promise.resolve()
    const deliver = (data: number) =>
      controlled.runs[0]!.deliver({
        phase: 'live',
        data,
        restarts: 0,
        replay: { delivered: 0 },
      })
    for (const data of [1, 2, 3]) await deliver(data)
    const immediate = [...states]
    await flush
    const afterFlush = [...states]
    await deliver(4)
    controller.abort()
    await controlled.runs[0]!.closed
    await new Promise((resolve) => setTimeout(resolve, 32))
    return {
      immediate,
      afterFlush,
      afterAbort: states,
      activeSessions: registry.inspect().activeSessions,
    }
  } finally {
    controller.abort()
    await registry.close()
  }
}
