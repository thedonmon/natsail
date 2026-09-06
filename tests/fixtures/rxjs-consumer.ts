import assert from 'node:assert/strict'

import { observeNatsSessionValues } from '@natsail/rxjs'
import { createSessionRegistry, type SessionSource } from '@natsail/session'

const registry = createSessionRegistry()
const firstController = new AbortController()
const secondController = new AbortController()
let deliver!: (value: number) => Promise<void>
let finish!: () => void
let starts = 0
let closes = 0
const closed = new Promise<void>((resolve) => {
  finish = resolve
})
const source: SessionSource<number> = (accept) => {
  deliver = accept
  starts += 1
  return {
    ready: Promise.resolve(),
    closed,
    close: async () => {
      closes += 1
      finish()
    },
  }
}

try {
  const values = observeNatsSessionValues(registry, 'packed-consumer', source)
  const first: string[] = []
  const second: number[] = []
  // No direct RxJS import: the shipped declarations must load their own ambient types.
  const result: void = values.subscribe((value) => first.push(value.toFixed(1)), {
    signal: firstController.signal,
  })
  values.subscribe((value) => second.push(value), { signal: secondController.signal })
  assert.equal(result, undefined)
  assert(values instanceof Observable)
  await Promise.resolve()
  await deliver(1)
  assert.equal(starts, 1)
  assert.equal(registry.inspect().sessions[0]?.references, 2)
  firstController.abort()
  await deliver(2)
  assert.deepEqual(first, ['1.0'])
  assert.deepEqual(second, [1, 2])
  assert.equal(closes, 0)
  secondController.abort()
  await closed
  assert.equal(closes, 1)
  assert.equal(registry.inspect().activeSessions, 0)
  console.log('Verified packed RxJS consumer types, values, sharing, and cancellation.')
} finally {
  firstController.abort()
  secondController.abort()
  await registry.close()
}
