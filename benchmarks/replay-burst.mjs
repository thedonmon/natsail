import { performance } from 'node:perf_hooks'

import { Effect, Stream } from 'effect'

import { createNatsailBatcher, createNatsailWorkController } from '../packages/core/dist/index.js'
import { materializeNatsJetStreamEvents } from '../packages/effect/dist/index.js'
import { observeNatsJetStreamState } from '../packages/rxjs/dist/index.js'
import { createSessionRegistry, defineSession } from '../packages/session/dist/index.js'

function argument(name, fallback) {
  const prefix = `--${name}=`
  return process.argv.find((value) => value.startsWith(prefix))?.slice(prefix.length) ?? fallback
}

function positiveIntegers(name, fallback) {
  const values = argument(name, fallback).split(',').map(Number)
  if (values.length === 0 || values.some((value) => !Number.isSafeInteger(value) || value <= 0)) {
    throw new TypeError(`--${name} must be a comma-separated list of positive integers`)
  }
  return values
}

function workScheduler(onYield) {
  return {
    now: () => performance.now(),
    schedule: (task, delayMs) => {
      const timer = setTimeout(task, delayMs)
      return { cancel: () => clearTimeout(timer) }
    },
    yield: async () => {
      onYield()
      await new Promise((resolve) => setTimeout(resolve, 0))
    },
  }
}

function controlledSource() {
  let accept
  let finish
  const closed = new Promise((resolve) => {
    finish = resolve
  })
  return {
    source: (next) => {
      accept = next
      return {
        ready: Promise.resolve(),
        closed,
        close: async () => finish(),
      }
    },
    deliver: (value) => accept(value),
    finish,
  }
}

function delivery(value, sequence, replay) {
  return {
    value,
    subject: 'benchmark.events',
    cursor: { stream: 'BENCHMARK', sequence },
    duplicate: false,
    redelivered: false,
    consumerPending: 0,
    replay,
  }
}

const replaySizes = positiveIntegers('replay', '1000,5000')
const burstSizes = positiveIntegers('bursts', '40,250,1000')
const batchSize = positiveIntegers('batch-size', '256')[0]
const iterations = positiveIntegers('iterations', '5')[0]
const yieldAfterMs = Number(argument('yield-after-ms', '4'))
if (!Number.isFinite(yieldAfterMs) || yieldAfterMs <= 0) {
  throw new TypeError('--yield-after-ms must be a positive finite number')
}

function summarize(adapter, kind, messages, samples, first, longestReducerSliceMs) {
  const durationMs = samples.reduce((total, sample) => total + sample, 0) / samples.length
  return {
    kind,
    adapter,
    messages,
    batchSize,
    yieldAfterMs,
    iterations,
    durationMs,
    minimumDurationMs: Math.min(...samples),
    maximumDurationMs: Math.max(...samples),
    messagesPerSecond: messages / Math.max(durationMs / 1_000, Number.EPSILON),
    checksum: first.checksum,
    deliveries: messages,
    emissions: first.emissions,
    commits: first.commits,
    ...(kind === 'replay' ? { replayTimeMs: durationMs } : {}),
    longestReducerSliceMs,
    observedBatchSizes: first.batchSizes,
    batchFlushes: first.batchFlushes,
    yieldCount: first.yieldCount,
  }
}

async function runRxjsScenario(kind, messages) {
  const samples = []
  let first
  let longestReducerSliceMs = 0

  for (let iteration = 0; iteration < iterations; iteration += 1) {
    let yields = 0
    const scheduler = workScheduler(() => {
      yields += 1
    })
    const work = createNatsailWorkController({ yieldAfterMs, scheduler })
    const registry = createSessionRegistry()
    const controlled = controlledSource()
    const definition = defineSession({
      key: `benchmark:rxjs:${kind}:${messages}:${iteration}`,
      contract: 'benchmark-state:v1',
      source: controlled.source,
    })
    let emissions = 0
    let state = 0
    let commits = 0
    let observedChecksum = 0
    let observedCommit = 0
    let batchFlushes = 0
    const batchSizes = []
    let complete
    const completed = new Promise((resolve, reject) => {
      complete = { resolve, reject }
    })
    const subscription = observeNatsJetStreamState(registry, definition, {
      batchPolicy: { maxItems: batchSize, maxWaitMs: 16 },
    }).subscribe({
      next: (snapshot) => {
        emissions += 1
        observedChecksum = snapshot.data
        observedCommit = snapshot.cursor?.sequence ?? observedCommit
      },
      error: complete.reject,
      complete: complete.resolve,
    })
    await Promise.resolve()

    const startedAt = performance.now()
    if (kind === 'replay') {
      await controlled.deliver({
        phase: 'replaying',
        data: state,
        restarts: 0,
        replay: { delivered: 0, remaining: messages },
      })
    }
    const batcher = createNatsailBatcher(
      { maxItems: batchSize, maxWaitMs: 16 },
      async (batch) => {
        const sliceStartedAt = performance.now()
        work.reset()
        batchFlushes += 1
        batchSizes.push(batch.length)
        for (const value of batch) {
          state += value + 1
          commits += 1
        }
        longestReducerSliceMs = Math.max(longestReducerSliceMs, performance.now() - sliceStartedAt)
        await work.checkpoint()
        if (kind === 'live-burst') {
          await controlled.deliver({
            phase: 'live',
            data: state,
            cursor: { stream: 'BENCHMARK', sequence: commits },
            restarts: 0,
            replay: { delivered: 0, remaining: 0 },
          })
        }
      },
      { scheduler, source: 'rxjs' }
    )
    for (let offset = 0; offset < messages; offset += batchSize) {
      const end = Math.min(messages, offset + batchSize)
      await Promise.all(
        Array.from({ length: end - offset }, (_, index) => batcher.add(offset + index))
      )
    }
    await batcher.complete()
    if (kind === 'replay') {
      await controlled.deliver({
        phase: 'live',
        data: state,
        cursor: { stream: 'BENCHMARK', sequence: commits },
        restarts: 0,
        replay: { delivered: messages, remaining: 0 },
      })
    }
    controlled.finish()
    await completed
    samples.push(performance.now() - startedAt)
    subscription.unsubscribe()
    await registry.close()

    if (observedChecksum !== state || observedCommit !== messages) {
      throw new Error('RxJS benchmark did not observe the final reduced state and cursor')
    }

    first ??= {
      checksum: observedChecksum,
      emissions,
      commits: observedCommit,
      batchSizes,
      batchFlushes,
      yieldCount: yields,
    }
  }

  return summarize('rxjs', kind, messages, samples, first, longestReducerSliceMs)
}

async function runEffectScenario(kind, messages) {
  const samples = []
  let first
  let longestReducerSliceMs = 0

  for (let iteration = 0; iteration < iterations; iteration += 1) {
    let yields = 0
    let commits = 0
    let batchFlushes = 0
    const batchSizes = []
    const scheduler = workScheduler(() => {
      yields += 1
    })
    const deliveries = Array.from({ length: messages }, (_, index) =>
      delivery(index, index + 1, kind === 'replay' ? 'initial' : 'live')
    )
    const caughtUp = {
      type: 'caught-up',
      catchUp: {
        delivered: kind === 'replay' ? messages : 0,
        ...(kind === 'replay' ? { cursor: { stream: 'BENCHMARK', sequence: messages } } : {}),
      },
    }
    const events =
      kind === 'replay'
        ? [...deliveries.map((event) => ({ type: 'delivery', delivery: event })), caughtUp]
        : [caughtUp, ...deliveries.map((event) => ({ type: 'delivery', delivery: event }))]
    const stream = materializeNatsJetStreamEvents(
      Stream.fromIterable(events),
      {
        initial: () => 0,
        reduceBatch: (state, batch) =>
          Effect.sync(() => {
            const sliceStartedAt = performance.now()
            batchFlushes += 1
            batchSizes.push(batch.length)
            commits += batch.length
            const next = batch.reduce((sum, event) => sum + event.value + 1, state)
            longestReducerSliceMs = Math.max(
              longestReducerSliceMs,
              performance.now() - sliceStartedAt
            )
            return next
          }),
      },
      {
        batchPolicy: { maxItems: batchSize, maxWaitMs: 16 },
        workBudget: { yieldAfterMs, scheduler },
      }
    )

    const startedAt = performance.now()
    const emitted = Array.from(await Effect.runPromise(stream.pipe(Stream.runCollect)))
    samples.push(performance.now() - startedAt)
    const observed = emitted.at(-1)
    if (
      observed?.data !== (commits * (commits + 1)) / 2 ||
      observed.cursor?.sequence !== messages
    ) {
      throw new Error('Effect benchmark did not observe the final reduced state and cursor')
    }
    first ??= {
      checksum: observed.data,
      emissions: emitted.length,
      commits: observed.cursor.sequence,
      batchSizes,
      batchFlushes,
      yieldCount: yields,
    }
  }

  return summarize('effect', kind, messages, samples, first, longestReducerSliceMs)
}

const results = []
for (const messages of replaySizes) {
  results.push(await runRxjsScenario('replay', messages))
  results.push(await runEffectScenario('replay', messages))
}
for (const messages of burstSizes) {
  results.push(await runRxjsScenario('live-burst', messages))
  results.push(await runEffectScenario('live-burst', messages))
}

const report = {
  schemaVersion: 2,
  suite: 'natsail-adapter-replay-burst',
  generatedAt: new Date().toISOString(),
  runtime: {
    node: process.version,
    platform: process.platform,
    architecture: process.arch,
  },
  batchPolicy: { maxItems: batchSize, maxWaitMs: 16 },
  workBudget: { yieldAfterMs },
  results,
}

process.stdout.write(`${JSON.stringify(report)}\n`)
