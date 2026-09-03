import { performance } from 'node:perf_hooks'

import {
  createNatsailBatcher,
  createNatsailTelemetryReporter,
  createNatsailWorkController,
} from '../packages/core/dist/index.js'

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

const replaySizes = positiveIntegers('replay', '1000,5000')
const burstSizes = positiveIntegers('bursts', '40,250,1000')
const batchSize = positiveIntegers('batch-size', '256')[0]
const iterations = positiveIntegers('iterations', '5')[0]
const yieldAfterMs = Number(argument('yield-after-ms', '4'))
if (!Number.isFinite(yieldAfterMs) || yieldAfterMs <= 0) {
  throw new TypeError('--yield-after-ms must be a positive finite number')
}

async function runScenario(kind, messages) {
  const samples = []
  let checksum = 0
  let flushes = 0
  let yields = 0
  for (let iteration = 0; iteration < iterations; iteration += 1) {
    const scheduler = {
      now: () => performance.now(),
      schedule: (task, delayMs) => {
        const timer = setTimeout(task, delayMs)
        return { cancel: () => clearTimeout(timer) }
      },
      yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
    }
    const telemetry = createNatsailTelemetryReporter({
      sink: {
        record(event) {
          if (event.name === 'natsail.batch.flushes') flushes += event.value
          if (event.name === 'natsail.work.yields') yields += event.value
        },
      },
    })
    const work = createNatsailWorkController({ yieldAfterMs, scheduler }, telemetry, 'benchmark')
    let state = 0
    const startedAt = performance.now()
    const batcher = createNatsailBatcher(
      { maxItems: batchSize, maxWaitMs: 16 },
      async (batch) => {
        work.reset()
        for (const value of batch) {
          state += value + 1
          await work.checkpoint()
        }
      },
      { scheduler, telemetry, source: 'benchmark' }
    )
    for (let offset = 0; offset < messages; offset += batchSize) {
      const end = Math.min(messages, offset + batchSize)
      const batch = Array.from({ length: end - offset }, (_, index) => offset + index)
      const accepted = batch.map((value) => batcher.add(value))
      await Promise.all(accepted)
    }
    await batcher.complete()
    const durationMs = performance.now() - startedAt
    checksum += state
    samples.push(durationMs)
  }

  const durationMs = samples.reduce((total, sample) => total + sample, 0) / samples.length
  return {
    kind,
    messages,
    batchSize,
    yieldAfterMs,
    iterations,
    durationMs,
    minimumDurationMs: Math.min(...samples),
    maximumDurationMs: Math.max(...samples),
    messagesPerSecond: messages / Math.max(durationMs / 1_000, Number.EPSILON),
    checksum,
    batchFlushes: flushes,
    cooperativeYields: yields,
  }
}

const report = {
  schemaVersion: 1,
  suite: 'natsail-local-replay-burst',
  generatedAt: new Date().toISOString(),
  runtime: {
    node: process.version,
    platform: process.platform,
    architecture: process.arch,
  },
  batchPolicy: { maxItems: batchSize, maxWaitMs: 16 },
  workBudget: { yieldAfterMs },
  results: [
    ...(await Promise.all(replaySizes.map((messages) => runScenario('replay', messages)))),
    ...(await Promise.all(burstSizes.map((messages) => runScenario('live-burst', messages)))),
  ],
}

process.stdout.write(`${JSON.stringify(report)}\n`)
