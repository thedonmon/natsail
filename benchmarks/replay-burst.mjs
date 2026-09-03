import { performance } from 'node:perf_hooks'

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

function runScenario(kind, messages) {
  const samples = []
  let checksum = 0
  for (let iteration = 0; iteration < iterations; iteration += 1) {
    const state = []
    const startedAt = performance.now()
    for (let offset = 0; offset < messages; offset += batchSize) {
      const end = Math.min(messages, offset + batchSize)
      const batch = Array.from({ length: end - offset }, (_, index) => offset + index)
      state.push(...batch)
    }
    const durationMs = performance.now() - startedAt
    checksum += state.length + (state.at(-1) ?? 0)
    samples.push(durationMs)
  }

  const durationMs = samples.reduce((total, sample) => total + sample, 0) / samples.length
  return {
    kind,
    messages,
    batchSize,
    iterations,
    durationMs,
    minimumDurationMs: Math.min(...samples),
    maximumDurationMs: Math.max(...samples),
    messagesPerSecond: messages / Math.max(durationMs / 1_000, Number.EPSILON),
    checksum,
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
  results: [
    ...replaySizes.map((messages) => runScenario('replay', messages)),
    ...burstSizes.map((messages) => runScenario('live-burst', messages)),
  ],
}

process.stdout.write(`${JSON.stringify(report)}\n`)
