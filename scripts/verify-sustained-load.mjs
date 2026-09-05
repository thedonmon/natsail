import assert from 'node:assert/strict'
import { mkdir, writeFile } from 'node:fs/promises'
import { monitorEventLoopDelay, performance } from 'node:perf_hooks'
import { connect } from '@nats-io/transport-node'
import { jetstream, jetstreamManager, StorageType } from '@nats-io/jetstream'
import { createNatsRuntime, natsCodecs } from '@natsail/core'
import { processJetStream } from '@natsail/jetstream'

const seconds = Number(process.env.NATSAIL_LOAD_SECONDS ?? 10)
assert(
  Number.isInteger(seconds) && seconds >= 5 && seconds <= 600,
  'NATSAIL_LOAD_SECONDS must be 5..600'
)
assert(process.env.NATSAIL_CLUSTER_TEST === '1', 'Run only against the isolated resilience fixture')
const servers = ['127.0.0.1:14223', '127.0.0.1:14224', '127.0.0.1:14225']
const admin = await connect({ servers, ignoreClusterUpdates: true })
const manager = await jetstreamManager(admin)
const publisher = jetstream(admin)
const stream = `LOAD_${crypto.randomUUID().replaceAll('-', '_')}`
const subject = `tests.${stream}`
const runtime = createNatsRuntime({
  connect: () => connect({ servers, ignoreClusterUpdates: true }),
  limits: { maxJetStreamConsumers: 1, maxBufferedMessages: 1 },
})
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const latencies = []
const samples = []
const loop = monitorEventLoopDelay({ resolution: 20 })
let sent = 0
let received = 0
let duplicateCount = 0
let sampleTimer
let lease

try {
  await manager.streams.add({
    name: stream,
    subjects: [subject],
    storage: StorageType.File,
    num_replicas: 3,
    max_msgs: 100_000,
  })
  const seen = new Set()
  lease = processJetStream(
    runtime,
    {
      stream,
      filter: subject,
      consumer: { mode: 'ensure', name: 'slow-worker' },
      start: 'all',
      codec: natsCodecs.json(),
      ackWaitMs: 2_000,
      progressIntervalMs: 250,
      acknowledgement: { mode: 'confirmed' },
      maxBufferedMessages: 1,
    },
    async ({ value }) => {
      await delay(15)
      if (seen.has(value.id)) duplicateCount += 1
      else {
        seen.add(value.id)
        received += 1
      }
      if (latencies.length < 20_000) latencies.push(Date.now() - value.sentAt)
    }
  )
  await lease.ready
  global.gc?.()
  const initialHeap = process.memoryUsage().heapUsed
  loop.enable()
  sampleTimer = setInterval(
    () =>
      samples.push({
        atMs: performance.now(),
        heapBytes: process.memoryUsage().heapUsed,
        received,
        sent,
        reservedMessages: runtime.inspect().usedBufferedMessages,
      }),
    500
  )
  const until = performance.now() + seconds * 1_000
  while (performance.now() < until) {
    const batch = Array.from({ length: 5 }, () => {
      const value = { id: ++sent, sentAt: Date.now(), padding: 'x'.repeat(1_024) }
      return publisher.publish(subject, natsCodecs.json().encode(value))
    })
    await Promise.all(batch)
    await delay(50)
  }
  const drainDeadline = performance.now() + seconds * 1_000 + 30_000
  while (received < sent && performance.now() < drainDeadline) await delay(50)
  assert.equal(received, sent, 'Every published message must finish processing')
  await lease.close()
  assert.equal(runtime.inspect().activeResources, 0)
  assert.equal(runtime.inspect().usedBufferedMessages, 0)
  assert(samples.every((sample) => sample.reservedMessages <= 1))
  global.gc?.()
  const finalHeap = process.memoryUsage().heapUsed
  // Broad leak guard, not a representative capacity or throughput claim.
  assert(finalHeap - initialHeap < 64 * 1_024 * 1_024, 'Retained heap grew by more than 64 MiB')
  latencies.sort((a, b) => a - b)
  const report = {
    scenario: 'isolated-three-node-slow-handler',
    publishDurationSeconds: seconds,
    sent,
    received,
    duplicateCount,
    initialHeap,
    finalHeap,
    latencyP95Ms: latencies[Math.floor(latencies.length * 0.95)],
    eventLoopP99Ms: loop.percentile(99) / 1_000_000,
    samples,
  }
  await mkdir('.generated', { recursive: true })
  await writeFile('.generated/sustained-load.json', `${JSON.stringify(report, null, 2)}\n`)
  console.log(JSON.stringify({ ...report, samples: samples.length }))
} finally {
  clearInterval(sampleTimer)
  loop.disable()
  try {
    await runtime.close()
  } finally {
    try {
      await manager.streams.delete(stream)
    } finally {
      await admin.close()
    }
  }
}
