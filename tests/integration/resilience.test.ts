import { execFileSync, fork } from 'node:child_process'
import { once } from 'node:events'
import { setTimeout as delay } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { connect } from '@nats-io/transport-node'
import { errors, nanos } from '@nats-io/nats-core'
import {
  jetstream,
  jetstreamManager,
  StorageType,
  JetStreamApiError,
  AckPolicy,
  DeliverPolicy,
  ReplayPolicy,
  type ClusterInfo,
} from '@nats-io/jetstream'
import { describe, expect, it } from 'vitest'
import {
  createNatsRuntime,
  natsCodecs,
  type NatsRuntime,
  type NatsRuntimeEvent,
} from '@natsail/core'
import { processJetStream } from '@natsail/jetstream'

const servers = ['127.0.0.1:14223', '127.0.0.1:14224', '127.0.0.1:14225']
const nodes = ['nats1', 'nats2', 'nats3']
const readinessMs = 30_000
// nats.js 3.4.0 can wait 15–45s after a failed consumer-info request; allow
// heartbeat detection and another request too. This is not our recovery.delayMs.
// https://github.com/nats-io/nats.js/blob/v3.4.0/jetstream/src/consumer.ts#L582
const recoveryMs = 75_000
const compose = fileURLToPath(new URL('../fixtures/cluster/compose.yml', import.meta.url))
const connection = (preferred?: string) =>
  connect({
    servers: preferred ? [preferred, ...servers.filter((server) => server !== preferred)] : servers,
    noRandomize: preferred !== undefined,
    ignoreClusterUpdates: true,
    timeout: 500,
    reconnectTimeWait: 100,
    maxReconnectAttempts: 100,
  })

async function retryTransient<T>(operation: () => Promise<T>): Promise<T> {
  const deadline = Date.now() + readinessMs
  for (;;) {
    try {
      return await operation()
    } catch (error) {
      // JetStream can wrap a transport error (e.g. no responders during election).
      const cause =
        error instanceof Error && error.cause instanceof errors.RequestError ? error.cause : error
      const transient =
        cause instanceof errors.TimeoutError ||
        cause instanceof errors.ConnectionError ||
        (cause instanceof errors.RequestError &&
          (cause.isNoResponders() || cause.cause instanceof errors.TimeoutError)) ||
        (cause instanceof JetStreamApiError && cause.status === 503)
      if (!transient || Date.now() >= deadline) throw error
      await delay(100)
    }
  }
}

async function waitForReplicas(read: () => Promise<{ cluster?: ClusterInfo }>, expected = nodes) {
  await expect
    .poll(
      async () => {
        const { cluster } = await read()
        if (!cluster?.leader || !expected.includes(cluster.leader)) return false
        const current = new Set([
          cluster.leader,
          ...(cluster.replicas ?? [])
            .filter((peer) => peer.current && (peer.lag ?? 0) === 0)
            .map((peer) => peer.name),
        ])
        return expected.every((name) => current.has(name))
      },
      { timeout: readinessMs }
    )
    .toBe(true)
}

async function fixture() {
  const admin = await retryTransient(connection)
  // A connected socket does not imply that JetStream's meta leader is ready.
  let manager: Awaited<ReturnType<typeof jetstreamManager>>
  const stream = `RESILIENCE_${crypto.randomUUID().replaceAll('-', '_')}`
  const subject = `tests.${stream}`
  try {
    manager = await retryTransient(() => jetstreamManager(admin))
    await retryTransient(() =>
      manager.streams.add({
        name: stream,
        subjects: [subject],
        storage: StorageType.File,
        num_replicas: 3,
      })
    )
    await waitForReplicas(() => manager.streams.info(stream))
  } catch (error) {
    await admin.close()
    throw error
  }
  const client = jetstream(admin)
  return {
    admin,
    manager,
    stream,
    subject,
    // A publish ACK can be lost during election. Keep one ID across retries so
    // accepting the same publish twice does not create two stored messages.
    publish(value: string) {
      return retryTransient(() =>
        client.publish(subject, value, {
          msgID: `${stream}:${value}`,
          timeout: 2_000,
        })
      )
    },
    async close(runtime?: NatsRuntime) {
      try {
        await runtime?.close()
      } finally {
        try {
          await manager.streams.delete(stream)
        } finally {
          await admin.close()
        }
      }
    },
  }
}

describe.skipIf(process.env.NATSAIL_CLUSTER_TEST !== '1')('isolated three-node resilience', () => {
  it.each(['consumer leader', 'connected non-leader'] as const)(
    'continues acknowledged processing after the %s is killed',
    async (fault) => {
      const f = await fixture()
      let runtime: NatsRuntime | undefined
      let lease: ReturnType<typeof processJetStream> | undefined
      let observing: Promise<void> | undefined
      const events: NatsRuntimeEvent[] = []
      const received = new Map<string, number>()
      const started = Date.now()
      const timeline: { stage: string; elapsedMs: number }[] = []
      const mark = (stage: string) => timeline.push({ stage, elapsedMs: Date.now() - started })
      let killed: string | undefined
      try {
        mark('prepare replicated consumer')
        await f.manager.consumers.add(f.stream, {
          durable_name: 'resilient',
          filter_subject: f.subject,
          ack_policy: AckPolicy.Explicit,
          deliver_policy: DeliverPolicy.All,
          replay_policy: ReplayPolicy.Instant,
          ack_wait: nanos(1_000),
          num_replicas: 3,
        })
        await waitForReplicas(() => f.manager.consumers.info(f.stream, 'resilient'))
        const streamLeader = (await f.manager.streams.info(f.stream)).cluster?.leader
        const consumerLeader = (await f.manager.consumers.info(f.stream, 'resilient')).cluster
          ?.leader
        expect(nodes).toContain(streamLeader)
        expect(nodes).toContain(consumerLeader)
        const connectedNode = nodes.find(
          (node) => node !== streamLeader && node !== consumerLeader
        )!
        const preferred = servers[nodes.indexOf(connectedNode)]!
        runtime = createNatsRuntime({ connect: () => connection(preferred) })
        const observedRuntime = runtime
        observing = (async () => {
          for await (const event of observedRuntime.events) events.push(event)
        })()
        lease = processJetStream(
          runtime,
          {
            stream: f.stream,
            filter: f.subject,
            consumer: { mode: 'ensure', name: 'resilient' },
            start: 'all',
            codec: natsCodecs.text,
            replicas: 3,
            ackWaitMs: 1_000,
            acknowledgement: { mode: 'confirmed' },
            recovery: { delayMs: 100 },
          },
          ({ value }) => {
            received.set(value, (received.get(value) ?? 0) + 1)
          }
        )
        await lease.ready
        const processor = lease
        const nc = await runtime.connection()
        expect(nc.getServer()).toBe(preferred)
        const before = await f.publish('before')
        await expect.poll(() => processor.inspect().acknowledged.stream).toBe(before.seq)
        // Recheck immediately before injecting the fault: the client connection
        // and both independent Raft leaders must still match this scenario.
        expect((await f.manager.streams.info(f.stream)).cluster?.leader).toBe(streamLeader)
        expect((await f.manager.consumers.info(f.stream, 'resilient')).cluster?.leader).toBe(
          consumerLeader
        )
        killed = fault === 'consumer leader' ? consumerLeader : connectedNode
        expect(nodes).toContain(killed)
        mark(
          `kill ${killed}; connected=${connectedNode}; stream=${streamLeader}; consumer=${consumerLeader}`
        )
        execFileSync('docker', ['compose', '-f', compose, 'kill', '-s', 'SIGKILL', killed!], {
          stdio: 'pipe',
        })
        mark('wait for surviving replicas')
        const survivors = nodes.filter((node) => node !== killed)
        await Promise.all([
          waitForReplicas(() => f.manager.streams.info(f.stream), survivors),
          waitForReplicas(() => f.manager.consumers.info(f.stream, 'resilient'), survivors),
        ])
        mark('publish after fault')
        let lastSequence = before.seq
        for (const value of ['after-1', 'after-2', 'after-3']) {
          lastSequence = (await f.publish(value)).seq
        }
        mark('wait for delivery and confirmed ACKs')
        await expect
          .poll(
            () => ({
              values: [...received.keys()].sort(),
              acknowledged: processor.inspect().acknowledged.stream,
            }),
            { timeout: recoveryMs }
          )
          .toEqual({
            values: ['after-1', 'after-2', 'after-3', 'before'],
            acknowledged: lastSequence,
          })
        const disconnected = events.some(
          (event) => event.type === 'status' && event.state === 'disconnected'
        )
        if (fault === 'consumer leader') {
          expect(nc.getServer()).toBe(preferred)
          expect(disconnected).toBe(false)
        } else {
          expect(nc.getServer()).not.toBe(preferred)
          expect(disconnected).toBe(true)
          expect((await f.manager.streams.info(f.stream)).cluster?.leader).toBe(streamLeader)
          expect((await f.manager.consumers.info(f.stream, 'resilient')).cluster?.leader).toBe(
            consumerLeader
          )
        }
        mark('recovered')
        console.info(JSON.stringify({ fault, timeline, deliveries: Object.fromEntries(received) }))
      } catch (cause) {
        const [stream, consumer] = await Promise.allSettled([
          f.manager.streams.info(f.stream),
          f.manager.consumers.info(f.stream, 'resilient'),
        ])
        throw new Error(
          `Cluster recovery failed: ${JSON.stringify({
            fault,
            killed,
            timeline,
            events,
            deliveries: Object.fromEntries(received),
            runtime: runtime?.inspect(),
            processor: lease?.inspect(),
            stream,
            consumer,
          })}`,
          { cause }
        )
      } finally {
        try {
          if (killed && nodes.includes(killed)) {
            execFileSync('docker', ['compose', '-f', compose, 'start', killed], { stdio: 'pipe' })
            // Restoring the process is not enough: do not leak replica catch-up
            // or a new election into the next test.
            await Promise.all([
              waitForReplicas(() => f.manager.streams.info(f.stream)),
              waitForReplicas(() => f.manager.consumers.info(f.stream, 'resilient')),
            ])
          }
        } finally {
          await f.close(runtime)
          await observing
        }
      }
    },
    180_000
  )

  it('redelivers unfinished work after its worker process is killed', async () => {
    const f = await fixture()
    const runtime = createNatsRuntime({ connect: connection })
    const child = fork(
      fileURLToPath(new URL('../fixtures/cluster/crash-worker.mjs', import.meta.url)),
      [f.stream, f.subject, 'crash-worker'],
      { stdio: ['ignore', 'ignore', 'inherit', 'ipc'], execArgv: [] }
    )
    const exited = once(child, 'exit')
    try {
      const handling = Promise.race([
        once(child, 'message', { signal: AbortSignal.timeout(15_000) }),
        exited.then(([code, signal]) => {
          throw new Error(`Worker exited before handling: code=${code}, signal=${signal}`)
        }),
      ])
      const published = await f.publish('unfinished')
      expect((await handling)[0]).toBe('handling')
      child.kill('SIGKILL')
      await exited
      const attempts: number[] = []
      const lease = processJetStream(
        runtime,
        {
          stream: f.stream,
          filter: f.subject,
          consumer: { mode: 'bind', name: 'crash-worker' },
          start: 'all',
          codec: natsCodecs.text,
          ackWaitMs: 1_000,
          acknowledgement: { mode: 'confirmed' },
        },
        ({ value, deliveryAttempt }) => {
          expect(value).toBe('unfinished')
          attempts.push(deliveryAttempt)
        }
      )
      await lease.ready
      await expect.poll(() => attempts.length, { timeout: 15_000 }).toBeGreaterThan(0)
      expect(attempts[0]).toBeGreaterThanOrEqual(2)
      await expect.poll(() => lease.inspect().acknowledged.stream).toBe(published.seq)
    } finally {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL')
      await exited
      await f.close(runtime)
    }
  }, 40_000)
})
