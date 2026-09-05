import { execFileSync, fork } from 'node:child_process'
import { once } from 'node:events'
import { fileURLToPath } from 'node:url'
import { connect } from '@nats-io/transport-node'
import { jetstream, jetstreamManager, StorageType } from '@nats-io/jetstream'
import { describe, expect, it } from 'vitest'
import { createNatsRuntime, natsCodecs } from '@natsail/core'
import { processJetStream } from '@natsail/jetstream'

const servers = ['127.0.0.1:14223', '127.0.0.1:14224', '127.0.0.1:14225']
const compose = fileURLToPath(new URL('../fixtures/cluster/compose.yml', import.meta.url))
const connection = () =>
  connect({
    servers,
    ignoreClusterUpdates: true,
    timeout: 500,
    reconnectTimeWait: 100,
    maxReconnectAttempts: 100,
  })

async function fixture() {
  let connected: Awaited<ReturnType<typeof connection>> | undefined
  await expect
    .poll(
      async () => {
        try {
          connected = await connection()
          return true
        } catch {
          return false
        }
      },
      { timeout: 20_000 }
    )
    .toBe(true)
  const admin = connected!
  const manager = await jetstreamManager(admin)
  const stream = `RESILIENCE_${crypto.randomUUID().replaceAll('-', '_')}`
  const subject = `tests.${stream}`
  await expect
    .poll(
      async () => {
        try {
          await manager.streams.add({
            name: stream,
            subjects: [subject],
            storage: StorageType.File,
            num_replicas: 3,
          })
          return true
        } catch {
          return false
        }
      },
      { timeout: 20_000 }
    )
    .toBe(true)
  const runtime = createNatsRuntime({ connect: connection })
  return {
    admin,
    manager,
    stream,
    subject,
    runtime,
    client: jetstream(admin),
    async close() {
      try {
        await runtime.close()
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
  it('continues acknowledged processing after the consumer leader is killed', async () => {
    const f = await fixture()
    const received = new Set<string>()
    let killed: string | undefined
    try {
      const lease = processJetStream(
        f.runtime,
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
          received.add(value)
        }
      )
      await lease.ready
      await f.client.publish(f.subject, 'before')
      await expect.poll(() => lease.inspect().acknowledged.stream).toBe(1)
      killed = (await f.manager.consumers.info(f.stream, 'resilient')).cluster?.leader
      expect(['nats1', 'nats2', 'nats3']).toContain(killed)
      execFileSync('docker', ['compose', '-f', compose, 'kill', '-s', 'SIGKILL', killed!], {
        stdio: 'pipe',
      })
      await expect
        .poll(
          async () => {
            try {
              const leader = (await f.manager.consumers.info(f.stream, 'resilient')).cluster?.leader
              return leader !== undefined && leader !== killed
            } catch {
              return false
            }
          },
          { timeout: 20_000 }
        )
        .toBe(true)
      await f.client.publish(f.subject, 'after')
      try {
        await expect.poll(() => received.has('after'), { timeout: 20_000 }).toBe(true)
      } catch (cause) {
        throw new Error(
          `Failover stalled: ${JSON.stringify({ runtime: f.runtime.inspect(), processor: lease.inspect(), consumer: await f.manager.consumers.info(f.stream, 'resilient') })}`,
          { cause }
        )
      }
      await expect.poll(() => lease.inspect().acknowledged.stream, { timeout: 20_000 }).toBe(2)
      expect(received).toEqual(new Set(['before', 'after']))
    } finally {
      if (killed && ['nats1', 'nats2', 'nats3'].includes(killed))
        execFileSync('docker', ['compose', '-f', compose, 'start', killed], { stdio: 'pipe' })
      await f.close()
    }
  }, 60_000)

  it('redelivers unfinished work after its worker process is killed', async () => {
    const f = await fixture()
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
      await f.client.publish(f.subject, 'unfinished')
      expect((await handling)[0]).toBe('handling')
      child.kill('SIGKILL')
      await exited
      const attempts: number[] = []
      const lease = processJetStream(
        f.runtime,
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
      await expect.poll(() => lease.inspect().acknowledged.stream).toBe(1)
    } finally {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL')
      await exited
      await f.close()
    }
  }, 40_000)
})
