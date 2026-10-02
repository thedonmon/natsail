import { headers } from '@nats-io/nats-core'
import {
  jetstream,
  jetstreamManager,
  StorageType,
  type JetStreamManager,
} from '@nats-io/jetstream'
import { afterEach, describe, expect, it } from 'vitest'

import { createNatsRuntime, natsCodecs, type NatsPayloadCodec } from '@natsail/core'
import {
  consumeJetStream,
  createReducingJetStreamSessionSource,
  JetStreamDecodeError,
  processJetStream,
  type JetStreamProcessorDecodeFailure,
  type JetStreamProcessorDisposition,
} from '@natsail/jetstream'

import { connectToTestNats, uniqueSubject } from './helpers.js'

const id = () => crypto.randomUUID().replaceAll('-', '_')

describe('JetStream failure handling and delivery headers', () => {
  const cleanups: Array<() => Promise<void>> = []

  afterEach(async () => {
    await Promise.allSettled(cleanups.splice(0).map((cleanup) => cleanup()))
  })

  async function fixture(prefix: string, stream = `FAIL_${id().toUpperCase()}`) {
    const admin = await connectToTestNats()
    const runtimeConnection = await connectToTestNats()
    const manager = await jetstreamManager(admin)
    const client = jetstream(admin)
    const subject = uniqueSubject(prefix)
    await manager.streams.add({ name: stream, subjects: [subject], storage: StorageType.Memory })
    const runtime = createNatsRuntime({ connect: async () => runtimeConnection })
    cleanups.push(async () => {
      await runtime.close()
      await manager.streams.delete(stream).catch(() => undefined)
      await admin.drain()
    })
    return { admin, manager, client, runtime, stream, subject, consumer: `processor_${id()}` }
  }

  // Throws for payloads that start with "bad", until `recover()` is called.
  function pickyCodec(): NatsPayloadCodec<string> & { recover(): void } {
    let healthy = false
    return {
      encode: natsCodecs.text.encode,
      decode(data) {
        const text = natsCodecs.text.decode(data)
        if (text.startsWith('bad') && !healthy) throw new Error(`malformed: ${text}`)
        return text
      },
      recover() {
        healthy = true
      },
    }
  }

  async function ackFloor(manager: JetStreamManager, stream: string, consumer: string) {
    return (await manager.consumers.info(stream, consumer)).ack_floor.stream_seq
  }

  describe('onDecodeFailure', () => {
    it('terminates an undecodable message and keeps processing the next one', async () => {
      const { manager, client, runtime, stream, subject, consumer } = await fixture('decode-term')
      await client.publish(subject, 'bad-1')
      await client.publish(subject, 'good-2')
      const failures: JetStreamProcessorDecodeFailure[] = []
      const handled: string[] = []

      const lease = processJetStream(
        runtime,
        {
          stream,
          consumer: { mode: 'ensure', name: consumer },
          filter: subject,
          start: 'all',
          codec: pickyCodec(),
          onDecodeFailure: (failure) => {
            failures.push(failure)
            return { action: 'term', reason: 'malformed' }
          },
        },
        (delivery) => {
          handled.push(delivery.value)
        }
      )
      cleanups.push(() => lease.close())

      await expect.poll(() => handled).toEqual(['good-2'])
      await expect.poll(() => ackFloor(manager, stream, consumer)).toBe(2)
      expect(lease.inspect()).toMatchObject({ phase: 'live' })
      expect(lease.inspect()).not.toHaveProperty('handlerFailure')
      expect(failures).toHaveLength(1)
      expect(failures[0]).toMatchObject({
        subject,
        deliveryAttempt: 1,
        cursor: { stream, sequence: 1 },
      })
      expect(failures[0]!.error).toEqual(new Error('malformed: bad-1'))
      expect(natsCodecs.text.decode(failures[0]!.data)).toBe('bad-1')
    })

    it('redelivers a retried message and decodes it once the payload becomes readable', async () => {
      const { client, runtime, stream, subject, consumer } = await fixture('decode-retry')
      await client.publish(subject, 'bad-1')
      const codec = pickyCodec()
      const attempts: number[] = []
      const handled: Array<{ value: string; attempt: number }> = []

      const lease = processJetStream(
        runtime,
        {
          stream,
          consumer: { mode: 'ensure', name: consumer },
          filter: subject,
          start: 'all',
          codec,
          onDecodeFailure: (failure): JetStreamProcessorDisposition => {
            attempts.push(failure.deliveryAttempt)
            if (attempts.length === 2) codec.recover()
            return { action: 'retry', delayMs: 20 }
          },
        },
        (delivery) => {
          handled.push({ value: delivery.value, attempt: delivery.deliveryAttempt })
        }
      )
      cleanups.push(() => lease.close())

      await expect.poll(() => handled).toEqual([{ value: 'bad-1', attempt: 3 }])
      expect(attempts).toEqual([1, 2])
    })

    it('stops the processor on an undecodable message when no hook is set', async () => {
      const { manager, client, runtime, stream, subject, consumer } = await fixture('decode-stop')
      await client.publish(subject, 'bad-1')

      const lease = processJetStream(
        runtime,
        {
          stream,
          consumer: { mode: 'ensure', name: consumer },
          filter: subject,
          start: 'all',
          codec: pickyCodec(),
        },
        () => undefined
      )
      cleanups.push(() => lease.close().catch(() => undefined))

      const error = (await lease.closed.catch((reason: unknown) => reason)) as JetStreamDecodeError
      expect(error).toBeInstanceOf(JetStreamDecodeError)
      expect(error).toMatchObject({ subject, deliveryAttempt: 1, cursor: { stream, sequence: 1 } })
      expect(error.cause).toEqual(new Error('malformed: bad-1'))
      expect(await ackFloor(manager, stream, consumer)).toBe(0)
    })
  })

  describe('ordered consumer decode failures', () => {
    it('ends a recovering session with a typed error instead of reopening the consumer', async () => {
      const { admin, client, runtime, stream, subject } = await fixture('decode-ordered')
      await client.publish(subject, 'ok-1')
      await client.publish(subject, 'bad-2')
      await client.publish(subject, 'ok-3')
      const consumerCreates: string[] = []
      const watcher = admin.subscribe(`$JS.API.CONSUMER.CREATE.${stream}.>`, {
        callback: (_error, message) => {
          consumerCreates.push(message.subject)
        },
      })
      cleanups.push(async () => watcher.unsubscribe())
      let decodeCalls = 0
      const codec = pickyCodec()
      let latest: { restarts: number } | undefined

      const lease = createReducingJetStreamSessionSource(
        runtime,
        {
          stream,
          filter: subject,
          start: 'all',
          recovery: { delayMs: 10, maxAttempts: 5 },
          decode: (message) => {
            decodeCalls += 1
            return codec.decode(message.data)
          },
        },
        { scope: 'count:v1', initial: () => 0, reduce: (count) => count + 1 }
      )(async (snapshot) => {
        latest = snapshot
      })
      cleanups.push(() => lease.close().catch(() => undefined))

      const error = (await lease.closed.catch((reason: unknown) => reason)) as JetStreamDecodeError
      expect(error).toBeInstanceOf(JetStreamDecodeError)
      expect(error).toMatchObject({ subject, cursor: { stream, sequence: 2 } })
      expect(error.cause).toEqual(new Error('malformed: bad-2'))
      expect(error.message).not.toContain('bad-2')
      expect(error.message).toContain('sequence 2')

      await new Promise((resolve) => setTimeout(resolve, 300))
      expect(decodeCalls).toBe(2)
      expect(consumerCreates).toHaveLength(1)
      expect(latest?.restarts ?? 0).toBe(0)
    })
  })

  describe('delivery headers', () => {
    const traceparent = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'

    function publishWithHeaders(client: ReturnType<typeof jetstream>, subject: string) {
      const hdrs = headers()
      hdrs.set('traceparent', traceparent)
      return client.publish(subject, 'with-headers', { headers: hdrs })
    }

    it('exposes message headers to processor handlers and decode-failure hooks', async () => {
      const { client, runtime, stream, subject, consumer } = await fixture('headers-processor')
      await publishWithHeaders(client, subject)
      await client.publish(subject, 'bad-plain')
      const seen: Array<string | undefined> = []
      const failureHeaders: Array<unknown> = []

      const lease = processJetStream(
        runtime,
        {
          stream,
          consumer: { mode: 'ensure', name: consumer },
          filter: subject,
          start: 'all',
          codec: pickyCodec(),
          onDecodeFailure: (failure) => {
            failureHeaders.push(failure.headers)
            return { action: 'term' }
          },
        },
        (delivery) => {
          seen.push(delivery.headers?.get('traceparent'))
        }
      )
      cleanups.push(() => lease.close())

      await expect.poll(() => seen).toEqual([traceparent])
      await expect.poll(() => failureHeaders).toEqual([undefined])
    })

    it('exposes message headers on ordered consumer deliveries', async () => {
      const { client, runtime, stream, subject } = await fixture('headers-consume')
      await publishWithHeaders(client, subject)
      await client.publish(subject, 'plain')
      const seen: Array<string | undefined> = []

      const lease = consumeJetStream(
        runtime,
        { stream, filter: subject, start: 'all', codec: natsCodecs.text },
        (delivery) => {
          seen.push(delivery.headers?.get('traceparent'))
        }
      )
      cleanups.push(() => lease.close())

      await expect.poll(() => seen).toEqual([traceparent, undefined])
    })
  })

  describe('dead-letter recipes', () => {
    it('publishes to a dead-letter subject before terminating on the last attempt', async () => {
      const { manager, client, runtime, stream, subject, consumer } = await fixture('dlq-handler')
      const dlqStream = `DLQ_${id().toUpperCase()}`
      const dlqSubject = uniqueSubject('dlq')
      await manager.streams.add({
        name: dlqStream,
        subjects: [dlqSubject],
        storage: StorageType.Memory,
      })
      cleanups.push(async () => {
        await manager.streams.delete(dlqStream)
      })
      await client.publish(subject, 'poison')
      const maxAttempts = 3

      const lease = processJetStream(
        runtime,
        {
          stream,
          consumer: { mode: 'ensure', name: consumer },
          filter: subject,
          start: 'all',
          maxDeliver: maxAttempts,
          codec: natsCodecs.text,
        },
        async (delivery): Promise<JetStreamProcessorDisposition> => {
          if (delivery.deliveryAttempt < maxAttempts) return { action: 'retry', delayMs: 20 }
          const hdrs = headers()
          hdrs.set('x-original-sequence', String(delivery.cursor.sequence))
          await client.publish(dlqSubject, delivery.value, { headers: hdrs })
          return { action: 'term', reason: 'exhausted' }
        }
      )
      cleanups.push(() => lease.close())

      await expect.poll(() => ackFloor(manager, stream, consumer)).toBe(1)
      const dead = await manager.streams.getMessage(dlqStream, { seq: 1 })
      expect(natsCodecs.text.decode(dead!.data)).toBe('poison')
      expect(dead!.header.get('x-original-sequence')).toBe('1')
    })
  })
})
