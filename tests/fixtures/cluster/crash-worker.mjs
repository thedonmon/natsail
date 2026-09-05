import { connect } from '@nats-io/transport-node'
import { createNatsRuntime, natsCodecs } from '../../../packages/core/dist/index.js'
import { processJetStream } from '../../../packages/jetstream/dist/index.js'

const [stream, subject, consumer] = process.argv.slice(2)
const runtime = createNatsRuntime({
  connect: () =>
    connect({
      servers: ['127.0.0.1:14223', '127.0.0.1:14224', '127.0.0.1:14225'],
      ignoreClusterUpdates: true,
    }),
})
const lease = processJetStream(
  runtime,
  {
    stream,
    filter: subject,
    consumer: { mode: 'ensure', name: consumer },
    start: 'all',
    codec: natsCodecs.text,
    ackWaitMs: 1_000,
    maxBufferedMessages: 1,
  },
  async () => {
    process.send?.('handling')
    await new Promise(() => undefined)
  }
)
await lease.ready
await lease.closed
