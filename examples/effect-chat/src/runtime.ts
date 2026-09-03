import { wsconnect } from '@nats-io/nats-core'
import { createNatsRuntime, type NatsailTelemetryEvent } from '@natsail/core'
import { makeNatsail } from '@natsail/effect'
import { createSessionRegistry } from '@natsail/session'

interface PerformanceTelemetrySnapshot {
  readonly telemetryMeasurements: number
  readonly replayTelemetryMs?: number
  readonly lastHandlerTelemetryMs?: number
  readonly averagePublishTelemetryMs?: number
  readonly bufferSignals: number
}

let telemetryMeasurements = 0
let replayTelemetryMs: number | undefined
let lastHandlerTelemetryMs: number | undefined
let publishDurationTotal = 0
let publishDurationCount = 0
let bufferSignals = 0

const telemetry = {
  record(event: NatsailTelemetryEvent): void {
    telemetryMeasurements += 1
    if (event.type === 'duration' && event.name === 'natsail.jetstream.replay.duration') {
      replayTelemetryMs = event.durationMs
    } else if (event.type === 'duration' && event.name === 'natsail.jetstream.handler.duration') {
      lastHandlerTelemetryMs = event.durationMs
    } else if (event.type === 'duration' && event.name === 'natsail.core.publish.duration') {
      publishDurationTotal += event.durationMs
      publishDurationCount += 1
    } else if (event.type === 'counter' && event.name === 'natsail.buffer.signals') {
      bufferSignals += event.value
    }
  },
}

export const readPerformanceTelemetry = (): PerformanceTelemetrySnapshot => ({
  telemetryMeasurements,
  ...(replayTelemetryMs === undefined ? {} : { replayTelemetryMs }),
  ...(lastHandlerTelemetryMs === undefined ? {} : { lastHandlerTelemetryMs }),
  ...(publishDurationCount === 0
    ? {}
    : { averagePublishTelemetryMs: publishDurationTotal / publishDurationCount }),
  bufferSignals,
})

export const resetPerformanceTelemetry = (): void => {
  telemetryMeasurements = 0
  replayTelemetryMs = undefined
  lastHandlerTelemetryMs = undefined
  publishDurationTotal = 0
  publishDurationCount = 0
  bufferSignals = 0
}

export const runtime = createNatsRuntime({
  connect: () => wsconnect({ servers: 'ws://127.0.0.1:9223', timeout: 2_000 }),
  initialConnectRetry: {
    maxAttempts: 3,
    delayMs: 250,
  },
  telemetry,
  telemetryAttributes: { example: 'effect-chat' },
})

export const sessions = createSessionRegistry({ idleCloseMs: 0, telemetry })
export const natsail = makeNatsail({ runtime, sessions })

let closePromise: Promise<void> | undefined

export const closeExampleRuntime = (): Promise<void> => {
  closePromise ??= Promise.allSettled([sessions.close(), runtime.close()]).then(() => undefined)
  return closePromise
}
