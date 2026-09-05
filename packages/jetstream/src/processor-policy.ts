import type { JsMsg } from '@nats-io/jetstream'

export type JetStreamProcessorDisposition =
  | { readonly action: 'retry'; readonly delayMs: number }
  | { readonly action: 'term'; readonly reason?: string }

export function validateProcessorDisposition(value: void | JetStreamProcessorDisposition): void {
  if (value === undefined) return
  if (value !== null && value.action === 'term') {
    if (value.reason !== undefined && typeof value.reason !== 'string')
      throw new TypeError('JetStream terminal reason must be a string')
    return
  }
  if (value === null || value.action !== 'retry')
    throw new TypeError('JetStream handler must return void, retry, or term')
  if (!Number.isSafeInteger(value.delayMs) || value.delayMs < 0 || value.delayMs > 2_147_483_647)
    throw new RangeError('JetStream retry delayMs must be a non-negative timer-safe integer')
}

export type JetStreamAcknowledgementPolicy =
  | { readonly mode: 'sent' }
  | { readonly mode: 'confirmed'; readonly timeoutMs?: number }

export class JetStreamAcknowledgementError extends Error {
  readonly name = 'JetStreamAcknowledgementError'

  constructor() {
    super('JetStream did not confirm the acknowledgement; processing may be redelivered')
  }
}

export function validateAcknowledgementPolicy(policy?: JetStreamAcknowledgementPolicy): void {
  if (policy === undefined) return
  if (policy.mode !== 'sent' && policy.mode !== 'confirmed') {
    throw new TypeError('JetStream acknowledgement mode must be sent or confirmed')
  }
  if (
    policy.mode === 'confirmed' &&
    policy.timeoutMs !== undefined &&
    (!Number.isSafeInteger(policy.timeoutMs) ||
      policy.timeoutMs <= 0 ||
      policy.timeoutMs > 2_147_483_647)
  ) {
    throw new RangeError(
      'JetStream acknowledgement timeoutMs must be a positive timer-safe integer'
    )
  }
}

export async function acknowledgeProcessorMessage(
  message: JsMsg,
  policy?: JetStreamAcknowledgementPolicy
): Promise<void> {
  if (policy?.mode === 'confirmed') {
    const accepted = await message.ackAck({ timeout: policy.timeoutMs ?? 5_000 })
    if (!accepted) throw new JetStreamAcknowledgementError()
  } else {
    message.ack()
  }
}

export function validateProgressInterval(intervalMs?: number, ackWaitMs?: number): void {
  if (intervalMs === undefined) return
  if (!Number.isSafeInteger(intervalMs) || intervalMs <= 0 || intervalMs > 2_147_483_647) {
    throw new RangeError('JetStream progressIntervalMs must be a positive timer-safe integer')
  }
  if (ackWaitMs !== undefined && intervalMs >= ackWaitMs) {
    throw new RangeError(
      'JetStream progressIntervalMs must be shorter than the effective acknowledgement wait'
    )
  }
}

export function startProcessorHeartbeat(
  message: JsMsg,
  intervalMs: number | undefined,
  cancellation: AbortController
): () => void {
  if (intervalMs === undefined || cancellation.signal.aborted) return () => undefined
  const stop = () => {
    clearInterval(timer)
    cancellation.signal.removeEventListener('abort', stop)
  }
  const timer = setInterval(() => {
    try {
      message.working()
    } catch (error) {
      cancellation.abort(error)
      stop()
    }
  }, intervalMs)
  cancellation.signal.addEventListener('abort', stop, { once: true })
  return stop
}
