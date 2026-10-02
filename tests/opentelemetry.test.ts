import { headers } from '@nats-io/nats-core'
import { propagation, ROOT_CONTEXT, trace, type Meter } from '@opentelemetry/api'
import { W3CTraceContextPropagator } from '@opentelemetry/core'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  createOpenTelemetrySink,
  extractTraceContext,
  injectTraceContext,
} from '@natsail/opentelemetry'

describe('OpenTelemetry sink', () => {
  it('maps the dependency-free event kinds to cached OpenTelemetry instruments', () => {
    const add = vi.fn()
    const recordGauge = vi.fn()
    const recordHistogram = vi.fn()
    const createCounter = vi.fn(() => ({ add }))
    const createGauge = vi.fn(() => ({ record: recordGauge }))
    const createHistogram = vi.fn(() => ({ record: recordHistogram }))
    const meter = { createCounter, createGauge, createHistogram } as unknown as Meter
    const sink = createOpenTelemetrySink({ meter })

    sink.record({
      type: 'counter',
      name: 'natsail.connection.attempts',
      value: 1,
      at: 10,
      attributes: { outcome: 'success' },
    })
    sink.record({
      type: 'counter',
      name: 'natsail.connection.attempts',
      value: 1,
      at: 11,
      attributes: { outcome: 'failure' },
    })
    sink.record({
      type: 'gauge',
      name: 'natsail.runtime.resources.active',
      value: 2,
      at: 12,
    })
    sink.record({
      type: 'duration',
      name: 'natsail.core.request.duration',
      durationMs: 4.5,
      at: 13,
      attributes: { outcome: 'success' },
    })

    expect(createCounter).toHaveBeenCalledOnce()
    expect(add).toHaveBeenNthCalledWith(1, 1, { outcome: 'success' })
    expect(add).toHaveBeenNthCalledWith(2, 1, { outcome: 'failure' })
    expect(createGauge).toHaveBeenCalledWith('natsail.runtime.resources.active')
    expect(recordGauge).toHaveBeenCalledWith(2, undefined)
    expect(createHistogram).toHaveBeenCalledWith('natsail.core.request.duration', { unit: 'ms' })
    expect(recordHistogram).toHaveBeenCalledWith(4.5, { outcome: 'success' })
  })
})

describe('trace context propagation', () => {
  const spanContext = {
    traceId: '4bf92f3577b34da6a3ce929d0e0e4736',
    spanId: '00f067aa0ba902b7',
    traceFlags: 1,
  }

  beforeEach(() => {
    propagation.setGlobalPropagator(new W3CTraceContextPropagator())
  })

  afterEach(() => {
    propagation.disable()
  })

  it('carries a span context through NATS headers and back', () => {
    const hdrs = injectTraceContext(headers(), trace.setSpanContext(ROOT_CONTEXT, spanContext))

    expect(hdrs.get('traceparent')).toBe(`00-${spanContext.traceId}-${spanContext.spanId}-01`)
    expect(trace.getSpanContext(extractTraceContext(hdrs))).toMatchObject({
      ...spanContext,
      isRemote: true,
    })
  })

  it('leaves the context unchanged when the message has no headers or no trace', () => {
    const context = trace.setSpanContext(ROOT_CONTEXT, spanContext)

    expect(extractTraceContext(undefined, context)).toBe(context)
    expect(trace.getSpanContext(extractTraceContext(headers(), ROOT_CONTEXT))).toBeUndefined()
  })
})
