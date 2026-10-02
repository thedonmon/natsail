import {
  context as otelContext,
  metrics,
  propagation,
  type Attributes,
  type Context,
  type Counter,
  type Gauge,
  type Histogram,
  type Meter,
  type TextMapGetter,
  type TextMapSetter,
} from '@opentelemetry/api'

import type { NatsailTelemetryEvent, NatsailTelemetrySink } from '@natsail/core'

export interface NatsailOpenTelemetryOptions {
  /** Uses the active global MeterProvider when omitted. */
  readonly meter?: Meter
  /** Meter identity used when `meter` is omitted. Defaults to `@natsail/opentelemetry`. */
  readonly meterName?: string
  readonly meterVersion?: string
}

function attributes(event: NatsailTelemetryEvent): Attributes | undefined {
  return event.attributes === undefined ? undefined : { ...event.attributes }
}

/**
 * Maps dependency-free NATSail measurements to OpenTelemetry counters, gauges,
 * and histograms. The application remains responsible for installing and
 * configuring an OpenTelemetry MeterProvider/exporter.
 */
export function createOpenTelemetrySink(
  options: NatsailOpenTelemetryOptions = {}
): NatsailTelemetrySink {
  const meter =
    options.meter ??
    metrics.getMeter(options.meterName ?? '@natsail/opentelemetry', options.meterVersion)
  const counters = new Map<string, Counter>()
  const gauges = new Map<string, Gauge>()
  const histograms = new Map<string, Histogram>()

  return Object.freeze({
    record(event: NatsailTelemetryEvent): void {
      const eventAttributes = attributes(event)
      switch (event.type) {
        case 'counter': {
          let counter = counters.get(event.name)
          if (!counter) {
            counter = meter.createCounter(event.name)
            counters.set(event.name, counter)
          }
          counter.add(event.value, eventAttributes)
          return
        }
        case 'gauge': {
          let gauge = gauges.get(event.name)
          if (!gauge) {
            gauge = meter.createGauge(event.name)
            gauges.set(event.name, gauge)
          }
          gauge.record(event.value, eventAttributes)
          return
        }
        case 'duration': {
          let histogram = histograms.get(event.name)
          if (!histogram) {
            histogram = meter.createHistogram(event.name, { unit: 'ms' })
            histograms.set(event.name, histogram)
          }
          histogram.record(event.durationMs, eventAttributes)
        }
      }
    },
  })
}

/** The subset of nats.js `MsgHdrs` used for propagation; the real type satisfies it. */
export interface NatsHeaderCarrier {
  get(key: string): string
  set(key: string, value: string): void
  keys(): string[]
}

const setter: TextMapSetter<NatsHeaderCarrier> = {
  set: (carrier, key, value) => carrier.set(key, value),
}

const getter: TextMapGetter<NatsHeaderCarrier> = {
  get: (carrier, key) => carrier.get(key) || undefined,
  keys: (carrier) => carrier.keys(),
}

/**
 * Writes the trace context into NATS message headers with the globally registered
 * propagator and returns the same headers. Pass `headers()` from nats.js to start fresh.
 */
export function injectTraceContext<H extends NatsHeaderCarrier>(
  headers: H,
  context: Context = otelContext.active()
): H {
  propagation.inject(context, headers, setter)
  return headers
}

/** Reads the trace context from NATS message headers; returns `context` unchanged without any. */
export function extractTraceContext(
  headers: NatsHeaderCarrier | undefined,
  context: Context = otelContext.active()
): Context {
  return headers === undefined ? context : propagation.extract(context, headers, getter)
}
