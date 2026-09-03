import {
  metrics,
  type Attributes,
  type Counter,
  type Gauge,
  type Histogram,
  type Meter,
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
