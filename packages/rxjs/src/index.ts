import { ColdObservable } from 'rxjs'
import { distinctUntilChanged } from 'rxjs/distinct-until-changed'
import { filter } from 'rxjs/filter'

import type {
  CoreSubscriptionOptions,
  NatsailBatchPolicy,
  NatsRuntime,
  NatsRuntimeEvent,
  NatsRuntimeStatusEvent,
  NatsailScheduledTask,
  NatsailScheduler,
} from '@natsail/core'
import { defineNatsailBatchPolicy, natsailDefaultScheduler } from '@natsail/core'
import {
  createJetStreamSessionSource,
  type JetStreamDelivery,
  type JetStreamSessionSourceOptions,
  type JetStreamStateSnapshot,
} from '@natsail/jetstream'
import { createCoreSessionSource } from '@natsail/session'
import type {
  SessionDefinition,
  SessionRegistryEvent,
  SessionRegistry,
  SessionSnapshot,
  SessionSource,
} from '@natsail/session'

/** Converts registry lifecycle and reference-count diagnostics into a cancellable Observable. */
export function observeNatsSessionEvents(
  registry: SessionRegistry
): ColdObservable<SessionRegistryEvent> {
  return new ColdObservable((subscriber) => {
    if (!subscriber.active) return
    const iterator = registry.events[Symbol.asyncIterator]()
    let cancelled = false
    subscriber.addTeardown(() => {
      cancelled = true
      void iterator.return?.()
    })

    void (async () => {
      try {
        while (!cancelled) {
          const next = await iterator.next()
          if (next.done) {
            if (!cancelled) subscriber.complete()
            return
          }
          subscriber.next(next.value)
        }
      } catch (error) {
        if (!cancelled) subscriber.error(error)
      } finally {
        await iterator.return?.()
      }
    })()
  })
}

/** Converts the runtime event iterable into a cancellable Observable. */
export function observeNatsRuntimeEvents(runtime: NatsRuntime): ColdObservable<NatsRuntimeEvent> {
  return new ColdObservable((subscriber) => {
    if (!subscriber.active) return
    const iterator = runtime.events[Symbol.asyncIterator]()
    let cancelled = false
    subscriber.addTeardown(() => {
      cancelled = true
      void iterator.return?.()
    })

    void (async () => {
      try {
        while (!cancelled) {
          const next = await iterator.next()
          if (next.done) {
            if (!cancelled) subscriber.complete()
            return
          }
          subscriber.next(next.value)
        }
      } catch (error) {
        if (!cancelled) subscriber.error(error)
      } finally {
        await iterator.return?.()
      }
    })()
  })
}

/** Emits distinct runtime connection states and omits diagnostic events. */
export function observeNatsRuntimeStatus(runtime: NatsRuntime): Observable<NatsRuntimeStatusEvent> {
  return observeNatsRuntimeEvents(runtime)
    [filter]((event): event is NatsRuntimeStatusEvent => event.type === 'status')
    [distinctUntilChanged](
      (previous, next) => previous.state === next.state && previous.server === next.server
    )
}

/** Emits values from one keyed, registry-shared Core NATS subscription. */
export function observeNatsCoreSubscription<T>(
  registry: SessionRegistry,
  runtime: NatsRuntime,
  key: string,
  options: CoreSubscriptionOptions<T>
): ColdObservable<T> {
  return observeNatsSessionValues(registry, key, createCoreSessionSource(runtime, options))
}

/** Emits deliveries from one registry-shared checkpointed JetStream session. */
export function observeNatsJetStreamSubscription<T>(
  registry: SessionRegistry,
  runtime: NatsRuntime,
  key: string,
  options: JetStreamSessionSourceOptions<T>
): ColdObservable<JetStreamDelivery<T>> {
  return observeNatsSessionValues(registry, key, createJetStreamSessionSource(runtime, options))
}

/** Emits one atomic replay/live reduced state from a validated shared definition. */
export function observeNatsJetStreamReducer<State>(
  registry: SessionRegistry,
  definition: SessionDefinition<JetStreamStateSnapshot<State>>
): ColdObservable<SessionSnapshot<JetStreamStateSnapshot<State>>> {
  return observeNatsSession(registry, definition)
}

export interface NatsailJetStreamStateOptions<State = unknown> {
  /**
   * Maximum time that subsequent cumulative live states are coalesced. The
   * initial replaying and hydrated live states remain immediate. Defaults to
   * 16ms; use 0 to observe every reduced live state.
   */
  readonly liveBatchMs?: number
  /** Shared count/byte/time bounds for cumulative live presentation. */
  readonly batchPolicy?: NatsailBatchPolicy<JetStreamStateSnapshot<State>>
  /** Overrides host timers using NATSail's cancellable task contract. */
  readonly scheduler?: Pick<NatsailScheduler, 'schedule'>
}

/**
 * Emits cumulative reduced JetStream state without duplicate session-lifecycle
 * notifications. Replay and recovery phase changes are immediate; subsequent
 * live states are coalesced to one latest value per bounded render window.
 */
export function observeNatsJetStreamState<State>(
  registry: SessionRegistry,
  definition: SessionDefinition<JetStreamStateSnapshot<State>>,
  options: NatsailJetStreamStateOptions<State> = {}
): ColdObservable<JetStreamStateSnapshot<State>> {
  const liveBatchMs = options.liveBatchMs ?? options.batchPolicy?.maxWaitMs ?? 16
  if (!Number.isFinite(liveBatchMs) || liveBatchMs < 0) {
    throw new TypeError('NATSail RxJS liveBatchMs must be a finite non-negative number')
  }

  const policy = defineNatsailBatchPolicy<JetStreamStateSnapshot<State>>(
    options.liveBatchMs === 0
      ? { maxItems: 1 }
      : {
          ...options.batchPolicy,
          ...(options.liveBatchMs === undefined ? {} : { maxWaitMs: options.liveBatchMs }),
          ...(options.batchPolicy === undefined && options.liveBatchMs === undefined
            ? { maxWaitMs: 16 }
            : {}),
        }
  )
  const values = observeNatsSessionValues(registry, definition)
  if (liveBatchMs === 0) return values
  const scheduler = options.scheduler ?? natsailDefaultScheduler

  return new ColdObservable((subscriber) => {
    if (!subscriber.active) return
    let seenLive = false
    let pendingLive: JetStreamStateSnapshot<State> | undefined
    let pendingCount = 0
    let pendingBytes = 0
    let scheduledFlush: NatsailScheduledTask | undefined

    const flush = () => {
      scheduledFlush = undefined
      if (pendingLive === undefined) return
      const value = pendingLive
      pendingLive = undefined
      pendingCount = 0
      pendingBytes = 0
      subscriber.next(value)
    }
    const cancelFlush = () => {
      scheduledFlush?.cancel()
      scheduledFlush = undefined
    }
    const scheduleFlush = () => {
      if (scheduledFlush !== undefined) return
      let ranSynchronously = false
      const scheduled = scheduler.schedule(() => {
        ranSynchronously = true
        flush()
      }, policy.maxWaitMs ?? 0)
      if (!ranSynchronously) scheduledFlush = scheduled
    }
    subscriber.addTeardown(() => {
      cancelFlush()
      pendingLive = undefined
    })
    values.subscribe(
      {
        next: (value) => {
          if (value.phase !== 'live') {
            cancelFlush()
            flush()
            seenLive = false
            subscriber.next(value)
            return
          }
          if (!seenLive) {
            seenLive = true
            subscriber.next(value)
            return
          }

          let size = 0
          if (policy.maxBytes !== undefined) {
            try {
              size = policy.sizeOf!(value)
            } catch (error) {
              cancelFlush()
              subscriber.error(error)
              return
            }
            if (!Number.isFinite(size) || size < 0) {
              cancelFlush()
              subscriber.error(
                new TypeError('NATSail batch sizeOf must return a finite non-negative number')
              )
              return
            }
            if (size > policy.maxBytes) {
              cancelFlush()
              subscriber.error(
                new RangeError(
                  `NATSail live state size ${size} exceeds maxBytes ${policy.maxBytes}`
                )
              )
              return
            }
            if (pendingLive !== undefined && pendingBytes + size > policy.maxBytes) flush()
          }

          pendingLive = value
          pendingCount += 1
          pendingBytes += size
          const countReached = policy.maxItems !== undefined && pendingCount >= policy.maxItems
          const bytesReached = policy.maxBytes !== undefined && pendingBytes >= policy.maxBytes
          if (countReached || bytesReached) {
            cancelFlush()
            flush()
          } else if (policy.maxWaitMs !== undefined) {
            scheduleFlush()
          }
        },
        error: (error) => {
          cancelFlush()
          pendingLive = undefined
          pendingCount = 0
          pendingBytes = 0
          subscriber.error(error)
        },
        complete: () => {
          cancelFlush()
          flush()
          subscriber.complete()
        },
      },
      { signal: subscriber.signal }
    )
  })
}

/**
 * Creates a cold Observable over one keyed logical session.
 *
 * Every Observable subscriber acquires a registry handle. Subscribers with the
 * same registry and key share the underlying NATS source.
 */
export function observeNatsSession<T>(
  registry: SessionRegistry,
  definition: SessionDefinition<T>
): ColdObservable<SessionSnapshot<T>>
export function observeNatsSession<T>(
  registry: SessionRegistry,
  key: string,
  source: SessionSource<T>
): ColdObservable<SessionSnapshot<T>>
export function observeNatsSession<T>(
  registry: SessionRegistry,
  definitionOrKey: SessionDefinition<T> | string,
  source?: SessionSource<T>
): ColdObservable<SessionSnapshot<T>> {
  return new ColdObservable((subscriber) => {
    if (!subscriber.active) return
    const handle =
      typeof definitionOrKey === 'string'
        ? registry.acquire(definitionOrKey, source!)
        : registry.acquire(definitionOrKey)
    const emit = () => {
      const snapshot = handle.getSnapshot()
      subscriber.next(snapshot)
      if (snapshot.phase === 'closed' || snapshot.phase === 'error') {
        subscriber.complete()
      }
    }
    const unsubscribe = handle.subscribe(emit)
    subscriber.addTeardown(() => {
      unsubscribe()
      void handle.release().catch(() => undefined)
    })
    emit()
  })
}

/**
 * Emits delivered values, errors on a failed session, and completes on close.
 *
 * A new subscriber receives the latest value once when one exists. Equal
 * consecutive values remain distinct deliveries.
 */
export function observeNatsSessionValues<T>(
  registry: SessionRegistry,
  definition: SessionDefinition<T>
): ColdObservable<T>
export function observeNatsSessionValues<T>(
  registry: SessionRegistry,
  key: string,
  source: SessionSource<T>
): ColdObservable<T>
export function observeNatsSessionValues<T>(
  registry: SessionRegistry,
  definitionOrKey: SessionDefinition<T> | string,
  source?: SessionSource<T>
): ColdObservable<T> {
  return new ColdObservable((subscriber) => {
    if (!subscriber.active) return
    let valueRevision = -1
    const snapshots =
      typeof definitionOrKey === 'string'
        ? observeNatsSession(registry, definitionOrKey, source!)
        : observeNatsSession(registry, definitionOrKey)
    snapshots.subscribe(
      {
        next: (snapshot) => {
          if (
            snapshot.valueRevision !== valueRevision &&
            Object.prototype.hasOwnProperty.call(snapshot, 'value')
          ) {
            valueRevision = snapshot.valueRevision
            subscriber.next(snapshot.value as T)
          }

          if (snapshot.phase === 'error') {
            subscriber.error(snapshot.error)
          } else if (snapshot.phase === 'closed') {
            subscriber.complete()
          }
        },
        error: (error) => subscriber.error(error),
        complete: () => subscriber.complete(),
      },
      { signal: subscriber.signal }
    )
  })
}
