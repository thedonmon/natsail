import { asyncScheduler, distinctUntilChanged, filter, Observable } from 'rxjs'
import type { SchedulerLike, Subscription } from 'rxjs'

import type {
  CoreSubscriptionOptions,
  NatsRuntime,
  NatsRuntimeEvent,
  NatsRuntimeStatusEvent,
} from '@natsail/core'
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
): Observable<SessionRegistryEvent> {
  return new Observable((subscriber) => {
    const iterator = registry.events[Symbol.asyncIterator]()
    let cancelled = false

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

    return () => {
      cancelled = true
      void iterator.return?.()
    }
  })
}

/** Converts the runtime event iterable into a cancellable Observable. */
export function observeNatsRuntimeEvents(runtime: NatsRuntime): Observable<NatsRuntimeEvent> {
  return new Observable((subscriber) => {
    const iterator = runtime.events[Symbol.asyncIterator]()
    let cancelled = false

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

    return () => {
      cancelled = true
      void iterator.return?.()
    }
  })
}

/** Emits distinct runtime connection states and omits diagnostic events. */
export function observeNatsRuntimeStatus(runtime: NatsRuntime): Observable<NatsRuntimeStatusEvent> {
  return observeNatsRuntimeEvents(runtime).pipe(
    filter((event): event is NatsRuntimeStatusEvent => event.type === 'status'),
    distinctUntilChanged(
      (previous, next) => previous.state === next.state && previous.server === next.server
    )
  )
}

/** Emits values from one keyed, registry-shared Core NATS subscription. */
export function observeNatsCoreSubscription<T>(
  registry: SessionRegistry,
  runtime: NatsRuntime,
  key: string,
  options: CoreSubscriptionOptions<T>
): Observable<T> {
  return observeNatsSessionValues(registry, key, createCoreSessionSource(runtime, options))
}

/** Emits deliveries from one registry-shared checkpointed JetStream session. */
export function observeNatsJetStreamSubscription<T>(
  registry: SessionRegistry,
  runtime: NatsRuntime,
  key: string,
  options: JetStreamSessionSourceOptions<T>
): Observable<JetStreamDelivery<T>> {
  return observeNatsSessionValues(registry, key, createJetStreamSessionSource(runtime, options))
}

/** Emits one atomic replay/live reduced state from a validated shared definition. */
export function observeNatsJetStreamReducer<State>(
  registry: SessionRegistry,
  definition: SessionDefinition<JetStreamStateSnapshot<State>>
): Observable<SessionSnapshot<JetStreamStateSnapshot<State>>> {
  return observeNatsSession(registry, definition)
}

export interface NatsailJetStreamStateOptions {
  /**
   * Maximum time that subsequent cumulative live states are coalesced. The
   * initial replaying and hydrated live states remain immediate. Defaults to
   * 16ms; use 0 to observe every reduced live state.
   */
  readonly liveBatchMs?: number
  /** Overrides the RxJS async scheduler, primarily for tests or custom hosts. */
  readonly scheduler?: SchedulerLike
}

/**
 * Emits cumulative reduced JetStream state without duplicate session-lifecycle
 * notifications. Replay and recovery phase changes are immediate; subsequent
 * live states are coalesced to one latest value per bounded render window.
 */
export function observeNatsJetStreamState<State>(
  registry: SessionRegistry,
  definition: SessionDefinition<JetStreamStateSnapshot<State>>,
  options: NatsailJetStreamStateOptions = {}
): Observable<JetStreamStateSnapshot<State>> {
  const liveBatchMs = options.liveBatchMs ?? 16
  if (!Number.isFinite(liveBatchMs) || liveBatchMs < 0) {
    throw new TypeError('NATSail RxJS liveBatchMs must be a finite non-negative number')
  }

  const values = observeNatsSessionValues(registry, definition)
  if (liveBatchMs === 0) return values
  const scheduler = options.scheduler ?? asyncScheduler

  return new Observable((subscriber) => {
    let seenLive = false
    let pendingLive: JetStreamStateSnapshot<State> | undefined
    let scheduledFlush: Subscription | undefined

    const flush = () => {
      scheduledFlush = undefined
      if (pendingLive === undefined) return
      const value = pendingLive
      pendingLive = undefined
      subscriber.next(value)
    }
    const cancelFlush = () => {
      scheduledFlush?.unsubscribe()
      scheduledFlush = undefined
    }
    const scheduleFlush = () => {
      if (scheduledFlush !== undefined) return
      let ranSynchronously = false
      const scheduled = scheduler.schedule(() => {
        ranSynchronously = true
        flush()
      }, liveBatchMs)
      if (!ranSynchronously) scheduledFlush = scheduled
    }
    const source = values.subscribe({
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

        pendingLive = value
        scheduleFlush()
      },
      error: (error) => {
        cancelFlush()
        flush()
        subscriber.error(error)
      },
      complete: () => {
        cancelFlush()
        flush()
        subscriber.complete()
      },
    })

    return () => {
      cancelFlush()
      pendingLive = undefined
      source.unsubscribe()
    }
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
): Observable<SessionSnapshot<T>>
export function observeNatsSession<T>(
  registry: SessionRegistry,
  key: string,
  source: SessionSource<T>
): Observable<SessionSnapshot<T>>
export function observeNatsSession<T>(
  registry: SessionRegistry,
  definitionOrKey: SessionDefinition<T> | string,
  source?: SessionSource<T>
): Observable<SessionSnapshot<T>> {
  return new Observable((subscriber) => {
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
    emit()

    return () => {
      unsubscribe()
      void handle.release().catch(() => undefined)
    }
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
): Observable<T>
export function observeNatsSessionValues<T>(
  registry: SessionRegistry,
  key: string,
  source: SessionSource<T>
): Observable<T>
export function observeNatsSessionValues<T>(
  registry: SessionRegistry,
  definitionOrKey: SessionDefinition<T> | string,
  source?: SessionSource<T>
): Observable<T> {
  return new Observable((subscriber) => {
    let valueRevision = -1
    const snapshots =
      typeof definitionOrKey === 'string'
        ? observeNatsSession(registry, definitionOrKey, source!)
        : observeNatsSession(registry, definitionOrKey)
    const subscription = snapshots.subscribe({
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
    })

    return () => subscription.unsubscribe()
  })
}
