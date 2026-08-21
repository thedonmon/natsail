import { distinctUntilChanged, filter, Observable } from 'rxjs'

import type {
  CoreSubscriptionOptions,
  NatsRuntime,
  NatsRuntimeEvent,
  NatsRuntimeStatusEvent,
} from '@natsail/core'
import { createCoreSessionSource } from '@natsail/session'
import type { SessionRegistry, SessionSnapshot, SessionSource } from '@natsail/session'

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

/**
 * Creates a cold Observable over one keyed logical session.
 *
 * Every Observable subscriber acquires a registry handle. Subscribers with the
 * same registry and key share the underlying NATS source.
 */
export function observeNatsSession<T>(
  registry: SessionRegistry,
  key: string,
  source: SessionSource<T>
): Observable<SessionSnapshot<T>> {
  return new Observable((subscriber) => {
    const handle = registry.acquire(key, source)
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
  key: string,
  source: SessionSource<T>
): Observable<T> {
  return new Observable((subscriber) => {
    let valueRevision = -1
    const subscription = observeNatsSession(registry, key, source).subscribe({
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
