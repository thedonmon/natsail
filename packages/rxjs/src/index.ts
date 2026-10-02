import { asyncScheduler, distinctUntilChanged, filter, Observable } from 'rxjs'
import type { OperatorFunction, SchedulerLike, Subscription } from 'rxjs'

import type {
  CoreSubscriptionOptions,
  NatsailBatchPolicy,
  NatsRuntime,
  NatsRuntimeEvent,
  NatsRuntimeStatusEvent,
} from '@natsail/core'
import { defineNatsailBatchPolicy, NatsailBatchItemTooLargeError } from '@natsail/core'
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

interface PolicyBuffer<T> {
  /** Throws on an invalid or oversized item without storing it. */
  add(value: T): void
  flush(): void
  /** Drops the bookkeeping and timer; the caller drops its own storage. */
  cancel(): void
}

/** Count/byte/time bookkeeping shared by RxJS batching; the caller owns value storage. */
function createPolicyBuffer<T>(
  policy: Readonly<NatsailBatchPolicy<T>>,
  scheduler: SchedulerLike,
  sink: { store(value: T): void; emit(): void }
): PolicyBuffer<T> {
  let pendingItems = 0
  let pendingBytes = 0
  let timer: Subscription | undefined
  let cancelled = false

  const cancelTimer = () => {
    timer?.unsubscribe()
    timer = undefined
  }
  const flush = () => {
    cancelTimer()
    if (pendingItems === 0) return
    pendingItems = 0
    pendingBytes = 0
    sink.emit()
  }

  return {
    add: (value) => {
      let size = 0
      if (policy.maxBytes !== undefined) {
        size = policy.sizeOf!(value)
        if (!Number.isFinite(size) || size < 0) {
          throw new TypeError('NATSail batch sizeOf must return a finite non-negative number')
        }
        if (size > policy.maxBytes) throw new NatsailBatchItemTooLargeError(size, policy.maxBytes)
        if (pendingItems > 0 && pendingBytes + size > policy.maxBytes) {
          flush()
          // A downstream operator may have unsubscribed during the flush.
          if (cancelled) return
        }
      }
      sink.store(value)
      pendingItems += 1
      pendingBytes += size
      if (
        (policy.maxItems !== undefined && pendingItems >= policy.maxItems) ||
        (policy.maxBytes !== undefined && pendingBytes >= policy.maxBytes)
      ) {
        flush()
      } else if (timer === undefined && policy.maxWaitMs !== undefined) {
        let ranSynchronously = false
        const scheduled = scheduler.schedule(() => {
          ranSynchronously = true
          flush()
        }, policy.maxWaitMs)
        if (!ranSynchronously) timer = scheduled
      }
    },
    flush,
    cancel: () => {
      cancelled = true
      cancelTimer()
      pendingItems = 0
      pendingBytes = 0
    },
  }
}

/**
 * Collects source values into arrays bounded by a shared NATSail batch policy.
 *
 * The timer starts at the first value of a batch, so an idle subscription
 * schedules nothing and never emits an empty array. Source `error` and
 * `complete` flush the pending batch first. An invalid or oversized item errors
 * the stream and drops the pending batch, as unsubscribe does. Order is preserved.
 */
export function batchWithPolicy<T>(
  policy: NatsailBatchPolicy<T>,
  options: { readonly scheduler?: SchedulerLike } = {}
): OperatorFunction<T, readonly T[]> {
  const validated = defineNatsailBatchPolicy(policy)
  const scheduler = options.scheduler ?? asyncScheduler

  return (source) =>
    new Observable<readonly T[]>((subscriber) => {
      let pending: T[] = []
      const buffer = createPolicyBuffer(validated, scheduler, {
        store: (value) => pending.push(value),
        emit: () => {
          const batch = pending
          pending = []
          subscriber.next(batch)
        },
      })
      const subscription = source.subscribe({
        next: (value) => {
          try {
            buffer.add(value)
          } catch (error) {
            buffer.cancel()
            subscriber.error(error)
          }
        },
        error: (error) => {
          buffer.flush()
          subscriber.error(error)
        },
        complete: () => {
          buffer.flush()
          subscriber.complete()
        },
      })

      return () => {
        buffer.cancel()
        subscription.unsubscribe()
      }
    })
}

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

export interface NatsailJetStreamStateOptions<State = unknown> {
  /**
   * Maximum time that subsequent cumulative live states are coalesced. The
   * initial replaying and hydrated live states remain immediate. Defaults to
   * 16ms; use 0 to observe every reduced live state.
   */
  readonly liveBatchMs?: number
  /** Shared count/byte/time bounds for cumulative live presentation. */
  readonly batchPolicy?: NatsailBatchPolicy<JetStreamStateSnapshot<State>>
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
  options: NatsailJetStreamStateOptions<State> = {}
): Observable<JetStreamStateSnapshot<State>> {
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
  const scheduler = options.scheduler ?? asyncScheduler

  return new Observable((subscriber) => {
    let seenLive = false
    let latest: JetStreamStateSnapshot<State> | undefined
    const buffer = createPolicyBuffer(policy, scheduler, {
      store: (value) => (latest = value),
      emit: () => subscriber.next(latest!),
    })
    const source = values.subscribe({
      next: (value) => {
        if (value.phase !== 'live') {
          buffer.flush()
          seenLive = false
          subscriber.next(value)
          return
        }
        if (!seenLive) {
          seenLive = true
          subscriber.next(value)
          return
        }
        try {
          buffer.add(value)
        } catch (error) {
          buffer.cancel()
          subscriber.error(error)
        }
      },
      error: (error) => {
        buffer.cancel()
        subscriber.error(error)
      },
      complete: () => {
        buffer.flush()
        subscriber.complete()
      },
    })

    return () => {
      buffer.cancel()
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
