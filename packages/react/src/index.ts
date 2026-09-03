import {
  createContext,
  createElement,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from 'react'

import type {
  CoreSubscriptionOptions,
  NatsailBatchPolicy,
  NatsailScheduler,
  NatsConnection,
  NatsRuntime,
  NatsRuntimeStatusEvent,
} from '@natsail/core'
import { defineNatsailBatchPolicy } from '@natsail/core'
import {
  createJetStreamSessionSource,
  processJetStream,
  type JetStreamDelivery,
  type JetStreamProcessorHandler,
  type JetStreamProcessorLease,
  type JetStreamProcessorOptions,
  type JetStreamProcessorPhase,
  type JetStreamSessionSourceOptions,
  type JetStreamStateSnapshot,
} from '@natsail/jetstream'
import { createCoreSessionSource, createReducingSessionSource } from '@natsail/session'
import type {
  SessionReducer,
  SessionDefinition,
  SessionHandle,
  SessionRegistry,
  SessionSnapshot,
  SessionSource,
} from '@natsail/session'

const CONNECTING_SNAPSHOT: SessionSnapshot<never> = Object.freeze({
  phase: 'connecting',
  revision: 0,
  valueRevision: 0,
})

const IDLE_RUNTIME_STATUS: NatsRuntimeStatusEvent = Object.freeze({
  type: 'status',
  state: 'idle',
  at: 0,
})

interface NatsContextValue {
  runtime: NatsRuntime
  sessions: SessionRegistry
}

export interface NatsProviderProps {
  children?: ReactNode
  runtime: NatsRuntime
  sessions: SessionRegistry
}

export interface NatsManagedResource {
  readonly runtime: NatsRuntime
  readonly sessions: SessionRegistry
  /** Defaults to closing the registry and then the runtime. */
  close?(): Promise<void>
}

export interface NatsManagedProviderProps {
  children?: ReactNode
  /** Remounts and replaces the managed resource when its connection identity changes. */
  identity: string
  create(): NatsManagedResource
  fallback?: ReactNode
  onCloseError?(error: unknown): void
}

export type SessionNotificationMode = 'immediate' | 'microtask' | 'animation-frame'

export interface NatsJetStreamReducerOptions<State = unknown> {
  /** React notification scheduling. Every delivery is still reduced serially. */
  notifications?: SessionNotificationMode
  /** Optional shared count/byte/time bounds for cumulative live notifications. */
  batchPolicy?: NatsailBatchPolicy<JetStreamStateSnapshot<State>>
  /** Host scheduler for deterministic time-bounded notifications. */
  scheduler?: NatsailScheduler
}

type ActiveSession<T> = {
  registry: SessionRegistry
  key: string
  handle: SessionHandle<T>
}

interface ResolvedSession<T> {
  registry: SessionRegistry
  key: string
  contract?: string
  source: SessionSource<T>
}

interface SelectionCache<T, Selected> {
  snapshot: SessionSnapshot<T>
  selector: (snapshot: SessionSnapshot<T>) => Selected
  value: Selected
}

const NatsContext = createContext<NatsContextValue | null>(null)

/**
 * Supplies one runtime and session registry to a React tree.
 *
 * The provider owns neither object. The application remains responsible for
 * closing them after the tree no longer needs NATS.
 */
export function NatsProvider({ children, runtime, sessions }: NatsProviderProps) {
  const value = useMemo(() => ({ runtime, sessions }), [runtime, sessions])
  return createElement(NatsContext.Provider, { value }, children)
}

/**
 * Creates and disposes one provider resource after commit. Development Strict
 * Mode effect replay reuses the same resource instead of closing it mid-remount.
 */
export function NatsManagedProvider(props: NatsManagedProviderProps) {
  return createElement(ManagedProviderInstance, { ...props, key: props.identity })
}

function ManagedProviderInstance({
  children,
  create,
  fallback = null,
  onCloseError,
}: NatsManagedProviderProps) {
  const createRef = useRef(create)
  const closeErrorRef = useRef(onCloseError)
  const resourceRef = useRef<NatsManagedResource | null>(null)
  const effectRun = useRef(0)
  const [resource, setResource] = useState<NatsManagedResource | null>(null)
  createRef.current = create
  closeErrorRef.current = onCloseError

  useEffect(() => {
    const run = ++effectRun.current
    const active = resourceRef.current ?? createRef.current()
    resourceRef.current = active
    setResource(active)

    return () => {
      queueMicrotask(() => {
        if (effectRun.current !== run || resourceRef.current !== active) return
        const close = active.close
          ? () => active.close!()
          : () => active.sessions.close().then(() => active.runtime.close())
        void close().catch((error) => closeErrorRef.current?.(error))
      })
    }
  }, [])

  if (!resource) return fallback
  return createElement(NatsProvider, {
    runtime: resource.runtime,
    sessions: resource.sessions,
    children,
  })
}

/** Returns the runtime supplied by the nearest NatsProvider. */
export function useNatsRuntime(): NatsRuntime {
  return useRequiredContext().runtime
}

export interface NatsConnectionSnapshot {
  connection: NatsConnection | null
  status: NatsRuntimeStatusEvent
  error?: unknown
}

/** Resolves and follows the runtime-owned connection for advanced nats.js operations. */
export function useNatsConnection(runtime?: NatsRuntime): NatsConnectionSnapshot {
  const context = useContext(NatsContext)
  const activeRuntime = runtime ?? context?.runtime
  if (!activeRuntime) {
    throw new Error('useNatsConnection requires a runtime or a NatsProvider')
  }
  const status = useNatsRuntimeStatus(activeRuntime)
  const [active, setActive] = useState<{
    runtime: NatsRuntime
    connection: NatsConnection | null
    error?: unknown
  } | null>(null)

  useEffect(() => {
    let cancelled = false
    if (status.state === 'closed') {
      setActive({ runtime: activeRuntime, connection: null })
      return () => {
        cancelled = true
      }
    }

    void activeRuntime.connection().then(
      (connection) => {
        if (!cancelled) setActive({ runtime: activeRuntime, connection })
      },
      (error) => {
        if (!cancelled) setActive({ runtime: activeRuntime, connection: null, error })
      }
    )

    return () => {
      cancelled = true
    }
  }, [activeRuntime, status.at, status.state])

  return {
    connection: active?.runtime === activeRuntime ? active.connection : null,
    status,
    ...(active?.runtime === activeRuntime && active.error !== undefined
      ? { error: active.error }
      : {}),
  }
}

/** Returns the session registry supplied by the nearest NatsProvider. */
export function useNatsSessionRegistry(): SessionRegistry {
  return useRequiredContext().sessions
}

/**
 * Reports the latest runtime connection state.
 *
 * Pass a runtime directly or omit it inside a NatsProvider.
 */
export function useNatsRuntimeStatus(runtime?: NatsRuntime): NatsRuntimeStatusEvent {
  const context = useContext(NatsContext)
  const activeRuntime = runtime ?? context?.runtime
  if (!activeRuntime) {
    throw new Error('useNatsRuntimeStatus requires a runtime or a NatsProvider')
  }

  const [active, setActive] = useState<{
    runtime: NatsRuntime
    status: NatsRuntimeStatusEvent
  } | null>(null)

  useEffect(() => {
    const iterator = activeRuntime.events[Symbol.asyncIterator]()
    let cancelled = false

    void (async () => {
      try {
        while (!cancelled) {
          const next = await iterator.next()
          if (next.done) return
          if (next.value.type === 'status') {
            setActive({ runtime: activeRuntime, status: next.value })
          }
        }
      } finally {
        await iterator.return?.()
      }
    })()

    return () => {
      cancelled = true
      void iterator.return?.()
    }
  }, [activeRuntime])

  return active && active.runtime === activeRuntime ? active.status : IDLE_RUNTIME_STATUS
}

export function useNatsSession<T>(definition: SessionDefinition<T>): SessionSnapshot<T>
export function useNatsSession<T>(
  registry: SessionRegistry,
  definition: SessionDefinition<T>
): SessionSnapshot<T>
export function useNatsSession<T>(key: string, source: SessionSource<T>): SessionSnapshot<T>
export function useNatsSession<T>(
  registry: SessionRegistry,
  key: string,
  source: SessionSource<T>
): SessionSnapshot<T>
/**
 * Acquires a keyed session after commit and exposes its immutable snapshot.
 *
 * The key identifies the source configuration. Change the key when the source
 * subject, decoder, credentials, or delivery policy changes.
 */
export function useNatsSession<T>(
  registryDefinitionOrKey: SessionRegistry | SessionDefinition<T> | string,
  definitionKeyOrSource?: SessionDefinition<T> | string | SessionSource<T>,
  source?: SessionSource<T>
): SessionSnapshot<T> {
  const resolved = useResolvedSession(registryDefinitionOrKey, definitionKeyOrSource, source)
  return useSessionSelection(resolved, selectSnapshot)
}

/** Opens a shared Core NATS subscription through the nearest NatsProvider. */
export function useNatsCoreSubscription<T>(
  key: string,
  options: CoreSubscriptionOptions<T>
): SessionSnapshot<T> {
  const { runtime } = useRequiredContext()
  return useNatsSession(key, createCoreSessionSource(runtime, options))
}

/** Opens one registry-shared checkpointed JetStream session through the nearest provider. */
export function useNatsJetStreamSubscription<T>(
  key: string,
  options: JetStreamSessionSourceOptions<T>
): SessionSnapshot<JetStreamDelivery<T>> {
  const { runtime } = useRequiredContext()
  return useNatsSession(key, createJetStreamSessionSource(runtime, options))
}

/** Renders one validated atomic replay/live reduced JetStream session. */
export function useNatsJetStreamReducer<State>(
  definition: SessionDefinition<JetStreamStateSnapshot<State>>,
  options: NatsJetStreamReducerOptions<State> = {}
): SessionSnapshot<JetStreamStateSnapshot<State>> {
  const resolved = useResolvedSession(definition)
  const batchPolicy = useMemo(
    () => (options.batchPolicy ? defineNatsailBatchPolicy(options.batchPolicy) : undefined),
    [options.batchPolicy]
  )
  return useSessionSelection(
    resolved,
    selectSnapshot,
    Object.is,
    options.notifications ?? 'animation-frame',
    batchPolicy,
    options.scheduler
  )
}

/** Selects one projection while preserving atomic replay and coalesced live renders. */
export function useNatsJetStreamReducerSelector<State, Selected>(
  definition: SessionDefinition<JetStreamStateSnapshot<State>>,
  selector: (snapshot: SessionSnapshot<JetStreamStateSnapshot<State>>) => Selected,
  isEqual: (previous: Selected, next: Selected) => boolean = Object.is,
  options: NatsJetStreamReducerOptions<State> = {}
): Selected {
  const resolved = useResolvedSession(definition)
  const batchPolicy = useMemo(
    () => (options.batchPolicy ? defineNatsailBatchPolicy(options.batchPolicy) : undefined),
    [options.batchPolicy]
  )
  return useSessionSelection(
    resolved,
    selector,
    isEqual,
    options.notifications ?? 'animation-frame',
    batchPolicy,
    options.scheduler
  )
}

export interface NatsJetStreamProcessorSnapshot {
  phase: JetStreamProcessorPhase
  /** Number of package-owned processor restarts. */
  restarts: number
  error?: unknown
}

/**
 * Owns one explicit-ack processor lease for the lifetime of `key`.
 * Change the key whenever the consumer configuration changes.
 */
export function useNatsJetStreamProcessor<T>(
  key: string,
  options: JetStreamProcessorOptions<T> | null,
  handler: JetStreamProcessorHandler<T>
): NatsJetStreamProcessorSnapshot {
  const { runtime } = useRequiredContext()
  const optionsRef = useRef(options)
  const handlerRef = useRef(handler)
  const closingRef = useRef<Promise<void>>(Promise.resolve())
  optionsRef.current = options
  handlerRef.current = handler
  const [active, setActive] = useState<{
    key: string
    snapshot: NatsJetStreamProcessorSnapshot
  } | null>(null)

  useEffect(() => {
    let cancelled = false
    let lease: JetStreamProcessorLease | undefined
    let unsubscribe: () => void = () => undefined

    if (optionsRef.current === null) {
      setActive({ key, snapshot: { phase: 'closed', restarts: 0 } })
      return () => {
        cancelled = true
      }
    }

    setActive({ key, snapshot: { phase: 'connecting', restarts: 0 } })

    const previousClose = closingRef.current
    const start = async () => {
      await previousClose
      if (cancelled || optionsRef.current === null) return

      try {
        lease = processJetStream(runtime, optionsRef.current, (delivery) =>
          handlerRef.current(delivery)
        )
      } catch (error) {
        setActive({
          key,
          snapshot: { phase: 'error', restarts: 0, error },
        })
        return
      }

      const activeLease = lease
      const updateSnapshot = () => {
        if (cancelled) return
        const inspection = activeLease.inspect()
        setActive({
          key,
          snapshot: {
            phase: inspection.phase,
            restarts: inspection.restarts,
            ...(inspection.error === undefined ? {} : { error: inspection.error }),
          },
        })
      }
      unsubscribe = activeLease.subscribe(updateSnapshot)
      updateSnapshot()

      void activeLease.ready.then(updateSnapshot, (error) => {
        if (!cancelled) {
          setActive({
            key,
            snapshot: { phase: 'error', restarts: activeLease.inspect().restarts, error },
          })
        }
      })
      void activeLease.closed.then(updateSnapshot, (error) => {
        if (!cancelled) {
          setActive({
            key,
            snapshot: { phase: 'error', restarts: activeLease.inspect().restarts, error },
          })
        }
      })
    }

    void start()

    return () => {
      cancelled = true
      unsubscribe()
      if (lease) closingRef.current = lease.close().catch(() => undefined)
    }
  }, [key, options !== null, runtime])

  if (options === null) return { phase: 'closed', restarts: 0 }
  return active?.key === key ? active.snapshot : { phase: 'connecting', restarts: 0 }
}

/** Selects state from one registry-shared checkpointed JetStream session. */
export function useNatsJetStreamSubscriptionSelector<T, Selected>(
  key: string,
  options: JetStreamSessionSourceOptions<T>,
  selector: (snapshot: SessionSnapshot<JetStreamDelivery<T>>) => Selected,
  isEqual?: (previous: Selected, next: Selected) => boolean
): Selected {
  const { runtime } = useRequiredContext()
  return useNatsSessionSelector(
    key,
    createJetStreamSessionSource(runtime, options),
    selector,
    isEqual
  )
}

/**
 * Folds every Core NATS delivery into one shared session snapshot.
 *
 * Use this when React must render a collection or other accumulated state.
 * The reducer runs serially at the session boundary, so render coalescing does
 * not skip intermediate deliveries.
 */
export function useNatsCoreSubscriptionReducer<Value, State>(
  key: string,
  options: CoreSubscriptionOptions<Value>,
  initialState: () => State,
  reducer: SessionReducer<Value, State>
): SessionSnapshot<State> {
  const { runtime } = useRequiredContext()
  return useNatsSession(
    key,
    createReducingSessionSource(createCoreSessionSource(runtime, options), initialState, reducer)
  )
}

/** Selects state from a shared provider-backed Core NATS subscription. */
export function useNatsCoreSubscriptionSelector<T, Selected>(
  key: string,
  options: CoreSubscriptionOptions<T>,
  selector: (snapshot: SessionSnapshot<T>) => Selected,
  isEqual?: (previous: Selected, next: Selected) => boolean
): Selected {
  const { runtime } = useRequiredContext()
  return useNatsSessionSelector(key, createCoreSessionSource(runtime, options), selector, isEqual)
}

export function useNatsSessionSelector<T, Selected>(
  key: string,
  source: SessionSource<T>,
  selector: (snapshot: SessionSnapshot<T>) => Selected,
  isEqual?: (previous: Selected, next: Selected) => boolean
): Selected
export function useNatsSessionSelector<T, Selected>(
  registry: SessionRegistry,
  key: string,
  source: SessionSource<T>,
  selector: (snapshot: SessionSnapshot<T>) => Selected,
  isEqual?: (previous: Selected, next: Selected) => boolean
): Selected
/** Selects session state and re-renders only when the selected value changes. */
export function useNatsSessionSelector<T, Selected>(
  registryOrKey: SessionRegistry | string,
  keyOrSource: string | SessionSource<T>,
  sourceOrSelector: SessionSource<T> | ((snapshot: SessionSnapshot<T>) => Selected),
  selectorOrIsEqual?:
    | ((snapshot: SessionSnapshot<T>) => Selected)
    | ((previous: Selected, next: Selected) => boolean),
  maybeIsEqual?: (previous: Selected, next: Selected) => boolean
): Selected {
  const providerCall = typeof registryOrKey === 'string'
  const resolved = useResolvedSession(
    registryOrKey,
    keyOrSource,
    providerCall ? undefined : (sourceOrSelector as SessionSource<T>)
  )
  const selector = (providerCall ? sourceOrSelector : selectorOrIsEqual) as (
    snapshot: SessionSnapshot<T>
  ) => Selected
  const isEqual = (providerCall ? selectorOrIsEqual : maybeIsEqual) as
    | ((previous: Selected, next: Selected) => boolean)
    | undefined

  return useSessionSelection(resolved, selector, isEqual)
}

function useRequiredContext(): NatsContextValue {
  const context = useContext(NatsContext)
  if (!context) {
    throw new Error('This hook requires a NatsProvider')
  }
  return context
}

function useResolvedSession<T>(
  registryDefinitionOrKey: SessionRegistry | SessionDefinition<T> | string,
  definitionKeyOrSource?: SessionDefinition<T> | string | SessionSource<T>,
  source?: SessionSource<T>
): ResolvedSession<T> {
  const context = useContext(NatsContext)

  if (isSessionDefinition(registryDefinitionOrKey)) {
    if (!context) {
      throw new Error('useNatsSession requires a SessionRegistry or a NatsProvider')
    }
    return {
      registry: context.sessions,
      key: registryDefinitionOrKey.key,
      contract: registryDefinitionOrKey.contract,
      source: registryDefinitionOrKey.source,
    }
  }

  if (typeof registryDefinitionOrKey === 'string') {
    if (!context) {
      throw new Error('useNatsSession requires a SessionRegistry or a NatsProvider')
    }
    return {
      registry: context.sessions,
      key: registryDefinitionOrKey,
      source: definitionKeyOrSource as SessionSource<T>,
    }
  }

  if (isSessionDefinition(definitionKeyOrSource)) {
    return {
      registry: registryDefinitionOrKey,
      key: definitionKeyOrSource.key,
      contract: definitionKeyOrSource.contract,
      source: definitionKeyOrSource.source,
    }
  }

  return {
    registry: registryDefinitionOrKey,
    key: definitionKeyOrSource as string,
    source: source!,
  }
}

function isSessionDefinition<T>(value: unknown): value is SessionDefinition<T> {
  return (
    typeof value === 'object' &&
    value !== null &&
    'key' in value &&
    'contract' in value &&
    'source' in value
  )
}

function useSessionSelection<T, Selected>(
  { registry, key, contract, source }: ResolvedSession<T>,
  selector: (snapshot: SessionSnapshot<T>) => Selected,
  isEqual: (previous: Selected, next: Selected) => boolean = Object.is,
  notifications: SessionNotificationMode = 'immediate',
  batchPolicy?: NatsailBatchPolicy<T>,
  batchScheduler?: NatsailScheduler
): Selected {
  const handle = useSessionHandle(registry, key, contract, source)
  const selectorRef = useRef(selector)
  const isEqualRef = useRef(isEqual)
  const cacheRef = useRef<SelectionCache<T, Selected> | undefined>(undefined)
  selectorRef.current = selector
  isEqualRef.current = isEqual

  const getSnapshot = useCallback(
    () => handle?.getSnapshot() ?? (CONNECTING_SNAPSHOT as SessionSnapshot<T>),
    [handle]
  )
  const getSelection = useCallback(() => {
    const snapshot = getSnapshot()
    const activeSelector = selectorRef.current
    const cached = cacheRef.current
    if (cached?.snapshot === snapshot && cached.selector === activeSelector) {
      return cached.value
    }

    const selected = activeSelector(snapshot)
    if (cached && isEqualRef.current(cached.value, selected)) {
      cacheRef.current = { snapshot, selector: activeSelector, value: cached.value }
      return cached.value
    }

    cacheRef.current = { snapshot, selector: activeSelector, value: selected }
    return selected
  }, [getSnapshot])
  const subscribe = useCallback(
    (listener: () => void) => {
      if (!handle) return () => undefined

      let selected = getSelection()
      let cancelled = false
      let scheduled = false
      let cancelScheduled: (() => void) | undefined
      let pendingCount = 0
      let pendingBytes = 0
      let previousValuePhase = cumulativeStatePhase(getSnapshot().value)
      const notify = (immediate: boolean, snapshot: SessionSnapshot<T>) => {
        const valuePhase = cumulativeStatePhase(snapshot.value)
        const phaseTransition =
          valuePhase !== undefined &&
          (valuePhase !== 'live' ||
            (previousValuePhase !== undefined && valuePhase !== previousValuePhase))
        previousValuePhase = valuePhase
        if (notifications === 'immediate' || immediate || phaseTransition) {
          cancelScheduled?.()
          scheduled = false
          cancelScheduled = undefined
          pendingCount = 0
          pendingBytes = 0
          listener()
          return
        }
        pendingCount += 1
        if (batchPolicy?.maxBytes !== undefined && snapshot.value !== undefined) {
          const size = batchPolicy.sizeOf!(snapshot.value)
          if (!Number.isFinite(size) || size < 0) {
            throw new TypeError('NATSail batch sizeOf must return a finite non-negative number')
          }
          if (size > batchPolicy.maxBytes) {
            throw new RangeError(
              `NATSail React selection size ${size} exceeds maxBytes ${batchPolicy.maxBytes}`
            )
          }
          if (pendingBytes > 0 && pendingBytes + size > batchPolicy.maxBytes) {
            cancelScheduled?.()
            scheduled = false
            cancelScheduled = undefined
            pendingCount = 0
            pendingBytes = 0
            listener()
            return
          }
          pendingBytes += size
        }
        const countReached =
          batchPolicy?.maxItems !== undefined && pendingCount >= batchPolicy.maxItems
        const bytesReached =
          batchPolicy?.maxBytes !== undefined && pendingBytes >= batchPolicy.maxBytes
        if (countReached || bytesReached) {
          cancelScheduled?.()
          scheduled = false
          cancelScheduled = undefined
          pendingCount = 0
          pendingBytes = 0
          listener()
          return
        }
        if (scheduled) return
        scheduled = true
        if (batchPolicy?.maxWaitMs !== undefined) {
          const scheduledTask = (
            batchScheduler ?? {
              now: () => globalThis.performance?.now() ?? Date.now(),
              schedule: (task: () => void, delayMs: number) => {
                const timer = setTimeout(task, delayMs)
                return { cancel: () => clearTimeout(timer) }
              },
              yield: () => Promise.resolve(),
            }
          ).schedule(() => {
            scheduled = false
            pendingCount = 0
            pendingBytes = 0
            if (!cancelled) listener()
          }, batchPolicy.maxWaitMs)
          cancelScheduled = () => scheduledTask.cancel()
          return
        }
        if (notifications === 'microtask') {
          queueMicrotask(() => {
            scheduled = false
            if (!cancelled) listener()
          })
          return
        }

        if (typeof requestAnimationFrame === 'function') {
          const frame = requestAnimationFrame(() => {
            scheduled = false
            if (!cancelled) listener()
          })
          cancelScheduled = () => cancelAnimationFrame(frame)
        } else {
          const timer = setTimeout(() => {
            scheduled = false
            if (!cancelled) listener()
          }, 16)
          cancelScheduled = () => clearTimeout(timer)
        }
      }
      const unsubscribe = handle.subscribe(() => {
        const snapshot = getSnapshot()
        const next = getSelection()
        if (!isEqualRef.current(selected, next)) {
          selected = next
          notify(false, snapshot)
        }
      })

      return () => {
        cancelled = true
        cancelScheduled?.()
        unsubscribe()
      }
    },
    [batchPolicy, batchScheduler, getSelection, getSnapshot, handle, notifications]
  )

  return useSyncExternalStore(subscribe, getSelection, getSelection)
}

function cumulativeStatePhase(value: unknown): string | undefined {
  if (typeof value !== 'object' || value === null || !('phase' in value)) return undefined
  const phase = (value as { phase?: unknown }).phase
  return typeof phase === 'string' ? phase : undefined
}

function useSessionHandle<T>(
  registry: SessionRegistry,
  key: string,
  contract: string | undefined,
  source: SessionSource<T>
): SessionHandle<T> | null {
  const sourceRef = useRef(source)
  sourceRef.current = source
  const [active, setActive] = useState<ActiveSession<T> | null>(null)

  useEffect(() => {
    const handle = contract
      ? registry.acquire({ key, contract, source: sourceRef.current })
      : registry.acquire(key, sourceRef.current)
    setActive({ registry, key, handle })

    return () => {
      void handle.release().catch(() => undefined)
    }
  }, [contract, key, registry])

  return active?.registry === registry && active.key === key ? active.handle : null
}

function selectSnapshot<T>(snapshot: SessionSnapshot<T>): SessionSnapshot<T> {
  return snapshot
}
