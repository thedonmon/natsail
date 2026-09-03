import { createNatsailTelemetryReporter, createNatsailWorkController } from '@natsail/core'
import type {
  CoreSubscriptionOptions,
  NatsailTelemetryAttributes,
  NatsailTelemetryClock,
  NatsailTelemetryCounterName,
  NatsailTelemetryGaugeName,
  NatsailTelemetryReporter,
  NatsailTelemetrySink,
  NatsailWorkBudget,
  NatsRuntime,
  SubscriptionLease,
} from '@natsail/core'

export type SessionPhase = 'connecting' | 'live' | 'closed' | 'error'

export interface SessionSnapshot<T> {
  phase: SessionPhase
  revision: number
  /** Increments only when the source delivers a value. */
  valueRevision: number
  value?: T
  error?: unknown
}

export type SessionListener = () => void

export type SessionSource<T> = (accept: (value: T) => Promise<void>) => SubscriptionLease

export type SessionReducer<Value, State> = (state: State, value: Value) => State | Promise<State>

export interface NatsailReducingSessionOptions {
  /** Yields between serial reducer applications after this cooperative slice is consumed. */
  readonly workBudget?: NatsailWorkBudget
  /** Adapter-owned reporter used only for low-cardinality work-yield measurements. */
  readonly telemetry?: NatsailTelemetryReporter
}

/** One validated logical source shared by every framework adapter. */
export interface SessionDefinition<T> {
  readonly key: string
  /** Stable description of every source option that affects delivery semantics. */
  readonly contract: string
  readonly source: SessionSource<T>
}

export class SessionContractMismatchError extends Error {
  readonly name = 'SessionContractMismatchError'

  constructor(
    readonly key: string,
    readonly activeContract: string,
    readonly requestedContract: string
  ) {
    super(
      `NATS session ${key} is already active with contract ${activeContract}; requested ${requestedContract}`
    )
  }
}

export interface SessionInspection {
  readonly key: string
  readonly contract?: string
  readonly phase: SessionPhase
  readonly references: number
  readonly revision: number
  readonly valueRevision: number
  readonly idle: boolean
}

export interface SessionRegistryInspection {
  readonly closed: boolean
  readonly activeSessions: number
  readonly sessions: readonly SessionInspection[]
}

export type SessionRegistryEventType =
  | 'opened'
  | 'retained'
  | 'released'
  | 'restarting'
  | 'updated'
  | 'closed'

export interface SessionRegistryEvent {
  readonly type: SessionRegistryEventType
  readonly key: string
  readonly contract?: string
  readonly at: number
  readonly phase: SessionPhase
  readonly references: number
  readonly revision: number
  readonly valueRevision: number
  readonly error?: unknown
}

export interface SessionHandle<T> {
  readonly key: string
  readonly ready: Promise<void>
  getSnapshot(): SessionSnapshot<T>
  subscribe(listener: SessionListener): () => void
  /** Reopens this logical source while preserving its shared handle and latest value. */
  restart(): Promise<void>
  release(): Promise<void>
}

export interface SessionRegistry {
  acquire<T>(definition: SessionDefinition<T>): SessionHandle<T>
  acquire<T>(key: string, source: SessionSource<T>): SessionHandle<T>
  /** Restarts an active logical session by key. */
  restart(key: string): Promise<void>
  /** Multicasts lifecycle and reference-count changes for leak and recovery diagnostics. */
  readonly events: AsyncIterable<SessionRegistryEvent>
  /** Returns a point-in-time view of every active logical session. */
  inspect(): SessionRegistryInspection
  close(): Promise<void>
}

export interface SessionRegistryOptions {
  /** Delay before closing a session with no callers. Defaults to 0. */
  idleCloseMs?: number
  /** Optional synchronous measurement sink. Session keys and contracts are never included. */
  telemetry?: NatsailTelemetrySink
  /** Low-cardinality primitive attributes added to each session measurement. */
  telemetryAttributes?: NatsailTelemetryAttributes
  /** Monotonic telemetry clock override, primarily for deterministic hosts and tests. */
  telemetryClock?: NatsailTelemetryClock
}

type Deferred<T> = {
  promise: Promise<T>
  resolve(value: T | PromiseLike<T>): void
}

function deferred<T>(): Deferred<T> {
  let resolve!: Deferred<T>['resolve']
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise
  })
  return { promise, resolve }
}

/** Freezes a validated session identity that React and RxJS can share safely. */
export function defineSession<T>(definition: SessionDefinition<T>): SessionDefinition<T> {
  if (definition.key.length === 0) throw new TypeError('Session definition key must not be empty')
  if (definition.contract.length === 0) {
    throw new TypeError('Session definition contract must not be empty')
  }
  return Object.freeze({ ...definition })
}

/** Adapts one Core NATS subscription into a shareable session source. */
export function createCoreSessionSource<T>(
  runtime: NatsRuntime,
  options: CoreSubscriptionOptions<T>
): SessionSource<T> {
  return (accept) => runtime.subscribe(options, accept)
}

/** Folds every source delivery serially before publishing the next snapshot. */
export function createReducingSessionSource<Value, State>(
  source: SessionSource<Value>,
  initialState: () => State,
  reducer: SessionReducer<Value, State>,
  options: NatsailReducingSessionOptions = {}
): SessionSource<State> {
  return (accept) => {
    let state = initialState()
    let pending = Promise.resolve()
    const work =
      options.workBudget === undefined
        ? undefined
        : createNatsailWorkController(options.workBudget, options.telemetry, 'session')

    return source((value) => {
      const update = pending.then(async () => {
        state = await reducer(state, value)
        await work?.checkpoint()
        await accept(state)
      })
      pending = update.catch(() => undefined)
      return update
    })
  }
}

class SharedSession<T> {
  private snapshot: SessionSnapshot<T> = {
    phase: 'connecting',
    revision: 0,
    valueRevision: 0,
  }
  private readonly listeners = new Set<SessionListener>()
  private lease?: SubscriptionLease
  private currentReady: Promise<void>
  private closePromise?: Promise<void>
  private restartPromise: Promise<void> | undefined
  private references = 0
  private generation = 0
  private closeRequested = false

  constructor(
    readonly key: string,
    readonly contract: string | undefined,
    private readonly source: SessionSource<T>,
    private readonly emit: (event: SessionRegistryEvent) => void,
    private readonly telemetry: NatsailTelemetryReporter
  ) {
    this.currentReady = this.start()
    void this.currentReady.catch(() => undefined)
  }

  get ready(): Promise<void> {
    return this.currentReady
  }

  retain(): void {
    this.references += 1
    this.recordReference('retained')
    this.report('retained')
  }

  release(): number {
    this.references -= 1
    this.recordReference('released-reference')
    this.report('released')
    return this.references
  }

  inspect(idle: boolean): SessionInspection {
    return {
      key: this.key,
      ...(this.contract === undefined ? {} : { contract: this.contract }),
      phase: this.snapshot.phase,
      references: this.references,
      revision: this.snapshot.revision,
      valueRevision: this.snapshot.valueRevision,
      idle,
    }
  }

  getSnapshot(): SessionSnapshot<T> {
    return this.snapshot
  }

  subscribe(listener: SessionListener): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  close(): Promise<void> {
    this.closeRequested = true
    this.generation += 1
    this.closePromise ??= (async () => {
      try {
        await (this.lease?.close() ?? Promise.resolve())
      } finally {
        if (this.snapshot.phase !== 'closed') {
          this.update({
            ...this.snapshot,
            phase: 'closed',
            revision: this.snapshot.revision + 1,
          })
        }
        recordCounter(this.telemetry, 'natsail.session.lifecycle', 1, {
          action: 'closed',
          phase: 'closed',
          source: 'session',
        })
        this.report('closed')
      }
    })()
    return this.closePromise
  }

  restart(): Promise<void> {
    if (this.closeRequested) {
      return Promise.reject(new Error('The NATS session is closed'))
    }

    if (!this.restartPromise) {
      recordCounter(this.telemetry, 'natsail.session.lifecycle', 1, {
        action: 'restarting',
        phase: 'reconnecting',
        source: 'session',
      })
      this.report('restarting')
      this.restartPromise = this.restartSession().finally(() => {
        this.restartPromise = undefined
      })
      this.currentReady = this.restartPromise
    }
    return this.restartPromise
  }

  private async start(): Promise<void> {
    const generation = ++this.generation
    try {
      const lease = this.source(async (value) => this.accept(generation, value))
      this.lease = lease
      void lease.closed.then(
        () => this.finish(generation, 'closed'),
        (error: unknown) => this.finish(generation, 'error', error)
      )

      await lease.ready
      if (generation !== this.generation || this.closeRequested) return
      this.update({
        ...this.snapshot,
        phase: 'live',
        revision: this.snapshot.revision + 1,
      })
    } catch (error) {
      this.finish(generation, 'error', error)
      throw error
    }
  }

  private async restartSession(): Promise<void> {
    const previous = this.lease
    this.generation += 1
    const { error: _error, ...current } = this.snapshot
    this.update({
      ...current,
      phase: 'connecting',
      revision: this.snapshot.revision + 1,
    })
    if (previous) {
      await previous.close().catch(() => undefined)
    }
    if (this.closeRequested) {
      throw new Error('The NATS session is closed')
    }

    this.currentReady = this.start()
    void this.currentReady.catch(() => undefined)
    return this.currentReady
  }

  private async accept(generation: number, value: T): Promise<void> {
    if (generation !== this.generation) return
    if (this.snapshot.phase === 'closed' || this.snapshot.phase === 'error') {
      return
    }

    this.update({
      phase: this.snapshot.phase,
      revision: this.snapshot.revision + 1,
      valueRevision: this.snapshot.valueRevision + 1,
      value,
    })
  }

  private finish(generation: number, phase: 'closed' | 'error', error?: unknown): void {
    if (generation !== this.generation || this.closeRequested) return
    if (this.snapshot.phase === 'closed' || this.snapshot.phase === 'error') {
      return
    }

    this.update({
      ...this.snapshot,
      phase,
      revision: this.snapshot.revision + 1,
      ...(error === undefined ? {} : { error }),
    })
  }

  private update(snapshot: SessionSnapshot<T>): void {
    const previousPhase = this.snapshot.phase
    this.snapshot = snapshot
    if (previousPhase !== snapshot.phase) {
      recordCounter(this.telemetry, 'natsail.session.lifecycle', 1, {
        action: 'phase-change',
        phase: snapshot.phase,
        source: 'session',
      })
    }
    this.report('updated')
    for (const listener of this.listeners) {
      listener()
    }
  }

  private report(type: SessionRegistryEventType): void {
    this.emit({
      type,
      key: this.key,
      ...(this.contract === undefined ? {} : { contract: this.contract }),
      at: Date.now(),
      phase: this.snapshot.phase,
      references: this.references,
      revision: this.snapshot.revision,
      valueRevision: this.snapshot.valueRevision,
      ...(this.snapshot.error === undefined ? {} : { error: this.snapshot.error }),
    })
  }

  private recordReference(action: 'retained' | 'released-reference'): void {
    recordCounter(this.telemetry, 'natsail.session.references', 1, {
      action,
      source: 'session',
    })
    recordGauge(this.telemetry, 'natsail.session.references.active', this.references, {
      source: 'session',
    })
  }
}

function recordCounter(
  telemetry: NatsailTelemetryReporter,
  name: NatsailTelemetryCounterName,
  value: number,
  attributes?: NatsailTelemetryAttributes
): void {
  if (!telemetry.enabled) return
  telemetry.record({
    type: 'counter',
    name,
    value,
    at: telemetry.now(),
    ...(attributes === undefined ? {} : { attributes }),
  })
}

function recordGauge(
  telemetry: NatsailTelemetryReporter,
  name: NatsailTelemetryGaugeName,
  value: number,
  attributes?: NatsailTelemetryAttributes
): void {
  if (!telemetry.enabled) return
  telemetry.record({
    type: 'gauge',
    name,
    value,
    at: telemetry.now(),
    ...(attributes === undefined ? {} : { attributes }),
  })
}

type SessionEventSubscriber = {
  readonly queue: SessionRegistryEvent[]
  waiting: Deferred<IteratorResult<SessionRegistryEvent, undefined>> | undefined
  closed: boolean
}

class SessionEventStream implements AsyncIterable<SessionRegistryEvent> {
  private readonly subscribers = new Set<SessionEventSubscriber>()
  private closed = false;

  [Symbol.asyncIterator](): AsyncIterator<SessionRegistryEvent> {
    const subscriber: SessionEventSubscriber = {
      queue: [],
      waiting: undefined,
      closed: this.closed,
    }
    if (!subscriber.closed) this.subscribers.add(subscriber)

    return {
      next: () => {
        const event = subscriber.queue.shift()
        if (event) return Promise.resolve({ done: false, value: event })
        if (subscriber.closed) return Promise.resolve({ done: true, value: undefined })
        const waiting = deferred<IteratorResult<SessionRegistryEvent, undefined>>()
        subscriber.waiting = waiting
        return waiting.promise
      },
      return: async () => {
        this.remove(subscriber)
        return { done: true, value: undefined }
      },
    }
  }

  emit(event: SessionRegistryEvent): void {
    if (this.closed) return
    for (const subscriber of this.subscribers) {
      if (subscriber.waiting) {
        subscriber.waiting.resolve({ done: false, value: event })
        subscriber.waiting = undefined
      } else if (event.type === 'updated') {
        let existing = -1
        for (let index = subscriber.queue.length - 1; index >= 0; index -= 1) {
          const queued = subscriber.queue[index]!
          if (queued.type === 'updated' && queued.key === event.key) {
            existing = index
            break
          }
        }
        if (existing === -1) subscriber.queue.push(event)
        else subscriber.queue[existing] = event
      } else {
        subscriber.queue.push(event)
      }
    }
  }

  close(): void {
    if (this.closed) return
    this.closed = true
    for (const subscriber of this.subscribers) this.remove(subscriber)
  }

  private remove(subscriber: SessionEventSubscriber): void {
    subscriber.closed = true
    this.subscribers.delete(subscriber)
    subscriber.waiting?.resolve({ done: true, value: undefined })
    subscriber.waiting = undefined
  }
}

class DefaultSessionRegistry implements SessionRegistry {
  private readonly sessions = new Map<string, SharedSession<unknown>>()
  private readonly cleanupTimers = new Map<string, ReturnType<typeof setTimeout>>()
  private closePromise?: Promise<void>
  private closeRequested = false
  private readonly eventStream = new SessionEventStream()
  private readonly telemetry: NatsailTelemetryReporter

  readonly events: AsyncIterable<SessionRegistryEvent> = this.eventStream

  constructor(private readonly options: SessionRegistryOptions) {
    this.telemetry = createNatsailTelemetryReporter({
      ...(options.telemetry === undefined ? {} : { sink: options.telemetry }),
      ...(options.telemetryClock === undefined ? {} : { clock: options.telemetryClock }),
      ...(options.telemetryAttributes === undefined
        ? {}
        : { attributes: options.telemetryAttributes }),
    })
  }

  acquire<T>(definition: SessionDefinition<T>): SessionHandle<T>
  acquire<T>(key: string, source: SessionSource<T>): SessionHandle<T>
  acquire<T>(
    definitionOrKey: SessionDefinition<T> | string,
    sourceArgument?: SessionSource<T>
  ): SessionHandle<T> {
    if (this.closeRequested) {
      throw new Error('The session registry is closed')
    }

    const key = typeof definitionOrKey === 'string' ? definitionOrKey : definitionOrKey.key
    const contract = typeof definitionOrKey === 'string' ? undefined : definitionOrKey.contract
    const source = typeof definitionOrKey === 'string' ? sourceArgument! : definitionOrKey.source

    let session = this.sessions.get(key) as SharedSession<T> | undefined
    if (!session) {
      session = new SharedSession(
        key,
        contract,
        source,
        (event) => this.eventStream.emit(event),
        this.telemetry
      )
      this.sessions.set(key, session as SharedSession<unknown>)
      this.recordLifecycle('opened', 'connecting')
      this.recordActiveSessions()
      this.eventStream.emit({
        type: 'opened',
        key,
        ...(contract === undefined ? {} : { contract }),
        at: Date.now(),
        phase: session.getSnapshot().phase,
        references: 0,
        revision: session.getSnapshot().revision,
        valueRevision: session.getSnapshot().valueRevision,
      })
    } else if (contract !== session.contract) {
      throw new SessionContractMismatchError(
        key,
        session.contract ?? '<unvalidated>',
        contract ?? '<unvalidated>'
      )
    }
    const cleanupTimer = this.cleanupTimers.get(key)
    if (cleanupTimer) {
      clearTimeout(cleanupTimer)
      this.cleanupTimers.delete(key)
    }
    session.retain()

    let released = false
    return {
      key,
      get ready() {
        return session.ready
      },
      getSnapshot: () => session.getSnapshot(),
      subscribe: (listener) => session.subscribe(listener),
      restart: () => session.restart(),
      release: async () => {
        if (released) {
          return
        }
        released = true

        if (session.release() === 0) {
          await this.releaseIdleSession(key, session)
        }
      },
    }
  }

  restart(key: string): Promise<void> {
    if (this.closeRequested) {
      return Promise.reject(new Error('The session registry is closed'))
    }
    const session = this.sessions.get(key)
    if (!session) {
      return Promise.reject(new Error(`No active NATS session exists for key ${key}`))
    }
    return session.restart()
  }

  close(): Promise<void> {
    this.closeRequested = true
    this.closePromise ??= this.closeRegistry()
    return this.closePromise
  }

  inspect(): SessionRegistryInspection {
    const sessions = [...this.sessions.entries()].map(([key, session]) =>
      session.inspect(this.cleanupTimers.has(key))
    )
    return {
      closed: this.closeRequested,
      activeSessions: sessions.length,
      sessions,
    }
  }

  private async closeRegistry(): Promise<void> {
    for (const timer of this.cleanupTimers.values()) {
      clearTimeout(timer)
    }
    this.cleanupTimers.clear()
    const sessions = [...this.sessions.values()]
    this.sessions.clear()
    await Promise.allSettled(sessions.map((session) => session.close()))
    this.recordActiveSessions()
    this.eventStream.close()
  }

  private async releaseIdleSession<T>(key: string, session: SharedSession<T>): Promise<void> {
    const idleCloseMs = this.options.idleCloseMs ?? 0
    if (idleCloseMs === 0) {
      this.sessions.delete(key)
      try {
        await session.close()
      } finally {
        this.recordActiveSessions()
      }
      return
    }

    const timer = setTimeout(() => {
      this.cleanupTimers.delete(key)
      if (this.sessions.get(key) === session) {
        this.sessions.delete(key)
        void session
          .close()
          .finally(() => {
            this.recordActiveSessions()
          })
          .catch(() => undefined)
      }
    }, idleCloseMs)
    this.cleanupTimers.set(key, timer)
  }

  private recordLifecycle(
    action: 'opened' | 'restarting' | 'closed',
    phase: NatsailTelemetryAttributes['phase']
  ): void {
    recordCounter(this.telemetry, 'natsail.session.lifecycle', 1, {
      action,
      ...(phase === undefined ? {} : { phase }),
      source: 'session',
    })
  }

  private recordActiveSessions(): void {
    recordGauge(this.telemetry, 'natsail.session.active', this.sessions.size, {
      source: 'session',
    })
  }
}

export function createSessionRegistry(options: SessionRegistryOptions = {}): SessionRegistry {
  return new DefaultSessionRegistry(options)
}
