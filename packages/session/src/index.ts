import type { CoreSubscriptionOptions, NatsRuntime, SubscriptionLease } from '@natsail/core'

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
  acquire<T>(key: string, source: SessionSource<T>): SessionHandle<T>
  /** Restarts an active logical session by key. */
  restart(key: string): Promise<void>
  close(): Promise<void>
}

export interface SessionRegistryOptions {
  /** Delay before closing a session with no callers. Defaults to 0. */
  idleCloseMs?: number
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
  reducer: SessionReducer<Value, State>
): SessionSource<State> {
  return (accept) => {
    let state = initialState()
    let pending = Promise.resolve()

    return source((value) => {
      const update = pending.then(async () => {
        state = await reducer(state, value)
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

  constructor(private readonly source: SessionSource<T>) {
    this.currentReady = this.start()
    void this.currentReady.catch(() => undefined)
  }

  get ready(): Promise<void> {
    return this.currentReady
  }

  retain(): void {
    this.references += 1
  }

  release(): number {
    this.references -= 1
    return this.references
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
      }
    })()
    return this.closePromise
  }

  restart(): Promise<void> {
    if (this.closeRequested) {
      return Promise.reject(new Error('The NATS session is closed'))
    }

    if (!this.restartPromise) {
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
    this.snapshot = snapshot
    for (const listener of this.listeners) {
      listener()
    }
  }
}

class DefaultSessionRegistry implements SessionRegistry {
  private readonly sessions = new Map<string, SharedSession<unknown>>()
  private readonly cleanupTimers = new Map<string, ReturnType<typeof setTimeout>>()
  private closePromise?: Promise<void>
  private closeRequested = false

  constructor(private readonly options: SessionRegistryOptions) {}

  acquire<T>(key: string, source: SessionSource<T>): SessionHandle<T> {
    if (this.closeRequested) {
      throw new Error('The session registry is closed')
    }

    let session = this.sessions.get(key) as SharedSession<T> | undefined
    if (!session) {
      session = new SharedSession(source)
      this.sessions.set(key, session as SharedSession<unknown>)
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

  private async closeRegistry(): Promise<void> {
    for (const timer of this.cleanupTimers.values()) {
      clearTimeout(timer)
    }
    this.cleanupTimers.clear()
    const sessions = [...this.sessions.values()]
    this.sessions.clear()
    await Promise.allSettled(sessions.map((session) => session.close()))
  }

  private async releaseIdleSession<T>(key: string, session: SharedSession<T>): Promise<void> {
    const idleCloseMs = this.options.idleCloseMs ?? 0
    if (idleCloseMs === 0) {
      this.sessions.delete(key)
      await session.close()
      return
    }

    const timer = setTimeout(() => {
      this.cleanupTimers.delete(key)
      if (this.sessions.get(key) === session) {
        this.sessions.delete(key)
        void session.close().catch(() => undefined)
      }
    }, idleCloseMs)
    this.cleanupTimers.set(key, timer)
  }
}

export function createSessionRegistry(options: SessionRegistryOptions = {}): SessionRegistry {
  return new DefaultSessionRegistry(options)
}
