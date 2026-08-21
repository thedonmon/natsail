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
  release(): Promise<void>
}

export interface SessionRegistry {
  acquire<T>(key: string, source: SessionSource<T>): SessionHandle<T>
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
  readonly ready: Promise<void>

  private snapshot: SessionSnapshot<T> = {
    phase: 'connecting',
    revision: 0,
    valueRevision: 0,
  }
  private readonly listeners = new Set<SessionListener>()
  private readonly lease: SubscriptionLease
  private closePromise?: Promise<void>
  private references = 0

  constructor(source: SessionSource<T>) {
    this.lease = source(async (value) => this.accept(value))
    this.ready = this.activate()
    void this.ready.catch(() => undefined)
    void this.lease.closed.then(
      () => this.finish('closed'),
      (error: unknown) => this.finish('error', error)
    )
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
    this.closePromise ??= this.lease.close()
    return this.closePromise
  }

  private async activate(): Promise<void> {
    try {
      await this.lease.ready
      this.update({
        ...this.snapshot,
        phase: 'live',
        revision: this.snapshot.revision + 1,
      })
    } catch (error) {
      this.finish('error', error)
      throw error
    }
  }

  private async accept(value: T): Promise<void> {
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

  private finish(phase: 'closed' | 'error', error?: unknown): void {
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
      ready: session.ready,
      getSnapshot: () => session.getSnapshot(),
      subscribe: (listener) => session.subscribe(listener),
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
