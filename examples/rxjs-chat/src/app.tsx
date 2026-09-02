import { useMemo, useState, useSyncExternalStore } from 'react'
import {
  catchError,
  combineLatest,
  distinctUntilChanged,
  EMPTY,
  map,
  shareReplay,
  startWith,
  tap,
  type Observable,
  type Subscription,
} from 'rxjs'

import type { NatsRuntimeConnectionState } from '@natsail/core'
import {
  rooms,
  Workspace,
  type ChatMessage,
  type ConnectionPhase,
  type TimelineEntry,
  type TimelineState,
} from '@natsail/example-chat-ui'
import type { JetStreamStateSnapshot } from '@natsail/jetstream'
import {
  observeNatsJetStreamState,
  observeNatsRuntimeStatus,
  observeNatsSessionEvents,
} from '@natsail/rxjs'
import { ChatFeed, type ChatFeedModel } from './chat-feed'
import { runtime, sessions } from './runtime'

const names = ['Avery', 'Mika', 'Noor', 'Sol', 'Tess', 'Zed']

const readIdentity = (): { author: string; clientId: string } => {
  const key = 'natsail-rxjs-chat.identity.v1'
  const stored = window.sessionStorage.getItem(key)
  if (stored) {
    try {
      const identity = JSON.parse(stored) as { author?: unknown; clientId?: unknown }
      if (typeof identity.author === 'string' && typeof identity.clientId === 'string') {
        return { author: identity.author, clientId: identity.clientId }
      }
    } catch {
      // Replace malformed example-only session data below.
    }
  }

  const identity = {
    author: `${names[Math.floor(Math.random() * names.length)]}-${Math.floor(10 + Math.random() * 90)}`,
    clientId: `rxjs-${crypto.randomUUID().slice(0, 8)}`,
  }
  window.sessionStorage.setItem(key, JSON.stringify(identity))
  return identity
}

const identity = readIdentity()
const feed = new ChatFeed(runtime)

const feedSnapshot$ = observeNatsJetStreamState(sessions, feed.definition, {
  liveBatchMs: 16,
}).pipe(
  tap((snapshot) => feed.follow(snapshot)),
  catchError((error) => {
    feed.follow(undefined, error)
    return EMPTY
  }),
  shareReplay({ bufferSize: 1, refCount: true })
)

const observedRoomCount$ = feedSnapshot$.pipe(
  map((snapshot) => snapshot.data.roomIds.length),
  distinctUntilChanged()
)

const registryInspection$ = observeNatsSessionEvents(sessions).pipe(
  map(() => sessions.inspect()),
  startWith(sessions.inspect())
)

interface ChatView {
  entries: TimelineEntry[]
  snapshot?: JetStreamStateSnapshot<ChatFeedModel>
  runtimeState: NatsRuntimeConnectionState
  feedState: ReturnType<typeof feed.state$.getValue>
  observedRoomCount: number
  sessionReferences: number
}

const view$ = combineLatest([
  feedSnapshot$,
  observedRoomCount$,
  feed.state$,
  observeNatsRuntimeStatus(runtime),
  registryInspection$,
]).pipe(
  map(
    ([snapshot, observedRoomCount, feedState, runtimeStatus, inspection]): ChatView => ({
      entries: [...snapshot.data.entries],
      snapshot,
      observedRoomCount,
      sessionReferences: inspection.sessions[0]?.references ?? 0,
      feedState,
      runtimeState: runtimeStatus.state,
    })
  ),
  shareReplay({ bufferSize: 1, refCount: true })
)

const initialView: ChatView = {
  entries: [],
  observedRoomCount: 0,
  sessionReferences: 0,
  feedState: feed.state$.value,
  runtimeState: 'idle',
}

const createObservableStore = <T,>(observable: Observable<T>, initial: T) => {
  let snapshot = initial
  let subscription: Subscription | undefined
  const listeners = new Set<() => void>()

  const emit = () => {
    for (const listener of listeners) listener()
  }

  return {
    getSnapshot: () => snapshot,
    subscribe: (listener: () => void) => {
      listeners.add(listener)
      if (!subscription) {
        subscription = observable.subscribe({
          next: (value) => {
            snapshot = value
            emit()
          },
        })
      }
      return () => {
        listeners.delete(listener)
        if (listeners.size === 0) {
          subscription?.unsubscribe()
          subscription = undefined
        }
      }
    },
  }
}

const viewStore = createObservableStore(view$, initialView)

const connectionPhase = (
  runtimeState: NatsRuntimeConnectionState,
  feedPhase: ChatView['feedState']['phase']
): ConnectionPhase => {
  if (feedPhase === 'error') return 'error'
  if (feedPhase === 'offline' || runtimeState === 'closed') return 'offline'
  if (feedPhase === 'gap') return 'gap'
  if (feedPhase === 'catching-up') return 'catching-up'
  if (feedPhase === 'live' && runtimeState === 'connected') return 'live'
  return 'connecting'
}

export function App() {
  const view = useSyncExternalStore(
    viewStore.subscribe,
    viewStore.getSnapshot,
    viewStore.getSnapshot
  )
  const [roomId, setRoomId] = useState('gateway-lab')
  const room = useMemo(
    () => rooms.find((candidate) => candidate.id === roomId) ?? rooms[0]!,
    [roomId]
  )
  const cursor = view.entries.at(-1)?.cursor ?? 0
  const phase = connectionPhase(view.runtimeState, view.feedState.phase)
  const timeline: TimelineState = {
    messages: view.entries,
    cursor,
    phase,
    catchUpCount: view.feedState.catchUpCount,
    gatewayCursor: cursor,
    ...(view.feedState.retainedFrom === undefined
      ? {}
      : { retainedFrom: view.feedState.retainedFrom }),
    ...(view.feedState.retainedThrough === undefined
      ? {}
      : { retainedThrough: view.feedState.retainedThrough }),
    ...(view.feedState.diagnostic === undefined ? {} : { diagnostic: view.feedState.diagnostic }),
  }

  return (
    <Workspace
      mode="rxjs"
      rooms={rooms}
      room={room}
      timeline={timeline}
      clientId={identity.clientId}
      author={identity.author}
      applicationSubtitle="RXJS JETSTREAM CHAT"
      architectureDescription={`One frame-coalesced RxJS state stream feeds two projections (${view.observedRoomCount} rooms observed, ${view.sessionReferences} active reference) from one atomic reducer session and ordered JetStream consumer.`}
      topologyLabel="ATOMIC REPLAY · FRAME-BATCHED LIVE STATE · ONE CONSUMER"
      upstreamLabel="JetStream"
      sourceStarts={1 + (view.snapshot?.restarts ?? 0)}
      connectionAction={{
        label:
          phase === 'gap'
            ? 'Reconnecting transport…'
            : phase === 'catching-up'
              ? 'Publishing recovery messages…'
              : 'Reconnect and publish 3',
        disabled: phase !== 'live',
        onClick: () => void feed.recover(room.id).catch(() => undefined),
      }}
      onRoomChange={setRoomId}
      onSend={async (body) =>
        feed.publish({
          id: crypto.randomUUID(),
          roomId: room.id,
          author: identity.author,
          body,
          sentAt: new Date().toISOString(),
          clientId: identity.clientId,
        })
      }
    />
  )
}
