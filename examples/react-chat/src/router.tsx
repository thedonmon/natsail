import { QueryClient, queryOptions, useSuspenseQuery } from '@tanstack/react-query'
import {
  Outlet,
  createRootRouteWithContext,
  createRoute,
  createRouter,
  redirect,
} from '@tanstack/react-router'
import { useCallback, useEffect, useRef, useState } from 'react'

import type { CoreSubscriptionOptions, NatsRuntimeConnectionState } from '@natsail/core'
import {
  isChatMessage,
  rooms,
  Workspace,
  type ChatMessage,
  type ConnectionPhase,
  type TimelineEntry,
  type TimelineState,
} from '@natsail/example-chat-ui'
import {
  useNatsCoreSubscriptionReducer,
  useNatsRuntime,
  useNatsRuntimeStatus,
} from '@natsail/react'
import type { SessionPhase } from '@natsail/session'

const subjectPrefix = 'example.chat'
const encoder = new TextEncoder()
const decoder = new TextDecoder()
const emptyTimeline: TimelineEntry[] = []
const names = ['Avery', 'Mika', 'Noor', 'Sol', 'Tess', 'Zed']

const chatFeedOptions: CoreSubscriptionOptions<ChatMessage> = {
  subject: `${subjectPrefix}.>`,
  decode: (message) => {
    const value: unknown = JSON.parse(decoder.decode(message.data))
    if (!isChatMessage(value)) throw new Error('Received an invalid chat message')
    return value
  },
}

const roomQuery = queryOptions({
  queryKey: ['react-chat-example', 'rooms'],
  queryFn: async () => rooms,
  staleTime: Number.POSITIVE_INFINITY,
  gcTime: Number.POSITIVE_INFINITY,
})

const reduceMessages = (entries: TimelineEntry[], message: ChatMessage): TimelineEntry[] => {
  if (entries.some((entry) => entry.message.id === message.id)) return entries
  const cursor = (entries.at(-1)?.cursor ?? 0) + 1
  return [...entries, { message, cursor, delivery: 'applied' as const }].slice(-256)
}

const readIdentity = (): { author: string; clientId: string } => {
  const key = 'natsail-react-chat.identity.v1'
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
    clientId: `react-${crypto.randomUUID().slice(0, 8)}`,
  }
  window.sessionStorage.setItem(key, JSON.stringify(identity))
  return identity
}

const identity = readIdentity()

interface RouterContext {
  queryClient: QueryClient
}

const rootRoute = createRootRouteWithContext<RouterContext>()({
  component: () => <Outlet />,
  notFoundComponent: () => (
    <main className="route-error">
      <strong>404 / ROOM ROUTE NOT FOUND</strong>
      <a href="/rooms/gateway-lab">Return to the React rooms example</a>
    </main>
  ),
  errorComponent: ({ error }) => (
    <main className="route-error">
      <strong>REACT ROOMS EXAMPLE FAILED TO LOAD</strong>
      <p>{error.message}</p>
      <p>Run `pnpm example:react-chat` so the local NATS WebSocket is available.</p>
    </main>
  ),
})

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  beforeLoad: () => {
    throw redirect({
      to: '/rooms/$roomId',
      params: { roomId: 'gateway-lab' },
    })
  },
})

const roomRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/rooms/$roomId',
  loader: ({ context: { queryClient } }) => queryClient.ensureQueryData(roomQuery),
  component: ReactChatRoom,
})

const routeTree = rootRoute.addChildren([indexRoute, roomRoute])

const connectionPhase = (
  runtimeState: NatsRuntimeConnectionState,
  sessionPhase: SessionPhase
): ConnectionPhase => {
  if (sessionPhase === 'error') return 'error'
  if (sessionPhase === 'closed' || runtimeState === 'closed') return 'offline'
  if (sessionPhase === 'live' && runtimeState === 'connected') return 'live'
  return 'connecting'
}

const seedMessages = async (publish: (message: ChatMessage) => Promise<void>): Promise<void> => {
  const values = [
    [
      'general',
      'Mika / systems',
      'This room is a live Core NATS subscription through @natsail/react.',
    ],
    [
      'gateway-lab',
      'Noor / frontend',
      'The provider owns one runtime and one shared session registry.',
    ],
    ['gateway-lab', 'Avery / app', 'A reducer session folds every delivery before React renders.'],
    [
      'edge-cases',
      'Sol / runtime',
      'Core NATS is live-only: reloads do not replay prior room messages.',
    ],
    ['release', 'Tess / release', 'This example is private workspace code and is never published.'],
  ]

  for (const [roomId, author, body] of values) {
    await publish({
      id: crypto.randomUUID(),
      roomId: roomId!,
      author: author!,
      body: body!,
      sentAt: new Date().toISOString(),
      clientId: 'example-seed',
    })
  }
}

function ReactChatRoom() {
  const { roomId } = roomRoute.useParams()
  const navigate = roomRoute.useNavigate()
  const { data: roomList } = useSuspenseQuery(roomQuery)
  const runtime = useNatsRuntime()
  const runtimeStatus = useNatsRuntimeStatus()
  const snapshot = useNatsCoreSubscriptionReducer(
    'react-chat:all-rooms',
    chatFeedOptions,
    () => emptyTimeline,
    reduceMessages
  )
  const [diagnostic, setDiagnostic] = useState<string>()
  const seeded = useRef(false)
  const room = roomList.find((candidate) => candidate.id === roomId) ?? rooms[0]!
  const messages = snapshot.value ?? emptyTimeline
  const cursor = messages.at(-1)?.cursor ?? 0
  const phase = connectionPhase(runtimeStatus.state, snapshot.phase)
  const shouldSeed = phase === 'live' && snapshot.valueRevision === 0

  const publish = useCallback(
    async (message: ChatMessage): Promise<void> => {
      await runtime.publish(
        `${subjectPrefix}.${message.roomId}`,
        encoder.encode(JSON.stringify(message))
      )
    },
    [runtime]
  )

  useEffect(() => {
    if (!shouldSeed || seeded.current) return
    seeded.current = true
    void seedMessages(publish).catch((error: unknown) => {
      setDiagnostic(error instanceof Error ? error.message : String(error))
    })
  }, [publish, shouldSeed])

  const timeline: TimelineState = {
    messages,
    cursor,
    phase,
    catchUpCount: 0,
    gatewayCursor: cursor,
    ...(snapshot.error === undefined
      ? diagnostic === undefined
        ? {}
        : { diagnostic }
      : {
          diagnostic:
            snapshot.error instanceof Error ? snapshot.error.message : String(snapshot.error),
        }),
  }

  return (
    <Workspace
      mode="core"
      rooms={roomList}
      room={room}
      timeline={timeline}
      clientId={identity.clientId}
      author={identity.author}
      applicationSubtitle="REACT PRIMITIVE BENCH"
      architectureDescription="React hook → shared session registry → one NATSail Core subscription and connection."
      topologyLabel="ONE RUNTIME · MANY ROOMS"
      upstreamLabel="runtime"
      onRoomChange={(nextRoomId) =>
        void navigate({
          to: '/rooms/$roomId',
          params: { roomId: nextRoomId },
        })
      }
      onSend={async (body) => {
        setDiagnostic(undefined)
        try {
          await publish({
            id: crypto.randomUUID(),
            roomId: room.id,
            author: identity.author,
            body,
            sentAt: new Date().toISOString(),
            clientId: identity.clientId,
          })
        } catch (error) {
          setDiagnostic(error instanceof Error ? error.message : String(error))
          throw error
        }
      }}
    />
  )
}

export const createReactExampleRouter = () => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: 1,
        refetchOnWindowFocus: false,
        staleTime: Number.POSITIVE_INFINITY,
      },
    },
  })
  const router = createRouter({
    routeTree,
    context: { queryClient },
    defaultPreload: 'intent',
    defaultPreloadStaleTime: 0,
    scrollRestoration: true,
  })
  return { queryClient, router }
}

declare module '@tanstack/react-router' {
  interface Register {
    router: ReturnType<typeof createReactExampleRouter>['router']
  }
}
