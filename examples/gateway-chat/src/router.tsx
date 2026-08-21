import { QueryClient, useMutation, useSuspenseQuery } from '@tanstack/react-query'
import {
  Outlet,
  createRootRouteWithContext,
  createRoute,
  createRouter,
  redirect,
} from '@tanstack/react-router'
import { useEffect, useMemo } from 'react'

import { rooms, Workspace, type ChatMessage } from '@natsail/example-chat-ui'
import { gatewayFor } from './gateway'
import { gatewayQueries } from './queries'

const tenant = 'demo'

interface RouterContext {
  queryClient: QueryClient
}

const rootRoute = createRootRouteWithContext<RouterContext>()({
  component: () => <Outlet />,
  notFoundComponent: () => (
    <main className="route-error">
      <strong>404 / ROUTE NOT FOUND</strong>
      <a href="/rooms/gateway-lab">Return to the gateway bench</a>
    </main>
  ),
  errorComponent: ({ error }) => (
    <main className="route-error">
      <strong>GATEWAY BENCH FAILED TO LOAD</strong>
      <p>{error.message}</p>
      <p>Run `pnpm example:gateway-chat` so the local gateway and NATS fixture are available.</p>
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
  loader: async ({ context: { queryClient } }) => {
    await Promise.all([
      queryClient.ensureQueryData(gatewayQueries.rooms()),
      queryClient.ensureQueryData(gatewayQueries.timeline(tenant)),
    ])
  },
  component: ChatRoom,
})

const routeTree = rootRoute.addChildren([indexRoute, roomRoute])

const names = ['Avery', 'Mika', 'Noor', 'Sol', 'Tess', 'Zed']

const operatorName = (): string => {
  const existing = window.sessionStorage.getItem('natsail-gateway-example-operator')
  if (existing) return existing
  const name = `${names[Math.floor(Math.random() * names.length)]}-${Math.floor(10 + Math.random() * 90)}`
  window.sessionStorage.setItem('natsail-gateway-example-operator', name)
  return name
}

function ChatRoom() {
  const queryClient = roomRoute.useRouteContext({ select: (context) => context.queryClient })
  const { roomId } = roomRoute.useParams()
  const navigate = roomRoute.useNavigate()
  const { data: roomList } = useSuspenseQuery(gatewayQueries.rooms())
  const { data: timeline } = useSuspenseQuery(gatewayQueries.timeline(tenant))
  const room = roomList.find((candidate) => candidate.id === roomId) ?? rooms[0]!
  const author = useMemo(operatorName, [])
  const gateway = useMemo(() => gatewayFor(queryClient, tenant), [queryClient])

  useEffect(() => {
    gateway.connect()
    return () => gateway.disconnect()
  }, [gateway])

  const send = useMutation({
    mutationFn: (message: ChatMessage) => gateway.publish(message),
    onMutate: (message) => gateway.addOptimistic(message),
    onError: (error, message) => gateway.failOptimistic(message.id, error),
  })

  return (
    <Workspace
      mode="gateway"
      rooms={roomList}
      room={room}
      timeline={timeline}
      clientId={gateway.clientId}
      author={author}
      applicationSubtitle="GATEWAY HYPOTHESIS BENCH"
      architectureDescription="Browser tab → tenant Durable Object → one shared NATSail JetStream consumer."
      topologyLabel="ONE OBJECT · MANY ROOMS"
      upstreamLabel="gateway"
      connectionAction={{
        label: timeline.phase === 'live' ? 'Simulate gap' : 'Reconnect',
        onClick: () => {
          if (timeline.phase === 'live') gateway.disconnect()
          else gateway.connect()
        },
      }}
      onRoomChange={(nextRoomId) =>
        void navigate({
          to: '/rooms/$roomId',
          params: { roomId: nextRoomId },
        })
      }
      onSend={async (body) => {
        const message: ChatMessage = {
          id: crypto.randomUUID(),
          roomId: room.id,
          author,
          body,
          sentAt: new Date().toISOString(),
          clientId: gateway.clientId,
        }
        await send.mutateAsync(message)
      }}
    />
  )
}

export const createGatewayExampleRouter = () => {
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

  return { router, queryClient }
}

declare module '@tanstack/react-router' {
  interface Register {
    router: ReturnType<typeof createGatewayExampleRouter>['router']
  }
}
