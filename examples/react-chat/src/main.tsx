import { QueryClientProvider } from '@tanstack/react-query'
import { RouterProvider } from '@tanstack/react-router'
import { createRoot } from 'react-dom/client'

import '@natsail/example-chat-ui/styles.css'
import { NatsProvider } from '@natsail/react'
import { closeExampleRuntime, runtime, sessions } from './runtime'
import { createReactExampleRouter } from './router'

const root = document.getElementById('app')
if (!root) throw new Error('Missing React example application root')

const { router, queryClient } = createReactExampleRouter()

window.addEventListener('pagehide', () => void closeExampleRuntime(), { once: true })

createRoot(root).render(
  <QueryClientProvider client={queryClient}>
    <NatsProvider runtime={runtime} sessions={sessions}>
      <RouterProvider router={router} />
    </NatsProvider>
  </QueryClientProvider>
)
