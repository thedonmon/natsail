import { QueryClientProvider } from '@tanstack/react-query'
import { RouterProvider } from '@tanstack/react-router'
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'

import '@natsail/example-chat-ui/styles.css'
import { NatsManagedProvider } from '@natsail/react'
import { createExampleNatsResource } from './runtime'
import { createReactExampleRouter } from './router'

const root = document.getElementById('app')
if (!root) throw new Error('Missing React example application root')

const { router, queryClient } = createReactExampleRouter()

createRoot(root).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <NatsManagedProvider
        identity="local-react-chat"
        create={createExampleNatsResource}
        fallback={<main className="route-error">Opening the shared NATS runtime…</main>}
      >
        <RouterProvider router={router} />
      </NatsManagedProvider>
    </QueryClientProvider>
  </StrictMode>
)
