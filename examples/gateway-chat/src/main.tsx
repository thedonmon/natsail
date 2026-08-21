import { QueryClientProvider } from '@tanstack/react-query'
import { RouterProvider } from '@tanstack/react-router'
import { createRoot } from 'react-dom/client'

import '@natsail/example-chat-ui/styles.css'
import { createGatewayExampleRouter } from './router'

const root = document.getElementById('app')
if (!root) throw new Error('Missing gateway example application root')

const { router, queryClient } = createGatewayExampleRouter()

createRoot(root).render(
  <QueryClientProvider client={queryClient}>
    <RouterProvider router={router} />
  </QueryClientProvider>
)
