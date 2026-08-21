import { createRoot } from 'react-dom/client'

import '@natsail/example-chat-ui/styles.css'
import { NatsProvider } from '@natsail/react'
import { App } from './app'
import './styles.css'
import { closeExampleRuntime, runtime, sessions } from './runtime'

const root = document.getElementById('app')
if (!root) throw new Error('Missing AI transport example application root')

window.addEventListener('pagehide', () => void closeExampleRuntime(), { once: true })

createRoot(root).render(
  <NatsProvider runtime={runtime} sessions={sessions}>
    <App />
  </NatsProvider>
)
