import { createRoot } from 'react-dom/client'

import '@natsail/example-chat-ui/styles.css'
import { App, closeChatController } from './app'
import { closeExampleRuntime } from './runtime'

const root = document.getElementById('app')
if (!root) throw new Error('Missing Effect example application root')

window.addEventListener(
  'pagehide',
  () => void Promise.allSettled([closeChatController(), closeExampleRuntime()]),
  { once: true }
)

createRoot(root).render(<App />)
