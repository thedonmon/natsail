import { createRoot } from 'react-dom/client'

import '@natsail/example-chat-ui/styles.css'
import { App } from './app'
import { closeExampleRuntime } from './runtime'

const root = document.getElementById('app')
if (!root) throw new Error('Missing RxJS example application root')

window.addEventListener('pagehide', () => void closeExampleRuntime(), { once: true })

createRoot(root).render(<App />)
