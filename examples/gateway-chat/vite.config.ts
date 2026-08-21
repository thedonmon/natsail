import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [tailwindcss(), react()],
  server: {
    host: '127.0.0.1',
    port: 4174,
    strictPort: true,
    proxy: {
      '/gateway': {
        target: 'http://127.0.0.1:8791',
        ws: true,
      },
      '/health': 'http://127.0.0.1:8791',
    },
  },
})
