import { expect, test } from '@playwright/test'

test('delivers through NATS WebSocket from the Cloudflare Workers runtime', async ({ request }) => {
  const response = await request.get('http://127.0.0.1:8787/probe')

  expect(response.ok()).toBe(true)
  await expect(response.json()).resolves.toEqual({
    connectionRequests: 1,
    received: 'cloudflare-websocket',
    transport: 'websocket',
    userAgent: 'Cloudflare-Workers',
    webSocketType: 'function',
  })
})

test('delivers through NATS TCP from the Cloudflare Workers runtime', async ({ request }) => {
  const response = await request.get('http://127.0.0.1:8787/probe-tcp')

  expect(response.ok()).toBe(true)
  await expect(response.json()).resolves.toEqual({
    connectionRequests: 1,
    received: 'cloudflare-tcp',
    transport: 'tcp',
    userAgent: 'Cloudflare-Workers',
    webSocketType: 'function',
  })
})
