import { expect, test } from '@playwright/test'

test('replaces two per-tab connections with one SharedWorker runtime', async ({ context }) => {
  const first = await context.newPage()
  const second = await context.newPage()
  const subject = `tests.shared-worker.${crypto.randomUUID()}`

  await Promise.all([first.goto('/'), second.goto('/')])
  await Promise.all(
    [first, second].map((page) =>
      expect(page.locator('html')).toHaveAttribute('data-natsail-ready', 'true')
    )
  )
  const perTabConnectionRequests = await Promise.all(
    [first, second].map((page) => page.evaluate(() => window.openNatsailBrowserConnection()))
  )
  expect(perTabConnectionRequests).toEqual([1, 1])

  await Promise.all([first.goto('/shared-worker.html'), second.goto('/shared-worker.html')])
  await Promise.all(
    [first, second].map((page) =>
      expect(page.locator('html')).toHaveAttribute('data-natsail-shared-worker-ready', 'true')
    )
  )

  await Promise.all(
    [first, second].map((page) =>
      page.evaluate((activeSubject) => window.natsailSharedWorker.subscribe(activeSubject), subject)
    )
  )

  const firstDelivery = first.evaluate(
    (activeSubject) => window.natsailSharedWorker.nextMessage(activeSubject),
    subject
  )
  const secondDelivery = second.evaluate(
    (activeSubject) => window.natsailSharedWorker.nextMessage(activeSubject),
    subject
  )

  await first.evaluate(
    ({ activeSubject, value }) => window.natsailSharedWorker.publish(activeSubject, value),
    { activeSubject: subject, value: 'shared-connection' }
  )

  await expect(Promise.all([firstDelivery, secondDelivery])).resolves.toEqual([
    'shared-connection',
    'shared-connection',
  ])

  const stats = await first.evaluate(() => window.natsailSharedWorker.stats())
  expect(stats).toEqual({ clientCount: 2, connectionRequests: 1, subscriptionCount: 2 })

  await Promise.all(
    [first, second].map((page) => page.evaluate(() => window.natsailSharedWorker.close()))
  )
})

declare global {
  interface Window {
    openNatsailBrowserConnection: () => Promise<number>
  }
}
