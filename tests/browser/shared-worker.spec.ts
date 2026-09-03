import { expect, test } from '@playwright/test'

test('shares one physical SessionSource across two tabs', async ({ context }) => {
  const first = await context.newPage()
  const second = await context.newPage()
  const subject = `tests.browser-broker.${crypto.randomUUID()}`

  await Promise.all([first.goto('/shared-worker.html'), second.goto('/shared-worker.html')])
  await Promise.all(
    [first, second].map((page) =>
      expect(page.locator('html')).toHaveAttribute('data-natsail-browser-broker-ready', 'true')
    )
  )
  await Promise.all(
    [first, second].map((page) =>
      page.evaluate(
        (activeSubject) => window.natsailBrowserBroker.subscribe(activeSubject),
        subject
      )
    )
  )

  const firstDelivery = first.evaluate(
    (activeSubject) => window.natsailBrowserBroker.nextMessage(activeSubject),
    subject
  )
  const secondDelivery = second.evaluate(
    (activeSubject) => window.natsailBrowserBroker.nextMessage(activeSubject),
    subject
  )
  await first.evaluate(
    ({ activeSubject, value }) => window.natsailBrowserBroker.publish(activeSubject, value),
    { activeSubject: subject, value: 'one-physical-source' }
  )

  await expect(Promise.all([firstDelivery, secondDelivery])).resolves.toEqual([
    'one-physical-source',
    'one-physical-source',
  ])
  await expect(first.evaluate(() => window.natsailBrowserBroker.stats())).resolves.toMatchObject({
    tabCount: 2,
    activeConnectionCount: 1,
    physicalSourceCount: 1,
    subscriptionCount: 2,
  })

  await Promise.all(
    [first, second].map((page) => page.evaluate(() => window.natsailBrowserBroker.close()))
  )
})
