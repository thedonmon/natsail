import { expect, test } from '@playwright/test'

test('shares one physical SessionSource across two tabs', async ({ context }) => {
  const first = await context.newPage()
  const second = await context.newPage()
  const source = 'acceptance-events'

  await Promise.all([first.goto('/shared-worker.html'), second.goto('/shared-worker.html')])
  await Promise.all(
    [first, second].map((page) =>
      expect(page.locator('html')).toHaveAttribute('data-natsail-browser-broker-ready', 'true')
    )
  )
  await Promise.all(
    [first, second].map((page) =>
      page.evaluate((sourceKey) => window.natsailBrowserBroker.subscribe(sourceKey), source)
    )
  )

  const firstDelivery = first.evaluate(
    (sourceKey) => window.natsailBrowserBroker.nextMessage(sourceKey),
    source
  )
  const secondDelivery = second.evaluate(
    (sourceKey) => window.natsailBrowserBroker.nextMessage(sourceKey),
    source
  )
  await first.evaluate(
    ({ operation, value }) => window.natsailBrowserBroker.publish(operation, value),
    { operation: 'publish-acceptance-event', value: 'one-physical-source' }
  )

  await expect(Promise.all([firstDelivery, secondDelivery])).resolves.toEqual([
    'one-physical-source',
    'one-physical-source',
  ])
  await expect(
    first.evaluate(
      ({ operation, value }) => window.natsailBrowserBroker.request(operation, value),
      { operation: 'echo', value: 'brokered-request' }
    )
  ).resolves.toBe('brokered-request')
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
