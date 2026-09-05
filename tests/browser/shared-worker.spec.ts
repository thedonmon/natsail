import { expect, test } from '@playwright/test'

test('releases a crashed tab while the surviving tab keeps receiving', async ({ context }) => {
  const first = await context.newPage()
  const second = await context.newPage()
  for (const page of [first, second]) {
    await page.goto('/shared-worker.html')
    await expect(page.locator('html')).toHaveAttribute('data-natsail-browser-broker-ready', 'true')
    await page.evaluate(() => window.natsailBrowserBroker.subscribe('acceptance-events'))
  }
  await first.close()
  await expect
    .poll(() => second.evaluate(() => window.natsailBrowserBroker.stats()), { timeout: 10_000 })
    .toMatchObject({ tabCount: 1, subscriptionCount: 1, physicalSourceCount: 1 })
  await second.evaluate(() =>
    window.natsailBrowserBroker.publish('publish-acceptance-event', 'still-live')
  )
  await expect(
    second.evaluate(() => window.natsailBrowserBroker.nextMessage('acceptance-events'))
  ).resolves.toBe('still-live')
  await second.evaluate(() => window.natsailBrowserBroker.close())
})

test('reattaches a live source after the worker realm is terminated', async ({ page }) => {
  await page.goto('/shared-worker.html')
  await expect(page.locator('html')).toHaveAttribute('data-natsail-browser-broker-ready', 'true')
  await page.evaluate(() => window.natsailBrowserBroker.subscribe('acceptance-events'))
  await page.evaluate(() => window.natsailBrowserBroker.request('terminate-test-worker', ''))
  await expect
    .poll(
      () =>
        page.evaluate(() =>
          window.natsailBrowserBroker.stats().then(
            () => true,
            () => false
          )
        ),
      { timeout: 8_000 }
    )
    .toBe(false)
  await page.evaluate(() => window.natsailBrowserBroker.reconnect())
  await expect(page.evaluate(() => window.natsailBrowserBroker.stats())).resolves.toMatchObject({
    physicalSourceCount: 1,
    subscriptionCount: 1,
  })
  await page.evaluate(() =>
    window.natsailBrowserBroker.publish('publish-acceptance-event', 'replacement-live')
  )
  await expect(
    page.evaluate(() => window.natsailBrowserBroker.nextMessage('acceptance-events'))
  ).resolves.toBe('replacement-live')
  await page.evaluate(() => window.natsailBrowserBroker.close())
})

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
