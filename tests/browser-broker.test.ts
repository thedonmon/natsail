import { describe, expect, it, vi } from 'vitest'

import {
  BrowserBrokerError,
  BrowserBrokerResumeRequiredError,
  createBrowserBrokerClient,
  createBrowserBrokerWorker,
  createTabLocalBrokerConnector,
  NATS_BROWSER_BROKER_PROTOCOL,
  NATS_BROWSER_BROKER_PROTOCOL_VERSION,
  parseBrowserBrokerCommand,
  parseBrowserBrokerMessage,
  type BrowserBrokerCredentialSnapshot,
  type BrowserBrokerCursor,
  type BrowserBrokerDelivery,
  type BrowserBrokerSourceContext,
  type BrowserBrokerSourceFactory,
  type BrowserBrokerWorkerHost,
} from '@natsail/browser-broker'
import type { NatsailTelemetryEvent, SubscriptionLease } from '@natsail/core'
import { createSessionRegistry, type SessionRegistry } from '@natsail/session'

const identity = { tenant: 'tenant-a', authenticationContext: 'browser-user-v1' }
const descriptor = { key: 'conversation', contract: 'conversation-events:v1' }
const encoder = new TextEncoder()
const decoder = new TextDecoder()

function cursor(sequence: number): BrowserBrokerCursor {
  return { stream: 'PRIVATE_STREAM', epoch: 'epoch-1', sequence }
}

function connector(host: BrowserBrokerWorkerHost) {
  return () => {
    const channel = new MessageChannel()
    host.connect(channel.port1)
    return channel.port2
  }
}

function credentials(revision = 1): BrowserBrokerCredentialSnapshot {
  return { revision, bytes: encoder.encode(`credential-${revision}`) }
}

function controlledSource() {
  const contexts: BrowserBrokerSourceContext[] = []
  const accepts: Array<(delivery: BrowserBrokerDelivery) => Promise<void>> = []
  let opens = 0
  let closes = 0

  const factory: BrowserBrokerSourceFactory = (context) => {
    contexts.push(context)
    return (accept) => {
      opens += 1
      accepts.push(accept)
      let resolveClosed!: () => void
      const closed = new Promise<void>((resolve) => {
        resolveClosed = resolve
      })
      return {
        ready: Promise.resolve(),
        closed,
        close: async () => {
          closes += 1
          resolveClosed()
        },
      } satisfies SubscriptionLease
    }
  }

  return {
    factory,
    contexts,
    get opens() {
      return opens
    },
    get closes() {
      return closes
    },
    emit: (source: number, value: string, activeCursor?: BrowserBrokerCursor) =>
      accepts[source]!({
        data: encoder.encode(value),
        ...(activeCursor === undefined ? {} : { cursor: activeCursor }),
      }),
  }
}

function createHost(
  source: ReturnType<typeof controlledSource>,
  options: Parameters<typeof createBrowserBrokerWorker>[0] = {
    sessions: createSessionRegistry(),
    createSource: source.factory,
  }
) {
  return createBrowserBrokerWorker({
    ...options,
    sweepIntervalMs: options.sweepIntervalMs ?? 0,
  })
}

async function closeAll(
  clients: Array<{ close(): Promise<void> }>,
  hosts: BrowserBrokerWorkerHost[],
  sessions: SessionRegistry[]
) {
  await Promise.all(clients.map((client) => client.close()))
  await Promise.all(hosts.map((host) => host.close()))
  await Promise.all(sessions.map((registry) => registry.close()))
}

describe('@natsail/browser-broker', () => {
  it('validates protocol version and every command shape', () => {
    expect(() =>
      parseBrowserBrokerCommand({
        protocol: NATS_BROWSER_BROKER_PROTOCOL,
        version: 2,
        type: 'stats',
        requestId: 1,
      })
    ).toThrow(expect.objectContaining({ code: 'protocol-version' }))

    expect(() =>
      parseBrowserBrokerCommand({
        protocol: NATS_BROWSER_BROKER_PROTOCOL,
        version: NATS_BROWSER_BROKER_PROTOCOL_VERSION,
        type: 'ack',
        requestId: 1,
        subscriptionId: 'source-1',
        batchId: 0,
      })
    ).toThrow(expect.objectContaining({ code: 'invalid-command' }))

    expect(
      parseBrowserBrokerCommand({
        protocol: NATS_BROWSER_BROKER_PROTOCOL,
        version: NATS_BROWSER_BROKER_PROTOCOL_VERSION,
        type: 'hello',
        requestId: 1,
        identity,
        credentialRevision: 1,
        credentials: new ArrayBuffer(0),
      })
    ).toMatchObject({ type: 'hello', identity })

    const result = {
      protocol: NATS_BROWSER_BROKER_PROTOCOL,
      version: NATS_BROWSER_BROKER_PROTOCOL_VERSION,
      type: 'result',
      requestId: 1,
    }
    expect(() =>
      parseBrowserBrokerMessage({
        ...result,
        ok: true,
        error: { code: 'unavailable', message: 'contradiction' },
      })
    ).toThrow(expect.objectContaining({ code: 'invalid-state' }))
    expect(() => parseBrowserBrokerMessage({ ...result, ok: false })).toThrow(
      expect.objectContaining({ code: 'invalid-state' })
    )
    expect(() =>
      parseBrowserBrokerMessage({
        ...result,
        ok: false,
        result: 'contradiction',
        error: { code: 'unavailable', message: 'failed' },
      })
    ).toThrow(expect.objectContaining({ code: 'invalid-state' }))
    expect(parseBrowserBrokerMessage({ ...result, ok: true, result: 'accepted' })).toMatchObject({
      ok: true,
      result: 'accepted',
    })
  })

  it('shares one physical SessionSource for two tabs and reference-counts final teardown', async () => {
    const source = controlledSource()
    const sessions = createSessionRegistry()
    const telemetry: NatsailTelemetryEvent[] = []
    const host = createHost(source, {
      sessions,
      createSource: source.factory,
      sweepIntervalMs: 0,
      telemetry: { record: (event) => telemetry.push(event) },
      telemetryClock: { now: () => 100 },
    })
    const first = await createBrowserBrokerClient({
      identity,
      credentials,
      connect: connector(host),
      heartbeatIntervalMs: 0,
    })
    const second = await createBrowserBrokerClient({
      identity,
      credentials,
      connect: connector(host),
      heartbeatIntervalMs: 0,
    })
    const firstValues: string[] = []
    const secondValues: string[] = []
    const firstLease = first.createSource(descriptor)(async (delivery) => {
      firstValues.push(decoder.decode(delivery.data))
    })
    const secondLease = second.createSource(descriptor)(async (delivery) => {
      secondValues.push(decoder.decode(delivery.data))
    })

    await Promise.all([firstLease.ready, secondLease.ready])
    host.reportConnection('opened')
    host.reportConnection('reconnected')
    expect(source.opens).toBe(1)
    expect(await first.stats()).toMatchObject({
      tabCount: 2,
      activeConnectionCount: 1,
      physicalSourceCount: 1,
      subscriptionCount: 2,
    })

    await source.emit(0, 'shared-update', cursor(1))
    await vi.waitFor(() => {
      expect(firstValues).toEqual(['shared-update'])
      expect(secondValues).toEqual(['shared-update'])
    })
    await vi.waitFor(async () => {
      expect(await first.stats()).toMatchObject({ queuedItems: 0, queuedBytes: 0 })
    })

    await firstLease.close()
    expect((await second.stats()).physicalSourceCount).toBe(1)
    await secondLease.close()
    await vi.waitFor(() => expect(source.closes).toBe(1))
    expect((await second.stats()).physicalSourceCount).toBe(0)
    expect(telemetry).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: 'natsail.browser.broker.tabs.active' }),
        expect.objectContaining({ name: 'natsail.browser.broker.sources.active' }),
        expect.objectContaining({ name: 'natsail.browser.broker.queue.depth' }),
        expect.objectContaining({ name: 'natsail.browser.broker.queue.bytes' }),
      ])
    )
    expect(JSON.stringify(telemetry)).not.toContain('tenant-a')
    expect(JSON.stringify(telemetry)).not.toContain('conversation')
    host.reportConnection('closed')
    expect(telemetry).toContainEqual(
      expect.objectContaining({
        attributes: expect.objectContaining({ action: 'physical-connection-reconnected' }),
      })
    )

    await closeAll([first, second], [host], [sessions])
  })

  it('rejects a conflicting contract without opening another physical source', async () => {
    const source = controlledSource()
    const sessions = createSessionRegistry()
    const host = createHost(source, { sessions, createSource: source.factory, sweepIntervalMs: 0 })
    const first = await createBrowserBrokerClient({
      identity,
      credentials,
      connect: connector(host),
      heartbeatIntervalMs: 0,
    })
    const second = await createBrowserBrokerClient({
      identity,
      credentials,
      connect: connector(host),
      heartbeatIntervalMs: 0,
    })
    const accepted = first.createSource(descriptor)(async () => undefined)
    await accepted.ready
    const conflicting = second.createSource({ ...descriptor, contract: 'conversation-events:v2' })(
      async () => undefined
    )

    await expect(conflicting.ready).rejects.toMatchObject<Partial<BrowserBrokerError>>({
      code: 'contract-mismatch',
    })
    expect(source.opens).toBe(1)
    await closeAll([first, second], [host], [sessions])
  })

  it('keeps tenant and authentication context in immutable physical source identity', async () => {
    const source = controlledSource()
    const sessions = createSessionRegistry()
    const host = createHost(source, { sessions, createSource: source.factory, sweepIntervalMs: 0 })
    const mutableIdentity = { ...identity }
    const first = await createBrowserBrokerClient({
      identity: mutableIdentity,
      credentials,
      connect: connector(host),
      heartbeatIntervalMs: 0,
    })
    mutableIdentity.tenant = 'mutated-after-connect'
    const second = await createBrowserBrokerClient({
      identity: { tenant: 'tenant-b', authenticationContext: 'browser-user-v1' },
      credentials,
      connect: connector(host),
      heartbeatIntervalMs: 0,
    })
    const third = await createBrowserBrokerClient({
      identity: { tenant: 'tenant-a', authenticationContext: 'browser-admin-v1' },
      credentials,
      connect: connector(host),
      heartbeatIntervalMs: 0,
    })
    const leases = [first, second, third].map((client) =>
      client.createSource(descriptor)(async () => undefined)
    )

    await Promise.all(leases.map((lease) => lease.ready))
    expect(source.opens).toBe(3)
    expect(source.contexts.map((context) => context.identity)).toEqual([
      identity,
      { tenant: 'tenant-b', authenticationContext: 'browser-user-v1' },
      { tenant: 'tenant-a', authenticationContext: 'browser-admin-v1' },
    ])
    await Promise.all(leases.map((lease) => lease.close()))
    await closeAll([first, second, third], [host], [sessions])
  })

  it('uses the configured idle delay before final physical source teardown', async () => {
    vi.useFakeTimers()
    try {
      const source = controlledSource()
      const sessions = createSessionRegistry()
      const host = createHost(source, {
        sessions,
        createSource: source.factory,
        idleTeardownMs: 25,
        sweepIntervalMs: 0,
      })
      const client = await createBrowserBrokerClient({
        identity,
        credentials,
        connect: connector(host),
        heartbeatIntervalMs: 0,
      })
      const lease = client.createSource(descriptor)(async () => undefined)
      await lease.ready
      await lease.close()

      await vi.advanceTimersByTimeAsync(24)
      expect(source.closes).toBe(0)
      expect(host.inspect().physicalSourceCount).toBe(1)
      await vi.advanceTimersByTimeAsync(1)
      expect(source.closes).toBe(1)
      expect(host.inspect().physicalSourceCount).toBe(0)

      await closeAll([client], [host], [sessions])
    } finally {
      vi.useRealTimers()
    }
  })

  it('bounds each tab independently and explicitly fails only a lagging tab', async () => {
    const source = controlledSource()
    const sessions = createSessionRegistry()
    const telemetry: NatsailTelemetryEvent[] = []
    const host = createHost(source, {
      sessions,
      createSource: source.factory,
      maxTabQueueItems: 2,
      maxTabQueueBytes: 64,
      maxBatchItems: 1,
      maxRetainedItems: 2,
      sweepIntervalMs: 0,
      telemetry: { record: (event) => telemetry.push(event) },
      telemetryClock: { now: () => 200 },
    })
    const slow = await createBrowserBrokerClient({
      identity,
      credentials,
      connect: connector(host),
      heartbeatIntervalMs: 0,
    })
    const fast = await createBrowserBrokerClient({
      identity,
      credentials,
      connect: connector(host),
      heartbeatIntervalMs: 0,
    })
    let releaseSlow!: () => void
    const slowGate = new Promise<void>((resolve) => {
      releaseSlow = resolve
    })
    const fastCursors: number[] = []
    const slowLease = slow.createSource(descriptor)(async () => slowGate)
    const fastLease = fast.createSource(descriptor)(async (delivery) => {
      fastCursors.push(delivery.cursor!.sequence)
    })
    await Promise.all([slowLease.ready, fastLease.ready])

    for (let sequence = 1; sequence <= 3; sequence += 1) {
      await source.emit(0, `event-${sequence}`, cursor(sequence))
      await vi.waitFor(() => expect(fastCursors).toHaveLength(sequence))
      await vi.waitFor(async () => {
        const stats = await fast.stats()
        expect(stats.queuedItems).toBeLessThanOrEqual(2)
      })
    }

    await expect(slowLease.closed).rejects.toBeInstanceOf(BrowserBrokerResumeRequiredError)
    expect(fastCursors).toEqual([1, 2, 3])
    expect(telemetry).toContainEqual(
      expect.objectContaining({
        type: 'counter',
        name: 'natsail.browser.broker.events',
        attributes: expect.objectContaining({ action: 'tab-lagged' }),
      })
    )
    releaseSlow()
    await Promise.all([slowLease.close(), fastLease.close()])
    await closeAll([slow, fast], [host], [sessions])
  })

  it('applies queue bounds across every source owned by one tab', async () => {
    const source = controlledSource()
    const sessions = createSessionRegistry()
    const host = createHost(source, {
      sessions,
      createSource: source.factory,
      maxTabQueueItems: 1,
      maxTabQueueBytes: 64,
      maxBatchItems: 1,
      sweepIntervalMs: 0,
    })
    const client = await createBrowserBrokerClient({
      identity,
      credentials,
      connect: connector(host),
      heartbeatIntervalMs: 0,
    })
    let releaseFirst!: () => void
    let firstStarted!: () => void
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve
    })
    const firstAccepted = new Promise<void>((resolve) => {
      firstStarted = resolve
    })
    const firstValues: string[] = []
    const firstLease = client.createSource({ key: 'first', contract: 'events:v1' })(async (
      delivery
    ) => {
      firstStarted()
      await firstGate
      firstValues.push(decoder.decode(delivery.data))
    })
    const secondLease = client.createSource({ key: 'second', contract: 'events:v1' })(
      async () => undefined
    )
    await Promise.all([firstLease.ready, secondLease.ready])

    await source.emit(0, 'first-event', cursor(1))
    await firstAccepted
    expect(host.inspect().queuedItems).toBe(1)
    await source.emit(1, 'second-event', cursor(1))

    await expect(secondLease.closed).rejects.toBeInstanceOf(BrowserBrokerResumeRequiredError)
    expect(host.inspect().queuedItems).toBe(1)
    releaseFirst()
    await vi.waitFor(() => expect(firstValues).toEqual(['first-event']))
    await vi.waitFor(() => expect(host.inspect().queuedItems).toBe(0))

    await Promise.all([firstLease.close(), secondLease.close()])
    await closeAll([client], [host], [sessions])
  })

  it('applies the hard batch-byte bound even to one oversized delivery', async () => {
    const source = controlledSource()
    const sessions = createSessionRegistry()
    const host = createHost(source, {
      sessions,
      createSource: source.factory,
      maxTabQueueItems: 10,
      maxTabQueueBytes: 64,
      maxBatchBytes: 5,
      sweepIntervalMs: 0,
    })
    const client = await createBrowserBrokerClient({
      identity,
      credentials,
      connect: connector(host),
      heartbeatIntervalMs: 0,
    })
    const lease = client.createSource(descriptor)(async () => undefined)
    await lease.ready
    await source.emit(0, 'six123', cursor(1))

    await expect(lease.closed).rejects.toBeInstanceOf(BrowserBrokerResumeRequiredError)
    expect(host.inspect()).toMatchObject({ queuedItems: 0, queuedBytes: 0 })
    await lease.close()
    await closeAll([client], [host], [sessions])
  })

  it('refreshes credentials and resumes from each tab cursor after worker replacement', async () => {
    const firstSource = controlledSource()
    const firstSessions = createSessionRegistry()
    const firstHost = createHost(firstSource, {
      sessions: firstSessions,
      createSource: firstSource.factory,
      sweepIntervalMs: 0,
    })
    const secondSource = controlledSource()
    const secondSessions = createSessionRegistry()
    const secondHost = createHost(secondSource, {
      sessions: secondSessions,
      createSource: secondSource.factory,
      sweepIntervalMs: 0,
    })
    let activeHost = firstHost
    let revision = 1
    const client = await createBrowserBrokerClient({
      identity,
      credentials: () => credentials(revision),
      connect: () => connector(activeHost)(),
      heartbeatIntervalMs: 0,
    })
    const values: string[] = []
    const lease = client.createSource(descriptor)(async (delivery) => {
      values.push(decoder.decode(delivery.data))
    })
    await lease.ready
    await firstSource.emit(0, 'before-restart', cursor(1))
    await vi.waitFor(() => expect(values).toEqual(['before-restart']))
    await vi.waitFor(async () => expect((await client.stats()).queuedItems).toBe(0))

    revision = 2
    await client.refreshCredentials()
    expect(firstSource.contexts[0]!.credentials.current()).toMatchObject({ revision: 2 })

    activeHost = secondHost
    await client.reconnect()
    expect(secondSource.contexts[0]!.resumeAfter).toEqual(cursor(1))
    await secondSource.emit(0, 'after-restart', cursor(2))
    await vi.waitFor(() => expect(values).toEqual(['before-restart', 'after-restart']))

    await lease.close()
    await closeAll([client], [firstHost, secondHost], [firstSessions, secondSessions])
  })

  it('never acknowledges an accepted batch into a replacement worker generation', async () => {
    const firstSource = controlledSource()
    const firstSessions = createSessionRegistry()
    const firstHost = createHost(firstSource, {
      sessions: firstSessions,
      createSource: firstSource.factory,
      sweepIntervalMs: 0,
    })
    const secondSource = controlledSource()
    const secondSessions = createSessionRegistry()
    const secondHost = createHost(secondSource, {
      sessions: secondSessions,
      createSource: secondSource.factory,
      sweepIntervalMs: 0,
    })
    let activeHost = firstHost
    const client = await createBrowserBrokerClient({
      identity,
      credentials,
      connect: () => connector(activeHost)(),
      heartbeatIntervalMs: 0,
    })
    let releaseOld!: () => void
    let oldStarted!: () => void
    let releaseReplacement!: () => void
    let replacementStarted!: () => void
    const oldGate = new Promise<void>((resolve) => {
      releaseOld = resolve
    })
    const oldAccepted = new Promise<void>((resolve) => {
      oldStarted = resolve
    })
    const replacementGate = new Promise<void>((resolve) => {
      releaseReplacement = resolve
    })
    const replacementAccepted = new Promise<void>((resolve) => {
      replacementStarted = resolve
    })
    const values: string[] = []
    const lease = client.createSource(descriptor)(async (delivery) => {
      const value = decoder.decode(delivery.data)
      if (value === 'old') {
        oldStarted()
        await oldGate
      } else {
        replacementStarted()
        await replacementGate
      }
      values.push(value)
    })
    await lease.ready
    await firstSource.emit(0, 'old', cursor(1))
    await oldAccepted

    activeHost = secondHost
    await client.reconnect()
    expect(secondSource.contexts[0]!.resumeAfter).toBeUndefined()
    await secondSource.emit(0, 'replacement', cursor(1))
    releaseOld()
    await replacementAccepted

    expect(secondHost.inspect().queuedItems).toBe(1)
    releaseReplacement()
    await vi.waitFor(() => expect(values).toEqual(['old', 'replacement']))
    await vi.waitFor(() => expect(secondHost.inspect().queuedItems).toBe(0))

    await lease.close()
    await closeAll([client], [firstHost, secondHost], [firstSessions, secondSessions])
  })

  it('uses an explicit tab-local fallback unless strict SharedWorker mode is requested', async () => {
    const source = controlledSource()
    const sessions = createSessionRegistry()
    const events: NatsailTelemetryEvent[] = []
    const host = createHost(source, { sessions, createSource: source.factory, sweepIntervalMs: 0 })
    const closeHost = vi.spyOn(host, 'close')
    const client = await createBrowserBrokerClient({
      identity,
      credentials,
      connect: () => {
        throw new Error('SharedWorker unavailable')
      },
      fallback: createTabLocalBrokerConnector(() => host),
      heartbeatIntervalMs: 0,
      telemetry: { record: (event) => events.push(event) },
      telemetryClock: { now: () => 300 },
    })
    expect(client.mode).toBe('tab-local')
    expect(events).toContainEqual(
      expect.objectContaining({
        name: 'natsail.browser.broker.events',
        attributes: expect.objectContaining({ action: 'fallback' }),
      })
    )

    await expect(
      createBrowserBrokerClient({
        identity,
        credentials,
        connect: () => {
          throw new Error('SharedWorker unavailable')
        },
        fallback: createTabLocalBrokerConnector(() => host),
        strict: true,
        heartbeatIntervalMs: 0,
      })
    ).rejects.toMatchObject<Partial<BrowserBrokerError>>({ code: 'unavailable' })
    await client.close()
    expect(closeHost).toHaveBeenCalledTimes(1)
    await sessions.close()
  })

  it('closes a tab-local host when client authentication fails', async () => {
    const source = controlledSource()
    const sessions = createSessionRegistry()
    const host = createHost(source, {
      sessions,
      createSource: source.factory,
      sweepIntervalMs: 10,
    })
    const closeHost = vi.spyOn(host, 'close')

    await expect(
      createBrowserBrokerClient({
        identity,
        credentials: () => ({ revision: -1, bytes: new Uint8Array() }),
        connect: () => {
          throw new Error('SharedWorker unavailable')
        },
        fallback: createTabLocalBrokerConnector(() => host),
        heartbeatIntervalMs: 0,
      })
    ).rejects.toMatchObject<Partial<BrowserBrokerError>>({ code: 'invalid-command' })
    expect(closeHost).toHaveBeenCalledTimes(1)
    await sessions.close()
  })

  it('rejects invalid credential bytes before sending them to a worker', async () => {
    const source = controlledSource()
    const sessions = createSessionRegistry()
    const host = createHost(source, { sessions, createSource: source.factory, sweepIntervalMs: 0 })

    await expect(
      createBrowserBrokerClient({
        identity,
        credentials: () =>
          ({
            revision: 1,
            bytes: new ArrayBuffer(0),
          }) as unknown as BrowserBrokerCredentialSnapshot,
        connect: connector(host),
        heartbeatIntervalMs: 0,
      })
    ).rejects.toMatchObject<Partial<BrowserBrokerError>>({ code: 'invalid-command' })
    await closeAll([], [host], [sessions])
  })

  it('rejects conflicting bytes for the same identity and credential revision', async () => {
    const source = controlledSource()
    const sessions = createSessionRegistry()
    const host = createHost(source, { sessions, createSource: source.factory, sweepIntervalMs: 0 })
    const first = await createBrowserBrokerClient({
      identity,
      credentials,
      connect: connector(host),
      heartbeatIntervalMs: 0,
    })

    await expect(
      createBrowserBrokerClient({
        identity,
        credentials: () => ({ revision: 1, bytes: encoder.encode('different-credential') }),
        connect: connector(host),
        heartbeatIntervalMs: 0,
      })
    ).rejects.toMatchObject<Partial<BrowserBrokerError>>({ code: 'credentials-stale' })
    await closeAll([first], [host], [sessions])
  })

  it('does not reopen or attach sources after client close', async () => {
    const source = controlledSource()
    const sessions = createSessionRegistry()
    const host = createHost(source, { sessions, createSource: source.factory, sweepIntervalMs: 0 })
    const client = await createBrowserBrokerClient({
      identity,
      credentials,
      connect: connector(host),
      heartbeatIntervalMs: 0,
    })
    const sourceBeforeClose = client.createSource(descriptor)
    await client.close()

    await expect(client.connect()).rejects.toMatchObject<Partial<BrowserBrokerError>>({
      code: 'invalid-state',
    })
    await expect(client.reconnect()).rejects.toMatchObject<Partial<BrowserBrokerError>>({
      code: 'invalid-state',
    })
    expect(() => client.createSource(descriptor)).toThrow(
      expect.objectContaining({ code: 'invalid-state' })
    )
    const lease = sourceBeforeClose(async () => undefined)
    await expect(lease.ready).rejects.toMatchObject<Partial<BrowserBrokerError>>({
      code: 'invalid-state',
    })
    await expect(lease.closed).rejects.toMatchObject<Partial<BrowserBrokerError>>({
      code: 'invalid-state',
    })
    await closeAll([], [host], [sessions])
  })
})
