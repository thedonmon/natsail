import { createNatsailTelemetryReporter } from '@natsail/core'
import type {
  NatsailTelemetryAttributes,
  NatsailTelemetryClock,
  NatsailTelemetryEvent,
  NatsailTelemetryReporter,
  NatsailTelemetrySink,
  SubscriptionLease,
} from '@natsail/core'
import { defineSession } from '@natsail/session'
import type { SessionHandle, SessionRegistry, SessionSource } from '@natsail/session'

export const NATS_BROWSER_BROKER_PROTOCOL = 'natsail.browser-broker' as const
export const NATS_BROWSER_BROKER_PROTOCOL_VERSION = 1 as const

export interface BrowserBrokerIdentity {
  /** Stable tenant boundary. It is part of source identity and cannot change on a port. */
  readonly tenant: string
  /** Stable authentication-policy identity, not a credential or token value. */
  readonly authenticationContext: string
}

export interface BrowserBrokerSourceDescriptor {
  /** Logical source identity inside one tenant/authentication boundary. */
  readonly key: string
  /** Stable description of every option that changes source delivery semantics. */
  readonly contract: string
}

export interface BrowserBrokerCursor {
  readonly stream: string
  readonly sequence: number
  readonly epoch?: string
}

export interface BrowserBrokerDelivery {
  readonly data: Uint8Array
  readonly cursor?: BrowserBrokerCursor
}

export interface BrowserBrokerCredentialSnapshot {
  readonly revision: number
  readonly bytes: Uint8Array
}

export interface BrowserBrokerCredentials {
  current(): BrowserBrokerCredentialSnapshot
  subscribe(listener: (snapshot: BrowserBrokerCredentialSnapshot) => void): () => void
}

export interface BrowserBrokerSourceContext {
  readonly identity: BrowserBrokerIdentity
  readonly descriptor: BrowserBrokerSourceDescriptor
  readonly credentials: BrowserBrokerCredentials
  /** Cursor supplied by the first tab opening this physical source. */
  readonly resumeAfter?: BrowserBrokerCursor
}

export type BrowserBrokerSourceFactory = (
  context: BrowserBrokerSourceContext
) => SessionSource<BrowserBrokerDelivery>

export interface BrowserBrokerOperationContext {
  readonly identity: BrowserBrokerIdentity
  /** Application-defined operation key. The worker must map it to an authorized NATS operation. */
  readonly operation: string
  readonly data: Uint8Array
  readonly credentials: BrowserBrokerCredentials
}

export type BrowserBrokerPublishHandler = (
  context: BrowserBrokerOperationContext
) => void | Promise<void>

export type BrowserBrokerRequestHandler = (
  context: BrowserBrokerOperationContext
) => Uint8Array | Promise<Uint8Array>

interface ProtocolEnvelope {
  readonly protocol: typeof NATS_BROWSER_BROKER_PROTOCOL
  readonly version: typeof NATS_BROWSER_BROKER_PROTOCOL_VERSION
}

export type BrowserBrokerCommand =
  | (ProtocolEnvelope & {
      readonly type: 'hello'
      readonly requestId: number
      readonly identity: BrowserBrokerIdentity
      readonly credentialRevision: number
      readonly credentials: ArrayBuffer
    })
  | (ProtocolEnvelope & {
      readonly type: 'attach'
      readonly requestId: number
      readonly subscriptionId: string
      readonly source: BrowserBrokerSourceDescriptor
      readonly reattach: boolean
      readonly resumeAfter?: BrowserBrokerCursor
    })
  | (ProtocolEnvelope & {
      readonly type: 'detach' | 'restart'
      readonly requestId: number
      readonly subscriptionId: string
    })
  | (ProtocolEnvelope & {
      readonly type: 'ack'
      readonly requestId: number
      readonly subscriptionId: string
      readonly batchId: number
      readonly cursor?: BrowserBrokerCursor
    })
  | (ProtocolEnvelope & {
      readonly type: 'refresh-credentials'
      readonly requestId: number
      readonly credentialRevision: number
      readonly credentials: ArrayBuffer
    })
  | (ProtocolEnvelope & {
      readonly type: 'publish' | 'request'
      readonly requestId: number
      readonly operation: string
      readonly data: ArrayBuffer
    })
  | (ProtocolEnvelope & {
      readonly type: 'close' | 'heartbeat' | 'stats'
      readonly requestId: number
    })

export interface BrowserBrokerProtocolFailure {
  readonly code: BrowserBrokerErrorCode
  readonly message: string
}

export interface BrowserBrokerStats {
  readonly tabCount: number
  readonly activeConnectionCount: number
  readonly physicalSourceCount: number
  readonly subscriptionCount: number
  readonly queuedItems: number
  readonly queuedBytes: number
}

export type BrowserBrokerResult =
  | (ProtocolEnvelope & {
      readonly type: 'result'
      readonly requestId: number
      readonly ok: true
      readonly result?: unknown
      readonly error?: never
    })
  | (ProtocolEnvelope & {
      readonly type: 'result'
      readonly requestId: number
      readonly ok: false
      readonly result?: never
      readonly error: BrowserBrokerProtocolFailure
    })

export type BrowserBrokerStateMessage = ProtocolEnvelope & {
  readonly type: 'state'
  readonly subscriptionId: string
  readonly state: 'connecting' | 'live' | 'resume-required' | 'closed' | 'error'
  readonly reason?: 'lagged' | 'source-error' | 'worker-restart'
  readonly cursor?: BrowserBrokerCursor
}

export interface BrowserBrokerBatchItem {
  readonly data: ArrayBuffer
  readonly cursor?: BrowserBrokerCursor
}

export type BrowserBrokerBatchMessage = ProtocolEnvelope & {
  readonly type: 'batch'
  readonly subscriptionId: string
  readonly batchId: number
  readonly items: readonly BrowserBrokerBatchItem[]
}

export type BrowserBrokerMessage =
  | BrowserBrokerResult
  | BrowserBrokerStateMessage
  | BrowserBrokerBatchMessage

export type BrowserBrokerErrorCode =
  | 'contract-mismatch'
  | 'credentials-stale'
  | 'identity-mismatch'
  | 'invalid-command'
  | 'invalid-state'
  | 'not-connected'
  | 'protocol-version'
  | 'resume-required'
  | 'source-failed'
  | 'subscription-exists'
  | 'subscription-missing'
  | 'unavailable'

const browserBrokerErrorCodes: readonly BrowserBrokerErrorCode[] = [
  'contract-mismatch',
  'credentials-stale',
  'identity-mismatch',
  'invalid-command',
  'invalid-state',
  'not-connected',
  'protocol-version',
  'resume-required',
  'source-failed',
  'subscription-exists',
  'subscription-missing',
  'unavailable',
]

export class BrowserBrokerError extends Error {
  readonly name: string = 'BrowserBrokerError'

  constructor(
    readonly code: BrowserBrokerErrorCode,
    message: string
  ) {
    super(message)
  }
}

export class BrowserBrokerResumeRequiredError extends BrowserBrokerError {
  readonly name = 'BrowserBrokerResumeRequiredError'

  constructor(readonly cursor?: BrowserBrokerCursor) {
    super('resume-required', 'The browser broker tab fell behind retained delivery history')
  }
}

const envelope = (): ProtocolEnvelope => ({
  protocol: NATS_BROWSER_BROKER_PROTOCOL,
  version: NATS_BROWSER_BROKER_PROTOCOL_VERSION,
})

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new BrowserBrokerError('invalid-command', 'Browser broker messages must be objects')
  }
  return value as Record<string, unknown>
}

function stringField(value: unknown, field: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new BrowserBrokerError('invalid-command', `${field} must be a non-empty string`)
  }
  return value
}

function integerField(value: unknown, field: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new BrowserBrokerError('invalid-command', `${field} must be a non-negative integer`)
  }
  return value as number
}

function booleanField(value: unknown, field: string): boolean {
  if (typeof value !== 'boolean') {
    throw new BrowserBrokerError('invalid-command', `${field} must be a boolean`)
  }
  return value
}

function requestId(value: unknown): number {
  const id = integerField(value, 'requestId')
  if (id === 0) throw new BrowserBrokerError('invalid-command', 'requestId must be positive')
  return id
}

function arrayBuffer(value: unknown, field: string): ArrayBuffer {
  if (Object.prototype.toString.call(value) !== '[object ArrayBuffer]') {
    throw new BrowserBrokerError('invalid-command', `${field} must be an ArrayBuffer`)
  }
  return value as ArrayBuffer
}

function byteArray(value: unknown, field: string): Uint8Array {
  if (!(value instanceof Uint8Array)) {
    throw new BrowserBrokerError('invalid-command', `${field} must be a Uint8Array`)
  }
  return value
}

function parseIdentity(value: unknown): BrowserBrokerIdentity {
  const candidate = record(value)
  return {
    tenant: stringField(candidate.tenant, 'identity.tenant'),
    authenticationContext: stringField(
      candidate.authenticationContext,
      'identity.authenticationContext'
    ),
  }
}

function parseSource(value: unknown): BrowserBrokerSourceDescriptor {
  const candidate = record(value)
  return {
    key: stringField(candidate.key, 'source.key'),
    contract: stringField(candidate.contract, 'source.contract'),
  }
}

function parseCursor(value: unknown): BrowserBrokerCursor {
  const candidate = record(value)
  const epoch = candidate.epoch
  if (epoch !== undefined && (typeof epoch !== 'string' || epoch.length === 0)) {
    throw new BrowserBrokerError('invalid-command', 'cursor.epoch must be a non-empty string')
  }
  return {
    stream: stringField(candidate.stream, 'cursor.stream'),
    sequence: integerField(candidate.sequence, 'cursor.sequence'),
    ...(epoch === undefined ? {} : { epoch }),
  }
}

function assertEnvelope(candidate: Record<string, unknown>): void {
  if (candidate.protocol !== NATS_BROWSER_BROKER_PROTOCOL) {
    throw new BrowserBrokerError('invalid-command', 'Unknown browser broker protocol')
  }
  if (candidate.version !== NATS_BROWSER_BROKER_PROTOCOL_VERSION) {
    throw new BrowserBrokerError(
      'protocol-version',
      `Unsupported browser broker protocol version ${String(candidate.version)}`
    )
  }
}

/** Strictly validates a tab-to-worker protocol-v1 command. */
export function parseBrowserBrokerCommand(value: unknown): BrowserBrokerCommand {
  const candidate = record(value)
  assertEnvelope(candidate)
  const id = requestId(candidate.requestId)
  switch (candidate.type) {
    case 'hello':
      return {
        ...envelope(),
        type: 'hello',
        requestId: id,
        identity: parseIdentity(candidate.identity),
        credentialRevision: integerField(candidate.credentialRevision, 'credentialRevision'),
        credentials: arrayBuffer(candidate.credentials, 'credentials'),
      }
    case 'attach':
      return {
        ...envelope(),
        type: 'attach',
        requestId: id,
        subscriptionId: stringField(candidate.subscriptionId, 'subscriptionId'),
        source: parseSource(candidate.source),
        reattach: booleanField(candidate.reattach, 'reattach'),
        ...(candidate.resumeAfter === undefined
          ? {}
          : { resumeAfter: parseCursor(candidate.resumeAfter) }),
      }
    case 'detach':
    case 'restart':
      return {
        ...envelope(),
        type: candidate.type,
        requestId: id,
        subscriptionId: stringField(candidate.subscriptionId, 'subscriptionId'),
      }
    case 'ack':
      return {
        ...envelope(),
        type: 'ack',
        requestId: id,
        subscriptionId: stringField(candidate.subscriptionId, 'subscriptionId'),
        batchId: requestId(candidate.batchId),
        ...(candidate.cursor === undefined ? {} : { cursor: parseCursor(candidate.cursor) }),
      }
    case 'refresh-credentials':
      return {
        ...envelope(),
        type: 'refresh-credentials',
        requestId: id,
        credentialRevision: integerField(candidate.credentialRevision, 'credentialRevision'),
        credentials: arrayBuffer(candidate.credentials, 'credentials'),
      }
    case 'publish':
    case 'request':
      return {
        ...envelope(),
        type: candidate.type,
        requestId: id,
        operation: stringField(candidate.operation, 'operation'),
        data: arrayBuffer(candidate.data, 'data'),
      }
    case 'close':
    case 'heartbeat':
    case 'stats':
      return { ...envelope(), type: candidate.type, requestId: id }
    default:
      throw new BrowserBrokerError('invalid-command', 'Unknown browser broker command')
  }
}

/** Strictly validates one worker-to-tab protocol-v1 message. */
export function parseBrowserBrokerMessage(value: unknown): BrowserBrokerMessage {
  const candidate = record(value)
  assertEnvelope(candidate)
  switch (candidate.type) {
    case 'result': {
      const ok = candidate.ok
      if (typeof ok !== 'boolean') {
        throw new BrowserBrokerError('invalid-state', 'result.ok must be boolean')
      }
      const hasError = Object.prototype.hasOwnProperty.call(candidate, 'error')
      const hasResult = Object.prototype.hasOwnProperty.call(candidate, 'result')
      if (ok) {
        if (hasError) {
          throw new BrowserBrokerError('invalid-state', 'Successful results cannot include error')
        }
        return {
          ...envelope(),
          type: 'result',
          requestId: requestId(candidate.requestId),
          ok: true,
          ...(hasResult ? { result: candidate.result } : {}),
        }
      }
      if (!hasError || hasResult) {
        throw new BrowserBrokerError(
          'invalid-state',
          'Failed results must include error and cannot include result'
        )
      }
      const parsed = record(candidate.error)
      const code = stringField(parsed.code, 'error.code')
      if (!browserBrokerErrorCodes.includes(code as BrowserBrokerErrorCode)) {
        throw new BrowserBrokerError('invalid-state', 'Unknown browser broker error code')
      }
      return {
        ...envelope(),
        type: 'result',
        requestId: requestId(candidate.requestId),
        ok: false,
        error: {
          code: code as BrowserBrokerErrorCode,
          message: stringField(parsed.message, 'error.message'),
        },
      }
    }
    case 'state': {
      const state = candidate.state
      if (!['connecting', 'live', 'resume-required', 'closed', 'error'].includes(String(state))) {
        throw new BrowserBrokerError('invalid-state', 'Unknown browser broker source state')
      }
      const reason = candidate.reason
      if (
        reason !== undefined &&
        !['lagged', 'source-error', 'worker-restart'].includes(String(reason))
      ) {
        throw new BrowserBrokerError('invalid-state', 'Unknown browser broker state reason')
      }
      return {
        ...envelope(),
        type: 'state',
        subscriptionId: stringField(candidate.subscriptionId, 'subscriptionId'),
        state: state as BrowserBrokerStateMessage['state'],
        ...(reason === undefined
          ? {}
          : {
              reason: reason as Exclude<BrowserBrokerStateMessage['reason'], undefined>,
            }),
        ...(candidate.cursor === undefined ? {} : { cursor: parseCursor(candidate.cursor) }),
      }
    }
    case 'batch': {
      if (!Array.isArray(candidate.items) || candidate.items.length === 0) {
        throw new BrowserBrokerError('invalid-state', 'batch.items must be a non-empty array')
      }
      return {
        ...envelope(),
        type: 'batch',
        subscriptionId: stringField(candidate.subscriptionId, 'subscriptionId'),
        batchId: requestId(candidate.batchId),
        items: candidate.items.map((item, index) => {
          const parsed = record(item)
          return {
            data: arrayBuffer(parsed.data, `items[${index}].data`),
            ...(parsed.cursor === undefined ? {} : { cursor: parseCursor(parsed.cursor) }),
          }
        }),
      }
    }
    default:
      throw new BrowserBrokerError('invalid-state', 'Unknown browser broker message')
  }
}

function cloneIdentity(identity: BrowserBrokerIdentity): BrowserBrokerIdentity {
  return Object.freeze({ ...identity })
}

function identityKey(identity: BrowserBrokerIdentity): string {
  return JSON.stringify([identity.tenant, identity.authenticationContext])
}

function sourceKey(
  identity: BrowserBrokerIdentity,
  descriptor: BrowserBrokerSourceDescriptor
): string {
  return `${identityKey(identity)}\u0000${descriptor.key}`
}

function cursorsEqual(
  left: BrowserBrokerCursor | undefined,
  right: BrowserBrokerCursor | undefined
) {
  if (left === undefined || right === undefined) return left === right
  return (
    left.stream === right.stream && left.sequence === right.sequence && left.epoch === right.epoch
  )
}

function copyBytes(bytes: Uint8Array): Uint8Array {
  return bytes.slice()
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false
  return left.every((value, index) => value === right[index])
}

function parseCredentialSnapshot(value: unknown): BrowserBrokerCredentialSnapshot {
  const candidate = record(value)
  return {
    revision: integerField(candidate.revision, 'credentials.revision'),
    bytes: byteArray(candidate.bytes, 'credentials.bytes'),
  }
}

function parseStats(value: unknown): BrowserBrokerStats {
  const candidate = record(value)
  return {
    tabCount: integerField(candidate.tabCount, 'stats.tabCount'),
    activeConnectionCount: integerField(
      candidate.activeConnectionCount,
      'stats.activeConnectionCount'
    ),
    physicalSourceCount: integerField(candidate.physicalSourceCount, 'stats.physicalSourceCount'),
    subscriptionCount: integerField(candidate.subscriptionCount, 'stats.subscriptionCount'),
    queuedItems: integerField(candidate.queuedItems, 'stats.queuedItems'),
    queuedBytes: integerField(candidate.queuedBytes, 'stats.queuedBytes'),
  }
}

function copyBuffer(bytes: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(bytes.byteLength)
  copy.set(bytes)
  return copy.buffer
}

function copyCursor(cursor: BrowserBrokerCursor): BrowserBrokerCursor
function copyCursor(cursor: undefined): undefined
function copyCursor(cursor: BrowserBrokerCursor | undefined): BrowserBrokerCursor | undefined
function copyCursor(cursor: BrowserBrokerCursor | undefined): BrowserBrokerCursor | undefined {
  return cursor === undefined ? undefined : { ...cursor }
}

class MutableCredentials implements BrowserBrokerCredentials {
  private listeners = new Set<(snapshot: BrowserBrokerCredentialSnapshot) => void>()
  private bytes: Uint8Array

  constructor(
    private revision: number,
    bytes: Uint8Array
  ) {
    this.bytes = copyBytes(bytes)
  }

  current(): BrowserBrokerCredentialSnapshot {
    return { revision: this.revision, bytes: copyBytes(this.bytes) }
  }

  subscribe(listener: (snapshot: BrowserBrokerCredentialSnapshot) => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  update(revision: number, bytes: Uint8Array): unknown | undefined {
    if (revision <= this.revision) {
      throw new BrowserBrokerError(
        'credentials-stale',
        `Credential revision ${revision} must be newer than ${this.revision}`
      )
    }
    this.bytes.fill(0)
    this.revision = revision
    this.bytes = copyBytes(bytes)
    let listenerFailure: unknown
    for (const listener of this.listeners) {
      try {
        listener(this.current())
      } catch (error) {
        listenerFailure ??= error
        // One source cannot prevent the remaining identity-bound sources from
        // observing a credential revision that has already been committed.
      }
    }
    return listenerFailure
  }

  accept(revision: number, bytes: Uint8Array): unknown | undefined {
    if (revision < this.revision) {
      throw new BrowserBrokerError(
        'credentials-stale',
        `Credential revision ${revision} is older than ${this.revision}`
      )
    }
    if (revision === this.revision) {
      if (!bytesEqual(this.bytes, bytes)) {
        throw new BrowserBrokerError(
          'credentials-stale',
          `Credential revision ${revision} has conflicting bytes`
        )
      }
      return undefined
    }
    return this.update(revision, bytes)
  }

  dispose(): void {
    this.bytes.fill(0)
    this.listeners.clear()
  }
}

interface LogEntry {
  readonly data: Uint8Array
  readonly cursor?: BrowserBrokerCursor
}

interface InflightBatch {
  readonly id: number
  readonly entries: readonly LogEntry[]
  readonly items: number
  readonly bytes: number
  readonly cursor?: BrowserBrokerCursor
}

interface HostSubscription {
  readonly id: string
  readonly client: HostClient
  readonly source: PhysicalSource
  pending: LogEntry[]
  pendingBytes: number
  inflight: InflightBatch | undefined
  nextBatchId: number
  lastCursor: BrowserBrokerCursor | undefined
  resumeRequired: boolean
}

interface HostClient {
  readonly port: MessagePort
  readonly subscriptions: Map<string, HostSubscription>
  identity?: BrowserBrokerIdentity
  credentialKey?: string
  lastSeenAt: number
  queuedCommands: number
  readonly activeCommands: Set<Promise<void>>
  closing: boolean
  closed: boolean
  publishPending: Promise<void>
  pending: Promise<void>
}

interface PhysicalSource {
  readonly key: string
  readonly identityKey: string
  readonly identity: BrowserBrokerIdentity
  readonly descriptor: BrowserBrokerSourceDescriptor
  readonly handle: SessionHandle<BrowserBrokerDelivery>
  readonly subscriptions: Set<HostSubscription>
  readonly unsubscribe: () => void
  log: LogEntry[]
  logBytes: number
  logTruncated: boolean
  lastValueRevision: number
  idleTimer: ReturnType<typeof setTimeout> | undefined
  closed: boolean
}

export interface BrowserBrokerWorkerOptions {
  readonly sessions: SessionRegistry
  readonly createSource: BrowserBrokerSourceFactory
  /** Optional application-authorized publish mapping for tab operation keys. */
  readonly publish?: BrowserBrokerPublishHandler
  /** Optional application-authorized request mapping for tab operation keys. */
  readonly request?: BrowserBrokerRequestHandler
  /**
   * Releases and resets worker-owned runtime resources after the final physical
   * source closes, and when the broker host closes. A later operation waits for
   * this hook before asking the application to lazily recreate those resources.
   */
  readonly closeIdleResources?: () => void | Promise<void>
  /** Per-tab outstanding item limit, including the one in-flight batch. Defaults to 256. */
  readonly maxTabQueueItems?: number
  /** Per-tab outstanding encoded-byte limit. Defaults to 1 MiB. */
  readonly maxTabQueueBytes?: number
  /** Shared retained source-log item limit. Defaults to 1,024. */
  readonly maxRetainedItems?: number
  /** Shared retained source-log byte limit. Defaults to 4 MiB. */
  readonly maxRetainedBytes?: number
  /** Maximum items transferred in one per-tab batch. Defaults to 64. */
  readonly maxBatchItems?: number
  /** Maximum bytes transferred in one per-tab batch. Defaults to 256 KiB. */
  readonly maxBatchBytes?: number
  /** Delay after the final detach before releasing the physical source. Defaults to 0. */
  readonly idleTeardownMs?: number
  /** Closes tabs that stop heartbeating after this duration. Defaults to 30 seconds. */
  readonly clientTimeoutMs?: number
  /** Host heartbeat sweep interval. Set to 0 for an externally driven sweep. Defaults to 10 seconds. */
  readonly sweepIntervalMs?: number
  readonly telemetry?: NatsailTelemetrySink
  readonly telemetryAttributes?: NatsailTelemetryAttributes
  readonly telemetryClock?: NatsailTelemetryClock
}

export interface BrowserBrokerWorkerHost {
  connect(port: MessagePort): void
  /** Reports lifecycle of the caller-owned upstream runtime connection. */
  reportConnection(state: 'opened' | 'closed' | 'reconnected'): void
  inspect(): BrowserBrokerStats
  sweepIdleClients(at?: number): Promise<void>
  close(): Promise<void>
}

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
  const resolved = value ?? fallback
  if (!Number.isSafeInteger(resolved) || resolved <= 0) {
    throw new RangeError(`${name} must be a positive integer`)
  }
  return resolved
}

function nonNegativeInteger(value: number | undefined, fallback: number, name: string): number {
  const resolved = value ?? fallback
  if (!Number.isSafeInteger(resolved) || resolved < 0) {
    throw new RangeError(`${name} must be a non-negative integer`)
  }
  return resolved
}

class DefaultBrowserBrokerWorkerHost implements BrowserBrokerWorkerHost {
  private readonly clients = new Map<MessagePort, HostClient>()
  private readonly sources = new Map<string, PhysicalSource>()
  private readonly credentialStores = new Map<string, MutableCredentials>()
  private readonly telemetry: NatsailTelemetryReporter
  private readonly maxTabQueueItems: number
  private readonly maxTabQueueBytes: number
  private readonly maxRetainedItems: number
  private readonly maxRetainedBytes: number
  private readonly maxBatchItems: number
  private readonly maxBatchBytes: number
  private readonly idleTeardownMs: number
  private readonly clientTimeoutMs: number
  private readonly sweepTimer?: ReturnType<typeof setInterval>
  private activeConnections = 0
  private activeOperations = 0
  private closed = false
  private resourcesActive = false
  private idleResourceClose: Promise<void> | undefined

  constructor(private readonly options: BrowserBrokerWorkerOptions) {
    this.maxTabQueueItems = positiveInteger(options.maxTabQueueItems, 256, 'maxTabQueueItems')
    this.maxTabQueueBytes = positiveInteger(
      options.maxTabQueueBytes,
      1024 * 1024,
      'maxTabQueueBytes'
    )
    this.maxRetainedItems = positiveInteger(options.maxRetainedItems, 1024, 'maxRetainedItems')
    this.maxRetainedBytes = positiveInteger(
      options.maxRetainedBytes,
      4 * 1024 * 1024,
      'maxRetainedBytes'
    )
    this.maxBatchItems = positiveInteger(options.maxBatchItems, 64, 'maxBatchItems')
    this.maxBatchBytes = positiveInteger(options.maxBatchBytes, 256 * 1024, 'maxBatchBytes')
    this.idleTeardownMs = nonNegativeInteger(options.idleTeardownMs, 0, 'idleTeardownMs')
    this.clientTimeoutMs = positiveInteger(options.clientTimeoutMs, 30_000, 'clientTimeoutMs')
    const sweepIntervalMs = nonNegativeInteger(options.sweepIntervalMs, 10_000, 'sweepIntervalMs')
    this.telemetry = createNatsailTelemetryReporter({
      ...(options.telemetry === undefined ? {} : { sink: options.telemetry }),
      ...(options.telemetryClock === undefined ? {} : { clock: options.telemetryClock }),
      ...(options.telemetryAttributes === undefined
        ? {}
        : { attributes: options.telemetryAttributes }),
    })
    if (sweepIntervalMs > 0) {
      this.sweepTimer = setInterval(() => {
        void this.sweepIdleClients()
      }, sweepIntervalMs)
    }
  }

  connect(port: MessagePort): void {
    if (this.closed) throw new BrowserBrokerError('invalid-state', 'The browser broker is closed')
    if (this.clients.has(port)) {
      throw new BrowserBrokerError('invalid-state', 'The MessagePort is already connected')
    }
    const client: HostClient = {
      port,
      subscriptions: new Map(),
      lastSeenAt: this.now(),
      queuedCommands: 0,
      activeCommands: new Set(),
      closing: false,
      closed: false,
      publishPending: Promise.resolve(),
      pending: Promise.resolve(),
    }
    this.clients.set(port, client)
    port.onmessage = (event: MessageEvent<unknown>) => {
      if (this.closed || client.closed || client.closing) return
      client.lastSeenAt = this.now()
      client.queuedCommands += 1
      const execute = () => this.receive(client, event.data)
      const type =
        typeof event.data === 'object' && event.data !== null
          ? (event.data as Record<string, unknown>).type
          : undefined
      if (type === 'close') client.closing = true
      if (type === 'publish' || type === 'request' || type === 'heartbeat') {
        const predecessor = type === 'publish' ? client.publishPending : Promise.resolve()
        const active = predecessor.then(execute).finally(() => {
          client.queuedCommands -= 1
        })
        if (type === 'publish') client.publishPending = active.catch(() => undefined)
        client.activeCommands.add(active)
        void active.finally(() => client.activeCommands.delete(active)).catch(() => undefined)
        return
      }
      client.pending = client.pending
        .then(execute)
        .finally(() => {
          client.queuedCommands -= 1
        })
        .catch(() => undefined)
    }
    port.onmessageerror = () => {
      void this.enqueueClientClose(client)
    }
    port.start()
  }

  reportConnection(state: 'opened' | 'closed' | 'reconnected'): void {
    if (state === 'opened') this.activeConnections += 1
    if (state === 'closed') this.activeConnections = Math.max(0, this.activeConnections - 1)
    this.telemetryEvent(`physical-connection-${state}`)
    this.telemetryGauge('natsail.browser.broker.connections.active', this.activeConnections)
  }

  inspect(): BrowserBrokerStats {
    let subscriptionCount = 0
    let queuedItems = 0
    let queuedBytes = 0
    for (const source of this.sources.values()) {
      subscriptionCount += source.subscriptions.size
      for (const subscription of source.subscriptions) {
        queuedItems += subscription.pending.length + (subscription.inflight?.items ?? 0)
        queuedBytes += subscription.pendingBytes + (subscription.inflight?.bytes ?? 0)
      }
    }
    return {
      tabCount: [...this.clients.values()].filter((client) => client.identity !== undefined).length,
      activeConnectionCount: this.activeConnections,
      physicalSourceCount: this.sources.size,
      subscriptionCount,
      queuedItems,
      queuedBytes,
    }
  }

  async sweepIdleClients(at = this.now()): Promise<void> {
    await Promise.all(
      [...this.clients.values()]
        .filter(
          (client) => client.queuedCommands === 0 && at - client.lastSeenAt >= this.clientTimeoutMs
        )
        .map((client) => this.enqueueClientClose(client))
    )
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    if (this.sweepTimer) clearInterval(this.sweepTimer)
    try {
      await Promise.all(
        [...this.clients.values()].flatMap((client) => [client.pending, ...client.activeCommands])
      )
      await Promise.all([...this.clients.values()].map((client) => this.closeClient(client)))
      await Promise.all([...this.sources.values()].map((source) => this.teardownSource(source)))
      await this.closeIdleResources()
    } finally {
      for (const store of this.credentialStores.values()) store.dispose()
      this.credentialStores.clear()
    }
  }

  private async receive(client: HostClient, value: unknown): Promise<void> {
    if (this.closed || client.closed) return
    let command: BrowserBrokerCommand
    try {
      command = parseBrowserBrokerCommand(value)
    } catch (error) {
      const candidate = typeof value === 'object' && value !== null ? value : {}
      const rawId = (candidate as Record<string, unknown>).requestId
      if (Number.isSafeInteger(rawId) && (rawId as number) > 0) {
        this.postFailure(client, rawId as number, error)
      }
      return
    }

    try {
      const handled = await this.handleCommand(client, command)
      this.postResult(client, command.requestId, handled.result, handled.transfers)
    } catch (error) {
      this.postFailure(client, command.requestId, error)
    }
  }

  private async handleCommand(
    client: HostClient,
    command: BrowserBrokerCommand
  ): Promise<{ readonly result?: unknown; readonly transfers?: Transferable[] }> {
    if (command.type === 'hello') {
      await this.hello(client, command)
      return { result: { protocolVersion: NATS_BROWSER_BROKER_PROTOCOL_VERSION } }
    }
    if (command.type === 'close') {
      await Promise.all([...client.activeCommands])
      await this.closeClient(client)
      return {}
    }
    if (!client.identity || !client.credentialKey) {
      throw new BrowserBrokerError('not-connected', 'hello must be the first command on a port')
    }

    switch (command.type) {
      case 'attach':
        await this.attach(client, command)
        return {}
      case 'detach':
        await this.detach(client, command.subscriptionId)
        return {}
      case 'ack':
        this.ack(client, command)
        return {}
      case 'refresh-credentials': {
        const incoming = new Uint8Array(command.credentials)
        let listenerFailure: unknown
        try {
          listenerFailure = this.credentialStores
            .get(client.credentialKey)!
            .update(command.credentialRevision, incoming)
        } finally {
          incoming.fill(0)
        }
        if (listenerFailure !== undefined) {
          await this.invalidateIdentity(client, client.credentialKey)
          throw new BrowserBrokerError(
            'source-failed',
            'A source rejected the refreshed credentials; the broker identity was closed'
          )
        }
        return {}
      }
      case 'publish': {
        if (!this.options.publish) {
          throw new BrowserBrokerError('unavailable', 'Brokered publish is not configured')
        }
        await this.withResources(async () => {
          await this.options.publish!(
            this.operationContext(client, command.operation, command.data)
          )
          this.telemetryEvent('publish')
        })
        return {}
      }
      case 'request': {
        if (!this.options.request) {
          throw new BrowserBrokerError('unavailable', 'Brokered request is not configured')
        }
        const response = await this.withResources(() =>
          this.options.request!(this.operationContext(client, command.operation, command.data))
        )
        if (!(response instanceof Uint8Array)) {
          throw new BrowserBrokerError('source-failed', 'Brokered request must return Uint8Array')
        }
        const result = copyBuffer(response)
        this.telemetryEvent('request')
        return { result, transfers: [result] }
      }
      case 'restart':
        await this.restart(client, command.subscriptionId)
        return {}
      case 'stats':
        return { result: this.inspect() }
      case 'heartbeat':
        return {}
      default:
        throw new BrowserBrokerError('invalid-command', 'Unexpected hello command')
    }
  }

  private async hello(
    client: HostClient,
    command: Extract<BrowserBrokerCommand, { type: 'hello' }>
  ): Promise<void> {
    if (client.identity) {
      if (identityKey(client.identity) !== identityKey(command.identity)) {
        throw new BrowserBrokerError(
          'identity-mismatch',
          'Tenant and authentication context are immutable on a broker port'
        )
      }
      throw new BrowserBrokerError('invalid-state', 'hello was already accepted on this port')
    }
    const identity = cloneIdentity(command.identity)
    const key = identityKey(identity)
    const incoming = new Uint8Array(command.credentials)
    let store = this.credentialStores.get(key)
    let listenerFailure: unknown
    try {
      if (!store) {
        store = new MutableCredentials(command.credentialRevision, incoming)
        this.credentialStores.set(key, store)
      } else {
        listenerFailure = store.accept(command.credentialRevision, incoming)
      }
    } finally {
      incoming.fill(0)
    }
    if (listenerFailure !== undefined) {
      await this.invalidateIdentity(client, key)
      throw new BrowserBrokerError(
        'source-failed',
        'A source rejected the refreshed credentials; the broker identity was closed'
      )
    }
    client.identity = identity
    client.credentialKey = key
    this.telemetryEvent('tab-connected')
    this.telemetryGauge('natsail.browser.broker.tabs.active', this.inspect().tabCount)
  }

  private async attach(
    client: HostClient,
    command: Extract<BrowserBrokerCommand, { type: 'attach' }>
  ): Promise<void> {
    await this.awaitIdleResources()
    if (client.subscriptions.has(command.subscriptionId)) {
      throw new BrowserBrokerError(
        'subscription-exists',
        `Subscription ${command.subscriptionId} already exists`
      )
    }
    const identity = client.identity!
    const key = sourceKey(identity, command.source)
    let source = this.sources.get(key)
    let created = false
    if (source && source.descriptor.contract !== command.source.contract) {
      throw new BrowserBrokerError(
        'contract-mismatch',
        `Source ${command.source.key} is active with a different contract`
      )
    }
    if (!source) {
      try {
        source = await this.openSource(identity, command.source, command.resumeAfter)
        created = true
      } catch (error) {
        await this.closeIdleResources().catch(() => {
          this.telemetryEvent('idle-resource-close-failed')
        })
        throw error
      }
    }
    if (source.idleTimer) {
      clearTimeout(source.idleTimer)
      source.idleTimer = undefined
    }

    const subscription: HostSubscription = {
      id: command.subscriptionId,
      client,
      source,
      pending: [],
      pendingBytes: 0,
      inflight: undefined,
      nextBatchId: 1,
      lastCursor: copyCursor(command.resumeAfter),
      resumeRequired: false,
    }
    client.subscriptions.set(subscription.id, subscription)
    source.subscriptions.add(subscription)
    this.postState(subscription, 'connecting')
    if (!created && command.reattach) this.enqueueRetained(subscription, command.resumeAfter)
    this.telemetryEvent('subscription-attached')

    try {
      const snapshot = source.handle.getSnapshot()
      if (!created && snapshot.phase === 'error') await this.restartSource(source)
      await source.handle.ready
      if (!subscription.resumeRequired) {
        this.postState(subscription, 'live', undefined, subscription.lastCursor)
        this.flush(subscription)
      }
      this.recordQueueTelemetry()
    } catch (error) {
      if (client.subscriptions.get(subscription.id) === subscription) {
        client.subscriptions.delete(subscription.id)
        source.subscriptions.delete(subscription)
        if (source.subscriptions.size === 0) await this.scheduleTeardown(source)
      }
      throw error
    }
  }

  private async openSource(
    identity: BrowserBrokerIdentity,
    descriptor: BrowserBrokerSourceDescriptor,
    resumeAfter?: BrowserBrokerCursor
  ): Promise<PhysicalSource> {
    this.resourcesActive = true
    const key = sourceKey(identity, descriptor)
    const credentials = this.credentialStores.get(identityKey(identity))
    if (!credentials) throw new BrowserBrokerError('not-connected', 'Credentials are unavailable')
    const source = this.options.createSource({
      identity: cloneIdentity(identity),
      descriptor: Object.freeze({ ...descriptor }),
      credentials,
      ...(resumeAfter === undefined ? {} : { resumeAfter: copyCursor(resumeAfter) }),
    })
    const handle = this.options.sessions.acquire(
      defineSession({ key, contract: descriptor.contract, source })
    )
    let unsubscribe: () => void = () => undefined
    const physical: PhysicalSource = {
      key,
      identityKey: identityKey(identity),
      identity: cloneIdentity(identity),
      descriptor: Object.freeze({ ...descriptor }),
      handle,
      subscriptions: new Set(),
      unsubscribe: () => unsubscribe(),
      log: [],
      logBytes: 0,
      logTruncated: false,
      lastValueRevision: 0,
      idleTimer: undefined,
      closed: false,
    }
    try {
      unsubscribe = handle.subscribe(() => this.syncSource(physical))
      this.sources.set(key, physical)
      this.syncSource(physical)
      this.telemetryEvent('physical-source-opened')
      this.telemetryGauge('natsail.browser.broker.sources.active', this.sources.size)
      return physical
    } catch (error) {
      physical.closed = true
      unsubscribe()
      if (this.sources.get(key) === physical) this.sources.delete(key)
      await handle.release().catch(() => {
        this.telemetryEvent('physical-source-release-failed')
      })
      throw error
    }
  }

  private syncSource(source: PhysicalSource): void {
    if (!source || source.closed) return
    const snapshot = source.handle.getSnapshot()
    if (snapshot.valueRevision > source.lastValueRevision && snapshot.value !== undefined) {
      source.lastValueRevision = snapshot.valueRevision
      this.appendDelivery(source, snapshot.value)
    }
    if (snapshot.phase === 'error') {
      for (const subscription of source.subscriptions) {
        this.postState(subscription, 'error', 'source-error', subscription.lastCursor)
      }
    } else if (snapshot.phase === 'closed') {
      for (const subscription of source.subscriptions) {
        this.postState(subscription, 'closed', undefined, subscription.lastCursor)
      }
    }
  }

  private appendDelivery(source: PhysicalSource, delivery: BrowserBrokerDelivery): void {
    if (!(delivery.data instanceof Uint8Array)) {
      throw new BrowserBrokerError('source-failed', 'Source delivery data must be Uint8Array')
    }
    if (delivery.cursor !== undefined) parseCursor(delivery.cursor)
    const entry: LogEntry = {
      data: copyBytes(delivery.data),
      ...(delivery.cursor === undefined ? {} : { cursor: copyCursor(delivery.cursor) }),
    }
    source.log.push(entry)
    source.logBytes += entry.data.byteLength
    while (source.log.length > this.maxRetainedItems || source.logBytes > this.maxRetainedBytes) {
      const removed = source.log.shift()
      if (!removed) break
      source.logBytes -= removed.data.byteLength
      source.logTruncated = true
    }
    for (const subscription of source.subscriptions) this.enqueue(subscription, entry)
    this.recordQueueTelemetry()
  }

  private enqueueRetained(
    subscription: HostSubscription,
    resumeAfter: BrowserBrokerCursor | undefined
  ): void {
    if (resumeAfter === undefined) {
      if (subscription.source.logTruncated) {
        this.requireResume(subscription)
        return
      }
      for (const entry of subscription.source.log) this.enqueue(subscription, entry)
      return
    }
    const index = subscription.source.log.findIndex((entry) =>
      cursorsEqual(entry.cursor, resumeAfter)
    )
    if (index < 0) {
      this.requireResume(subscription)
      return
    }
    for (const entry of subscription.source.log.slice(index + 1)) this.enqueue(subscription, entry)
  }

  private enqueue(subscription: HostSubscription, entry: LogEntry): void {
    if (subscription.resumeRequired) return
    const queued = this.clientQueueUsage(subscription.client)
    if (
      entry.data.byteLength > this.maxBatchBytes ||
      queued.items + 1 > this.maxTabQueueItems ||
      queued.bytes + entry.data.byteLength > this.maxTabQueueBytes
    ) {
      this.requireResume(subscription)
      return
    }
    subscription.pending.push(entry)
    subscription.pendingBytes += entry.data.byteLength
    this.flush(subscription)
  }

  private clientQueueUsage(client: HostClient): { readonly items: number; readonly bytes: number } {
    let items = 0
    let bytes = 0
    for (const subscription of client.subscriptions.values()) {
      items += subscription.pending.length + (subscription.inflight?.items ?? 0)
      bytes += subscription.pendingBytes + (subscription.inflight?.bytes ?? 0)
    }
    return { items, bytes }
  }

  private requireResume(subscription: HostSubscription): void {
    subscription.resumeRequired = true
    subscription.pending = []
    subscription.pendingBytes = 0
    subscription.inflight = undefined
    this.postState(subscription, 'resume-required', 'lagged', subscription.lastCursor)
    this.telemetryEvent('tab-lagged')
    this.recordQueueTelemetry()
  }

  private flush(subscription: HostSubscription): void {
    if (
      subscription.resumeRequired ||
      subscription.inflight !== undefined ||
      subscription.pending.length === 0
    ) {
      return
    }
    const entries: LogEntry[] = []
    let bytes = 0
    while (entries.length < this.maxBatchItems && subscription.pending.length > 0) {
      const next = subscription.pending[0]!
      if (entries.length > 0 && bytes + next.data.byteLength > this.maxBatchBytes) break
      subscription.pending.shift()
      subscription.pendingBytes -= next.data.byteLength
      entries.push(next)
      bytes += next.data.byteLength
    }
    const cursor = copyCursor(entries.at(-1)?.cursor)
    const inflight: InflightBatch = {
      id: subscription.nextBatchId,
      entries,
      items: entries.length,
      bytes,
      ...(cursor === undefined ? {} : { cursor }),
    }
    subscription.nextBatchId += 1
    subscription.inflight = inflight

    const items = entries.map((entry) => {
      const data = copyBuffer(entry.data)
      return {
        data,
        ...(entry.cursor === undefined ? {} : { cursor: copyCursor(entry.cursor) }),
      }
    })
    const message: BrowserBrokerBatchMessage = {
      ...envelope(),
      type: 'batch',
      subscriptionId: subscription.id,
      batchId: inflight.id,
      items,
    }
    this.post(
      subscription.client,
      message,
      items.map((item) => item.data)
    )
    this.recordQueueTelemetry()
  }

  private ack(client: HostClient, command: Extract<BrowserBrokerCommand, { type: 'ack' }>): void {
    const subscription = this.subscription(client, command.subscriptionId)
    const inflight = subscription.inflight
    if (!inflight || inflight.id !== command.batchId) {
      throw new BrowserBrokerError('invalid-state', 'The acknowledgement batch is not in flight')
    }
    if (!cursorsEqual(inflight.cursor, command.cursor)) {
      throw new BrowserBrokerError('invalid-state', 'The acknowledgement cursor does not match')
    }
    subscription.lastCursor = copyCursor(command.cursor)
    subscription.inflight = undefined
    this.flush(subscription)
    this.recordQueueTelemetry()
  }

  private async restart(client: HostClient, subscriptionId: string): Promise<void> {
    const subscription = this.subscription(client, subscriptionId)
    await this.restartSource(subscription.source)
  }

  private async restartSource(source: PhysicalSource): Promise<void> {
    this.telemetryEvent('physical-source-reconnect')
    await source.handle.restart()
  }

  private subscription(client: HostClient, subscriptionId: string): HostSubscription {
    const subscription = client.subscriptions.get(subscriptionId)
    if (!subscription) {
      throw new BrowserBrokerError(
        'subscription-missing',
        `Subscription ${subscriptionId} does not exist`
      )
    }
    return subscription
  }

  private async detach(client: HostClient, subscriptionId: string): Promise<void> {
    const subscription = this.subscription(client, subscriptionId)
    client.subscriptions.delete(subscriptionId)
    subscription.source.subscriptions.delete(subscription)
    this.telemetryEvent('subscription-detached')
    this.recordQueueTelemetry()
    if (subscription.source.subscriptions.size === 0) {
      await this.scheduleTeardown(subscription.source)
    }
  }

  private async scheduleTeardown(source: PhysicalSource): Promise<void> {
    if (source.closed || source.idleTimer) return
    if (this.idleTeardownMs === 0) {
      await this.teardownSource(source)
      return
    }
    source.idleTimer = setTimeout(() => {
      source.idleTimer = undefined
      if (source.subscriptions.size === 0) void this.teardownSource(source)
    }, this.idleTeardownMs)
  }

  private async teardownSource(source: PhysicalSource): Promise<void> {
    if (source.closed) return
    source.closed = true
    if (source.idleTimer) clearTimeout(source.idleTimer)
    source.unsubscribe()
    if (this.sources.get(source.key) === source) this.sources.delete(source.key)
    await source.handle.release().catch(() => undefined)
    source.log = []
    source.logBytes = 0
    this.telemetryEvent('physical-source-closed')
    this.telemetryGauge('natsail.browser.broker.sources.active', this.sources.size)
    this.cleanupCredentials(source.identityKey)
    if (this.sources.size === 0) await this.closeIdleResources()
  }

  private operationContext(
    client: HostClient,
    operation: string,
    data: ArrayBuffer
  ): BrowserBrokerOperationContext {
    return {
      identity: cloneIdentity(client.identity!),
      operation,
      data: new Uint8Array(data),
      credentials: this.credentialStores.get(client.credentialKey!)!,
    }
  }

  private async awaitIdleResources(): Promise<void> {
    await this.idleResourceClose?.catch(() => undefined)
  }

  private async withResources<T>(operation: () => T | Promise<T>): Promise<T> {
    await this.awaitIdleResources()
    this.resourcesActive = true
    this.activeOperations += 1
    try {
      return await operation()
    } finally {
      this.activeOperations -= 1
      if (this.activeOperations === 0 && this.sources.size === 0) {
        await this.closeIdleResources().catch(() => {
          this.telemetryEvent('idle-resource-close-failed')
        })
      }
    }
  }

  private closeIdleResources(): Promise<void> {
    if (
      !this.resourcesActive ||
      !this.options.closeIdleResources ||
      this.sources.size > 0 ||
      this.activeOperations > 0
    ) {
      return Promise.resolve()
    }
    if (this.idleResourceClose) return this.idleResourceClose
    const closing = Promise.resolve().then(() => this.options.closeIdleResources!())
    this.idleResourceClose = closing
    void closing.then(
      () => {
        this.resourcesActive = false
        if (this.idleResourceClose === closing) this.idleResourceClose = undefined
      },
      () => {
        if (this.idleResourceClose === closing) this.idleResourceClose = undefined
      }
    )
    return closing
  }

  private async closeClient(client: HostClient): Promise<void> {
    if (client.closed) return
    client.closing = true
    client.closed = true
    for (const subscription of [...client.subscriptions.values()]) {
      await this.detach(client, subscription.id).catch(() => undefined)
    }
    this.clients.delete(client.port)
    client.port.onmessage = null
    client.port.onmessageerror = null
    if (client.identity) {
      this.telemetryEvent('tab-disconnected')
      this.telemetryGauge('natsail.browser.broker.tabs.active', this.inspect().tabCount)
    }
    if (client.credentialKey) this.cleanupCredentials(client.credentialKey)
  }

  private async invalidateIdentity(current: HostClient, key: string): Promise<void> {
    const invalidate = async (client: HostClient) => {
      if (client.credentialKey !== key) return
      const subscriptions = [...client.subscriptions.values()]
      for (const subscription of subscriptions) {
        this.postState(subscription, 'error', 'source-error', subscription.lastCursor)
        await this.detach(client, subscription.id).catch(() => undefined)
      }
      delete client.identity
      delete client.credentialKey
      this.telemetryEvent('identity-closed')
    }
    const invalidations = [...this.clients.values()]
      .filter((client) => client.credentialKey === key)
      .map((client) => {
        if (client === current) return invalidate(client)
        const queued = client.pending.then(() => invalidate(client))
        client.pending = queued.catch(() => undefined)
        return queued
      })
    await Promise.all(invalidations)
    this.cleanupCredentials(key)
    this.telemetryGauge('natsail.browser.broker.tabs.active', this.inspect().tabCount)
  }

  private enqueueClientClose(client: HostClient): Promise<void> {
    if (client.closed) return Promise.resolve()
    client.closing = true
    const close = client.pending
      .then(() => Promise.all([...client.activeCommands]))
      .then(() => this.closeClient(client))
    client.pending = close.catch(() => undefined)
    return close
  }

  private cleanupCredentials(key: string): void {
    const usedByClient = [...this.clients.values()].some((client) => client.credentialKey === key)
    const usedBySource = [...this.sources.values()].some((source) => source.identityKey === key)
    if (usedByClient || usedBySource) return
    this.credentialStores.get(key)?.dispose()
    this.credentialStores.delete(key)
  }

  private postResult(
    client: HostClient,
    id: number,
    result?: unknown,
    transfers: Transferable[] = []
  ): void {
    if (client.closed && result !== undefined) return
    const message: BrowserBrokerResult = {
      ...envelope(),
      type: 'result',
      requestId: id,
      ok: true,
      ...(result === undefined ? {} : { result }),
    }
    this.post(client, message, transfers)
  }

  private postFailure(client: HostClient, id: number, error: unknown): void {
    const failure =
      error instanceof BrowserBrokerError
        ? error
        : new BrowserBrokerError(
            'source-failed',
            error instanceof Error ? error.message : String(error)
          )
    const message: BrowserBrokerResult = {
      ...envelope(),
      type: 'result',
      requestId: id,
      ok: false,
      error: { code: failure.code, message: failure.message },
    }
    this.post(client, message)
  }

  private postState(
    subscription: HostSubscription,
    state: BrowserBrokerStateMessage['state'],
    reason?: BrowserBrokerStateMessage['reason'],
    cursor?: BrowserBrokerCursor
  ): void {
    this.post(subscription.client, {
      ...envelope(),
      type: 'state',
      subscriptionId: subscription.id,
      state,
      ...(reason === undefined ? {} : { reason }),
      ...(cursor === undefined ? {} : { cursor: copyCursor(cursor) }),
    } satisfies BrowserBrokerStateMessage)
  }

  private telemetryEvent(action: string): void {
    if (!this.telemetry.enabled) return
    this.recordTelemetry({
      type: 'counter',
      name: 'natsail.browser.broker.events',
      value: 1,
      at: this.telemetry.now(),
      attributes: { action, source: 'browser-broker' },
    })
  }

  private telemetryGauge(
    name: Extract<NatsailTelemetryEvent, { type: 'gauge' }>['name'],
    value: number
  ) {
    if (!this.telemetry.enabled) return
    this.recordTelemetry({
      type: 'gauge',
      name,
      value,
      at: this.telemetry.now(),
      attributes: { source: 'browser-broker' },
    })
  }

  private recordQueueTelemetry(): void {
    const inspection = this.inspect()
    this.telemetryGauge('natsail.browser.broker.queue.depth', inspection.queuedItems)
    this.telemetryGauge('natsail.browser.broker.queue.bytes', inspection.queuedBytes)
  }

  private recordTelemetry(event: NatsailTelemetryEvent): void {
    if (this.telemetry.enabled) this.telemetry.record(event)
  }

  private post(
    client: HostClient,
    message: BrowserBrokerMessage,
    transfers: Transferable[] = []
  ): void {
    try {
      client.port.postMessage(message, transfers)
    } catch {
      void this.closeClient(client)
    }
  }

  private now(): number {
    return this.telemetry.now()
  }
}

/** Creates the protocol-v1 host that belongs in a SharedWorker global scope. */
export function createBrowserBrokerWorker(
  options: BrowserBrokerWorkerOptions
): BrowserBrokerWorkerHost {
  return new DefaultBrowserBrokerWorkerHost(options)
}

export type BrowserBrokerPortConnector = () => MessagePort | Promise<MessagePort>

const tabLocalBrokerHosts = new WeakMap<MessagePort, BrowserBrokerWorkerHost>()

export interface BrowserBrokerClientOptions {
  readonly identity: BrowserBrokerIdentity
  readonly credentials: () =>
    | BrowserBrokerCredentialSnapshot
    | Promise<BrowserBrokerCredentialSnapshot>
  /** Opens a MessagePort to the SharedWorker. */
  readonly connect: BrowserBrokerPortConnector
  /** Explicit tab-local connector used only when `connect` fails and strict mode is disabled. */
  readonly fallback?: BrowserBrokerPortConnector
  readonly strict?: boolean
  /** Heartbeat interval. Set to 0 for tests or externally managed heartbeats. Defaults to 10 seconds. */
  readonly heartbeatIntervalMs?: number
  /** Request timeout used to surface an unavailable/restarted worker. Defaults to 5 seconds. */
  readonly requestTimeoutMs?: number
  readonly telemetry?: NatsailTelemetrySink
  readonly telemetryAttributes?: NatsailTelemetryAttributes
  readonly telemetryClock?: NatsailTelemetryClock
}

export interface BrowserBrokerClient {
  readonly mode: 'shared-worker' | 'tab-local'
  connect(): Promise<void>
  createSource(descriptor: BrowserBrokerSourceDescriptor): BrowserBrokerSessionSource
  /** Publishes through the worker's application-authorized operation mapping. */
  publish(operation: string, data?: Uint8Array): Promise<void>
  /** Requests through the worker's application-authorized operation mapping. */
  request(operation: string, data?: Uint8Array): Promise<Uint8Array>
  refreshCredentials(): Promise<void>
  restart(subscriptionId: string): Promise<void>
  reconnect(): Promise<void>
  stats(): Promise<BrowserBrokerStats>
  close(): Promise<void>
}

export interface BrowserBrokerSubscriptionLease extends SubscriptionLease {
  readonly subscriptionId: string
  restart(): Promise<void>
}

export type BrowserBrokerSessionSource = (
  accept: (value: BrowserBrokerDelivery) => Promise<void>
) => BrowserBrokerSubscriptionLease

type Deferred<T> = {
  readonly promise: Promise<T>
  readonly resolve: (value: T | PromiseLike<T>) => void
  readonly reject: (reason?: unknown) => void
}

function deferred<T>(): Deferred<T> {
  let resolve!: Deferred<T>['resolve']
  let reject!: Deferred<T>['reject']
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, resolve, reject }
}

interface PendingClientRequest {
  readonly resolve: (value: unknown) => void
  readonly reject: (error: unknown) => void
  readonly timer: ReturnType<typeof setTimeout>
}

class ClientSourceLease implements BrowserBrokerSubscriptionLease {
  readonly ready: Promise<void>
  readonly closed: Promise<void>
  readonly subscriptionId: string
  readonly descriptor: BrowserBrokerSourceDescriptor
  lastCursor: BrowserBrokerCursor | undefined

  private readonly readyState = deferred<void>()
  private readonly closedState = deferred<void>()
  private acceptChain = Promise.resolve()
  private readySettled = false
  private closedSettled = false
  private terminal = false

  constructor(
    private readonly client: DefaultBrowserBrokerClient,
    descriptor: BrowserBrokerSourceDescriptor,
    private readonly accept: (value: BrowserBrokerDelivery) => Promise<void>
  ) {
    this.subscriptionId = `subscription-${client.nextSubscriptionNumber()}`
    this.descriptor = Object.freeze({ ...descriptor })
    this.ready = this.readyState.promise
    this.closed = this.closedState.promise
    void this.closed.catch(() => undefined)
    void this.attach()
  }

  close(): Promise<void> {
    return this.client.detachLease(this)
  }

  restart(): Promise<void> {
    return this.client.restart(this.subscriptionId)
  }

  attached(): void {
    if (!this.readySettled) {
      this.readySettled = true
      this.readyState.resolve()
    }
  }

  receive(batch: BrowserBrokerBatchMessage, generation: number): void {
    this.acceptChain = this.acceptChain
      .then(async () => {
        let cursor: BrowserBrokerCursor | undefined
        for (const item of batch.items) {
          if (this.terminal || !this.client.isCurrentGeneration(generation)) return
          cursor = copyCursor(item.cursor)
          await this.accept({
            data: new Uint8Array(item.data),
            ...(cursor === undefined ? {} : { cursor }),
          })
          if (this.terminal || !this.client.isCurrentGeneration(generation)) return
        }
        this.lastCursor = copyCursor(cursor)
        await this.client.ack(this, batch.batchId, cursor, generation)
      })
      .catch((error) => {
        // A replaced port rejects its pending acknowledgements. That failure
        // belongs to the old worker generation; the reattached lease remains live.
        if (this.client.isCurrentGeneration(generation)) this.client.failLease(this, error)
      })
  }

  fail(error: unknown): void {
    this.terminal = true
    if (!this.readySettled) {
      this.readySettled = true
      this.readyState.reject(error)
    }
    if (!this.closedSettled) {
      this.closedSettled = true
      this.closedState.reject(error)
    }
  }

  finish(): void {
    this.terminal = true
    if (!this.readySettled) {
      this.readySettled = true
      this.readyState.resolve()
    }
    if (!this.closedSettled) {
      this.closedSettled = true
      this.closedState.resolve()
    }
  }

  async reattach(): Promise<void> {
    if (this.terminal) return
    await this.client.attachLease(this, true)
  }

  private async attach(): Promise<void> {
    try {
      await this.client.attachLease(this)
      this.attached()
    } catch (error) {
      this.client.failLease(this, error)
    }
  }
}

class DefaultBrowserBrokerClient implements BrowserBrokerClient {
  mode: 'shared-worker' | 'tab-local' = 'shared-worker'
  private readonly telemetry: NatsailTelemetryReporter
  private readonly pending = new Map<number, PendingClientRequest>()
  private readonly leases = new Map<string, ClientSourceLease>()
  private readonly heartbeatIntervalMs: number
  private readonly requestTimeoutMs: number
  private port: MessagePort | undefined
  private localHost: BrowserBrokerWorkerHost | undefined
  private heartbeatTimer: ReturnType<typeof setInterval> | undefined
  private requestNumber = 1
  private subscriptionNumber = 1
  private connectPromise: Promise<void> | undefined
  private connectionGeneration = 0
  private closed = false

  constructor(private readonly options: BrowserBrokerClientOptions) {
    this.heartbeatIntervalMs = nonNegativeInteger(
      options.heartbeatIntervalMs,
      10_000,
      'heartbeatIntervalMs'
    )
    this.requestTimeoutMs = positiveInteger(options.requestTimeoutMs, 5_000, 'requestTimeoutMs')
    this.telemetry = createNatsailTelemetryReporter({
      ...(options.telemetry === undefined ? {} : { sink: options.telemetry }),
      ...(options.telemetryClock === undefined ? {} : { clock: options.telemetryClock }),
      ...(options.telemetryAttributes === undefined
        ? {}
        : { attributes: options.telemetryAttributes }),
    })
  }

  async connect(): Promise<void> {
    this.assertOpen()
    this.connectPromise ??= this.establish(false).finally(() => {
      this.connectPromise = undefined
    })
    return this.connectPromise
  }

  createSource(descriptor: BrowserBrokerSourceDescriptor): BrowserBrokerSessionSource {
    this.assertOpen()
    const parsed = Object.freeze(parseSource(descriptor))
    return (accept) => new ClientSourceLease(this, parsed, accept)
  }

  async refreshCredentials(): Promise<void> {
    this.assertOpen()
    const credentials = parseCredentialSnapshot(await this.options.credentials())
    const buffer = copyBuffer(credentials.bytes)
    await this.sendCommand(
      {
        ...envelope(),
        type: 'refresh-credentials',
        requestId: this.nextRequestId(),
        credentialRevision: credentials.revision,
        credentials: buffer,
      },
      [buffer]
    )
  }

  async publish(operation: string, data: Uint8Array = new Uint8Array(0)): Promise<void> {
    this.assertOpen()
    const buffer = copyBuffer(byteArray(data, 'data'))
    await this.sendCommand(
      {
        ...envelope(),
        type: 'publish',
        requestId: this.nextRequestId(),
        operation: stringField(operation, 'operation'),
        data: buffer,
      },
      [buffer]
    )
  }

  async request(operation: string, data: Uint8Array = new Uint8Array(0)): Promise<Uint8Array> {
    this.assertOpen()
    const buffer = copyBuffer(byteArray(data, 'data'))
    const result = await this.sendCommand(
      {
        ...envelope(),
        type: 'request',
        requestId: this.nextRequestId(),
        operation: stringField(operation, 'operation'),
        data: buffer,
      },
      [buffer]
    )
    if (Object.prototype.toString.call(result) !== '[object ArrayBuffer]') {
      throw new BrowserBrokerError('invalid-state', 'Brokered request returned invalid bytes')
    }
    return new Uint8Array(result as ArrayBuffer)
  }

  restart(subscriptionId: string): Promise<void> {
    this.assertOpen()
    return this.sendCommand({
      ...envelope(),
      type: 'restart',
      requestId: this.nextRequestId(),
      subscriptionId,
    }).then(() => undefined)
  }

  async reconnect(): Promise<void> {
    if (this.closed)
      throw new BrowserBrokerError('invalid-state', 'The browser broker client is closed')
    this.connectPromise ??= this.establish(true).finally(() => {
      this.connectPromise = undefined
    })
    await this.connectPromise
  }

  async stats(): Promise<BrowserBrokerStats> {
    this.assertOpen()
    return parseStats(
      await this.sendCommand({
        ...envelope(),
        type: 'stats',
        requestId: this.nextRequestId(),
      })
    )
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer)
    if (this.port) {
      await this.sendCommand({
        ...envelope(),
        type: 'close',
        requestId: this.nextRequestId(),
      }).catch(() => undefined)
      this.port.close()
      this.port = undefined
    }
    const localHost = this.localHost
    this.localHost = undefined
    await localHost?.close().catch(() => undefined)
    for (const lease of this.leases.values()) lease.finish()
    this.leases.clear()
    this.rejectPending(new BrowserBrokerError('unavailable', 'The browser broker client closed'))
  }

  nextSubscriptionNumber(): number {
    const value = this.subscriptionNumber
    this.subscriptionNumber += 1
    return value
  }

  async attachLease(lease: ClientSourceLease, reattach = false): Promise<void> {
    this.assertOpen()
    if (!this.port) await this.connect()
    this.leases.set(lease.subscriptionId, lease)
    try {
      await this.sendCommand({
        ...envelope(),
        type: 'attach',
        requestId: this.nextRequestId(),
        subscriptionId: lease.subscriptionId,
        source: lease.descriptor,
        reattach,
        ...(lease.lastCursor === undefined ? {} : { resumeAfter: copyCursor(lease.lastCursor) }),
      })
    } catch (error) {
      if (
        error instanceof BrowserBrokerError &&
        error.code !== 'unavailable' &&
        this.leases.get(lease.subscriptionId) === lease
      ) {
        this.leases.delete(lease.subscriptionId)
      }
      throw error
    }
  }

  async detachLease(lease: ClientSourceLease): Promise<void> {
    if (this.leases.get(lease.subscriptionId) !== lease) {
      lease.finish()
      return
    }
    this.leases.delete(lease.subscriptionId)
    if (this.port) {
      await this.sendCommand({
        ...envelope(),
        type: 'detach',
        requestId: this.nextRequestId(),
        subscriptionId: lease.subscriptionId,
      }).catch(() => undefined)
    }
    lease.finish()
  }

  ack(
    lease: ClientSourceLease,
    batchId: number,
    cursor: BrowserBrokerCursor | undefined,
    generation: number
  ): Promise<void> {
    if (!this.isCurrentGeneration(generation)) return Promise.resolve()
    return this.sendCommand({
      ...envelope(),
      type: 'ack',
      requestId: this.nextRequestId(),
      subscriptionId: lease.subscriptionId,
      batchId,
      ...(cursor === undefined ? {} : { cursor: copyCursor(cursor) }),
    }).then(() => undefined)
  }

  private async establish(reconnect: boolean): Promise<void> {
    this.assertOpen()
    const generation = this.connectionGeneration + 1
    this.connectionGeneration = generation
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer)
    const oldPort = this.port
    const oldLocalHost = this.localHost
    this.port = undefined
    this.localHost = undefined
    if (oldPort) {
      oldPort.onmessage = null
      oldPort.onmessageerror = null
      oldPort.close()
    }
    await oldLocalHost?.close().catch(() => undefined)
    this.rejectPending(new BrowserBrokerError('unavailable', 'The browser broker port changed'))

    let port: MessagePort
    try {
      port = await this.options.connect()
      this.mode = 'shared-worker'
    } catch (error) {
      if (this.options.strict || !this.options.fallback) {
        const unavailable = new BrowserBrokerError(
          'unavailable',
          `SharedWorker broker is unavailable: ${error instanceof Error ? error.message : String(error)}`
        )
        if (reconnect) {
          for (const lease of [...this.leases.values()]) this.failLease(lease, unavailable)
        }
        throw unavailable
      }
      try {
        port = await this.options.fallback()
      } catch (fallbackError) {
        const unavailable = new BrowserBrokerError(
          'unavailable',
          `The tab-local broker fallback is unavailable: ${fallbackError instanceof Error ? fallbackError.message : String(fallbackError)}`
        )
        if (reconnect) {
          for (const lease of [...this.leases.values()]) this.failLease(lease, unavailable)
        }
        throw unavailable
      }
      this.mode = 'tab-local'
      this.telemetryEvent('fallback')
    }
    const localHost = tabLocalBrokerHosts.get(port)
    try {
      this.assertOpen()
      this.port = port
      this.localHost = localHost
      port.onmessage = (event: MessageEvent<unknown>) => this.receive(event.data, generation)
      port.onmessageerror = () => {
        if (!this.isCurrentGeneration(generation)) return
        void this.reconnect().catch((error) => {
          for (const lease of [...this.leases.values()]) this.failLease(lease, error)
        })
      }
      port.start()

      const credentials = parseCredentialSnapshot(await this.options.credentials())
      const buffer = copyBuffer(credentials.bytes)
      await this.sendCommand(
        {
          ...envelope(),
          type: 'hello',
          requestId: this.nextRequestId(),
          identity: cloneIdentity(this.options.identity),
          credentialRevision: credentials.revision,
          credentials: buffer,
        },
        [buffer]
      )
      if (reconnect) {
        this.telemetryEvent('worker-reconnect')
        for (const lease of [...this.leases.values()]) {
          try {
            await lease.reattach()
          } catch (error) {
            if (error instanceof BrowserBrokerError && error.code === 'unavailable') throw error
            this.failLease(lease, error)
          }
        }
      }
      if (this.heartbeatIntervalMs > 0) {
        this.heartbeatTimer = setInterval(() => {
          if (!this.port) return
          void this.sendCommand({
            ...envelope(),
            type: 'heartbeat',
            requestId: this.nextRequestId(),
          }).catch(() => undefined)
        }, this.heartbeatIntervalMs)
      }
    } catch (error) {
      if (this.port === port) this.port = undefined
      if (this.localHost === localHost) this.localHost = undefined
      port.onmessage = null
      port.onmessageerror = null
      port.close()
      await localHost?.close().catch(() => undefined)
      if (reconnect) {
        for (const lease of [...this.leases.values()]) this.failLease(lease, error)
      }
      throw error
    }
  }

  private receive(value: unknown, generation: number): void {
    if (generation !== this.connectionGeneration) return
    let message: BrowserBrokerMessage
    try {
      message = parseBrowserBrokerMessage(value)
    } catch (error) {
      for (const lease of [...this.leases.values()]) this.failLease(lease, error)
      return
    }
    if (message.type === 'result') {
      const pending = this.pending.get(message.requestId)
      if (!pending) return
      clearTimeout(pending.timer)
      this.pending.delete(message.requestId)
      if (message.ok) {
        pending.resolve(message.result)
      } else {
        pending.reject(
          new BrowserBrokerError(
            message.error?.code ?? 'invalid-state',
            message.error?.message ?? 'Browser broker request failed'
          )
        )
      }
      return
    }
    if (this.closed) return
    const lease = this.leases.get(message.subscriptionId)
    if (!lease) return
    if (message.type === 'batch') {
      lease.receive(message, generation)
      return
    }
    if (message.state === 'live') {
      return
    } else if (message.state === 'resume-required') {
      this.telemetryEvent('tab-lagged')
      this.failLease(lease, new BrowserBrokerResumeRequiredError(message.cursor))
    } else if (message.state === 'error') {
      this.failLease(
        lease,
        new BrowserBrokerError('source-failed', 'The physical broker source failed')
      )
    } else if (message.state === 'closed') {
      void this.detachLease(lease).catch(() => lease.finish())
    }
  }

  failLease(lease: ClientSourceLease, error: unknown): void {
    lease.fail(error)
    void this.detachLease(lease).catch(() => undefined)
  }

  private sendCommand(
    command: BrowserBrokerCommand,
    transfers: Transferable[] = []
  ): Promise<unknown> {
    if (!this.port) {
      return Promise.reject(new BrowserBrokerError('unavailable', 'No browser broker port exists'))
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(command.requestId)
        const error = new BrowserBrokerError('unavailable', 'The browser broker request timed out')
        reject(error)
        if (command.type !== 'hello' && !this.closed) {
          void this.reconnect().catch((reconnectError) => {
            for (const lease of [...this.leases.values()]) {
              this.failLease(lease, reconnectError)
            }
          })
        }
      }, this.requestTimeoutMs)
      this.pending.set(command.requestId, { resolve, reject, timer })
      try {
        this.port!.postMessage(command, transfers)
      } catch (error) {
        clearTimeout(timer)
        this.pending.delete(command.requestId)
        const unavailable = new BrowserBrokerError(
          'unavailable',
          `The browser broker request could not be sent: ${error instanceof Error ? error.message : String(error)}`
        )
        reject(unavailable)
        if (command.type !== 'hello' && !this.closed) {
          void this.reconnect().catch((reconnectError) => {
            for (const lease of [...this.leases.values()]) {
              this.failLease(lease, reconnectError)
            }
          })
        }
      }
    })
  }

  private rejectPending(error: BrowserBrokerError): void {
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer)
      pending.reject(error)
    }
    this.pending.clear()
  }

  private nextRequestId(): number {
    const id = this.requestNumber
    this.requestNumber += 1
    return id
  }

  isCurrentGeneration(generation: number): boolean {
    return !this.closed && generation === this.connectionGeneration
  }

  private assertOpen(): void {
    if (this.closed) {
      throw new BrowserBrokerError('invalid-state', 'The browser broker client is closed')
    }
  }

  private telemetryEvent(action: string): void {
    if (!this.telemetry.enabled) return
    this.telemetry.record({
      type: 'counter',
      name: 'natsail.browser.broker.events',
      value: 1,
      at: this.telemetry.now(),
      attributes: { action, source: 'browser-broker' },
    })
  }
}

/** Creates and authenticates a protocol-v1 tab client. */
export async function createBrowserBrokerClient(
  options: BrowserBrokerClientOptions
): Promise<BrowserBrokerClient> {
  const client = new DefaultBrowserBrokerClient({
    ...options,
    identity: Object.freeze(parseIdentity(options.identity)),
  })
  await client.connect()
  return client
}

/** Standard SharedWorker connector. Construction stays in the tab, where credentials originate. */
export function createSharedWorkerConnector(
  url: string | URL,
  options: { readonly name?: string; readonly type?: WorkerType } = {}
): BrowserBrokerPortConnector {
  return () => {
    const Constructor = (globalThis as typeof globalThis & { SharedWorker?: typeof SharedWorker })
      .SharedWorker
    if (!Constructor) throw new BrowserBrokerError('unavailable', 'SharedWorker is unavailable')
    return new Constructor(url, {
      type: options.type ?? 'module',
      ...(options.name === undefined ? {} : { name: options.name }),
    }).port
  }
}

/**
 * Explicit non-strict fallback. The host and its runtime live only in the current
 * tab, but use the same validated protocol and SessionSource path.
 */
export function createTabLocalBrokerConnector(
  createHost: () => BrowserBrokerWorkerHost
): BrowserBrokerPortConnector {
  return async () => {
    const channel = new MessageChannel()
    const host = createHost()
    try {
      host.connect(channel.port1)
      tabLocalBrokerHosts.set(channel.port2, host)
      return channel.port2
    } catch (error) {
      channel.port1.close()
      channel.port2.close()
      await host.close().catch(() => undefined)
      throw error
    }
  }
}
