import type { SessionSource } from '@natsail/session'

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

export const envelope = (): ProtocolEnvelope => ({
  protocol: NATS_BROWSER_BROKER_PROTOCOL,
  version: NATS_BROWSER_BROKER_PROTOCOL_VERSION,
})

export function record(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new BrowserBrokerError('invalid-command', 'Browser broker messages must be objects')
  }
  return value as Record<string, unknown>
}

export function stringField(value: unknown, field: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new BrowserBrokerError('invalid-command', `${field} must be a non-empty string`)
  }
  return value
}

export function integerField(value: unknown, field: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new BrowserBrokerError('invalid-command', `${field} must be a non-negative integer`)
  }
  return value as number
}

export function booleanField(value: unknown, field: string): boolean {
  if (typeof value !== 'boolean') {
    throw new BrowserBrokerError('invalid-command', `${field} must be a boolean`)
  }
  return value
}

export function requestId(value: unknown): number {
  const id = integerField(value, 'requestId')
  if (id === 0) throw new BrowserBrokerError('invalid-command', 'requestId must be positive')
  return id
}

export function arrayBuffer(value: unknown, field: string): ArrayBuffer {
  if (Object.prototype.toString.call(value) !== '[object ArrayBuffer]') {
    throw new BrowserBrokerError('invalid-command', `${field} must be an ArrayBuffer`)
  }
  return value as ArrayBuffer
}

export function byteArray(value: unknown, field: string): Uint8Array {
  if (!(value instanceof Uint8Array)) {
    throw new BrowserBrokerError('invalid-command', `${field} must be a Uint8Array`)
  }
  return value
}

export function parseIdentity(value: unknown): BrowserBrokerIdentity {
  const candidate = record(value)
  return {
    tenant: stringField(candidate.tenant, 'identity.tenant'),
    authenticationContext: stringField(
      candidate.authenticationContext,
      'identity.authenticationContext'
    ),
  }
}

export function parseSource(value: unknown): BrowserBrokerSourceDescriptor {
  const candidate = record(value)
  return {
    key: stringField(candidate.key, 'source.key'),
    contract: stringField(candidate.contract, 'source.contract'),
  }
}

export function parseCursor(value: unknown): BrowserBrokerCursor {
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

export function assertEnvelope(candidate: Record<string, unknown>): void {
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

export function cloneIdentity(identity: BrowserBrokerIdentity): BrowserBrokerIdentity {
  return Object.freeze({ ...identity })
}

export function identityKey(identity: BrowserBrokerIdentity): string {
  return JSON.stringify([identity.tenant, identity.authenticationContext])
}

export function sourceKey(
  identity: BrowserBrokerIdentity,
  descriptor: BrowserBrokerSourceDescriptor
): string {
  return `${identityKey(identity)}\u0000${descriptor.key}`
}

export function cursorsEqual(
  left: BrowserBrokerCursor | undefined,
  right: BrowserBrokerCursor | undefined
) {
  if (left === undefined || right === undefined) return left === right
  return (
    left.stream === right.stream && left.sequence === right.sequence && left.epoch === right.epoch
  )
}

export function copyBytes(bytes: Uint8Array): Uint8Array {
  return bytes.slice()
}

export function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false
  return left.every((value, index) => value === right[index])
}

export function parseCredentialSnapshot(value: unknown): BrowserBrokerCredentialSnapshot {
  const candidate = record(value)
  return {
    revision: integerField(candidate.revision, 'credentials.revision'),
    bytes: byteArray(candidate.bytes, 'credentials.bytes'),
  }
}

export function parseStats(value: unknown): BrowserBrokerStats {
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

export function copyBuffer(bytes: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(bytes.byteLength)
  copy.set(bytes)
  return copy.buffer
}

export function copyCursor(cursor: BrowserBrokerCursor): BrowserBrokerCursor
export function copyCursor(cursor: undefined): undefined
export function copyCursor(cursor: BrowserBrokerCursor | undefined): BrowserBrokerCursor | undefined
export function copyCursor(
  cursor: BrowserBrokerCursor | undefined
): BrowserBrokerCursor | undefined {
  return cursor === undefined ? undefined : { ...cursor }
}
