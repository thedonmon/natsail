import {
  AckPolicy,
  DeliverPolicy,
  jetstreamManager,
  ReplayPolicy,
  type ConsumerAPI,
  type ConsumerConfig,
  type ConsumerInfo,
  type ConsumerUpdateConfig,
} from '@nats-io/jetstream'

import { type NatsConnection, type NatsRuntime } from '@natsail/core'

export type JetStreamProcessorStart = 'all' | 'new' | { after: number }

export type JetStreamProcessorConsumer =
  | { mode: 'bind'; name: string }
  | { mode: 'ensure'; name: string }
  | { mode: 'owned'; name: string }

export type JetStreamProcessorDriftPolicy = 'error' | 'update-editable' | 'recreate-owned'

export interface JetStreamProcessorAdminOptions {
  stream: string
  consumer: JetStreamProcessorConsumer
  filter: string | readonly string[]
  start: JetStreamProcessorStart
  /** Server wait before an unacknowledged delivery becomes eligible for redelivery. */
  ackWaitMs?: number
  /** Ordered redelivery delays. The first delay is also the effective acknowledgement wait. */
  backoffMs?: readonly number[]
  maxDeliver?: number
  maxAckPending?: number
  /** Consumer metadata. Keys beginning with `_nats` and NATSail's ownership key are reserved. */
  metadata?: Readonly<Record<string, string>>
  /** Percentage of acknowledgements sampled by the server, from 0 through 100. */
  ackSamplePercent?: number
  /** Consumer replica count. */
  replicas?: number
  /** Keeps consumer state in memory instead of inheriting the stream storage. */
  memoryStorage?: boolean
  replayPolicy?: ReplayPolicy
  /** Action when the named consumer's active configuration differs from the requested contract. */
  driftPolicy?: JetStreamProcessorDriftPolicy
}

const ownedMetadataKey = 'natsail.io/processor-owner'
const ownedMetadataValue = 'natsail'

export type JetStreamProcessorEditableField =
  | 'filters'
  | 'ackWaitMs'
  | 'backoffMs'
  | 'maxDeliver'
  | 'maxAckPending'
  | 'metadata'
  | 'ackSamplePercent'
  | 'replicas'

export type JetStreamProcessorImmutableField =
  | 'durableName'
  | 'deliveryKind'
  | 'ackPolicy'
  | 'deliverPolicy'
  | 'startSequence'
  | 'replayPolicy'
  | 'memoryStorage'

export interface JetStreamProcessorNormalizedConfig {
  readonly durableName: string
  readonly deliveryKind: 'pull' | 'push'
  readonly ackPolicy: string
  readonly deliverPolicy: string
  readonly startSequence?: number
  readonly replayPolicy: string
  readonly filters: readonly string[]
  readonly ackWaitMs?: number
  readonly backoffMs?: readonly number[]
  readonly maxDeliver?: number
  readonly maxAckPending?: number
  readonly metadata?: Readonly<Record<string, string>>
  readonly ackSamplePercent?: number
  readonly replicas?: number
  readonly memoryStorage?: boolean
}

export interface JetStreamProcessorSequenceInspection {
  readonly consumer: number
  readonly stream: number
}

export interface JetStreamProcessorConsumerStateInspection {
  readonly pendingAcknowledgements: number
  readonly pendingMessages: number
  readonly delivered: JetStreamProcessorSequenceInspection
  readonly acknowledged: JetStreamProcessorSequenceInspection
  readonly redeliveries: number
  readonly paused: boolean
}

export interface JetStreamProcessorReconciliationInspection {
  readonly stream: string
  readonly consumer: {
    readonly name: string
    readonly mode: JetStreamProcessorConsumer['mode']
    readonly owned: boolean
  }
  readonly desired: JetStreamProcessorNormalizedConfig
  readonly active?: JetStreamProcessorNormalizedConfig
  readonly state?: JetStreamProcessorConsumerStateInspection
  readonly lastReconciliation?: JetStreamProcessorReconciliationResult
}

interface JetStreamProcessorReconciliationBase {
  readonly policy: JetStreamProcessorDriftPolicy
  readonly editableDrift: readonly JetStreamProcessorEditableField[]
  readonly immutableDrift: readonly JetStreamProcessorImmutableField[]
  readonly before?: JetStreamProcessorNormalizedConfig
  readonly after?: JetStreamProcessorNormalizedConfig
  /** First stream sequence eligible after recreation (the prior acknowledgement floor plus one). */
  readonly deliveryBoundary?: number
}

export type JetStreamProcessorReconciliationResult =
  | (JetStreamProcessorReconciliationBase & { readonly status: 'unchanged' })
  | (JetStreamProcessorReconciliationBase & { readonly status: 'created' })
  | (JetStreamProcessorReconciliationBase & { readonly status: 'updated' })
  | (JetStreamProcessorReconciliationBase & { readonly status: 'recreated' })
  | (JetStreamProcessorReconciliationBase & {
      readonly status: 'rejected'
      readonly reason: 'missing' | 'drift' | 'ownership'
    })

export type JetStreamProcessorConfigurationErrorCode =
  | 'ack-policy'
  | 'push-consumer'
  | 'filter-mismatch'
  | 'start-mismatch'
  | 'ack-wait-mismatch'
  | 'max-deliver-mismatch'
  | 'max-ack-pending-mismatch'
  | 'replay-policy-mismatch'
  | 'consumer-missing'
  | 'reconciliation-rejected'
  | 'ownership-required'

export class JetStreamProcessorConfigurationError extends Error {
  readonly name: string = 'JetStreamProcessorConfigurationError'

  constructor(
    readonly code: JetStreamProcessorConfigurationErrorCode,
    message: string
  ) {
    super(message)
  }
}

export class JetStreamProcessorReconciliationError extends JetStreamProcessorConfigurationError {
  override readonly name = 'JetStreamProcessorReconciliationError'

  constructor(
    readonly result: Extract<JetStreamProcessorReconciliationResult, { status: 'rejected' }>
  ) {
    const field = result.immutableDrift[0] ?? result.editableDrift[0]
    const compatibilityCode: JetStreamProcessorConfigurationErrorCode =
      field === 'ackPolicy'
        ? 'ack-policy'
        : field === 'deliveryKind'
          ? 'push-consumer'
          : field === 'filters'
            ? 'filter-mismatch'
            : field === 'deliverPolicy' || field === 'startSequence'
              ? 'start-mismatch'
              : field === 'ackWaitMs' || field === 'backoffMs'
                ? 'ack-wait-mismatch'
                : field === 'maxDeliver'
                  ? 'max-deliver-mismatch'
                  : field === 'maxAckPending'
                    ? 'max-ack-pending-mismatch'
                    : field === 'replayPolicy'
                      ? 'replay-policy-mismatch'
                      : result.reason === 'missing'
                        ? 'consumer-missing'
                        : result.reason === 'ownership'
                          ? 'ownership-required'
                          : 'reconciliation-rejected'
    super(
      compatibilityCode,
      result.reason === 'missing'
        ? 'The requested JetStream processor consumer does not exist'
        : result.reason === 'ownership'
          ? 'This JetStream processor operation requires an owned consumer'
          : `JetStream processor configuration drift was rejected by policy ${result.policy}`
    )
  }
}

export interface JetStreamProcessorController {
  /** Returns the latest cached management view without performing I/O. */
  inspect(): JetStreamProcessorReconciliationInspection
  /** Refreshes the authoritative server view. */
  refresh(): Promise<JetStreamProcessorReconciliationInspection>
  /** Creates, validates, updates, or safely recreates the consumer according to its mode and policy. */
  reconcile(): Promise<JetStreamProcessorReconciliationResult>
  pause(until: Date): Promise<JetStreamProcessorPauseResult>
  resume(): Promise<JetStreamProcessorResumeResult>
  /** Deletes only package-owned consumers. The ownership guard is enforced at runtime. */
  delete(): Promise<JetStreamProcessorDeleteResult>
}

export interface JetStreamProcessorPauseResult {
  readonly status: 'paused'
  readonly until: string
  readonly inspection: JetStreamProcessorReconciliationInspection
}

export interface JetStreamProcessorResumeResult {
  readonly status: 'resumed'
  readonly inspection: JetStreamProcessorReconciliationInspection
}

export interface JetStreamProcessorDeleteResult {
  readonly status: 'deleted'
}

interface JetStreamProcessorReconcileContext {
  /** Safe local acknowledgement boundary used when a deleted owned consumer must be recreated. */
  resumeAfter?: number
}

const editableOrder: readonly JetStreamProcessorEditableField[] = [
  'filters',
  'ackWaitMs',
  'backoffMs',
  'maxDeliver',
  'maxAckPending',
  'metadata',
  'ackSamplePercent',
  'replicas',
]
const immutableOrder: readonly JetStreamProcessorImmutableField[] = [
  'durableName',
  'deliveryKind',
  'ackPolicy',
  'deliverPolicy',
  'startSequence',
  'replayPolicy',
  'memoryStorage',
]

function millisecondsToNanos(value: number): number {
  const nanos = value * 1_000_000
  if (!Number.isSafeInteger(nanos)) {
    throw new RangeError('JetStream millisecond duration is too large')
  }
  return nanos
}

function nanosToMilliseconds(value: number | undefined): number | undefined {
  return value === undefined ? undefined : value / 1_000_000
}

function normalizedFilters(filter: string | readonly string[]): readonly string[] {
  return (typeof filter === 'string' ? [filter] : [...filter]).sort()
}

function normalizedMetadata(
  metadata: Readonly<Record<string, string>> | undefined
): Readonly<Record<string, string>> | undefined {
  if (metadata === undefined) return undefined
  return Object.fromEntries(
    Object.entries(metadata)
      .filter(([key]) => !key.startsWith('_nats'))
      .sort(([left], [right]) => left.localeCompare(right))
  )
}

function desiredPolicy(options: JetStreamProcessorAdminOptions): JetStreamProcessorDriftPolicy {
  return (
    options.driftPolicy ??
    (options.consumer.mode === 'bind'
      ? 'error'
      : options.consumer.mode === 'ensure'
        ? 'update-editable'
        : 'recreate-owned')
  )
}

export function validateJetStreamProcessorAdminOptions(
  options: JetStreamProcessorAdminOptions
): void {
  if (options.stream.trim().length === 0) {
    throw new TypeError('JetStream processor stream must not be empty')
  }
  if (options.consumer.name.trim().length === 0) {
    throw new TypeError('JetStream processor consumer name must not be empty')
  }
  const consumerModes: readonly JetStreamProcessorConsumer['mode'][] = ['bind', 'ensure', 'owned']
  if (!consumerModes.includes(options.consumer.mode)) {
    throw new TypeError('JetStream processor consumer.mode must be bind, ensure, or owned')
  }
  const filters = typeof options.filter === 'string' ? [options.filter] : options.filter
  if (filters.length === 0 || filters.some((filter) => filter.trim().length === 0)) {
    throw new TypeError('JetStream processor filter must contain at least one subject')
  }
  if (options.start !== 'all' && options.start !== 'new' && typeof options.start !== 'object') {
    throw new TypeError('JetStream processor start must be all, new, or an after sequence')
  }
  if (
    typeof options.start === 'object' &&
    (options.start === null ||
      !Number.isSafeInteger(options.start.after) ||
      options.start.after < 0 ||
      options.start.after === Number.MAX_SAFE_INTEGER)
  ) {
    throw new RangeError(
      'JetStream processor start.after must be a non-negative safe integer with room for the next sequence'
    )
  }
  if (
    options.ackWaitMs !== undefined &&
    (!Number.isSafeInteger(options.ackWaitMs) || options.ackWaitMs <= 0)
  ) {
    throw new RangeError('JetStream processor ackWaitMs must be a positive integer')
  }
  if (options.ackWaitMs !== undefined) millisecondsToNanos(options.ackWaitMs)
  if (options.backoffMs !== undefined) {
    if (
      options.backoffMs.length === 0 ||
      options.backoffMs.some((delay) => !Number.isSafeInteger(delay) || delay <= 0)
    ) {
      throw new RangeError('JetStream processor backoffMs must contain positive integers')
    }
    for (let index = 1; index < options.backoffMs.length; index += 1) {
      if (options.backoffMs[index]! < options.backoffMs[index - 1]!) {
        throw new RangeError('JetStream processor backoffMs must be ordered')
      }
    }
    for (const delay of options.backoffMs) millisecondsToNanos(delay)
    if (options.ackWaitMs !== undefined && options.ackWaitMs !== options.backoffMs[0]) {
      throw new RangeError('JetStream processor ackWaitMs must equal the first backoffMs delay')
    }
  }
  if (
    options.maxDeliver !== undefined &&
    options.maxDeliver !== -1 &&
    (!Number.isSafeInteger(options.maxDeliver) || options.maxDeliver <= 0)
  ) {
    throw new RangeError('JetStream processor maxDeliver must be -1 or a positive integer')
  }
  if (
    options.backoffMs !== undefined &&
    options.maxDeliver !== undefined &&
    options.maxDeliver !== -1 &&
    options.backoffMs.length > options.maxDeliver
  ) {
    throw new RangeError('JetStream processor backoffMs length must not exceed maxDeliver')
  }
  if (
    options.maxAckPending !== undefined &&
    (!Number.isSafeInteger(options.maxAckPending) || options.maxAckPending <= 0)
  ) {
    throw new RangeError('JetStream processor maxAckPending must be a positive integer')
  }
  if (
    options.ackSamplePercent !== undefined &&
    (!Number.isInteger(options.ackSamplePercent) ||
      options.ackSamplePercent < 0 ||
      options.ackSamplePercent > 100)
  ) {
    throw new RangeError('JetStream processor ackSamplePercent must be an integer from 0 to 100')
  }
  if (
    options.replicas !== undefined &&
    (!Number.isSafeInteger(options.replicas) || options.replicas <= 0)
  ) {
    throw new RangeError('JetStream processor replicas must be a positive integer')
  }
  if (options.memoryStorage !== undefined && typeof options.memoryStorage !== 'boolean') {
    throw new TypeError('JetStream processor memoryStorage must be a boolean')
  }
  if (options.metadata !== undefined) {
    for (const [key, value] of Object.entries(options.metadata)) {
      if (
        key.trim().length === 0 ||
        key.startsWith('_nats') ||
        key === ownedMetadataKey ||
        typeof value !== 'string'
      ) {
        throw new TypeError(
          'JetStream processor metadata must use non-empty, non-reserved keys and string values'
        )
      }
    }
  }
  if (
    options.replayPolicy !== undefined &&
    options.replayPolicy !== ReplayPolicy.Instant &&
    options.replayPolicy !== ReplayPolicy.Original
  ) {
    throw new TypeError('JetStream processor replayPolicy must be Instant or Original')
  }
  const policies: readonly JetStreamProcessorDriftPolicy[] = [
    'error',
    'update-editable',
    'recreate-owned',
  ]
  if (options.driftPolicy !== undefined && !policies.includes(options.driftPolicy)) {
    throw new TypeError(
      'JetStream processor driftPolicy must be error, update-editable, or recreate-owned'
    )
  }
  const policy = desiredPolicy(options)
  if (options.consumer.mode === 'bind' && policy !== 'error') {
    throw new TypeError('A bound JetStream processor only supports driftPolicy error')
  }
  if (options.consumer.mode === 'ensure' && policy === 'recreate-owned') {
    throw new TypeError('An ensured JetStream processor cannot use driftPolicy recreate-owned')
  }
}

export function normalizeJetStreamProcessorDesired(
  options: JetStreamProcessorAdminOptions,
  start: JetStreamProcessorStart = options.start
): JetStreamProcessorNormalizedConfig {
  const backoffMs = options.backoffMs === undefined ? undefined : [...options.backoffMs]
  const metadata =
    options.consumer.mode === 'owned'
      ? normalizedMetadata({ ...options.metadata, [ownedMetadataKey]: ownedMetadataValue })
      : normalizedMetadata(options.metadata)
  return {
    durableName: options.consumer.name,
    deliveryKind: 'pull',
    ackPolicy: AckPolicy.Explicit,
    deliverPolicy:
      start === 'all'
        ? DeliverPolicy.All
        : start === 'new'
          ? DeliverPolicy.New
          : DeliverPolicy.StartSequence,
    ...(typeof start === 'object' ? { startSequence: start.after + 1 } : {}),
    replayPolicy: options.replayPolicy ?? ReplayPolicy.Instant,
    filters: normalizedFilters(options.filter),
    ...((options.ackWaitMs ?? backoffMs?.[0]) === undefined
      ? {}
      : { ackWaitMs: options.ackWaitMs ?? backoffMs![0] }),
    ...(backoffMs === undefined ? {} : { backoffMs }),
    ...(options.maxDeliver === undefined ? {} : { maxDeliver: options.maxDeliver }),
    ...(options.maxAckPending === undefined ? {} : { maxAckPending: options.maxAckPending }),
    ...(metadata === undefined ? {} : { metadata }),
    ...(options.ackSamplePercent === undefined
      ? {}
      : { ackSamplePercent: options.ackSamplePercent }),
    ...(options.replicas === undefined ? {} : { replicas: options.replicas }),
    ...(options.memoryStorage === undefined ? {} : { memoryStorage: options.memoryStorage }),
  }
}

export function normalizeJetStreamProcessorActive(
  info: ConsumerInfo
): JetStreamProcessorNormalizedConfig {
  const config = info.config
  const sample = config.sample_freq?.replace('%', '')
  const parsedSample = sample === undefined ? undefined : Number(sample)
  return {
    durableName: config.durable_name ?? config.name ?? info.name,
    deliveryKind: config.deliver_subject ? 'push' : 'pull',
    ackPolicy: config.ack_policy,
    deliverPolicy: config.deliver_policy,
    ...(config.opt_start_seq === undefined ? {} : { startSequence: config.opt_start_seq }),
    replayPolicy: config.replay_policy,
    filters: [
      ...(config.filter_subject ? [config.filter_subject] : []),
      ...(config.filter_subjects ?? []),
    ].sort(),
    ...(config.ack_wait === undefined ? {} : { ackWaitMs: nanosToMilliseconds(config.ack_wait)! }),
    ...(config.backoff === undefined
      ? {}
      : { backoffMs: config.backoff.map((delay) => nanosToMilliseconds(delay)!) }),
    ...(config.max_deliver === undefined ? {} : { maxDeliver: config.max_deliver }),
    ...(config.max_ack_pending === undefined ? {} : { maxAckPending: config.max_ack_pending }),
    ...(config.metadata === undefined ? {} : { metadata: normalizedMetadata(config.metadata)! }),
    ...(parsedSample === undefined || Number.isNaN(parsedSample)
      ? {}
      : { ackSamplePercent: parsedSample }),
    ...(config.num_replicas === undefined ? {} : { replicas: config.num_replicas }),
    ...(config.mem_storage === undefined ? {} : { memoryStorage: config.mem_storage }),
  }
}

export function inspectJetStreamProcessorConsumerState(
  info: ConsumerInfo
): JetStreamProcessorConsumerStateInspection {
  const partial = info as ConsumerInfo & {
    delivered?: Partial<ConsumerInfo['delivered']>
    ack_floor?: Partial<ConsumerInfo['ack_floor']>
    num_ack_pending?: number
    num_pending?: number
    num_redelivered?: number
  }
  return {
    pendingAcknowledgements: partial.num_ack_pending ?? 0,
    pendingMessages: partial.num_pending ?? 0,
    delivered: {
      consumer: partial.delivered?.consumer_seq ?? 0,
      stream: partial.delivered?.stream_seq ?? 0,
    },
    acknowledged: {
      consumer: partial.ack_floor?.consumer_seq ?? 0,
      stream: partial.ack_floor?.stream_seq ?? 0,
    },
    redeliveries: partial.num_redelivered ?? 0,
    paused: info.paused ?? false,
  }
}

function same(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right)
}

export function classifyJetStreamProcessorDrift(
  options: JetStreamProcessorAdminOptions,
  active: JetStreamProcessorNormalizedConfig,
  effectiveStart: JetStreamProcessorStart = options.start
): {
  readonly editable: readonly JetStreamProcessorEditableField[]
  readonly immutable: readonly JetStreamProcessorImmutableField[]
} {
  const desired = normalizeJetStreamProcessorDesired(options, effectiveStart)
  const editable = editableOrder.filter((field) => {
    if (field === 'filters') return !same(desired.filters, active.filters)
    if (
      field === 'ackWaitMs' &&
      options.ackWaitMs === undefined &&
      options.backoffMs === undefined
    ) {
      return false
    }
    if (field === 'backoffMs' && options.backoffMs === undefined) return false
    if (field === 'maxDeliver' && options.maxDeliver === undefined) return false
    if (field === 'maxAckPending' && options.maxAckPending === undefined) return false
    if (
      field === 'metadata' &&
      options.metadata === undefined &&
      options.consumer.mode !== 'owned'
    ) {
      return false
    }
    if (field === 'ackSamplePercent' && options.ackSamplePercent === undefined) return false
    if (field === 'replicas' && options.replicas === undefined) return false
    return !same(desired[field], active[field])
  })
  const immutable = immutableOrder.filter((field) => {
    if (field === 'memoryStorage' && options.memoryStorage === undefined) return false
    if (field === 'replayPolicy' && options.replayPolicy === undefined) return false
    return !same(desired[field], active[field])
  })
  return { editable, immutable }
}

export function jetStreamProcessorConsumerConfig(
  options: JetStreamProcessorAdminOptions,
  start: JetStreamProcessorStart = options.start
): ConsumerConfig {
  const desired = normalizeJetStreamProcessorDesired(options, start)
  return {
    durable_name: options.consumer.name,
    ack_policy: AckPolicy.Explicit,
    deliver_policy: desired.deliverPolicy as ConsumerConfig['deliver_policy'],
    replay_policy: desired.replayPolicy as ConsumerConfig['replay_policy'],
    ...(desired.startSequence === undefined ? {} : { opt_start_seq: desired.startSequence }),
    ...(desired.filters.length === 1
      ? { filter_subject: desired.filters[0]! }
      : { filter_subjects: [...desired.filters] }),
    ...(desired.ackWaitMs === undefined
      ? {}
      : { ack_wait: millisecondsToNanos(desired.ackWaitMs) }),
    ...(desired.backoffMs === undefined
      ? {}
      : { backoff: desired.backoffMs.map(millisecondsToNanos) }),
    ...(desired.maxDeliver === undefined ? {} : { max_deliver: desired.maxDeliver }),
    ...(desired.maxAckPending === undefined ? {} : { max_ack_pending: desired.maxAckPending }),
    ...(desired.metadata === undefined ? {} : { metadata: { ...desired.metadata } }),
    ...(desired.ackSamplePercent === undefined
      ? {}
      : { sample_freq: `${desired.ackSamplePercent}%` }),
    ...(desired.replicas === undefined ? {} : { num_replicas: desired.replicas }),
    ...(desired.memoryStorage === undefined ? {} : { mem_storage: desired.memoryStorage }),
  }
}

function updateConfig(
  options: JetStreamProcessorAdminOptions,
  drift: readonly JetStreamProcessorEditableField[]
): Partial<ConsumerUpdateConfig> {
  const desired = normalizeJetStreamProcessorDesired(options)
  const config: Partial<ConsumerUpdateConfig> = {}
  if (drift.includes('filters')) {
    if (desired.filters.length === 1) {
      config.filter_subject = desired.filters[0]!
      config.filter_subjects = []
    } else {
      config.filter_subject = ''
      config.filter_subjects = [...desired.filters]
    }
  }
  if (drift.includes('ackWaitMs')) config.ack_wait = millisecondsToNanos(desired.ackWaitMs!)
  if (drift.includes('backoffMs')) config.backoff = desired.backoffMs!.map(millisecondsToNanos)
  if (drift.includes('maxDeliver')) config.max_deliver = desired.maxDeliver!
  if (drift.includes('maxAckPending')) config.max_ack_pending = desired.maxAckPending!
  if (drift.includes('metadata')) config.metadata = { ...desired.metadata }
  if (drift.includes('ackSamplePercent')) config.sample_freq = `${desired.ackSamplePercent}%`
  if (drift.includes('replicas')) config.num_replicas = desired.replicas!
  return config
}

function isMissingConsumer(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false
  const candidate = error as {
    name?: unknown
    code?: unknown
    status?: unknown
    api_error?: { code?: unknown; err_code?: unknown }
  }
  return (
    candidate.name === 'ConsumerNotFoundError' ||
    candidate.code === 10_014 ||
    candidate.code === '404' ||
    candidate.code === 404 ||
    candidate.api_error?.err_code === 10_014 ||
    (candidate.status === 404 && candidate.name === 'ConsumerNotFoundError')
  )
}

function deliverySequenceAfter(boundary: number): number {
  if (!Number.isSafeInteger(boundary) || boundary < 0 || boundary === Number.MAX_SAFE_INTEGER) {
    throw new JetStreamProcessorConfigurationError(
      'reconciliation-rejected',
      'The JetStream acknowledgement boundary cannot be represented safely'
    )
  }
  return boundary + 1
}

function rollbackConfigAtBoundary(config: ConsumerConfig, boundary: number): ConsumerConfig {
  const { opt_start_seq: _startSequence, opt_start_time: _startTime, ...retained } = config
  return {
    ...retained,
    deliver_policy: DeliverPolicy.StartSequence,
    opt_start_seq: deliverySequenceAfter(boundary),
  }
}

function processorRecreationBoundary(info: ConsumerInfo): number {
  const acknowledged = info.ack_floor.stream_seq
  if (acknowledged > 0) return acknowledged
  if (
    info.config.deliver_policy === DeliverPolicy.StartSequence &&
    info.config.opt_start_seq !== undefined &&
    info.config.opt_start_seq > 0
  ) {
    return info.config.opt_start_seq - 1
  }
  if (info.config.deliver_policy === DeliverPolicy.New) {
    if (info.delivered.consumer_seq === 0 && info.num_ack_pending === 0) {
      return info.delivered.stream_seq
    }
    throw new JetStreamProcessorConfigurationError(
      'reconciliation-rejected',
      'The unacknowledged start:new creation boundary is unavailable; refusing unsafe recreation'
    )
  }
  return acknowledged
}

class ProcessorController implements JetStreamProcessorController {
  private tail: Promise<void> = Promise.resolve()
  private consumerConnection?: NatsConnection
  private consumerApi?: ConsumerAPI
  private active: JetStreamProcessorNormalizedConfig | undefined
  private state: JetStreamProcessorConsumerStateInspection | undefined
  private lastReconciliation?: JetStreamProcessorReconciliationResult
  private effectiveStart: JetStreamProcessorStart | undefined

  constructor(
    private readonly runtime: NatsRuntime,
    private readonly options: JetStreamProcessorAdminOptions,
    private readonly context: JetStreamProcessorReconcileContext
  ) {}

  inspect(): JetStreamProcessorReconciliationInspection {
    return {
      stream: this.options.stream,
      consumer: {
        name: this.options.consumer.name,
        mode: this.options.consumer.mode,
        owned: this.options.consumer.mode === 'owned',
      },
      desired: normalizeJetStreamProcessorDesired(
        this.options,
        this.effectiveStart ?? this.options.start
      ),
      ...(this.active === undefined ? {} : { active: this.active }),
      ...(this.state === undefined ? {} : { state: this.state }),
      ...(this.lastReconciliation === undefined
        ? {}
        : { lastReconciliation: this.lastReconciliation }),
    }
  }

  refresh(): Promise<JetStreamProcessorReconciliationInspection> {
    return this.serialize(async () => {
      const consumers = await this.consumers()
      await this.read(consumers)
      return this.inspect()
    })
  }

  reconcile(): Promise<JetStreamProcessorReconciliationResult> {
    return this.serialize(async () => {
      const consumers = await this.consumers()
      const result = await this.reconcileWith(consumers)
      this.lastReconciliation = result
      return result
    })
  }

  pause(until: Date): Promise<JetStreamProcessorPauseResult> {
    if (this.options.consumer.mode === 'bind') {
      return Promise.reject(
        new JetStreamProcessorConfigurationError(
          'ownership-required',
          'A bound JetStream processor is inspect-only and cannot be paused'
        )
      )
    }
    if (Number.isNaN(until.getTime()) || until.getTime() <= Date.now()) {
      return Promise.reject(new RangeError('JetStream processor pause date must be in the future'))
    }
    return this.serialize(async () => {
      const consumers = await this.consumers()
      await consumers.pause(this.options.stream, this.options.consumer.name, until)
      await this.read(consumers)
      return { status: 'paused', until: until.toISOString(), inspection: this.inspect() }
    })
  }

  resume(): Promise<JetStreamProcessorResumeResult> {
    if (this.options.consumer.mode === 'bind') {
      return Promise.reject(
        new JetStreamProcessorConfigurationError(
          'ownership-required',
          'A bound JetStream processor is inspect-only and cannot be resumed'
        )
      )
    }
    return this.serialize(async () => {
      const consumers = await this.consumers()
      await consumers.resume(this.options.stream, this.options.consumer.name)
      await this.read(consumers)
      return { status: 'resumed', inspection: this.inspect() }
    })
  }

  delete(): Promise<JetStreamProcessorDeleteResult> {
    if (this.options.consumer.mode !== 'owned') {
      return Promise.reject(
        new JetStreamProcessorConfigurationError(
          'ownership-required',
          'Only an owned JetStream processor consumer can be deleted'
        )
      )
    }
    return this.serialize(async () => {
      const consumers = await this.consumers()
      const current = await this.read(consumers)
      if (!this.isOwned(current)) {
        throw new JetStreamProcessorConfigurationError(
          'ownership-required',
          'The active JetStream consumer is not marked as owned by NATSail'
        )
      }
      const deleted = await consumers.delete(this.options.stream, this.options.consumer.name)
      if (!deleted) {
        throw new JetStreamProcessorConfigurationError(
          'reconciliation-rejected',
          'The owned JetStream consumer was not deleted'
        )
      }
      this.active = undefined
      this.state = undefined
      return { status: 'deleted' }
    })
  }

  private serialize<T>(operation: () => Promise<T>): Promise<T> {
    const run = this.tail.then(operation, operation)
    this.tail = run.then(
      () => undefined,
      () => undefined
    )
    return run
  }

  private async consumers(): Promise<ConsumerAPI> {
    let connection: NatsConnection
    try {
      connection = await this.runtime.connection()
    } catch (error) {
      // Runtime shutdown rejects new connection acquisition before managed
      // processor resources close. The already-bound API remains the only path
      // for deleting an owned consumer during that ordered shutdown.
      if (this.consumerApi !== undefined) return this.consumerApi
      throw error
    }
    if (this.consumerApi !== undefined && this.consumerConnection === connection) {
      return this.consumerApi
    }
    this.consumerConnection = connection
    this.consumerApi = (await jetstreamManager(connection)).consumers
    return this.consumerApi
  }

  private async read(consumers: ConsumerAPI): Promise<ConsumerInfo> {
    const info = await consumers.info(this.options.stream, this.options.consumer.name)
    this.active = normalizeJetStreamProcessorActive(info)
    this.state = inspectJetStreamProcessorConsumerState(info)
    return info
  }

  private isOwned(info: ConsumerInfo): boolean {
    return info.config.metadata?.[ownedMetadataKey] === ownedMetadataValue
  }

  private rejected(
    policy: JetStreamProcessorDriftPolicy,
    reason: 'missing' | 'drift' | 'ownership',
    editableDrift: readonly JetStreamProcessorEditableField[],
    immutableDrift: readonly JetStreamProcessorImmutableField[],
    before?: JetStreamProcessorNormalizedConfig
  ): Extract<JetStreamProcessorReconciliationResult, { status: 'rejected' }> {
    return {
      status: 'rejected',
      reason,
      policy,
      editableDrift,
      immutableDrift,
      ...(before === undefined ? {} : { before, after: before }),
    }
  }

  private async reconcileWith(
    consumers: ConsumerAPI
  ): Promise<JetStreamProcessorReconciliationResult> {
    const policy = desiredPolicy(this.options)
    let beforeInfo: ConsumerInfo | undefined
    try {
      beforeInfo = await this.read(consumers)
    } catch (error) {
      if (!isMissingConsumer(error)) throw error
    }

    if (beforeInfo === undefined) {
      if (this.options.consumer.mode === 'bind') {
        return this.rejected(policy, 'missing', [], ['durableName'])
      }
      const boundary = this.options.consumer.mode === 'owned' ? this.context.resumeAfter : undefined
      const deliveryBoundary = boundary === undefined ? undefined : deliverySequenceAfter(boundary)
      const start = boundary === undefined ? this.options.start : ({ after: boundary } as const)
      await consumers.add(
        this.options.stream,
        jetStreamProcessorConsumerConfig(this.options, start)
      )
      if (boundary !== undefined) this.effectiveStart = start
      const afterInfo = await this.read(consumers)
      return {
        status: 'created',
        policy,
        editableDrift: [],
        immutableDrift: [],
        after: normalizeJetStreamProcessorActive(afterInfo),
        ...(deliveryBoundary === undefined ? {} : { deliveryBoundary }),
      }
    }

    const before = normalizeJetStreamProcessorActive(beforeInfo)
    if (this.options.consumer.mode === 'owned' && !this.isOwned(beforeInfo)) {
      return this.rejected(policy, 'ownership', [], [], before)
    }
    if (
      this.effectiveStart === undefined &&
      this.options.consumer.mode === 'owned' &&
      this.options.start === 'new' &&
      before.deliverPolicy === DeliverPolicy.StartSequence &&
      before.startSequence !== undefined &&
      before.startSequence > 0
    ) {
      // Owned start:new consumers recreated by NATSail retain the explicit safe boundary.
      this.effectiveStart = { after: before.startSequence - 1 }
    }
    const drift = classifyJetStreamProcessorDrift(
      this.options,
      before,
      this.effectiveStart ?? this.options.start
    )
    if (drift.editable.length === 0 && drift.immutable.length === 0) {
      return {
        status: 'unchanged',
        policy,
        editableDrift: [],
        immutableDrift: [],
        before,
        after: before,
      }
    }

    if (this.options.consumer.mode === 'bind' || policy === 'error') {
      return this.rejected(policy, 'drift', drift.editable, drift.immutable, before)
    }

    if (drift.immutable.length > 0) {
      if (this.options.consumer.mode !== 'owned' || policy !== 'recreate-owned') {
        return this.rejected(policy, 'drift', drift.editable, drift.immutable, before)
      }
      const boundary = processorRecreationBoundary(beforeInfo)
      const deliveryBoundary = deliverySequenceAfter(boundary)
      await consumers.delete(this.options.stream, this.options.consumer.name)
      try {
        await consumers.add(
          this.options.stream,
          jetStreamProcessorConsumerConfig(this.options, { after: boundary })
        )
      } catch (error) {
        let rollbackInfo: ConsumerInfo
        try {
          rollbackInfo = await consumers.add(
            this.options.stream,
            rollbackConfigAtBoundary(beforeInfo.config, boundary)
          )
        } catch (rollbackError) {
          throw new AggregateError(
            [error, rollbackError],
            'JetStream consumer recreation and safe rollback both failed'
          )
        }
        this.active = normalizeJetStreamProcessorActive(rollbackInfo)
        this.state = inspectJetStreamProcessorConsumerState(rollbackInfo)
        this.effectiveStart = { after: boundary }
        throw error
      }
      this.effectiveStart = { after: boundary }
      const afterInfo = await this.read(consumers)
      return {
        status: 'recreated',
        policy,
        editableDrift: drift.editable,
        immutableDrift: drift.immutable,
        before,
        after: normalizeJetStreamProcessorActive(afterInfo),
        deliveryBoundary,
      }
    }

    await consumers.update(
      this.options.stream,
      this.options.consumer.name,
      updateConfig(this.options, drift.editable)
    )
    const afterInfo = await this.read(consumers)
    const remaining = classifyJetStreamProcessorDrift(
      this.options,
      normalizeJetStreamProcessorActive(afterInfo),
      this.effectiveStart ?? this.options.start
    )
    if (remaining.editable.length > 0 || remaining.immutable.length > 0) {
      throw new JetStreamProcessorConfigurationError(
        'reconciliation-rejected',
        `JetStream consumer update did not produce the requested configuration (editable: ${remaining.editable.join(', ') || 'none'}; immutable: ${remaining.immutable.join(', ') || 'none'})`
      )
    }
    return {
      status: 'updated',
      policy,
      editableDrift: drift.editable,
      immutableDrift: [],
      before,
      after: normalizeJetStreamProcessorActive(afterInfo),
    }
  }
}

export function createJetStreamProcessorController(
  runtime: NatsRuntime,
  options: JetStreamProcessorAdminOptions
): JetStreamProcessorController {
  validateJetStreamProcessorAdminOptions(options)
  return new ProcessorController(runtime, options, {})
}

/** @internal Recovery-only factory. The public controller cannot claim a missing boundary. */
export function createJetStreamProcessorControllerForRecovery(
  runtime: NatsRuntime,
  options: JetStreamProcessorAdminOptions,
  resumeAfter: number | undefined
): JetStreamProcessorController {
  validateJetStreamProcessorAdminOptions(options)
  return new ProcessorController(runtime, options, resumeAfter === undefined ? {} : { resumeAfter })
}
