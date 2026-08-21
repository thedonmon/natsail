export interface StreamCheckpoint {
  readonly stream: string
  readonly epoch: string
  readonly sequence: number
}

export interface CheckpointStore {
  load(key: string): Promise<StreamCheckpoint | undefined>
  save(key: string, checkpoint: StreamCheckpoint): Promise<void>
  clear(key: string): Promise<void>
}

export interface IndexedDbCheckpointStoreOptions {
  /** Database name. Defaults to `natsail`. */
  databaseName?: string
}

export type CheckpointValidationErrorCode = 'invalid-stream' | 'invalid-epoch' | 'invalid-sequence'

export class CheckpointValidationError extends Error {
  readonly name = 'CheckpointValidationError'

  constructor(readonly code: CheckpointValidationErrorCode) {
    super(`The checkpoint has an ${code.replace('-', ' ')}`)
  }
}

export class CheckpointConflictError extends Error {
  readonly name = 'CheckpointConflictError'
  readonly code = 'sequence-regression'

  constructor(
    readonly storedSequence: number,
    readonly incomingSequence: number
  ) {
    super(`Checkpoint sequence ${incomingSequence} is older than stored sequence ${storedSequence}`)
  }
}

function validateCheckpoint(checkpoint: StreamCheckpoint): void {
  if (typeof checkpoint.stream !== 'string' || checkpoint.stream.length === 0) {
    throw new CheckpointValidationError('invalid-stream')
  }
  if (typeof checkpoint.epoch !== 'string' || checkpoint.epoch.length === 0) {
    throw new CheckpointValidationError('invalid-epoch')
  }
  if (!Number.isSafeInteger(checkpoint.sequence) || checkpoint.sequence < 0) {
    throw new CheckpointValidationError('invalid-sequence')
  }
}

function copyCheckpoint(checkpoint: StreamCheckpoint): StreamCheckpoint {
  validateCheckpoint(checkpoint)
  return { ...checkpoint }
}

function rejectSequenceRegression(
  stored: StreamCheckpoint | undefined,
  incoming: StreamCheckpoint
): void {
  if (
    stored &&
    stored.stream === incoming.stream &&
    stored.epoch === incoming.epoch &&
    incoming.sequence < stored.sequence
  ) {
    throw new CheckpointConflictError(stored.sequence, incoming.sequence)
  }
}

class MemoryCheckpointStore implements CheckpointStore {
  private readonly checkpoints = new Map<string, StreamCheckpoint>()

  async load(key: string): Promise<StreamCheckpoint | undefined> {
    const checkpoint = this.checkpoints.get(key)
    return checkpoint === undefined ? undefined : copyCheckpoint(checkpoint)
  }

  async save(key: string, checkpoint: StreamCheckpoint): Promise<void> {
    const next = copyCheckpoint(checkpoint)
    rejectSequenceRegression(this.checkpoints.get(key), next)
    this.checkpoints.set(key, next)
  }

  async clear(key: string): Promise<void> {
    this.checkpoints.delete(key)
  }
}

const INDEXED_DB_STORE = 'checkpoints'

function requestResult<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error ?? new Error('The IndexedDB request failed'))
  })
}

function transactionComplete(transaction: IDBTransaction): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    transaction.oncomplete = () => resolve()
    transaction.onerror = () =>
      reject(transaction.error ?? new Error('The IndexedDB transaction failed'))
    transaction.onabort = () =>
      reject(transaction.error ?? new Error('The IndexedDB transaction stopped'))
  })
}

class IndexedDbCheckpointStore implements CheckpointStore {
  private databasePromise?: Promise<IDBDatabase>

  constructor(private readonly options: IndexedDbCheckpointStoreOptions) {}

  async load(key: string): Promise<StreamCheckpoint | undefined> {
    const database = await this.database()
    const transaction = database.transaction(INDEXED_DB_STORE, 'readonly')
    const checkpoint = await requestResult<StreamCheckpoint | undefined>(
      transaction.objectStore(INDEXED_DB_STORE).get(key)
    )
    return checkpoint === undefined ? undefined : copyCheckpoint(checkpoint)
  }

  async save(key: string, checkpoint: StreamCheckpoint): Promise<void> {
    const database = await this.database()
    const next = copyCheckpoint(checkpoint)
    const transaction = database.transaction(INDEXED_DB_STORE, 'readwrite')
    const objectStore = transaction.objectStore(INDEXED_DB_STORE)

    await new Promise<void>((resolve, reject) => {
      let conflict: unknown
      transaction.oncomplete = () => resolve()
      transaction.onerror = () =>
        reject(transaction.error ?? new Error('The IndexedDB transaction failed'))
      transaction.onabort = () =>
        reject(conflict ?? transaction.error ?? new Error('The IndexedDB transaction stopped'))

      const request = objectStore.get(key)
      request.onsuccess = () => {
        try {
          const stored = request.result as StreamCheckpoint | undefined
          if (stored) {
            validateCheckpoint(stored)
          }
          rejectSequenceRegression(stored, next)
          objectStore.put(next, key)
        } catch (error) {
          conflict = error
          transaction.abort()
        }
      }
    })
  }

  async clear(key: string): Promise<void> {
    const database = await this.database()
    const transaction = database.transaction(INDEXED_DB_STORE, 'readwrite')
    const completed = transactionComplete(transaction)
    transaction.objectStore(INDEXED_DB_STORE).delete(key)
    await completed
  }

  private database(): Promise<IDBDatabase> {
    this.databasePromise ??= new Promise<IDBDatabase>((resolve, reject) => {
      const factory = globalThis.indexedDB
      if (!factory) {
        reject(new Error('IndexedDB is not available in this environment'))
        return
      }

      const request = factory.open(this.options.databaseName ?? 'natsail', 1)
      request.onupgradeneeded = () => {
        if (!request.result.objectStoreNames.contains(INDEXED_DB_STORE)) {
          request.result.createObjectStore(INDEXED_DB_STORE)
        }
      }
      request.onsuccess = () => resolve(request.result)
      request.onerror = () => reject(request.error ?? new Error('NATSail could not open IndexedDB'))
    })

    return this.databasePromise
  }
}

export function createMemoryCheckpointStore(): CheckpointStore {
  return new MemoryCheckpointStore()
}

export function createIndexedDbCheckpointStore(
  options: IndexedDbCheckpointStoreOptions = {}
): CheckpointStore {
  return new IndexedDbCheckpointStore(options)
}
