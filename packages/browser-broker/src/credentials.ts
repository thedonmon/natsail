import {
  BrowserBrokerError,
  bytesEqual,
  copyBytes,
  type BrowserBrokerCredentials,
  type BrowserBrokerCredentialSnapshot,
} from './protocol.js'

export class MutableCredentials implements BrowserBrokerCredentials {
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
