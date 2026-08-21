import { fileURLToPath } from 'node:url'
import { readFile } from 'node:fs/promises'

import {
  connect,
  credsAuthenticator,
  type NatsConnection,
  nkeyAuthenticator,
  wsconnect,
} from '@nats-io/transport-node'

const server = process.env.NATS_URL ?? 'nats://127.0.0.1:4223'
const websocketServer = process.env.NATS_WS_URL ?? 'ws://127.0.0.1:9223'
const tokenServer = process.env.NATS_TOKEN_URL ?? 'nats://127.0.0.1:4224'
const userPasswordServer = process.env.NATS_USER_PASSWORD_URL ?? 'nats://127.0.0.1:4225'
const nkeyServer = process.env.NATS_NKEY_URL ?? 'nats://127.0.0.1:4226'
const tlsServer = process.env.NATS_TLS_URL ?? 'tls://127.0.0.1:4227'
const jwtServer = process.env.NATS_JWT_URL ?? 'nats://127.0.0.1:4228'
const generatedFixtureRoot = new URL('../../.generated/nats-fixtures/', import.meta.url)
const nkeySeedFile = fileURLToPath(new URL('nkey/client.seed', generatedFixtureRoot))
const tlsCaFile = fileURLToPath(new URL('tls/server-cert.pem', generatedFixtureRoot))
const jwtCredsFile = fileURLToPath(new URL('jwt/client.creds', generatedFixtureRoot))

export async function connectToTestNats(): Promise<NatsConnection> {
  let cause: unknown

  for (let attempt = 0; attempt < 20; attempt += 1) {
    try {
      return await connect({ servers: server, timeout: 500 })
    } catch (error) {
      cause = error
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
  }

  throw new Error(`Cannot connect to the test NATS server at ${server}`, { cause })
}

export async function connectToTestNatsWebSocket(): Promise<NatsConnection> {
  let cause: unknown

  for (let attempt = 0; attempt < 20; attempt += 1) {
    try {
      return await wsconnect({ servers: websocketServer, timeout: 500 })
    } catch (error) {
      cause = error
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
  }

  throw new Error(`Cannot connect to the test NATS server at ${websocketServer}`, { cause })
}

export async function connectToTokenTestNats(): Promise<NatsConnection> {
  let cause: unknown

  for (let attempt = 0; attempt < 20; attempt += 1) {
    try {
      return await connect({
        servers: tokenServer,
        token: 'natsail-test-token',
        timeout: 500,
      })
    } catch (error) {
      cause = error
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
  }

  throw new Error(`Cannot connect to the token-authenticated NATS server at ${tokenServer}`, {
    cause,
  })
}

export async function connectToUserPasswordTestNats(): Promise<NatsConnection> {
  let cause: unknown

  for (let attempt = 0; attempt < 20; attempt += 1) {
    try {
      return await connect({
        servers: userPasswordServer,
        user: 'natsail',
        pass: 'test-password',
        timeout: 500,
      })
    } catch (error) {
      cause = error
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
  }

  throw new Error(`Cannot connect to the user/password NATS server at ${userPasswordServer}`, {
    cause,
  })
}

export async function connectToNkeyTestNats(): Promise<NatsConnection> {
  let cause: unknown

  for (let attempt = 0; attempt < 20; attempt += 1) {
    try {
      return await connect({
        servers: nkeyServer,
        authenticator: nkeyAuthenticator(await readFile(nkeySeedFile)),
        timeout: 500,
      })
    } catch (error) {
      cause = error
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
  }

  throw new Error(`Cannot connect to the NKey NATS server at ${nkeyServer}`, { cause })
}

export async function connectToTlsTestNats(): Promise<NatsConnection> {
  let cause: unknown

  for (let attempt = 0; attempt < 20; attempt += 1) {
    try {
      return await connect({
        servers: tlsServer,
        tls: { caFile: tlsCaFile },
        timeout: 500,
      })
    } catch (error) {
      cause = error
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
  }

  throw new Error(`Cannot connect to the TLS NATS server at ${tlsServer}`, { cause })
}

export async function connectToJwtTestNats(): Promise<NatsConnection> {
  let cause: unknown

  for (let attempt = 0; attempt < 20; attempt += 1) {
    try {
      return await connect({
        servers: jwtServer,
        authenticator: credsAuthenticator(await readFile(jwtCredsFile)),
        timeout: 500,
      })
    } catch (error) {
      cause = error
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
  }

  throw new Error(`Cannot connect to the JWT-authenticated NATS server at ${jwtServer}`, { cause })
}

export function uniqueSubject(prefix: string): string {
  return `tests.${prefix}.${crypto.randomUUID()}`
}
