import { execFileSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { mkdir, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { createUser as createNkeyUser } from '@nats-io/nkeys'
import {
  createAccount,
  createOperator,
  createUser as createJwtUser,
  encodeAccount,
  encodeOperator,
  encodeUser,
  fmtCreds,
} from 'nats-jwt'

const repositoryRoot = fileURLToPath(new URL('..', import.meta.url))
const fixtureRoot = join(repositoryRoot, '.generated', 'nats-fixtures')
const nkeyRoot = join(fixtureRoot, 'nkey')
const tlsRoot = join(fixtureRoot, 'tls')
const jwtRoot = join(fixtureRoot, 'jwt')
const force = process.argv.includes('--force')

const expectedFiles = [
  join(nkeyRoot, 'client.seed'),
  join(nkeyRoot, 'nats-nkey.conf'),
  join(tlsRoot, 'server-cert.pem'),
  join(tlsRoot, 'server-key.pem'),
  join(jwtRoot, 'client.creds'),
  join(jwtRoot, 'nats-jwt.conf'),
]

if (!force && expectedFiles.every((file) => existsSync(file))) {
  console.log('Disposable NATS authentication fixtures are ready.')
  process.exit(0)
}

await rm(fixtureRoot, { recursive: true, force: true })
await Promise.all([
  mkdir(nkeyRoot, { recursive: true }),
  mkdir(tlsRoot, { recursive: true }),
  mkdir(jwtRoot, { recursive: true }),
])

const nkeyUser = createNkeyUser()
try {
  await writeFile(join(nkeyRoot, 'client.seed'), nkeyUser.getSeed(), { mode: 0o600 })
  await writeFile(
    join(nkeyRoot, 'nats-nkey.conf'),
    `port: 4222\n\nauthorization {\n  users: [\n    { nkey: ${nkeyUser.getPublicKey()} }\n  ]\n}\n`
  )
} finally {
  nkeyUser.clear()
}

execFileSync(
  'openssl',
  [
    'req',
    '-x509',
    '-newkey',
    'rsa:2048',
    '-sha256',
    '-nodes',
    '-keyout',
    join(tlsRoot, 'server-key.pem'),
    '-out',
    join(tlsRoot, 'server-cert.pem'),
    '-days',
    '2',
    '-subj',
    '/CN=localhost',
    '-addext',
    'subjectAltName=DNS:localhost,IP:127.0.0.1',
  ],
  { stdio: 'ignore' }
)

const operator = createOperator()
const account = createAccount()
const jwtUser = createJwtUser()

try {
  const operatorJwt = await encodeOperator('NATSail test operator', operator)
  const accountJwt = await encodeAccount(
    'NATSail test account',
    account,
    {
      limits: {
        conn: -1,
        data: -1,
        exports: -1,
        imports: -1,
        leaf: -1,
        payload: -1,
        subs: -1,
        wildcards: true,
      },
    },
    { signer: operator }
  )
  const userJwt = await encodeUser('NATSail test client', jwtUser, account)

  await writeFile(join(jwtRoot, 'client.creds'), fmtCreds(userJwt, jwtUser), { mode: 0o600 })
  await writeFile(
    join(jwtRoot, 'nats-jwt.conf'),
    `port: 4222\n\noperator: ${operatorJwt}\n\nresolver: MEMORY\n\nresolver_preload: {\n  ${account.getPublicKey()}: ${accountJwt}\n}\n`
  )
} finally {
  operator.clear()
  account.clear()
  jwtUser.clear()
}

console.log('Generated disposable NATS authentication fixtures under .generated/.')
