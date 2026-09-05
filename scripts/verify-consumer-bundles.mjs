import assert from 'node:assert/strict'
import { writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { gzipSync } from 'node:zlib'
import { build } from 'vite'

// Gzipped NATSail code, including workspace dependencies but excluding host libraries/transports.
const budgets = {
  '@natsail/core': 8_500,
  '@natsail/checkpoints': 1_600,
  '@natsail/session': 4_000,
  '@natsail/jetstream': 18_000,
  '@natsail/react': 18_500,
  '@natsail/rxjs': 8_500,
  '@natsail/effect': 20_000,
  '@natsail/browser-broker': 12_000,
  '@natsail/opentelemetry': 600,
}

export async function verifyConsumerBundles(consumerRoot) {
  const measurements = []
  async function bundle(name, entrySource, limit) {
    const entry = join(consumerRoot, 'bundle-entry.js')
    await writeFile(entry, entrySource)
    const result = await build({
      configFile: false,
      root: consumerRoot,
      logLevel: 'silent',
      build: {
        write: false,
        minify: 'esbuild',
        lib: { entry, formats: ['es'] },
        rollupOptions: {
          external: (id) =>
            !id.startsWith('.') && !id.startsWith('/') && !id.startsWith('@natsail/'),
          output: { inlineDynamicImports: true },
        },
      },
    })
    const outputs = (Array.isArray(result) ? result : [result]).flatMap((item) => item.output)
    const code = outputs
      .filter((item) => item.type === 'chunk')
      .map((item) => item.code)
      .join('\n')
    assert(code.length > 0, `${name} must produce a consumer bundle`)
    const gzipBytes = gzipSync(code).byteLength
    assert(gzipBytes <= limit, `${name}: ${gzipBytes} gzip bytes exceeds the ${limit} byte budget`)
    measurements.push({ name, gzipBytes, budgetBytes: limit })
    console.log(`Consumer bundle ${name}: ${gzipBytes}/${limit} gzip bytes`)
    return { code, outputs }
  }

  for (const [name, budget] of Object.entries(budgets)) {
    await bundle(name, `export * from '${name}'\n`, budget)
  }
  const codecs = await bundle(
    'core/codecs-only',
    "export { natsCodecs } from '@natsail/core'\n",
    400
  )
  assert(
    !codecs.code.includes('NatsRuntimeShutdownTimeoutError'),
    'Codec-only import retained the runtime'
  )
  assert(
    !codecs.outputs.some((item) => item.type === 'chunk' && item.imports.length > 0),
    'Codec-only import pulled in a dependency'
  )
  const runtime = await bundle(
    'core/runtime-only',
    "export { createNatsRuntime } from '@natsail/core'\n",
    7_000
  )
  assert(
    !runtime.outputs.some(
      (item) =>
        item.type === 'chunk' &&
        item.imports.some((id) => /jetstream|react|rxjs|effect|opentelemetry/.test(id))
    ),
    'Core imported an optional integration'
  )
  return measurements
}
