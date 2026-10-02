import { Schema } from 'effect'

import type { NatsPayloadCodec } from '@natsail/core'

/** Schema-backed JSON payload codec. A decode failure is terminal for the subscription. */
export function natsSchemaCodec<S extends Schema.Codec<unknown, unknown>>(
  schema: S
): NatsPayloadCodec<S['Type']> {
  const json = Schema.fromJsonString(Schema.toCodecJson(schema))
  const decode = Schema.decodeUnknownSync(json)
  const encode = Schema.encodeSync(json)
  const textEncoder = new TextEncoder()
  const textDecoder = new TextDecoder()
  return {
    decode: (data) => decode(textDecoder.decode(data)),
    encode: (value) => textEncoder.encode(encode(value)),
  }
}
