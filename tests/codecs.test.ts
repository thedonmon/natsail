import { describe, expect, it } from 'vitest'

import { natsCodecs, type NatsPayloadCodec } from '@natsail/core'

describe('NATS payload codecs', () => {
  it('round-trips text, JSON, and raw bytes behind one codec interface', () => {
    const json = natsCodecs.json<{ ok: boolean; count: number }>()
    const bytes = new Uint8Array([1, 2, 3])

    expect(natsCodecs.text.decode(natsCodecs.text.encode('hello'))).toBe('hello')
    expect(json.decode(json.encode({ ok: true, count: 2 }))).toEqual({ ok: true, count: 2 })
    expect(natsCodecs.bytes.decode(natsCodecs.bytes.encode(bytes))).toBe(bytes)
  })

  it('accepts application codecs without changing NATSail', () => {
    const codec: NatsPayloadCodec<number> = {
      encode: (value) => new Uint8Array([value]),
      decode: (data) => data[0] ?? 0,
    }

    expect(codec.decode(codec.encode(42))).toBe(42)
  })
})
