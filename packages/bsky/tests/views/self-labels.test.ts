import { describe, expect, it } from 'vitest'
import { Views } from '../../src/views/index.js'

const record = {
  $type: 'app.bsky.feed.post',
  text: 'hello',
  createdAt: '2026-08-06T12:00:00.000Z',
  labels: {
    $type: 'com.atproto.label.defs#selfLabels',
    values: [{ val: 'porn' }],
  },
} as any

const uri = 'at://did:plc:host/app.bsky.feed.post/3k' as any

describe('Views.selfLabels', () => {
  it('attributes labels to the uri host by default', () => {
    const labels = Views.prototype.selfLabels({ uri, cid: 'bafy', record })

    expect(labels).toEqual([
      {
        src: 'did:plc:host',
        uri,
        cid: 'bafy',
        val: 'porn',
        cts: '2026-08-06T12:00:00.000Z',
      },
    ])
  })

  it('uses an explicit src verbatim', () => {
    const labels = Views.prototype.selfLabels({
      uri,
      cid: 'bafy',
      record,
      src: 'did:plc:author' as any,
    })

    expect(labels[0].src).toBe('did:plc:author')
  })
})
