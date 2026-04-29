import http from 'node:http'
import { AddressInfo } from 'node:net'
import { fetchKnownFollowersFromRskyGraph } from '../../src/data-plane/server/routes/follows'

type Handler = (
  req: http.IncomingMessage,
  res: http.ServerResponse,
) => void | Promise<void>

async function withServer<T>(
  handler: Handler,
  fn: (baseUrl: string) => Promise<T>,
): Promise<T> {
  const server = http.createServer((req, res) => {
    Promise.resolve(handler(req, res)).catch((err) => {
      res.statusCode = 500
      res.end(String(err))
    })
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const { port } = server.address() as AddressInfo
  try {
    return await fn(`http://127.0.0.1:${port}`)
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()))
  }
}

describe('rsky-graph proxy: fetchKnownFollowersFromRskyGraph', () => {
  it('returns null when no base URL is configured', async () => {
    const result = await fetchKnownFollowersFromRskyGraph(
      'did:plc:viewer',
      ['did:plc:a', 'did:plc:b'],
      '',
      80,
    )
    expect(result).toBeNull()
  })

  it('builds the upstream URL with viewer + comma-separated targets', async () => {
    let capturedUrl = ''
    const result = await withServer(
      (req, res) => {
        capturedUrl = req.url || ''
        res.setHeader('Content-Type', 'application/json')
        res.end(
          JSON.stringify({
            results: [
              { targetDid: 'did:plc:a', dids: ['did:plc:m1'] },
              { targetDid: 'did:plc:b', dids: ['did:plc:m2', 'did:plc:m3'] },
            ],
          }),
        )
      },
      (baseUrl) =>
        fetchKnownFollowersFromRskyGraph(
          'did:plc:viewer',
          ['did:plc:a', 'did:plc:b'],
          baseUrl,
          1000,
        ),
    )
    expect(capturedUrl).toMatch(/^\/v1\/follows-following\?/)
    expect(capturedUrl).toContain('viewer=did%3Aplc%3Aviewer')
    expect(capturedUrl).toContain('targets=did%3Aplc%3Aa%2Cdid%3Aplc%3Ab')
    expect(result?.get('did:plc:a')).toEqual(['did:plc:m1'])
    expect(result?.get('did:plc:b')).toEqual(['did:plc:m2', 'did:plc:m3'])
  })

  it('returns null on non-2xx status', async () => {
    const result = await withServer(
      (_req, res) => {
        res.statusCode = 503
        res.end('upstream busy')
      },
      (baseUrl) =>
        fetchKnownFollowersFromRskyGraph(
          'did:plc:viewer',
          ['did:plc:a'],
          baseUrl,
          1000,
        ),
    )
    expect(result).toBeNull()
  })

  it('returns null on timeout', async () => {
    const result = await withServer(
      async (_req, _res) => {
        // Hold the connection open longer than the timeout.
        await new Promise((r) => setTimeout(r, 200))
      },
      (baseUrl) =>
        fetchKnownFollowersFromRskyGraph(
          'did:plc:viewer',
          ['did:plc:a'],
          baseUrl,
          50,
        ),
    )
    expect(result).toBeNull()
  })

  it('returns null on malformed JSON without throwing', async () => {
    const result = await withServer(
      (_req, res) => {
        res.setHeader('Content-Type', 'application/json')
        res.end('not json')
      },
      (baseUrl) =>
        fetchKnownFollowersFromRskyGraph(
          'did:plc:viewer',
          ['did:plc:a'],
          baseUrl,
          1000,
        ),
    )
    expect(result).toBeNull()
  })

  it('returns null on connection refused (no server)', async () => {
    const result = await fetchKnownFollowersFromRskyGraph(
      'did:plc:viewer',
      ['did:plc:a'],
      'http://127.0.0.1:1', // refused
      1000,
    )
    expect(result).toBeNull()
  })

  it('handles empty results array', async () => {
    const result = await withServer(
      (_req, res) => {
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify({ results: [] }))
      },
      (baseUrl) =>
        fetchKnownFollowersFromRskyGraph(
          'did:plc:viewer',
          ['did:plc:a', 'did:plc:b'],
          baseUrl,
          1000,
        ),
    )
    // Map exists but is empty; the route's targetDids.map() will fill in [] per target.
    expect(result?.size).toBe(0)
  })

  it('handles missing dids field on a result entry', async () => {
    const result = await withServer(
      (_req, res) => {
        res.setHeader('Content-Type', 'application/json')
        res.end(
          JSON.stringify({
            results: [{ targetDid: 'did:plc:a' }],
          }),
        )
      },
      (baseUrl) =>
        fetchKnownFollowersFromRskyGraph(
          'did:plc:viewer',
          ['did:plc:a'],
          baseUrl,
          1000,
        ),
    )
    expect(result?.get('did:plc:a')).toEqual([])
  })
})
