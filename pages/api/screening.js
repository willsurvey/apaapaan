// pages/api/screening.js
// Endpoint: GET /api/screening
// Proxy ke latest_screening.json di GitHub repo
// Menghindari CORS issue jika frontend fetch langsung ke GitHub raw

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' })
  }

  const owner = process.env.GITHUB_OWNER
  const repo = process.env.GITHUB_REPO

  if (!owner || !repo) {
    return res.status(500).json({
      error: 'Server misconfiguration',
      message: 'GITHUB_OWNER dan GITHUB_REPO harus diset di environment variables',
    })
  }

  try {
    const url = `https://raw.githubusercontent.com/${owner}/${repo}/main/latest_screening.json`
    const response = await fetch(url, {
      headers: {
        'Cache-Control': 'no-cache',
        'User-Agent': 'stock-screener-vercel',
      },
      next: { revalidate: 300 }, // cache 5 menit
    })

    if (!response.ok) {
      if (response.status === 404) {
        return res.status(404).json({
          error: 'Data belum tersedia',
          message: 'latest_screening.json belum ada. Tunggu GitHub Actions berjalan.',
          url,
        })
      }
      throw new Error(`GitHub raw returned ${response.status}`)
    }

    const data = await response.json()

    // Set cache headers
    res.setHeader('Cache-Control', 'public, s-maxage=300, stale-while-revalidate=600')
    return res.status(200).json(data)
  } catch (error) {
    console.error('Screening data fetch error:', error)
    return res.status(500).json({
      error: 'Gagal mengambil data screening',
      message: error.message,
      timestamp: new Date().toISOString(),
    })
  }
}
