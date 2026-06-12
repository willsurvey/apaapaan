// pages/api/cron.js
// Endpoint: POST /api/cron
// Dipanggil oleh Vercel Cron Job untuk trigger GitHub Actions workflow
// Schedule di vercel.json: "0 11 * * 1-5" = Senin–Jumat 11:00 UTC = 18:00 WIB
//
// ENV VARS yang diperlukan (set di Vercel Dashboard → Settings → Environment Variables):
//   GITHUB_OWNER          : username GitHub Anda
//   GITHUB_REPO           : nama repo screener-modular (bukan repo lama!)
//   GITHUB_TOKEN_WORKFLOW : GitHub Personal Access Token (scope: workflow)
//   CRON_SECRET           : rahasia untuk verifikasi request dari Vercel Cron
//   WORKFLOW_FILE         : "screener.yml" (default)

export default async function handler(req, res) {
  // Hanya terima POST
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed', message: 'Use POST' })
  }

  // Verifikasi CRON_SECRET — Vercel otomatis kirim header Authorization
  const authHeader   = req.headers['authorization']
  const expectedToken = process.env.CRON_SECRET

  if (expectedToken && authHeader !== `Bearer ${expectedToken}`) {
    return res.status(401).json({ error: 'Unauthorized', message: 'Invalid or missing authorization header' })
  }

  const owner        = process.env.GITHUB_OWNER
  const repo         = process.env.GITHUB_REPO
  const workflowFile = process.env.WORKFLOW_FILE || 'screener.yml'
  const githubToken  = process.env.GITHUB_TOKEN_WORKFLOW

  if (!owner || !repo || !githubToken) {
    console.error('Missing env vars: GITHUB_OWNER, GITHUB_REPO, GITHUB_TOKEN_WORKFLOW')
    return res.status(500).json({
      error: 'Server misconfiguration',
      message: 'Required environment variables are not set',
      missing: {
        GITHUB_OWNER: !owner,
        GITHUB_REPO: !repo,
        GITHUB_TOKEN_WORKFLOW: !githubToken,
      }
    })
  }

  try {
    console.log(`🚀 Triggering GitHub Actions: ${owner}/${repo} → ${workflowFile}`)

    const response = await fetch(
      `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflowFile}/dispatches`,
      {
        method: 'POST',
        headers: {
          Authorization: `token ${githubToken}`,
          Accept:        'application/vnd.github.v3+json',
          'Content-Type': 'application/json',
          'User-Agent':  'stock-screener-modular-vercel-cron',
        },
        body: JSON.stringify({
          ref: 'main',
          // Input sesuai workflow baru screener-modular (force_mode, bukan trigger_source)
          inputs: {
            force_mode: '',   // kosong = AUTO mode (Stockbit jika token ada, else Yahoo)
          },
        }),
      }
    )

    if (!response.ok) {
      const errorText = await response.text()
      console.error(`GitHub API error ${response.status}:`, errorText)
      return res.status(502).json({
        success: false,
        error:   `GitHub API returned ${response.status}`,
        detail:  errorText,
        timestamp: new Date().toISOString(),
      })
    }

    console.log('✅ GitHub Actions triggered successfully')

    return res.status(200).json({
      success: true,
      message: 'GitHub Actions workflow triggered successfully',
      timestamp: new Date().toISOString(),
      data: { owner, repo, workflow: workflowFile, ref: 'main', mode: 'AUTO' },
    })

  } catch (error) {
    console.error('❌ Cron trigger error:', error)
    return res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString(),
    })
  }
}
