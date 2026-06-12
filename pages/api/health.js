// pages/api/health.js
// Endpoint: GET /api/health
// Digunakan untuk monitoring uptime Vercel deployment

export default function handler(req, res) {
  return res.status(200).json({
    status: 'ok',
    service: 'stock-screener-id',
    timestamp: new Date().toISOString(),
    message: 'Vercel API is running!',
    env: {
      hasGithubOwner: !!process.env.GITHUB_OWNER,
      hasGithubRepo: !!process.env.GITHUB_REPO,
      hasGithubToken: !!process.env.GITHUB_TOKEN_WORKFLOW,
      hasWorkflowFile: !!process.env.WORKFLOW_FILE,
      hasCronSecret: !!process.env.CRON_SECRET,
    },
  })
}
