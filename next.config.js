/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // Izinkan fetch ke GitHub raw URL untuk ambil data screening
  async headers() {
    return [
      {
        source: '/api/:path*',
        headers: [
          { key: 'Access-Control-Allow-Credentials', value: 'true' },
          { key: 'Access-Control-Allow-Origin', value: '*' },
          { key: 'Access-Control-Allow-Methods', value: 'GET,POST,OPTIONS' },
          { key: 'Access-Control-Allow-Headers', value: 'Authorization, Content-Type' },
        ],
      },
    ]
  },
}

module.exports = nextConfig
