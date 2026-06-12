// pages/_app.js
import { useState, useEffect, createContext } from 'react'
import '../styles/globals.css'

export const ThemeContext = createContext({
  theme: 'dark',
  toggleTheme: () => {}
})

export default function App({ Component, pageProps }) {
  const [theme, setTheme] = useState('dark')
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
    const savedTheme = localStorage.getItem('theme')
    if (savedTheme) {
      setTheme(savedTheme)
    } else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) {
      setTheme('light')
    }
  }, [])

  useEffect(() => {
    if (mounted) {
      document.documentElement.setAttribute('data-theme', theme)
      localStorage.setItem('theme', theme)
    }
  }, [theme, mounted])

  const toggleTheme = () => setTheme(prev => prev === 'dark' ? 'light' : 'dark')

  // Prevent hydration mismatch by not rendering until mounted
  if (!mounted) return null

  return (
    <ThemeContext.Provider value={{ theme, toggleTheme }}>
      <Component {...pageProps} />
    </ThemeContext.Provider>
  )
}
