/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{vue,js}'],
  theme: {
    extend: {
      colors: {
        primary: '#8b5cf6',
        secondary: '#ec4899',
        dark: '#0f172a',
        slate: {
          800: '#1e293b',
          900: '#0f172a'
        }
      }
    }
  },
  plugins: []
}
