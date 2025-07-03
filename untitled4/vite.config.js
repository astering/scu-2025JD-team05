import { fileURLToPath, URL } from 'node:url'

import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import vueDevTools from 'vite-plugin-vue-devtools'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    vue(),
    vueDevTools(),
  ],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    },
  },
  server: {
    proxy: {
      // 将以 /api 开头的请求代理到 FastAPI 后端
      '/api': {
        target: 'http://localhost:8000', // 你的 FastAPI 后端运行地址
        changeOrigin: true,
        rewrite: path => path.replace(/^\/api/, '/api') // 保留 /api 前缀
      }
    }
  }
})


