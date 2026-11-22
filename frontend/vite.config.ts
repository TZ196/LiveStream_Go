import { defineConfig, loadEnv, type ConfigEnv } from 'vite'
import vue from '@vitejs/plugin-vue'

// Vite 启动时调用此函数，读取开发代理地址；该变量不会注入浏览器。
function createConfig({ mode }: ConfigEnv) {
  const env = loadEnv(mode, process.cwd(), 'LIVESTREAM_')
  const apiTarget = env.LIVESTREAM_API_TARGET || 'http://127.0.0.1:8080'

  return {
    plugins: [vue()],
    server: {
      host: '127.0.0.1',
      port: 5173,
      strictPort: true,
      // 浏览器请求当前站点，Vite 转发给后端，避免开发时跨域。
      proxy: {
        '/healthz': apiTarget,
        '/readyz': apiTarget,
        '/api': apiTarget,
        '/ws': { target: apiTarget, ws: true },
      },
    },
  }
}

export default defineConfig(createConfig)
