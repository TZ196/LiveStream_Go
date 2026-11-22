<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue'
import { checkHealth } from '../api/health'

const state = ref<'checking' | 'online' | 'offline'>('checking')
const message = ref('正在检查后端连接…')
let controller: AbortController | undefined

async function checkConnection() {
  // 检查期间只保留一个请求，重复点击不会叠加请求。
  if (controller) {
    return
  }

  const currentRequest = new AbortController()
  controller = currentRequest
  state.value = 'checking'
  message.value = '正在检查后端连接…'

  try {
    await checkHealth(currentRequest.signal)
    if (currentRequest.signal.aborted) {
      return
    }
    state.value = 'online'
    message.value = '后端可以响应。'
  } catch (error) {
    if (currentRequest.signal.aborted) {
      return
    }
    state.value = 'offline'
    message.value = '连接检查失败，请稍后重试。'
    if (error instanceof Error) {
      message.value = error.message
    }
  } finally {
    controller = undefined
  }
}

function cancelCheck() {
  controller?.abort()
}

// Vue 在组件显示后执行首次检查，在移除组件前取消尚未完成的请求。
onMounted(checkConnection)
onBeforeUnmount(cancelCheck)
</script>

<template>
  <details class="health-panel" open>
    <summary>开发连接检查</summary>
    <div class="health-content">
      <div>
        <p class="health-message" :data-state="state" role="status">{{ message }}</p>
        <p class="health-note">GET /healthz 仅确认进程能响应，不代表注册、登录功能可用。</p>
      </div>
      <button class="retry-button" type="button" :disabled="state === 'checking'" @click="checkConnection">
        重新检查
      </button>
    </div>
  </details>
</template>
