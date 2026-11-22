<script setup lang="ts">
import { onBeforeUnmount, ref } from 'vue'
import { currentUser, type User } from '../api/auth'
import { ApiError } from '../api/http'

const props = defineProps<{ user: User; token: string }>()
const emit = defineEmits<{ logout: []; expired: []; updated: [user: User] }>()
const checking = ref(false)
const message = ref('登录成功，你的账号已准备好。')
const hasError = ref(false)
let controller: AbortController | undefined

function formatDate(value: string): string {
  return new Date(value).toLocaleString('zh-CN', { hour12: false })
}

async function refreshAccount() {
  if (checking.value) return
  const attempt = new AbortController()
  controller = attempt
  checking.value = true
  hasError.value = false
  try {
    const account = await currentUser(props.token, attempt.signal)
    if (attempt.signal.aborted) return
    emit('updated', account)
    message.value = '账号信息已更新，登录状态有效。'
  } catch (error) {
    if (attempt.signal.aborted) return
    if (error instanceof ApiError && error.status === 401) {
      emit('expired')
      return
    }
    hasError.value = true
    message.value = error instanceof Error ? error.message : '暂时无法读取账号信息，请稍后重试。'
  } finally {
    checking.value = false
    controller = undefined
  }
}

onBeforeUnmount(() => controller?.abort())
</script>

<template>
  <section class="account-layout" aria-labelledby="account-title">
    <div class="account-heading">
      <div>
        <span class="eyebrow">MY ACCOUNT</span>
        <h1 id="account-title">你好，{{ user.username }}</h1>
        <p class="welcome-description">欢迎来到 LiveStream。</p>
      </div>
      <button type="button" class="retry-button" @click="emit('logout')">退出登录</button>
    </div>
    <section class="account-card" aria-labelledby="profile-title">
      <h2 id="profile-title">我的账号</h2>
      <dl class="account-details">
        <div><dt>用户名</dt><dd>{{ user.username }}</dd></div>
        <div><dt>用户编号</dt><dd>{{ user.id }}</dd></div>
        <div><dt>注册时间</dt><dd>{{ formatDate(user.created_at) }}</dd></div>
      </dl>
      <p class="form-message" :data-kind="hasError ? 'error' : 'success'" :role="hasError ? 'alert' : 'status'">{{ message }}</p>
      <button type="button" class="retry-button" :disabled="checking" @click="refreshAccount">
        {{ checking ? '正在更新…' : '刷新账号信息' }}
      </button>
      <p class="account-notice">登录有效期为 30 分钟，刷新页面不会延长有效期。到期或主动退出后需要重新登录。</p>
    </section>
  </section>
</template>
