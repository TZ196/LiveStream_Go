<script setup lang="ts">
import { onBeforeUnmount, ref } from 'vue'
import { currentUser, login, register, type LoginResult } from '../api/auth'
import { ApiError, RequestError } from '../api/http'

const props = defineProps<{ notice: string }>()
const emit = defineEmits<{ loggedIn: [result: LoginResult] }>()
const activeForm = ref<'login' | 'register'>('login')
const username = ref('')
const password = ref('')
const busy = ref(false)
const message = ref(props.notice)
const messageKind = ref<'info' | 'success' | 'error'>('info')
const traceId = ref('')
const retrySeconds = ref(0)
let controller: AbortController | undefined
let retryTimer: ReturnType<typeof setInterval> | undefined

function changeForm(mode: 'login' | 'register') {
  if (busy.value) return
  activeForm.value = mode
  password.value = ''
  message.value = ''
  traceId.value = ''
}

function validateInput(): string {
  const name = username.value.replace(/^ +| +$/g, '')
  if (!/^[A-Za-z][A-Za-z0-9_]{2,31}$/.test(name)) {
    return '用户名须为 3–32 位英文字母、数字或下划线，并以字母开头。'
  }
  // Unicode 字符与 UTF-8 字节分别计数，密码不去空格、不截断。
  const characters = Array.from(password.value).length
  const bytes = new TextEncoder().encode(password.value).length
  if (characters < 12 || characters > 128 || bytes > 512) {
    return '密码须为 12–128 个字符，且不超过 512 个 UTF-8 字节。'
  }
  return ''
}

function waitBeforeRetry(seconds: number) {
  clearInterval(retryTimer)
  retrySeconds.value = Math.max(1, seconds)
  retryTimer = setInterval(() => {
    retrySeconds.value -= 1
    if (retrySeconds.value <= 0) clearInterval(retryTimer)
  }, 1000)
}

async function submit() {
  if (busy.value || retrySeconds.value > 0) return
  message.value = validateInput()
  messageKind.value = 'error'
  traceId.value = ''
  if (message.value) return

  const attempt = new AbortController()
  controller = attempt
  busy.value = true
  const mode = activeForm.value
  const input = { username: username.value, password: password.value }
  try {
    if (mode === 'register') {
      const account = await register(input, attempt.signal)
      if (attempt.signal.aborted) return
      username.value = account.username
      activeForm.value = 'login'
      messageKind.value = 'success'
      message.value = '注册成功，请使用刚才的密码登录。'
    } else {
      const result = await login(input, attempt.signal)
      // 登录后用凭证读取当前用户，确认受保护接口也能正常工作。
      result.user = await currentUser(result.access_token, attempt.signal)
      if (attempt.signal.aborted) return
      emit('loggedIn', result)
    }
  } catch (error) {
    if (attempt.signal.aborted) return
    message.value = error instanceof Error ? error.message : '操作失败，请稍后再试。'
    if (mode === 'register' && error instanceof RequestError) {
      message.value += ' 注册结果尚未确认，可以先尝试登录。'
    }
    if (error instanceof ApiError) {
      traceId.value = error.traceId
      if (error.status === 429) waitBeforeRetry(error.retryAfter)
    }
  } finally {
    password.value = ''
    busy.value = false
    controller = undefined
  }
}

onBeforeUnmount(() => {
  controller?.abort()
  clearInterval(retryTimer)
  password.value = ''
})
</script>

<template>
  <section class="auth-layout" aria-labelledby="welcome-title">
    <div class="welcome">
      <span class="eyebrow">LIVE TOGETHER</span>
      <h1 id="welcome-title">让互动，<br />实时发生。</h1>
      <p class="welcome-description">弹幕、点赞与直播间互动，<br />从一个账号开始。</p>
      <p class="welcome-note">每一次表达，都让彼此更近一点。</p>
    </div>

    <section class="auth-card" aria-labelledby="auth-title">
      <div class="form-switch" aria-label="选择登录或注册">
        <button type="button" :disabled="busy" :aria-pressed="activeForm === 'login'" @click="changeForm('login')">登录</button>
        <button type="button" :disabled="busy" :aria-pressed="activeForm === 'register'" @click="changeForm('register')">注册</button>
      </div>
      <!-- 阻止浏览器默认提交，凭据只通过 JSON 请求体发送。 -->
      <form aria-describedby="account-notice" :aria-busy="busy" @submit.prevent="submit">
        <h2 id="auth-title">{{ activeForm === 'login' ? '欢迎回来' : '创建账号' }}</h2>
        <p class="form-description">{{ activeForm === 'login' ? '使用你的 LiveStream 账号登录。' : '为你的下一次互动做好准备。' }}</p>
        <div class="form-field">
          <label for="auth-username">用户名</label>
          <input id="auth-username" v-model="username" name="username" autocomplete="username"
            placeholder="输入用户名" :disabled="busy" required aria-describedby="username-help" />
          <p id="username-help" class="field-help">3–32 位英文字母、数字或下划线，以字母开头；大小写视为同一用户名。</p>
        </div>
        <div class="form-field">
          <label for="auth-password">密码</label>
          <input id="auth-password" v-model="password" name="password" type="password"
            :autocomplete="activeForm === 'login' ? 'current-password' : 'new-password'"
            placeholder="输入密码" :disabled="busy" required aria-describedby="password-help" />
          <p id="password-help" class="field-help">12–128 个字符，保留你输入的空格。</p>
        </div>
        <button class="submit-button" type="submit" :disabled="busy || retrySeconds > 0">
          {{ busy ? '正在处理…' : retrySeconds > 0 ? `请等待 ${retrySeconds} 秒` : activeForm === 'login' ? '登录' : '注册' }}
        </button>
      </form>
      <p v-if="message" class="form-message" :data-kind="messageKind" :role="messageKind === 'error' ? 'alert' : 'status'">{{ message }}</p>
      <p v-if="traceId" class="trace-note">问题编号：{{ traceId }}</p>
      <p id="account-notice" class="account-notice">登录后 30 分钟内保持登录，刷新页面不会退出。使用公共设备时请主动退出登录。</p>
    </section>
  </section>
</template>
