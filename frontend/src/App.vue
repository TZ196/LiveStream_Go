<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref, shallowRef } from 'vue'
import AuthPage from './pages/AuthPage.vue'
import AccountPage from './pages/AccountPage.vue'
import RoomsPage from './pages/RoomsPage.vue'
import { currentUser, type LoginResult, type User } from './api/auth'
import { ApiError } from './api/http'
import { SESSION_KEY, tokenExpiresAt, type Session } from './session'

const session = shallowRef<Session>()
const notice = ref('')
const activePage = ref<'rooms' | 'account'>('rooms')
const roomsVersion = ref(0)
const restoring = ref(true)
const restoreError = ref('')
let expiryTimer: ReturnType<typeof setTimeout> | undefined
let restoreRequest: AbortController | undefined

function startSession(result: LoginResult) {
  const expiresAt = tokenExpiresAt(result.access_token)
  if (expiresAt <= Date.now()) {
    expireSession()
    return
  }
  // 只保存有固定期限的凭证，不保存密码；刷新不得延长原 JWT 的期限。
  notice.value = ''
  try {
    localStorage.setItem(SESSION_KEY, result.access_token)
  } catch {
    notice.value = '浏览器无法保存登录状态，刷新后需要重新登录。'
  }
  activateSession(result.access_token, result.user, expiresAt)
}

function activateSession(token: string, user: User, expiresAt: number) {
  clearTimeout(expiryTimer)
  session.value = { access_token: token, user, expires_at: expiresAt }
  activePage.value = 'rooms'
  expiryTimer = setTimeout(expireSession, Math.max(0, expiresAt - Date.now()))
}

function endSession(message: string) {
  restoreRequest?.abort()
  restoreRequest = undefined
  clearTimeout(expiryTimer)
  session.value = undefined
  restoring.value = false
  restoreError.value = ''
  notice.value = message
  try {
    localStorage.removeItem(SESSION_KEY)
  } catch {
    notice.value += ' 浏览器无法清除保存的登录状态，请检查站点存储权限。'
  }
}

function logout() { endSession('你已退出登录。') }
function showRooms() {
  // 从直播间返回发现页时卸载旧连接，让页面正常执行资源清理。
  roomsVersion.value++
  activePage.value = 'rooms'
}
function expireSession() { endSession('登录已失效，请重新登录。') }
function updateUser(user: User) {
  if (session.value) session.value = { ...session.value, user }
}

async function restoreSession() {
  restoreRequest?.abort()
  clearTimeout(expiryTimer)
  const attempt = new AbortController()
  restoreRequest = attempt
  session.value = undefined
  restoring.value = true
  restoreError.value = ''
  notice.value = ''
  let token: string | null
  let expiresAt: number
  try {
    token = localStorage.getItem(SESSION_KEY)
    if (!token) {
      restoring.value = false
      restoreRequest = undefined
      return
    }
    expiresAt = tokenExpiresAt(token)
  } catch {
    endSession('无法恢复保存的登录状态，请重新登录。')
    return
  }
  if (expiresAt <= Date.now()) {
    expireSession()
    return
  }
  expiryTimer = setTimeout(expireSession, expiresAt - Date.now())
  try {
    const user = await currentUser(token, attempt.signal)
    if (attempt.signal.aborted) return
    if (expiresAt <= Date.now()) {
      expireSession()
      return
    }
    activateSession(token, user, expiresAt)
  } catch (error) {
    if (attempt.signal.aborted) return
    if (error instanceof ApiError && error.status === 401) {
      expireSession()
      return
    }
    // 网络或数据库暂不可用不等于凭证失效，保留凭证并允许用户重试。
    restoreError.value = '暂时无法恢复登录，请检查连接后重试。'
  } finally {
    if (restoreRequest === attempt) {
      restoring.value = false
      restoreRequest = undefined
    }
  }
}

function syncStoredSession(event: StorageEvent) {
  if (event.storageArea !== localStorage || (event.key !== SESSION_KEY && event.key !== null)) return
  // 同一浏览器的标签页共享登录状态；账号切换时卸载旧页面并取消旧请求。
  void restoreSession()
}

function checkExpiry() {
  // 后台标签页的定时器可能被暂停，回到页面时再次核对绝对到期时间。
  if (session.value && session.value.expires_at <= Date.now()) expireSession()
}

onMounted(() => {
  window.addEventListener('storage', syncStoredSession)
  window.addEventListener('focus', checkExpiry)
  document.addEventListener('visibilitychange', checkExpiry)
  void restoreSession()
})
onBeforeUnmount(() => {
  restoreRequest?.abort()
  clearTimeout(expiryTimer)
  window.removeEventListener('storage', syncStoredSession)
  window.removeEventListener('focus', checkExpiry)
  document.removeEventListener('visibilitychange', checkExpiry)
})
</script>

<template>
  <header class="site-header">
    <a class="brand" href="/" aria-label="LiveStream 首页">
      <img src="/favicon.svg" width="36" height="36" alt="" />
      <span>LiveStream</span>
    </a>
    <nav v-if="session" class="header-navigation" aria-label="主导航">
      <button class="header-nav-link" :aria-current="activePage === 'rooms' ? 'page' : undefined" @click="showRooms">发现直播</button>
      <button class="header-nav-link header-account" aria-label="我的账号" :aria-current="activePage === 'account' ? 'page' : undefined" @click="activePage = 'account'"><span class="account-avatar" aria-hidden="true">{{ session.user.username.slice(0, 1).toUpperCase() }}</span><span>{{ session.user.username }}</span></button>
      <button class="header-logout" @click="logout">退出</button>
    </nav>
    <span v-else class="site-description">一起分享此刻</span>
  </header>
  <main class="page-content">
    <section v-if="restoring || restoreError" class="account-card" aria-label="恢复登录">
      <p v-if="restoring" role="status">正在恢复登录…</p>
      <template v-else>
        <p class="form-message" data-kind="error" role="alert">{{ restoreError }}</p>
        <div class="room-actions">
          <button class="retry-button" type="button" @click="restoreSession">重试</button>
          <button class="retry-button" type="button" @click="logout">退出登录</button>
        </div>
      </template>
    </section>
    <template v-else-if="session">
      <p v-if="notice" class="form-message" role="status">{{ notice }}</p>
      <RoomsPage v-if="activePage === 'rooms'" :key="roomsVersion" :token="session.access_token" :user-id="session.user.id" @expired="expireSession" />
      <AccountPage v-else :user="session.user" :token="session.access_token"
        @logout="logout" @expired="expireSession" @updated="updateUser" />
    </template>
    <AuthPage v-else :key="notice" :notice="notice" @logged-in="startSession" />
  </main>
</template>
