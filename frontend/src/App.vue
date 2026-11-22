<script setup lang="ts">
import { onBeforeUnmount, ref, shallowRef } from 'vue'
import AuthPage from './pages/AuthPage.vue'
import AccountPage from './pages/AccountPage.vue'
import RoomsPage from './pages/RoomsPage.vue'
import HealthStatus from './components/HealthStatus.vue'
import type { LoginResult, User } from './api/auth'

// 凭证仅在当前页面内存中保存，不写入 localStorage、Cookie 或 URL。
const session = shallowRef<LoginResult>()
const notice = ref('')
const activePage = ref<'rooms' | 'account'>('rooms')
const isDevelopment = import.meta.env.DEV
let expiryTimer: ReturnType<typeof setTimeout> | undefined

function startSession(result: LoginResult) {
  clearTimeout(expiryTimer)
  session.value = result
  activePage.value = 'rooms'
  notice.value = ''
  expiryTimer = setTimeout(expireSession, result.expires_in * 1000)
}

function endSession(message: string) {
  clearTimeout(expiryTimer)
  session.value = undefined
  notice.value = message
}

function logout() { endSession('你已退出当前页面的登录。') }
function expireSession() { endSession('登录已失效，请重新登录。') }
function updateUser(user: User) {
  if (session.value) session.value = { ...session.value, user }
}

onBeforeUnmount(() => clearTimeout(expiryTimer))
</script>

<template>
  <header class="site-header">
    <a class="brand" href="/" aria-label="LiveStream 首页">
      <img src="/favicon.svg" width="36" height="36" alt="" />
      <span>LiveStream</span>
    </a>
    <span class="site-description">实时直播互动</span>
  </header>
  <main class="page-content">
    <template v-if="session">
      <nav class="session-nav" aria-label="主导航">
        <button class="retry-button" :aria-current="activePage === 'rooms' ? 'page' : undefined" @click="activePage = 'rooms'">直播间</button>
        <button class="retry-button" :aria-current="activePage === 'account' ? 'page' : undefined" @click="activePage = 'account'">我的账号</button>
        <button class="retry-button" @click="logout">退出登录</button>
      </nav>
      <RoomsPage v-if="activePage === 'rooms'" :token="session.access_token" :user-id="session.user.id" @expired="expireSession" />
      <AccountPage v-else :user="session.user" :token="session.access_token"
        @logout="logout" @expired="expireSession" @updated="updateUser" />
    </template>
    <AuthPage v-else :notice="notice" @logged-in="startSession" />
    <HealthStatus v-if="isDevelopment" />
  </main>
</template>
