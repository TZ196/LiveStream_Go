<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue'
import type { useRoomDanmaku } from '../danmaku'

const props = defineProps<{ state: ReturnType<typeof useRoomDanmaku>; open: boolean }>()
const { draft, messages, queue, available, connected, notice, toast, length, needsConfirmation, historyBusy, hasMore, sendDraft, loadHistory, confirmPending } = props.state
const log = ref<HTMLOListElement>()
const input = ref<HTMLTextAreaElement>()
const following = ref(true)
const hasNew = ref(false)
let loadingOlder = false
const connectionText = computed(() => !props.open ? '已结束' : connected.value && available.value ? '已连接' : '连接中')

function onKey(event: KeyboardEvent) {
  if (event.key === 'Enter' && !event.shiftKey && !event.isComposing && event.keyCode !== 229) {
    event.preventDefault()
    submit()
  }
}
function submit() {
  sendDraft()
  input.value?.focus()
}
function trackScroll() {
  const element = log.value
  if (!element || loadingOlder) return
  following.value = element.scrollHeight - element.scrollTop - element.clientHeight < 48
  if (following.value) hasNew.value = false
}
function latest() {
  if (log.value) log.value.scrollTop = log.value.scrollHeight
  following.value = true
  hasNew.value = false
}
async function older() {
  const element = log.value
  if (!element || historyBusy.value) return
  loadingOlder = true
  following.value = false
  const height = element.scrollHeight
  const top = element.scrollTop
  const firstVisible = Array.from(element.querySelectorAll<HTMLElement>('.chat-message')).find(item => item.getBoundingClientRect().bottom > element.getBoundingClientRect().top)
  const anchorId = firstVisible?.dataset.eventId
  const anchorTop = firstVisible?.getBoundingClientRect().top
  try {
    await loadHistory(true)
    await nextTick()
    // 优先按正在阅读的消息定位，同时到来的实时弹幕不会混入前插高度。
    if (log.value === element) {
      const anchor = Array.from(element.querySelectorAll<HTMLElement>('.chat-message')).find(item => item.dataset.eventId === anchorId)
      if (anchor && anchorTop !== undefined) element.scrollTop += anchor.getBoundingClientRect().top - anchorTop
      else element.scrollTop = top + element.scrollHeight - height
    }
  } finally { loadingOlder = false }
}
watch(messages, async (next, previous) => {
  if (loadingOlder) return
  const appended = next.at(-1)?.event_id !== previous.at(-1)?.event_id
  const element = log.value
  await nextTick()
  if (!element || log.value !== element) return
  if (following.value) latest()
  else if (appended) hasNew.value = true
})
</script>

<template>
  <section class="chat-panel" aria-labelledby="danmaku-title">
    <header class="chat-heading">
      <div class="panel-title"><span class="chat-mark" aria-hidden="true">聊</span><h2 id="danmaku-title">{{ open ? '弹幕聊天' : '历史弹幕' }}</h2></div>
      <div class="chat-heading-actions">
        <span class="connection-status" :data-connected="connected && available">{{ connectionText }}</span>
        <button class="quiet-button" type="button" :disabled="historyBusy" aria-label="刷新弹幕" @click="loadHistory(false)">刷新</button>
      </div>
    </header>
    <div class="chat-welcome">欢迎来到直播间，友善表达，一起分享此刻。</div>
    <div v-if="hasMore" class="history-action"><button class="quiet-button" type="button" :disabled="historyBusy" @click="older">{{ historyBusy ? '正在加载…' : '查看更早消息' }}</button></div>
    <ol ref="log" class="chat-log" aria-live="polite" aria-relevant="additions" aria-label="弹幕消息" @scroll="trackScroll">
      <li v-for="item in messages" :key="item.event_id" :data-event-id="item.event_id" class="chat-message">
        <span class="chat-name" :title="item.username">{{ item.username }}</span><span class="chat-colon">：</span><span class="chat-content">{{ item.content }}</span>
      </li>
      <li v-if="!messages.length" class="chat-empty">
        <span class="empty-chat-icon" aria-hidden="true">“</span>
        <p>{{ historyBusy ? '正在加载弹幕…' : '还没有弹幕' }}</p>
        <small>{{ open ? '发一句问候，让这里热闹起来。' : '这个房间还没有历史消息。' }}</small>
      </li>
    </ol>
    <button v-if="hasNew" class="new-messages" type="button" @click="latest">有新消息 · 回到最新 ↓</button>
    <div v-if="needsConfirmation" class="chat-notice" role="status">
      <span>有消息的发送结果尚未确认。</span>
      <button class="quiet-button" type="button" :disabled="!available || historyBusy" @click="confirmPending">重试确认</button>
    </div>
    <p v-if="notice" class="chat-notice" role="status">{{ notice }}</p>
    <form v-if="open" class="chat-composer" @submit.prevent="submit">
      <label class="visually-hidden" for="danmaku-content">发送弹幕</label>
      <textarea id="danmaku-content" ref="input" v-model="draft" rows="2" placeholder="发条弹幕，聊聊此刻…" aria-describedby="composer-help" :aria-invalid="length > 200" @keydown="onKey" />
      <div class="composer-actions">
        <span id="composer-help" class="composer-help">{{ !connected ? '正在连接直播间…' : !available ? '暂不可发送，草稿已保留' : 'Enter 发送 · Shift+Enter 换行' }}</span>
        <span v-if="length" class="character-count" :data-invalid="length > 200">{{ length }}/200</span>
        <button class="send-button" type="submit" :disabled="!available || !draft.trim() || length > 200 || queue.length >= 20 || needsConfirmation">发送</button>
      </div>
    </form>
    <p v-else class="chat-ended">直播已结束，欢迎下次再来。</p>
    <Transition name="toast"><div v-if="toast" class="notice-toast" role="alert"><span class="toast-icon" aria-hidden="true">!</span>{{ toast }}</div></Transition>
  </section>
</template>

<style scoped>
.chat-panel { display: flex; flex: 1; flex-direction: column; min-height: 300px; position: relative; background: #fff; border: 1px solid var(--border); border-radius: 16px; overflow: hidden; }
.chat-heading { display: flex; align-items: center; justify-content: space-between; gap: 10px; min-height: 62px; padding: 0 18px; border-bottom: 1px solid var(--border-soft); }
.panel-title, .chat-heading-actions { display: flex; align-items: center; gap: 9px; }
h2 { margin: 0; font-size: 15px; font-weight: 700; }
.chat-mark { display: grid; place-items: center; width: 25px; height: 25px; border-radius: 8px; background: var(--accent-soft); color: var(--accent); font-size: 12px; font-weight: 700; }
.connection-status { color: #89909d; font-size: 11px; white-space: nowrap; }
.connection-status::before { display: inline-block; content: ''; width: 5px; height: 5px; margin-right: 5px; border-radius: 50%; background: #abb0ba; }
.connection-status[data-connected="true"]::before { background: #24ae83; }
.quiet-button { border: 0; background: transparent; padding: 5px 0; color: var(--muted); font-size: 12px; }
.quiet-button:hover:enabled { color: var(--accent); }
.quiet-button:disabled { opacity: .45; }
.chat-welcome { margin: 12px 14px 4px; padding: 9px 11px; border-radius: 7px; color: #85839b; background: #f6f5fb; font-size: 11px; line-height: 1.7; }
.history-action { text-align: center; padding: 2px 12px; }
.chat-log { list-style: none; margin: 0; padding: 13px 17px; flex: 1; min-height: 0; overflow-y: auto; overscroll-behavior: contain; scrollbar-width: thin; scrollbar-color: #dce0e8 transparent; }
.chat-message { margin: 0 0 13px; font-size: 13px; line-height: 1.85; overflow-wrap: anywhere; }
.chat-name { font-weight: 600; color: #858b9a; }
.chat-colon { color: #b5b8c2; }
.chat-content { color: #414656; white-space: pre-wrap; }
.chat-empty { padding: 35px 12px; text-align: center; color: #9a9eab; }
.empty-chat-icon { display: grid; place-items: center; width: 48px; height: 48px; margin: 0 auto 5px; border-radius: 16px; background: #f3f4f9; font-family: Georgia, serif; font-size: 50px; line-height: 1; color: #c8cbd8; }
.chat-empty p { font-size: 13px; margin: 12px 0 3px; }
.chat-empty small { font-size: 11px; }
.new-messages { align-self: center; margin: 3px 10px 10px; padding: 5px 13px; background: var(--accent-soft); color: var(--accent); border: 0; border-radius: 20px; font-size: 12px; }
.chat-notice { display: flex; align-items: center; justify-content: space-between; gap: 7px; margin: 0; padding: 8px 14px; background: #fff9eb; color: #9b7026; font-size: 12px; line-height: 1.6; overflow-wrap: anywhere; }
.chat-composer { padding: 13px; border-top: 1px solid var(--border-soft); background: #fff; }
textarea { display: block; width: 100%; resize: none; min-height: 76px; padding: 11px 12px; color: var(--ink); background: #f7f8fb; border: 1px solid #ebeef3; border-radius: 10px; font: inherit; font-size: 13px; line-height: 1.6; transition: border-color .15s; }
textarea:focus { outline: 0; border-color: #a6a1f8; box-shadow: 0 0 0 3px #7265ee0d; }
textarea::placeholder { color: #a5a8b3; }
.composer-actions { display: flex; align-items: center; gap: 7px; margin-top: 10px; }
.composer-help { flex: 1; min-width: 0; font-size: 10px; color: #9a9eab; line-height: 1.5; }
.character-count { font-size: 10px; color: #a2a5b0; white-space: nowrap; }
.character-count[data-invalid="true"] { color: #cf4c66; }
.send-button { flex-shrink: 0; padding: 7px 20px; border: 0; border-radius: 8px; color: #fff; background: var(--accent); font-size: 12px; font-weight: 600; }
.send-button:hover:enabled { background: var(--accent-hover); }
.send-button:disabled { background: #e0def1; color: #fff; }
.chat-ended { margin: 0; padding: 17px; text-align: center; border-top: 1px solid var(--border-soft); font-size: 12px; color: var(--muted); }
.notice-toast { position: fixed; top: 90px; left: 50%; z-index: 100; transform: translateX(-50%); display: flex; align-items: center; gap: 10px; max-width: calc(100vw - 32px); padding: 13px 20px; border: 1px solid #f0dfbf; border-radius: 12px; background: #fffdf7; box-shadow: 0 8px 35px #322b2b1a; color: #92672e; font-size: 13px; }
.toast-icon { display: grid; place-items: center; flex-shrink: 0; width: 19px; height: 19px; border-radius: 50%; color: white; background: #d4a553; font-weight: 700; }
.toast-enter-active, .toast-leave-active { transition: opacity .16s ease, top .16s ease; }
.toast-enter-from, .toast-leave-to { opacity: 0; top: 80px; }
@media (prefers-reduced-motion: reduce) { .toast-enter-active, .toast-leave-active { transition: none; } }
</style>
