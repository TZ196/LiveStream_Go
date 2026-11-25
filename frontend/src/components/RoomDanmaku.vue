<script setup lang="ts">
import type { useRoomDanmaku } from '../danmaku'

const props = defineProps<{ state: ReturnType<typeof useRoomDanmaku>; open: boolean }>()
const { draft, messages, queue, available, notice, length, historyBusy, hasMore, sendDraft, loadHistory, retry, editRejected } = props.state

function onKey(event: KeyboardEvent) {
  if (event.key === 'Enter' && !event.shiftKey && !event.isComposing && event.keyCode !== 229) {
    event.preventDefault()
    sendDraft()
  }
}
function formatTime(value: string) { return new Date(value).toLocaleTimeString('zh-CN', { hour12: false }) }
</script>

<template>
  <section class="danmaku-panel" aria-labelledby="danmaku-title">
    <h2 id="danmaku-title">{{ open ? '直播弹幕' : '历史弹幕' }}</h2>
    <div class="room-actions">
      <button class="retry-button" type="button" :disabled="historyBusy" @click="loadHistory(false)">刷新弹幕</button>
      <button v-if="hasMore" class="retry-button" type="button" :disabled="historyBusy" @click="loadHistory(true)">加载更早弹幕</button>
    </div>
    <ol class="danmaku-list" aria-live="polite" aria-relevant="additions">
      <li v-for="item in messages" :key="item.event_id">
        <time :datetime="item.created_at">{{ formatTime(item.created_at) }}</time>
        <strong>{{ item.username }}</strong>
        <span class="danmaku-content">{{ item.content }}</span>
      </li>
    </ol>
    <p v-if="!messages.length && !historyBusy" class="account-notice">暂无弹幕，欢迎来聊聊。</p>
    <p v-if="notice" role="status">{{ notice }}</p>
    <form v-if="open" @submit.prevent="sendDraft">
      <label for="danmaku-content">发送弹幕</label>
      <textarea id="danmaku-content" v-model="draft" rows="2" placeholder="说点什么，Enter 发送，Shift+Enter 换行" @keydown="onKey" />
      <div class="room-actions">
        <span :data-invalid="length > 200">{{ length }}/200 字符</span>
        <button class="retry-button" type="submit" :disabled="!available || !draft.trim() || length > 200 || queue.length >= 20">发送</button>
        <span v-if="!available">暂不可发送，草稿会保留在当前页面。</span>
      </div>
    </form>
    <ul v-if="queue.length" class="pending-danmaku" aria-label="待确认弹幕">
      <li v-for="item in queue" :key="item.requestId">
        <span>{{ item.content }}</span>
        <small>{{ item.message || '正在等待接受确认…' }}</small>
        <button v-if="item.status === 'rejected'" class="retry-button" type="button" @click="editRejected(item.requestId)">修改后重新发送</button>
        <button v-else-if="item.status === 'unconfirmed'" class="retry-button" type="button" @click="retry(item.requestId)">核对原请求</button>
      </li>
    </ul>
  </section>
</template>

<style scoped>
.danmaku-panel { margin-top: 1.5rem; border-top: 1px solid #d7dfeb; padding-top: 1rem; }
.danmaku-list { list-style: none; margin: 1rem 0; padding: 0; max-height: 22rem; overflow-y: auto; }
.danmaku-list li { display: flex; flex-wrap: wrap; gap: .6rem; padding: .4rem 0; border-bottom: 1px solid #edf1f7; }
.danmaku-list time { color: #627083; font-size: .85rem; }
.danmaku-content { white-space: pre-wrap; overflow-wrap: anywhere; }
textarea { display: block; width: 100%; box-sizing: border-box; margin: .5rem 0; padding: .65rem; border: 1px solid #b9c5d7; border-radius: .4rem; font: inherit; resize: vertical; }
[data-invalid="true"] { color: #b42318; }
.pending-danmaku { padding-left: 1.25rem; }
.pending-danmaku li { margin: .6rem 0; overflow-wrap: anywhere; }
.pending-danmaku small { display: block; color: #627083; }
</style>
