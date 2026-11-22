<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue'
import { closeRoom, getRoom, reopenRoom, type Room } from '../api/rooms'
import { ApiError } from '../api/http'

const props = defineProps<{ token: string; userId: string; roomId: string }>()
const emit = defineEmits<{ back: []; expired: [] }>()
const room = ref<Room>()
const busy = ref(false)
const confirming = ref(false)
const needsRefresh = ref(false)
const message = ref('')
let controller: AbortController | undefined

function formatDate(value: string): string { return new Date(value).toLocaleString('zh-CN', { hour12: false }) }

async function loadRoom() {
  if (busy.value) return
  const attempt = new AbortController()
  controller = attempt
  busy.value = true
  message.value = ''
  try {
    const value = await getRoom(props.token, props.roomId, attempt.signal)
    if (attempt.signal.aborted) return
    room.value = value
    confirming.value = false
    needsRefresh.value = false
  } catch (error) {
    if (attempt.signal.aborted) return
    if (error instanceof ApiError && error.status === 401) emit('expired')
    message.value = error instanceof Error ? error.message : '无法读取房间，请稍后重试。'
  } finally {
    busy.value = false
    controller = undefined
  }
}

async function changeStatus(action: 'close' | 'reopen') {
  if (busy.value || needsRefresh.value || (action === 'close' && !confirming.value)) return
  const attempt = new AbortController()
  controller = attempt
  busy.value = true
  message.value = ''
  try {
    const value = action === 'close'
      ? await closeRoom(props.token, props.roomId, attempt.signal)
      : await reopenRoom(props.token, props.roomId, attempt.signal)
    if (!attempt.signal.aborted) room.value = value
  } catch (error) {
    if (attempt.signal.aborted) return
    if (error instanceof ApiError && error.status === 401) emit('expired')
    message.value = error instanceof Error ? error.message : '操作结果未确认。'
    if (!(error instanceof ApiError) || error.status >= 500) {
      needsRefresh.value = true
      message.value += ' 请刷新房间状态确认结果，再进行下一次操作。'
    }
  } finally {
    busy.value = false
    confirming.value = false
    controller = undefined
  }
}

onMounted(() => { void loadRoom() })
onBeforeUnmount(() => controller?.abort())
</script>

<template>
  <section aria-labelledby="room-detail-title">
    <div class="room-actions">
      <button class="retry-button" type="button" @click="emit('back')">返回房间列表</button>
      <button class="retry-button" type="button" :disabled="busy" @click="loadRoom">刷新房间状态</button>
    </div>
    <p v-if="busy" role="status">正在处理…</p>
    <p v-if="message" class="form-message" data-kind="error" role="alert">{{ message }}</p>
    <article v-if="room" class="account-card">
      <span class="room-badge" :data-status="room.status">{{ room.status === 'OPEN' ? '直播中' : '已结束' }}</span>
      <h1 id="room-detail-title" class="room-title">{{ room.title }}</h1>
      <dl class="account-details">
        <div><dt>房间编号</dt><dd>{{ room.id }}</dd></div>
        <div><dt>房主</dt><dd>{{ room.owner_id === userId ? '我' : room.owner_id }}</dd></div>
        <div><dt>创建时间</dt><dd>{{ formatDate(room.created_at) }}</dd></div>
        <div v-if="room.closed_at"><dt>本次结束时间</dt><dd>{{ formatDate(room.closed_at) }}</dd></div>
      </dl>
      <p class="account-notice">{{ room.status === 'CLOSED' ? '本次直播已结束，房主可以在这个房间重新开播。' : '房间已开放，点赞与弹幕功能即将接入。' }}</p>
      <div v-if="room.owner_id === userId" class="room-close">
        <button v-if="room.status === 'CLOSED'" class="retry-button" type="button" :disabled="busy || needsRefresh" @click="changeStatus('reopen')">重新开播</button>
        <button v-else-if="!confirming" class="retry-button danger-button" type="button" :disabled="busy || needsRefresh" @click="confirming = true">结束直播</button>
        <div v-else>
          <p>确认结束本次直播？房间会保留，下次可以重新开播。</p>
          <div class="room-actions">
            <button class="retry-button danger-button" type="button" :disabled="busy || needsRefresh" @click="changeStatus('close')">确认结束</button>
            <button class="retry-button" type="button" :disabled="busy" @click="confirming = false">取消</button>
          </div>
        </div>
      </div>
    </article>
  </section>
</template>
