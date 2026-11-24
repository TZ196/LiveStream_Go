<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue'
import { closeRoom, getRoom, reopenRoom, type Room } from '../api/rooms'
import { ApiError } from '../api/http'
import { useRoomLikes } from '../likes'
import LikeLeaderboard from '../components/LikeLeaderboard.vue'

const props = defineProps<{ token: string; userId: string; roomId: string }>()
const emit = defineEmits<{ back: []; expired: [] }>()
const room = ref<Room>()
const { board, canCollect, likeCount, queuedClicks, full, notice: likeNotice, like, start: startLikes, stop: stopLikes } = useRoomLikes(props.token, props.userId, props.roomId)
const { rows, mine, onBoard, status: boardStatus } = board
const feedback = ref<HTMLSpanElement>()
let pulse: Animation | undefined

// 点击立即播放一次局部反馈，权威总数由服务端快照更新。
function clickLike() {
  if (!like() || !feedback.value) return
  pulse?.cancel()
  const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches
  pulse = feedback.value.animate([
    { opacity: 1, transform: 'translateY(0)' },
    { opacity: 0, transform: reducedMotion ? 'none' : 'translateY(-14px)' },
  ], { duration: 350 })
}

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
    if (value.status === 'OPEN') startLikes(); else stopLikes()
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
    if (!attempt.signal.aborted) {
      room.value = value
      if (value.status === 'OPEN') startLikes(); else stopLikes()
    }
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
onBeforeUnmount(() => { controller?.abort(); pulse?.cancel(); stopLikes() })
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
      <p class="account-notice">{{ room.status === 'CLOSED' ? '本次直播已结束，房主可以在这个房间重新开播。' : '房间已开放，欢迎点赞。' }}</p>
      <div v-if="room.status === 'OPEN'" class="room-actions">
        <button class="retry-button like-button" type="button" :disabled="!canCollect || full" @click="clickLike">
          点赞<span ref="feedback" class="like-feedback" aria-hidden="true">+1</span>
        </button>
        <span>本场点赞：{{ likeCount ?? '暂不可用' }}</span>
        <span v-if="queuedClicks !== '0'">待发送或确认：{{ queuedClicks }} 赞</span>
      </div>
      <p v-if="likeNotice" role="status">{{ likeNotice }}</p>
      <LikeLeaderboard v-if="room.status === 'OPEN'" :rows="rows" :mine="mine" :on-board="onBoard" :status="boardStatus" :user-id="userId" />
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

<style scoped>
.like-button {
  position: relative;
}

.like-feedback {
  position: absolute;
  right: 0;
  top: 0;
  opacity: 0;
  pointer-events: none;
}
</style>
