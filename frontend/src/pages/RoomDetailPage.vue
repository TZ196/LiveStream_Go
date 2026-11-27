<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue'
import { closeRoom, getRoom, reopenRoom, type Room } from '../api/rooms'
import { ApiError } from '../api/http'
import { useRoomLikes } from '../likes'
import LikeLeaderboard from '../components/LikeLeaderboard.vue'
import RoomDanmaku from '../components/RoomDanmaku.vue'
import LiveStage from '../components/LiveStage.vue'
import { useRoomConnection } from '../realtime'
import { useRoomDanmaku } from '../danmaku'

const props = defineProps<{ token: string; userId: string; roomId: string }>()
const emit = defineEmits<{ back: []; expired: [] }>()
const room = ref<Room>()
const channel = useRoomConnection(props.token, props.roomId)
const danmaku = useRoomDanmaku(props.token, props.userId, props.roomId, channel)
const { board, canCollect, likeCount, full, notice: likeNotice, like, start: startLikes, stop: stopLikes } = useRoomLikes(props.token, props.userId, props.roomId, channel)
const { rows, mine, onBoard, status: boardStatus } = board
const { liveMessages } = danmaku
const { sessionId: activeSession, onlineCount, watchEnded } = channel

function startRoom() { if (!watchEnded.value) { startLikes(); danmaku.start(); channel.start() } }
function stopRoom() { channel.stop(); stopLikes(); danmaku.stop() }
function reenterRoom() { startLikes(); danmaku.start(); channel.reenter() }
const feedback = ref<HTMLSpanElement>()
let pulse: Animation | undefined

// 点击立即播放一次局部反馈，权威总数由服务端快照更新。
function clickLike() {
  if (watchEnded.value || !like() || !feedback.value) return
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
let unsubscribeRoom: (() => void) | undefined

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
    if (value.status === 'OPEN') startRoom(); else { stopRoom(); void danmaku.loadHistory(false) }
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
      if (value.status === 'OPEN') startRoom(); else { stopRoom(); void danmaku.loadHistory(false) }
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

onMounted(() => {
  unsubscribeRoom = channel.onFrame(frame => {
    // 另一页面关房后，重连查询的结果也要更新画面，不能一直显示直播中。
    if (frame.type === 'ROOM_CLOSED' || (frame.type === 'ROOM_CONTEXT' && frame.data.room_status === 'CLOSED')) void loadRoom()
    if (frame.type === 'CONNECTION_REPLACED' || frame.type === 'WATCH_EXPIRED') { stopLikes(); danmaku.stop() }
    if (frame.type === 'AUTH_EXPIRED') emit('expired')
  })
  void loadRoom()
})
onBeforeUnmount(() => { unsubscribeRoom?.(); controller?.abort(); pulse?.cancel(); stopRoom() })
</script>

<template>
  <section class="live-room-page" aria-labelledby="room-detail-title">
    <div class="room-breadcrumb">
      <button class="back-link" type="button" @click="emit('back')"><span aria-hidden="true">←</span> 发现直播</button>
      <span class="breadcrumb-divider" aria-hidden="true">/</span><span>直播间</span>
      <button class="refresh-room" type="button" :disabled="busy" @click="loadRoom"><svg viewBox="0 0 18 18" fill="none" stroke="currentColor" stroke-width="1.4" aria-hidden="true"><path d="M14.8 7A6 6 0 1 0 15 11M15 3v4h-4" /></svg>刷新状态</button>
    </div>
    <p v-if="message" class="form-message" data-kind="error" role="alert">{{ message }}</p>
    <div v-if="watchEnded && room?.status === 'OPEN'" class="watch-notice" role="status">
      <span>{{ watchEnded === 'replaced' ? '你的账号已在其他窗口进入直播间。' : '观看连接已失效，请重新进入。' }}</span>
      <button class="retry-button" type="button" @click="reenterRoom">重新进入</button>
    </div>
    <div v-if="busy && !room" class="room-loading" role="status"><span class="loading-ring" />正在进入直播间…</div>
    <div v-if="room" class="live-layout">
      <article class="live-main">
        <header class="host-header">
          <div class="host-avatar" aria-hidden="true"><span>{{ room.owner_id === userId ? '我' : '主' }}</span><span v-if="room.status === 'OPEN'" class="avatar-live-dot" /></div>
          <div class="host-copy">
            <div class="host-title-line"><h1 id="room-detail-title">{{ room.title }}</h1><span class="room-badge" :data-status="room.status">{{ room.status === 'OPEN' ? '直播中' : '已结束' }}</span></div>
            <div class="host-subline">
              <span>{{ room.owner_id === userId ? '你的直播间' : '主播 #' + room.owner_id }}</span>
              <span v-if="room.status === 'OPEN'">在线 <strong>{{ onlineCount ?? '—' }}</strong> 人</span>
              <span v-if="room.status === 'OPEN'" class="host-likes"><svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true"><path d="M10 17.1 3.4 10.8A4.4 4.4 0 0 1 10 5a4.4 4.4 0 0 1 6.6 5.8Z" /></svg><strong>{{ likeCount ?? '—' }}</strong><span>本场点赞</span></span>
            </div>
          </div>
          <button v-if="room.status === 'OPEN'" class="host-like-button" type="button" :disabled="!canCollect || full || !!watchEnded" aria-label="为主播点赞" @click="clickLike"><svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><path d="M10 17.1 3.4 10.8A4.4 4.4 0 0 1 10 5a4.4 4.4 0 0 1 6.6 5.8Z" /></svg><span>点赞</span><span ref="feedback" class="like-feedback" aria-hidden="true">+1</span></button>
        </header>
        <p v-if="likeNotice" class="like-status-notice" role="status">{{ likeNotice }}</p>
        <LiveStage :key="activeSession || 'waiting'" :open="room.status === 'OPEN'" :messages="liveMessages" />
        <div class="room-bottom">
          <details class="room-information">
            <summary>关于直播间 <span aria-hidden="true">⌄</span></summary>
            <dl class="account-details">
              <div><dt>房间编号</dt><dd>{{ room.id }}</dd></div>
              <div><dt>房主</dt><dd>{{ room.owner_id === userId ? '我' : room.owner_id }}</dd></div>
              <div><dt>创建时间</dt><dd>{{ formatDate(room.created_at) }}</dd></div>
              <div v-if="room.closed_at"><dt>本次结束时间</dt><dd>{{ formatDate(room.closed_at) }}</dd></div>
            </dl>
          </details>
          <div v-if="room.owner_id === userId" class="owner-actions">
            <button v-if="room.status === 'CLOSED'" class="retry-button" type="button" :disabled="busy || needsRefresh" @click="changeStatus('reopen')">重新开播</button>
            <button v-else-if="!confirming" class="end-live-button" type="button" :disabled="busy || needsRefresh" @click="confirming = true">结束直播</button>
            <div v-else class="end-confirmation">
              <span>确认结束本次直播？</span>
              <button class="retry-button danger-button" type="button" :disabled="busy || needsRefresh" @click="changeStatus('close')">确认结束</button>
              <button class="retry-button" type="button" :disabled="busy" @click="confirming = false">取消</button>
            </div>
          </div>
        </div>
      </article>
      <aside class="live-sidebar" aria-label="直播互动">
        <LikeLeaderboard v-if="room.status === 'OPEN'" :rows="rows" :mine="mine" :on-board="onBoard" :status="boardStatus" :user-id="userId" />
        <RoomDanmaku :state="danmaku" :open="room.status === 'OPEN'" />
      </aside>
    </div>
  </section>
</template>

<style scoped>
.watch-notice { display: flex; align-items: center; flex-wrap: wrap; gap: 12px; margin: 0 0 16px; color: #645d75; font-size: 13px; }
.room-breadcrumb { display: flex; align-items: center; gap: 13px; margin-bottom: 22px; color: #a1a5b3; font-size: 12px; }
.back-link, .refresh-room { display: inline-flex; align-items: center; gap: 7px; border: 0; padding: 2px 0; background: transparent; color: #888e9d; font-size: 12px; }
.back-link:hover, .refresh-room:hover:enabled { color: var(--accent); }
.back-link > span { font-size: 18px; line-height: 1; }
.breadcrumb-divider { color: #d0d3de; }
.refresh-room { margin-left: auto; font-size: 11px; }
.refresh-room svg { width: 14px; height: 14px; }
.live-layout { display: grid; grid-template-columns: minmax(0, 1fr) 352px; gap: 22px; align-items: start; }
.live-main { min-width: 0; padding: 0; }
.host-header { display: flex; align-items: center; gap: 14px; min-height: 90px; padding: 2px 4px 20px; }
.host-avatar { display: grid; place-items: center; position: relative; flex-shrink: 0; width: 53px; height: 53px; border-radius: 18px; background: linear-gradient(145deg, #e8ddfd, #d5e6f7); color: #8272b1; font-size: 22px; font-weight: 600; box-shadow: inset 0 0 0 3px #ffffffa1; }
.avatar-live-dot { position: absolute; right: -2px; bottom: 0; width: 12px; height: 12px; border: 3px solid var(--page-bg); border-radius: 50%; background: #34b294; }
.host-copy { flex: 1; min-width: 0; }
.host-title-line { display: flex; align-items: center; gap: 10px; min-width: 0; }
h1 { margin: 0; font-size: clamp(18px, 1.7vw, 24px); line-height: 1.45; letter-spacing: -.5px; font-weight: 650; overflow-wrap: anywhere; }
.host-title-line .room-badge { flex-shrink: 0; padding: 2px 7px; font-size: 10px; }
.host-subline { display: flex; flex-wrap: wrap; align-items: center; gap: 16px; margin-top: 7px; font-size: 11px; color: #959aaa; }
.host-likes { display: inline-flex; align-items: center; gap: 5px; }
.host-likes svg { width: 12px; height: 12px; color: #ef92a7; }
.host-likes strong { font-size: 12px; color: #ec819b; font-weight: 600; font-variant-numeric: tabular-nums; }
.host-likes > span { color: #a4a6b4; }
.host-like-button { position: relative; display: inline-flex; align-items: center; gap: 7px; flex-shrink: 0; margin-left: 8px; border: 1px solid #f7d5df; border-radius: 10px; padding: 9px 18px; background: #fff3f6; color: #df7896; font-size: 12px; font-weight: 600; }
.host-like-button:hover:enabled { background: #ffe5ee; border-color: #edb2c7; }
.host-like-button:disabled { opacity: .5; }
.host-like-button > svg { width: 16px; height: 16px; }
.like-feedback { position: absolute; right: 14px; top: -8px; opacity: 0; color: #ec7196; font-weight: 700; pointer-events: none; }
.like-status-notice { margin: 0 0 12px; padding: 10px 15px; border: 1px solid #f1e5c9; border-radius: 10px; background: #fffaf0; color: #a18751; font-size: 12px; }
.live-sidebar { display: flex; flex-direction: column; gap: 16px; height: min(760px, calc(100dvh - 164px)); min-height: 570px; }
.room-bottom { display: flex; align-items: flex-start; justify-content: space-between; gap: 20px; margin-top: 17px; padding: 15px 5px; }
.room-information { min-width: 0; flex: 1; color: var(--muted); font-size: 12px; }
.room-information summary { display: inline-flex; align-items: center; gap: 9px; font-weight: 500; cursor: pointer; list-style: none; }
.room-information summary::-webkit-details-marker { display: none; }
.room-information summary > span { color: #a4a9b6; }
.room-information .account-details { margin: 15px 0 0; font-size: 11px; gap: 8px; }
.room-information .account-details div { grid-template-columns: 80px 1fr; }
.owner-actions { flex-shrink: 0; }
.end-live-button { background: transparent; border: 0; padding: 0; color: #a4a9b5; font-size: 11px; }
.end-live-button:hover:enabled { color: #c55b74; }
.end-confirmation { display: flex; flex-wrap: wrap; justify-content: flex-end; align-items: center; gap: 8px; color: #9a7d83; font-size: 11px; max-width: 300px; }
.end-confirmation .retry-button { padding: 4px 8px; font-size: 10px; }
.room-loading { display: flex; align-items: center; justify-content: center; gap: 12px; min-height: 400px; border: 1px solid var(--border); border-radius: 16px; background: #fff; color: #9297a5; font-size: 13px; }
.loading-ring { width: 18px; height: 18px; border: 2px solid #e8e5fa; border-top-color: var(--accent); border-radius: 50%; animation: loading .7s linear infinite; }
@keyframes loading { to { transform: rotate(360deg); } }
@media (max-width: 1080px) { .live-layout { grid-template-columns: minmax(0, 1fr) 318px; gap: 16px; } }
@media (max-width: 880px) { .live-layout { grid-template-columns: 1fr; } .live-sidebar { height: 660px; min-height: 0; } .host-header { min-height: 84px; } .room-breadcrumb { margin-bottom: 16px; } }
@media (max-width: 540px) { .host-header { gap: 10px; } .host-avatar { width: 43px; height: 43px; border-radius: 14px; font-size: 18px; } .host-title-line { flex-wrap: wrap; gap: 6px; } .host-subline { gap: 9px; font-size: 10px; } .host-like-button { padding: 8px 11px; margin-left: 0; } .host-like-button > svg { width: 14px; height: 14px; } .room-bottom { padding-left: 3px; padding-right: 3px; gap: 10px; } .end-confirmation { max-width: 200px; } }
@media (prefers-reduced-motion: reduce) { .loading-ring { animation: none; } }
</style>
