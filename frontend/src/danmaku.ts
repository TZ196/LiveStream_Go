import { computed, ref } from 'vue'
import { newRequestId } from './api/likes'
import { getRecentDanmaku, isDanmaku, type DanmakuMessage } from './api/danmaku'
import { ApiError, isRecord } from './api/http'
import type { RoomConnection, RoomFrame } from './realtime'

type Pending = {
  requestId: string; sessionId: string; content: string; attempts: number; dueAt: number
  status: 'pending' | 'rejected' | 'unconfirmed'; message: string
}
const windowMs = 24 * 60 * 60 * 1000
function requestTime(value: string) { return Number.parseInt(value.replaceAll('-', '').slice(0, 12), 16) }

// 弹幕只通过共享房间连接发送，状态、草稿和有限重试队列与点赞分别维护。
export function useRoomDanmaku(token: string, userId: string, roomId: string, channel: RoomConnection) {
  const draft = ref('')
  const messages = ref<DanmakuMessage[]>([])
  const queue = ref<Pending[]>([])
  const available = ref(false)
  const notice = ref('')
  const historyBusy = ref(false)
  const hasMore = ref(false)
  const length = computed(() => Array.from(draft.value).length)
  const seen = new Map<string, string>()
  let session = ''
  let connectionGeneration = -1
  let cursor = ''
  let stopped = true
  let pending: Pending[] = []
  let timer: ReturnType<typeof setTimeout> | undefined
  let historyTimer: ReturnType<typeof setTimeout> | undefined
  let warmingTimer: ReturnType<typeof setTimeout> | undefined
  let warmingAttempts = 0
  let controller: AbortController | undefined
  let unsubscribe: (() => void) | undefined
  let storageReadable = true

  function key() { return 'livestream.danmaku.' + userId + '.' + roomId + '.' + session }
  function save() {
    queue.value = [...pending]
    if (!session) return
    if (!storageReadable) { notice.value = "待确认缓存无法读取，原记录已保留；本页新增内容暂无法保存。"; return }
    try {
      if (pending.length) sessionStorage.setItem(key(), JSON.stringify(pending))
      else sessionStorage.removeItem(key())
    } catch { notice.value = '浏览器无法保存待确认弹幕，请勿刷新或关闭页面。' }
  }
  function load(next: string) {
    save()
    session = next
    connectionGeneration = -1
    pending = []
    storageReadable = true
    messages.value = []
    seen.clear()
    cursor = ''
    clearTimeout(warmingTimer); warmingTimer = undefined; warmingAttempts = 0
    hasMore.value = false
    controller?.abort()
    historyBusy.value = false
    try {
      const stored: unknown = JSON.parse(sessionStorage.getItem(key()) ?? '[]')
      if (!Array.isArray(stored)) throw new Error('缓存格式无效')
      for (const item of stored.slice(0, 20)) {
        if (!isRecord(item) || typeof item.requestId !== 'string'
          || !/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(item.requestId)
          || item.sessionId !== session || typeof item.content !== 'string' || Array.from(item.content).length > 200
          || !Number.isSafeInteger(item.attempts) || Number(item.attempts) < 0 || !Number.isSafeInteger(item.dueAt)
          || !['pending', 'rejected', 'unconfirmed'].includes(String(item.status))) throw new Error('缓存记录无效')
        pending.push(item as Pending)
      }
    } catch { storageReadable = false; notice.value = '待确认弹幕缓存无法读取，原记录已保留。' }
    save()
  }
  function merge(items: DanmakuMessage[], older = false) {
    const additions: DanmakuMessage[] = []
    for (const item of items) {
      if (item.room_id !== roomId || (session && item.session_id !== session)) continue
      const fingerprint = item.user_id + '\n' + item.session_id + '\n' + item.content
      const prior = seen.get(item.event_id)
      if (prior !== undefined) {
        if (prior !== fingerprint) notice.value = '弹幕记录内容冲突，已停止合并该记录。'
        continue
      }
      seen.set(item.event_id, fingerprint)
      if (seen.size > 5000) seen.delete(seen.keys().next().value!)
      additions.push(item)
    }
    messages.value = older ? [...additions, ...messages.value].slice(0, 200) : [...messages.value, ...additions].slice(-200)
  }
  async function loadHistory(older = false) {
    if (historyBusy.value || (older && !cursor)) return
    if (!session) { notice.value = '暂无可查询的直播场次。'; return }
    controller?.abort()
    const attempt = new AbortController()
    controller = attempt
    const expected = session
    historyBusy.value = true
    try {
      const result = await getRecentDanmaku(token, roomId, session, older ? cursor : '', attempt.signal)
      if (attempt.signal.aborted || session !== expected) return
      clearTimeout(warmingTimer); warmingTimer = undefined; warmingAttempts = 0
      merge([...result.items].reverse(), older)
      // 缓存事件来自 Kafka 消费或已落库记录；匹配完整请求可确认接受，但不表示已落库。
      pending = pending.filter(batch => !result.items.some(item => item.request_id === batch.requestId
        && item.user_id === userId && item.room_id === roomId && item.session_id === batch.sessionId && item.content === batch.content))
      cursor = result.nextCursor ?? ''
      hasMore.value = !!cursor
      save()
      schedule()
    } catch (error) {
      if (!attempt.signal.aborted) notice.value = error instanceof Error ? error.message : '暂时无法查询弹幕历史。'
      if (!attempt.signal.aborted && !stopped && error instanceof ApiError && error.code === 'CACHE_WARMING' && warmingAttempts < 50) {
        warmingAttempts++
        clearTimeout(warmingTimer)
        warmingTimer = setTimeout(() => { warmingTimer = undefined; if (!stopped && session === expected) void loadHistory(older) }, 1000)
      }
    } finally { if (controller === attempt) { controller = undefined; historyBusy.value = false } }
  }
  function schedule() {
    clearTimeout(timer); timer = undefined
    if (stopped || !available.value || !channel.connected.value) return
    const head = pending.find(item => item.status !== 'rejected')
    if (!head || head.status === 'unconfirmed') return
    if (Date.now() - requestTime(head.requestId) >= windowMs || head.attempts >= 5) {
      head.status = 'unconfirmed'
      head.message = '原请求结果仍未确认，已停止自动重发。'
      save()
      return
    }
    timer = setTimeout(sendHead, Math.max(0, head.dueAt - Date.now()))
  }
  function sendHead() {
    timer = undefined
    if (stopped || !available.value) return
    const head = pending.find(item => item.status !== 'rejected')
    if (!head || head.status !== 'pending') return
    head.attempts++
    head.dueAt = Date.now() + 3000
    save()
    if (!channel.send('DANMAKU', head.requestId, head.sessionId, { content: head.content })) {
      head.attempts--
      head.dueAt = Date.now() + 1000
      save()
    }
    schedule()
  }
  function sendDraft(): boolean {
    if (!draft.value.trim() || length.value > 200) { notice.value = '弹幕须为 1 至 200 个字符，不能全为空白。'; return false }
    if (!session || !available.value || !channel.connected.value) { notice.value = '弹幕暂不可用，草稿已保留。'; return false }
    if (pending.length >= 20) { notice.value = '待确认队列已满，请先处理已有记录。'; return false }
    pending.push({ requestId: newRequestId(), sessionId: session, content: draft.value, attempts: 0, dueAt: Date.now(), status: 'pending', message: '' })
    draft.value = ''
    notice.value = ''
    save()
    schedule()
    return true
  }
  function retry(id: string) {
    const item = pending.find(value => value.requestId === id)
    if (!item || item.status === 'rejected') return
    if (Date.now() - requestTime(item.requestId) >= windowMs) { notice.value = '原请求已超过重试期限，记录继续保留。'; return }
    item.status = 'pending'
    item.attempts = 0
    item.dueAt = Date.now()
    save()
    schedule()
  }
  function editRejected(id: string) {
    const item = pending.find(value => value.requestId === id && value.status === 'rejected')
    if (!item) return
    draft.value = item.content
    pending = pending.filter(value => value !== item)
    save()
    notice.value = '修改后点击发送，将创建新的请求。'
  }
  function receive(frame: RoomFrame) {
    const data = frame.data
    if (frame.type === 'ROOM_CONTEXT' && typeof data.session_id === 'string' && data.session_id !== session) load(data.session_id)
    if (frame.type === 'ROOM_STATE' && data.session_id === session) {
      const wasAvailable = available.value
      available.value = data.danmaku_available === true
      if (!wasAvailable && available.value) {
        notice.value = ''
        schedule()
      }
      const generation = Number(data.connection_generation ?? 0)
      const newConnection = generation !== connectionGeneration
      connectionGeneration = generation
      if (data.history_available === true && (newConnection || !wasAvailable || !messages.value.length)) void loadHistory()
    }
    if (frame.type === 'CONNECTION_CLOSED' || frame.type === 'AUTH_EXPIRED' || frame.type === 'ROOM_CLOSED') {
      available.value = false
      clearTimeout(timer); timer = undefined
    }
    if (frame.type === 'DANMAKU_BROADCAST' && typeof frame.event_id === 'string' && typeof frame.request_id === 'string') {
      const item = { ...data, event_id: frame.event_id, request_id: frame.request_id }
      if (isDanmaku(item)) merge([item])
    }
    const batch = pending.find(item => item.requestId === frame.request_id)
    if (!batch) return
    if (frame.type === 'ACK' && data.status === 'accepted') {
      pending = pending.filter(item => item !== batch)
      save()
      schedule()
      clearTimeout(historyTimer)
      historyTimer = setTimeout(() => { historyTimer = undefined; if (!stopped) void loadHistory() }, 500)
    }
    if (frame.type === 'ERROR') {
      batch.message = typeof data.message === 'string' ? data.message : '弹幕结果需要核对。'
      if (data.outcome === 'rejected' && data.retryable !== true) batch.status = 'rejected'
      else if (data.retryable === true) batch.dueAt = Date.now() + Math.max(1000, Math.min(30000, Number(data.retry_after_ms) || 1000))
      else batch.status = 'unconfirmed'
      notice.value = batch.message
      save()
      schedule()
    }
  }
  function start() {
    if (!stopped) return
    stopped = false
    unsubscribe = channel.onFrame(receive)
  }
  function stop() {
    stopped = true
    unsubscribe?.(); unsubscribe = undefined
    available.value = false
    clearTimeout(timer); timer = undefined
    clearTimeout(historyTimer); historyTimer = undefined
    clearTimeout(warmingTimer); warmingTimer = undefined; warmingAttempts = 0
    controller?.abort()
    save()
  }
  return { draft, messages, queue, available, notice, length, historyBusy, hasMore, sendDraft, loadHistory, retry, editRejected, start, stop }
}
