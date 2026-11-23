import { ref } from 'vue'
import { useLeaderboard } from './leaderboard'
import { getLikeTicket, newRequestId } from './api/likes'
import { isRecord } from './api/http'
import { deliveryTiming } from './deliveryTiming'

const batchWindowMs = 500
const ackTimeoutMs = 3000
const maxPending = 100
const maxAttempts = 5

type Batch = {
  requestId: string
  sessionId: string
  count: number
  attempts: number
  // 首发时是窗口结束时间，此后是本批次的下一次 ACK 超时。
  dueAt: number
}

function restoreBatch(value: unknown): Batch | undefined {
  if (!isRecord(value) || typeof value.requestId !== 'string' || typeof value.sessionId !== 'string') return
  // 旧页面已发送的单击请求没有数量和截止时间，按原 ID 作为一个赞恢复。
  const count = value.count === undefined ? 1 : value.count
  const attempts = value.attempts ?? 0
  const dueAt = value.dueAt ?? 0
  if (typeof count !== 'number' || !Number.isSafeInteger(count) || count < 1
    || typeof attempts !== 'number' || !Number.isSafeInteger(attempts) || attempts < 0
    || typeof dueAt !== 'number' || !Number.isSafeInteger(dueAt)) return
  return { requestId: value.requestId, sessionId: value.sessionId, count, attempts, dueAt }
}

// 一次房间页面只连接一个场次。已发批次冻结，重试和后续点击各自独立。
export function useRoomLikes(token: string, userId: string, roomId: string) {
  const board = useLeaderboard(token, roomId, userId)
  const connected = ref(false)
  const likeCount = ref<string | null>(null)
  const pendingCount = ref(0)
  const full = ref(false)
  const notice = ref('')
  const roomStorageKey = `livestream.likes.${userId}.${roomId}`
  let socket: WebSocket | undefined
  let timing: ReturnType<typeof deliveryTiming> | undefined
  let controller: AbortController | undefined
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined
  let sendTimer: ReturnType<typeof setTimeout> | undefined
  let scheduledAt: number | undefined
  let stopped = true
  let reconnects = 0
  let sessionId = ''
  let epoch = ''
  let revision = -1n
  let pending: Batch[] = []
  function loadSession(nextSession: string) {
    sessionId = nextSession
    pending = []
    likeCount.value = null
    epoch = ''
    revision = -1n
    const key = `${roomStorageKey}.${sessionId}`
    try {
      const stored = sessionStorage.getItem(key)
      const saved: unknown = JSON.parse(stored ?? sessionStorage.getItem(roomStorageKey) ?? '[]')
      if (!Array.isArray(saved)) return
      for (const item of saved.slice(0, maxPending)) {
        const batch = restoreBatch(item)
        if (batch?.sessionId === sessionId) pending.push(batch)
      }
      // 旧版按房间存储。先写入当前场次，再移走旧键中对应的条目。
      if (stored === null && pending.length) {
        sessionStorage.setItem(key, JSON.stringify(pending))
        const others = saved.filter(item => isRecord(item) && item.sessionId !== sessionId)
        if (others.length) sessionStorage.setItem(roomStorageKey, JSON.stringify(others))
        else sessionStorage.removeItem(roomStorageKey)
      }
    } catch {
      notice.value = '无法完整恢复此前待确认的点赞。'
    }
  }

  function openBatch(now: number) {
    const last = pending[pending.length - 1]
    if (last && last.attempts === 0 && now < last.dueAt) return last
  }

  function save() {
    pendingCount.value = pending.length
    full.value = pending.length >= maxPending && !openBatch(Date.now())
    const key = `${roomStorageKey}.${sessionId}`
    try {
      if (pending.length) sessionStorage.setItem(key, JSON.stringify(pending))
      else sessionStorage.removeItem(key)
    } catch {
      notice.value = '浏览器无法保存待确认请求，请勿关闭页面。'
    }
  }

  function cancelSendTimer() {
    clearTimeout(sendTimer)
    sendTimer = undefined
    scheduledAt = undefined
  }

  // 只为最早到期的批次设置一个定时器；新点击不推迟已打开的 500 ms 窗口。
  function scheduleSend() {
    if (!socket || socket.readyState !== WebSocket.OPEN) return
    let earliest: number | undefined
    for (const batch of pending) {
      if (batch.attempts < maxAttempts && (earliest === undefined || batch.dueAt < earliest)) {
        earliest = batch.dueAt
      }
    }
    if (earliest === scheduledAt) return
    cancelSendTimer()
    if (earliest !== undefined) {
      scheduledAt = earliest
      sendTimer = setTimeout(sendDue, Math.max(0, earliest - Date.now()))
    }
  }

  function sendDue() {
    cancelSendTimer()
    if (!socket || socket.readyState !== WebSocket.OPEN) return
    const now = Date.now()
    for (const batch of pending) {
      if (batch.attempts >= maxAttempts || batch.dueAt > now) continue
      try {
        socket.send(JSON.stringify({
          version: 1,
          type: 'LIKE',
          session_id: batch.sessionId,
          request_id: batch.requestId,
          data: { count: batch.count },
        }))
      } catch {
        socket.close()
        break
      }
      batch.attempts++
      batch.dueAt = now + ackTimeoutMs
    }
    save()
    scheduleSend()
  }

  function receive(event: MessageEvent<string>) {
    const receivedAt = performance.now()
    let message: unknown
    try { message = JSON.parse(event.data) } catch { return }
    if (!isRecord(message) || !isRecord(message.data)) return
    timing?.receive(message, receivedAt)
    if (message.type === 'LIKES_UNAVAILABLE') {
      likeCount.value = null
      notice.value = '实时点赞暂不可用，正在恢复。'
      board.stop('本场点赞榜暂不可用')
    }
    if (message.type === 'ROOM_STATE' && message.data.session_id === sessionId && typeof message.data.projection_epoch === 'string') {
      board.start(sessionId, message.data.projection_epoch)
    } else {
      board.receive(message.type, message.data)
    }
    if (message.type === 'ACK' && typeof message.request_id === 'string') {
      pending = pending.filter(batch => batch.requestId !== message.request_id)
      save()
      scheduleSend()
    }
    if ((message.type === 'LIKE_UPDATE' || message.type === 'ROOM_STATE') && message.data.session_id === sessionId
      && typeof message.data.like_count === 'string' && /^\d+$/.test(message.data.like_count)
      && typeof message.data.likes_revision === 'string' && /^\d+$/.test(message.data.likes_revision)
      && typeof message.data.projection_epoch === 'string') {
      const nextRevision = BigInt(message.data.likes_revision)
      if (epoch !== message.data.projection_epoch || nextRevision >= revision) {
        epoch = message.data.projection_epoch
        revision = nextRevision
        likeCount.value = message.data.like_count
        notice.value = ''
      }
    }
  }

  function scheduleReconnect() {
    if (stopped || reconnectTimer !== undefined) return
    reconnects++
    if (reconnects > 10) { notice.value = '连接已断开，请刷新页面重试。'; return }
    reconnectTimer = setTimeout(() => {
      reconnectTimer = undefined
      void connect()
    }, Math.min(30000, 1000 * 2 ** (reconnects - 1)))
  }

  async function connect() {
    const attempt = new AbortController()
    controller = attempt
    try {
      const ticket = await getLikeTicket(token, roomId, attempt.signal)
      if (attempt.signal.aborted || stopped) return
      if (sessionId !== ticket.session_id) loadSession(ticket.session_id)
      const url = new URL(ticket.ws_path, window.location.href)
      url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
      const ws = new WebSocket(url)
      socket = ws
      timing = deliveryTiming(ws, sessionId)
      ws.onopen = () => {
        if (socket !== ws) return
        connected.value = true
        reconnects = 0
        notice.value = ''
        // 重连只提早重试已发过的同场批次。尚未首发的批次沿用原窗口。
        for (const batch of pending) {
          if (batch.attempts > 0) batch.dueAt = Date.now()
        }
        save()
        scheduleSend()
      }
      ws.onmessage = event => { if (socket === ws) receive(event) }
      ws.onerror = () => { if (socket === ws) notice.value = '连接暂不可用，正在尝试恢复。' }
      ws.onclose = () => {
        if (socket !== ws) return
        connected.value = false
        socket = undefined
        timing?.stop()
        timing = undefined
        board.stop()
        cancelSendTimer()
        scheduleReconnect()
      }
    } catch {
      if (!attempt.signal.aborted && !stopped) { notice.value = '连接暂不可用，正在尝试恢复。'; scheduleReconnect() }
    }
  }

  function like(): boolean {
    if (!connected.value || !sessionId) return false
    const now = Date.now()
    let batch = openBatch(now)
    if (!batch) {
      if (pending.length >= maxPending) return false
      batch = { requestId: newRequestId(), sessionId, count: 0, attempts: 0, dueAt: now + batchWindowMs }
      pending.push(batch)
    }
    batch.count++
    save()
    scheduleSend()
    return true
  }

  function start() {
    if (!stopped) return
    stopped = false
    reconnects = 0
    void connect()
  }

  function stop() {
    stopped = true
    board.stop()
    connected.value = false
    controller?.abort()
    clearTimeout(reconnectTimer)
    reconnectTimer = undefined
    cancelSendTimer()
    const ws = socket
    socket = undefined
    timing?.stop()
    timing = undefined
    ws?.close()
  }

  return { connected, likeCount, pendingCount, full, notice, like, start, stop, board }
}
