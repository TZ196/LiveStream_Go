import { ref } from 'vue'
import { useLeaderboard } from './leaderboard'
import { getLikeState, getLikeTicket, newRequestId, type LikeState } from './api/likes'
import { ApiError, isRecord } from './api/http'
import { deliveryTiming } from './deliveryTiming'

const batchWindowMs = 500
const ackTimeoutMs = 3000
const maxPending = 100
const maxAttempts = 5
const maxCollected = 9223372036854775807n
const requestWindowMs = 24 * 60 * 60 * 1000

type Batch = { requestId: string; sessionId: string; count: number; attempts: number; dueAt: number }

function requestTime(id: string): number {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(id)) return 0
  return Number.parseInt(id.replaceAll('-', '').slice(0, 12), 16)
}

function restoreBatch(value: unknown): Batch | undefined {
  if (!isRecord(value) || typeof value.requestId !== 'string' || !requestTime(value.requestId) || typeof value.sessionId !== 'string') return
  const count = value.count ?? 1
  const attempts = value.attempts ?? 0
  const dueAt = value.dueAt ?? 0
  if (typeof count !== 'number' || !Number.isSafeInteger(count) || count < 1
    || typeof attempts !== 'number' || !Number.isSafeInteger(attempts) || attempts < 0
    || typeof dueAt !== 'number' || !Number.isSafeInteger(dueAt)) return
  return { requestId: value.requestId, sessionId: value.sessionId, count, attempts, dueAt }
}

// 点击采集与网络发送独立：未发送点击按场次累积，已尝试发送的批次始终冻结。
export function useRoomLikes(token: string, userId: string, roomId: string) {
  const board = useLeaderboard(token, roomId, userId)
  const connected = ref(false)
  const canCollect = ref(false)
  const likeCount = ref<string | null>(null)
  const pendingCount = ref(0)
  const queuedClicks = ref('0')
  const full = ref(false)
  const notice = ref('')
  const roomStorageKey = `livestream.likes.${userId}.${roomId}`
  let socket: WebSocket | undefined
  let timing: ReturnType<typeof deliveryTiming> | undefined
  let controller: AbortController | undefined
  let probeTimer: ReturnType<typeof setTimeout> | undefined
  let connectTimer: ReturnType<typeof setTimeout> | undefined
  let sendTimer: ReturnType<typeof setTimeout> | undefined
  let stopped = true
  let reconnects = 0
  let sessionId = ''
  let epoch = ''
  let revision = -1n
  let serverState: LikeState['state'] = 'CHECKING'
  let snapshotReady = false
  let storageFailed = false
  let storageReadable = true
  let collected = 0n
  let collectionDue = 0
  let nextFlush = 0
  let pending: Batch[] = []

  function updateNotice() {
    if (storageFailed) notice.value = '浏览器无法保存待发送点赞，请勿关闭或刷新页面。'
    else if (pending.some(batch => Date.now() - requestTime(batch.requestId) >= requestWindowMs)) notice.value = '部分旧请求已超过重试期限，记录已保留，结果仍待核对。'
    else if (serverState === 'BLOCKED') notice.value = '点赞服务暂不可用，点击已暂存在当前标签页。'
    else if (!snapshotReady) notice.value = '正在连接或恢复，点击将暂存，恢复后自动发送。'
    else if (pending.some(batch => batch.attempts >= maxAttempts)) notice.value = '部分点赞结果未确认，已保留原请求，重连后继续核对。'
    else notice.value = ''
  }

  function save() {
    pendingCount.value = pending.length
    queuedClicks.value = (collected + pending.reduce((sum, batch) => sum + BigInt(batch.count), 0n)).toString()
    full.value = collected >= maxCollected
    if (sessionId && storageReadable) {
      try {
        const key = `${roomStorageKey}.${sessionId}`
        if (collected || pending.length) sessionStorage.setItem(key, JSON.stringify({ version: 2, collected: collected.toString(), pending }))
        else sessionStorage.removeItem(key)
        storageFailed = false
      } catch { storageFailed = true }
    }
    updateNotice()
  }

  function loadSession(nextSession: string) {
    sessionId = nextSession
    collected = 0n
    collectionDue = 0
    pending = []
    storageReadable = true
    epoch = ''
    revision = -1n
    likeCount.value = null
    const key = `${roomStorageKey}.${sessionId}`
    try {
      const stored = sessionStorage.getItem(key)
      const saved: unknown = JSON.parse(stored ?? sessionStorage.getItem(roomStorageKey) ?? '[]')
      if (!Array.isArray(saved) && (!isRecord(saved) || saved.version !== 2 || !Array.isArray(saved.pending)
        || typeof saved.collected !== 'string' || !/^(0|[1-9][0-9]{0,18})$/.test(saved.collected))) throw new Error('本地缓存格式无效')
      const batches = Array.isArray(saved) ? saved : isRecord(saved) && Array.isArray(saved.pending) ? saved.pending : []
      if (isRecord(saved) && typeof saved.collected === 'string' && /^(0|[1-9][0-9]{0,18})$/.test(saved.collected)) {
        collected = BigInt(saved.collected)
        if (collected > maxCollected) throw new Error('本地计数超出范围')
      }
      for (const item of batches.slice(0, maxPending)) {
        const batch = restoreBatch(item)
        if (batch?.sessionId === sessionId) pending.push(batch)
      }
      // 兼容旧版按房间保存的数组，先保存新键，再移走该场次的旧记录。
      if (stored === null && pending.length) {
        sessionStorage.setItem(key, JSON.stringify({ version: 2, collected: collected.toString(), pending }))
        const others = batches.filter(item => isRecord(item) && item.sessionId !== sessionId)
        if (others.length) sessionStorage.setItem(roomStorageKey, JSON.stringify(others))
        else sessionStorage.removeItem(roomStorageKey)
      }
    } catch { storageFailed = true; storageReadable = false }
    save()
  }

  function cancelSendTimer() { clearTimeout(sendTimer); sendTimer = undefined }
  function canSend() { return snapshotReady && serverState === 'READY' && socket?.readyState === WebSocket.OPEN }

  function scheduleSend() {
    cancelSendTimer()
    if (!canSend()) return
    let earliest = collected > 0n && pending.length < maxPending ? collectionDue : Infinity
    for (const batch of pending) {
      if (batch.attempts < maxAttempts && Date.now() - requestTime(batch.requestId) < requestWindowMs) earliest = Math.min(earliest, batch.dueAt)
    }
    if (Number.isFinite(earliest)) sendTimer = setTimeout(sendDue, Math.max(0, earliest - Date.now(), nextFlush - Date.now()))
  }

  function sendDue() {
    cancelSendTimer()
    if (!canSend()) return
    const now = Date.now()
    if (collected > 0n && now >= collectionDue && pending.length < maxPending) {
      // 未发送的累计数首次成为请求时才生成 ID；线上 JSON 数量保持安全整数精度。
      const count = Number(collected > BigInt(Number.MAX_SAFE_INTEGER) ? BigInt(Number.MAX_SAFE_INTEGER) : collected)
      pending.push({ requestId: newRequestId(), sessionId, count, attempts: 0, dueAt: now })
      collected -= BigInt(count)
      collectionDue = collected ? now + batchWindowMs : 0
    }
    let sent = 0
    for (const batch of pending) {
      if (batch.attempts >= maxAttempts || batch.dueAt > now || now - requestTime(batch.requestId) >= requestWindowMs) continue
      if (!canSend() || socket!.bufferedAmount > 64 * 1024) break
      // 先记录已尝试状态，再调用 send；ACK 丢失或刷新后仍使用相同 ID 与数量。
      batch.attempts++
      batch.dueAt = now + ackTimeoutMs
      save()
      try {
        socket!.send(JSON.stringify({ version: 1, type: 'LIKE', session_id: batch.sessionId, request_id: batch.requestId, data: { count: batch.count } }))
      } catch { socket?.close(); break }
      if (++sent >= 10) break
    }
    nextFlush = now + 100
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
      snapshotReady = false
      serverState = 'RECOVERING'
      likeCount.value = null
      cancelSendTimer()
      board.stop('本场点赞榜正在恢复')
      updateNotice()
      socket?.close()
      return
    }
    if (message.type === 'ROOM_STATE' && message.data.session_id === sessionId && typeof message.data.projection_epoch === 'string') {
      if (message.data.likes_available !== true) { socket?.close(); return }
      snapshotReady = true
      reconnects = 0
      clearTimeout(connectTimer)
      board.start(sessionId, message.data.projection_epoch)
      // 同场次的新连接开启一轮有限重试，原 ID、数量和 24 小时期限保持不变。
      for (const batch of pending) { if (batch.attempts > 0) { batch.attempts = 0; batch.dueAt = Date.now() } }
      save()
      scheduleSend()
    } else board.receive(message.type, message.data)
    if (message.type === 'ACK' && message.data.status === 'accepted' && typeof message.request_id === 'string') {
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
      }
    }
  }

  function scheduleProbe(delay: number) {
    if (stopped || probeTimer !== undefined) return
    probeTimer = setTimeout(() => { probeTimer = undefined; void probe() }, delay)
  }

  async function probe() {
    if (stopped) return
    const attempt = new AbortController()
    controller = attempt
    try {
      const state = await getLikeState(token, roomId, attempt.signal)
      if (attempt.signal.aborted || stopped) return
      serverState = state.state
      if (state.room_status === 'CLOSED' || state.session_id === null) { stop(); notice.value = '本次直播已结束，旧场次的待确认记录已保留。'; return }
      if (state.session_id !== sessionId) loadSession(state.session_id)
      canCollect.value = true
      updateNotice()
      if (state.state !== 'READY') { scheduleProbe(2000); return }
      const ticket = await getLikeTicket(token, roomId, attempt.signal)
      if (attempt.signal.aborted || stopped) return
      if (ticket.session_id !== sessionId) { scheduleProbe(1000); return }
      const url = new URL(ticket.ws_path, window.location.href)
      url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
      const ws = new WebSocket(url)
      socket = ws
      snapshotReady = false
      timing = deliveryTiming(ws, sessionId)
      connectTimer = setTimeout(() => { if (socket === ws && !snapshotReady) ws.close() }, 8000)
      ws.onopen = () => { if (socket === ws) connected.value = true }
      ws.onmessage = event => { if (socket === ws) receive(event) }
      ws.onerror = () => { if (socket === ws) updateNotice() }
      ws.onclose = () => {
        if (socket !== ws) return
        clearTimeout(connectTimer)
        connected.value = false
        snapshotReady = false
        likeCount.value = null
        socket = undefined
        timing?.stop()
        timing = undefined
        board.stop()
        cancelSendTimer()
        updateNotice()
        scheduleProbe(Math.min(30000, 1000 * 2 ** Math.min(reconnects++, 5)))
      }
    } catch (error) {
      if (attempt.signal.aborted || stopped) return
      if (error instanceof ApiError && error.status === 401) {
        canCollect.value = false
        notice.value = '登录已到期，暂存记录已保留，请使用原账号重新登录。'
        return
      }
      updateNotice()
      scheduleProbe(Math.min(30000, 1000 * 2 ** Math.min(reconnects++, 5)))
    }
  }

  function like(): boolean {
    if (stopped || !canCollect.value || !sessionId || collected >= maxCollected) return false
    if (collected === 0n) collectionDue = Date.now() + batchWindowMs
    collected++
    save()
    scheduleSend()
    return true
  }

  function start() {
    if (!stopped) return
    stopped = false
    reconnects = 0
    void probe()
  }

  function stop() {
    stopped = true
    canCollect.value = false
    snapshotReady = false
    connected.value = false
    controller?.abort()
    clearTimeout(probeTimer)
    probeTimer = undefined
    clearTimeout(connectTimer)
    cancelSendTimer()
    board.stop()
    const ws = socket
    socket = undefined
    timing?.stop()
    timing = undefined
    ws?.close()
  }

  return { connected, canCollect, likeCount, pendingCount, queuedClicks, full, notice, like, start, stop, board }
}
