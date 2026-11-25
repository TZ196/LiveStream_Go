import { ref } from 'vue'
import { useLeaderboard } from './leaderboard'
import { newRequestId } from './api/likes'
import { isRecord } from './api/http'
import { useRoomConnection, type RoomConnection, type RoomFrame } from './realtime'

const batchWindowMs = 500
const ackTimeoutMs = 3000
const maxBatch = BigInt(Number.MAX_SAFE_INTEGER)
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
export function useRoomLikes(token: string, userId: string, roomId: string, shared?: RoomConnection) {
  const channel = shared ?? useRoomConnection(token, roomId)
  const connected = channel.connected
  const canCollect = ref(false)
  const likeCount = ref<string | null>(null)
  const pendingCount = ref(0)
  const queuedClicks = ref('0')
  const full = ref(false)
  const notice = ref('')
  const board = useLeaderboard(token, roomId, userId)
  const roomStorageKey = 'livestream.likes.' + userId + '.' + roomId
  let sessionId = ''
  let serverState = 'CHECKING'
  let snapshotReady = false
  let epoch = ''
  let revision = 0n
  let projectionGeneration = -1n
  let projectionConnection = -1
  let collected = 0n
  let collectionDue = 0
  let pending: Batch[] = []
  let storageFailed = false
  let storageReadable = true
  let stopped = true
  let sendTimer: ReturnType<typeof setTimeout> | undefined
  let unsubscribe: (() => void) | undefined
  let connectionGeneration = 0

  function updateNotice() {
    if (storageFailed) notice.value = '浏览器无法保存待发送点赞，请勿关闭或刷新页面。'
    else if (pending.some(batch => Date.now() - requestTime(batch.requestId) >= requestWindowMs)) notice.value = '部分旧请求已超过重试期限，记录已保留，结果仍待核对。'
    else if (serverState !== 'READY' || !snapshotReady) notice.value = '点赞暂不可用，点击会暂存在当前页面，恢复后补发。'
    else if (!connected.value) notice.value = '连接已断开，点赞已暂存，正在等待重连。'
    else if (pending.some(batch => batch.attempts >= maxAttempts)) notice.value = '部分点赞结果未确认，已保留原请求，重连后继续核对。'
    else notice.value = ''
  }
  function save() {
    pendingCount.value = pending.length
    queuedClicks.value = (collected + pending.reduce((sum, batch) => sum + BigInt(batch.count), 0n)).toString()
    full.value = collected >= maxCollected
    if (sessionId && storageReadable) {
      const key = roomStorageKey + '.' + sessionId
      try {
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
    projectionGeneration = -1n
    projectionConnection = -1
    connectionGeneration = -1
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
  function canSend() { return !stopped && snapshotReady && serverState === 'READY' && connected.value }
  function scheduleSend() {
    cancelSendTimer()
    if (!canSend()) return
    let earliest = collected > 0n && pending.length < maxPending ? collectionDue : Infinity
    for (const batch of pending) {
      if (batch.attempts < maxAttempts && Date.now() - requestTime(batch.requestId) < requestWindowMs) earliest = Math.min(earliest, batch.dueAt)
    }
    if (Number.isFinite(earliest)) sendTimer = setTimeout(sendDue, Math.max(0, earliest - Date.now()))
  }
  function sendDue() {
    sendTimer = undefined
    if (!canSend()) return
    const now = Date.now()
    if (collected > 0n && now >= collectionDue && pending.length < maxPending) {
      const count = Number(collected > maxBatch ? maxBatch : collected)
      collected -= BigInt(count)
      pending.push({ requestId: newRequestId(), sessionId, count, attempts: 0, dueAt: now })
      collectionDue = now + batchWindowMs
      save()
    }
    let sent = 0
    for (const batch of pending) {
      if (sent >= 10) { batch.dueAt = Math.max(batch.dueAt, now + 100); continue }
      if (batch.attempts >= maxAttempts || batch.dueAt > now || now - requestTime(batch.requestId) >= requestWindowMs) continue
      // 写入浏览器前冻结，异常重连仍使用相同 ID 和数量。
      batch.attempts++
      batch.dueAt = now + ackTimeoutMs
      save()
      if (!channel.send('LIKE', batch.requestId, batch.sessionId, { count: batch.count })) {
        batch.attempts--
        batch.dueAt = now + 100
        save()
        break
      }
      sent++
    }
    scheduleSend()
  }
  function receive(message: RoomFrame) {
    const data = message.data
    if (message.type === 'ROOM_CONTEXT') {
      if (data.room_status === 'CLOSED' || data.session_id === null) { canCollect.value = false; snapshotReady = false; cancelSendTimer(); return }
      if (typeof data.session_id === 'string' && data.session_id !== sessionId) loadSession(data.session_id)
      if (typeof data.state === 'string') serverState = data.state
      canCollect.value = true
      updateNotice()
      return
    }
    if (message.type === 'LIKES_UNAVAILABLE' || message.type === 'CONNECTION_CLOSED') {
      if (typeof data.projection_generation === 'string' && /^\d+$/.test(data.projection_generation)
        && BigInt(data.projection_generation) < projectionGeneration) return
      if (message.type === 'CONNECTION_CLOSED') { projectionGeneration = -1n; epoch = ''; revision = 0n }
      snapshotReady = false
      serverState = 'RECOVERING'
      likeCount.value = null
      cancelSendTimer()
      board.stop('本场点赞榜正在恢复')
      updateNotice()
      return
    }
    if (message.type === 'AUTH_EXPIRED') { canCollect.value = false; notice.value = '登录已到期，暂存记录已保留，请重新登录。'; return }
    if (message.type === 'ROOM_STATE' && data.session_id === sessionId) {
      if (isRecord(data.delivery_modes) && data.delivery_modes.LIKE === null) {
        canCollect.value = false
        notice.value = '点赞功能未启用。'
      }
      const incomingConnection = Number(data.connection_generation ?? 0)
      if (incomingConnection !== projectionConnection) {
        projectionGeneration = -1n
        projectionConnection = incomingConnection
      }
      const nextGeneration = typeof data.projection_generation === 'string' && /^\d+$/.test(data.projection_generation) ? BigInt(data.projection_generation) : 0n
      if (nextGeneration < projectionGeneration) return
      projectionGeneration = nextGeneration
      const refreshBoard = !snapshotReady || epoch !== data.projection_epoch
      if (data.likes_available !== true || typeof data.projection_epoch !== 'string') {
        snapshotReady = false
        serverState = typeof data.likes_state === 'string' ? data.likes_state : 'RECOVERING'
        likeCount.value = null
        cancelSendTimer()
        board.stop('本场点赞榜正在恢复')
        updateNotice()
        return
      }
      snapshotReady = true
      serverState = 'READY'
      const generation = Number(data.connection_generation ?? 0)
      if (generation !== connectionGeneration) {
        connectionGeneration = generation
        for (const batch of pending) if (batch.attempts > 0) { batch.attempts = 0; batch.dueAt = Date.now() }
      }
      if (refreshBoard) board.start(sessionId, data.projection_epoch)
      save()
      scheduleSend()
    } else board.receive(message.type, data)
    if (message.type === 'ACK' && data.status === 'accepted' && typeof message.request_id === 'string') {
      pending = pending.filter(batch => batch.requestId !== message.request_id)
      save()
      scheduleSend()
    }
    if ((message.type === 'LIKE_UPDATE' || message.type === 'ROOM_STATE') && data.session_id === sessionId
      && typeof data.like_count === 'string' && /^\d+$/.test(data.like_count)
      && typeof data.likes_revision === 'string' && /^\d+$/.test(data.likes_revision)
      && typeof data.projection_epoch === 'string') {
      const next = BigInt(data.likes_revision)
      if (message.type === 'ROOM_STATE' || data.projection_epoch === epoch) {
        if (message.type === 'ROOM_STATE' || next >= revision) { epoch = data.projection_epoch; revision = next; likeCount.value = data.like_count }
      }
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
    unsubscribe = channel.onFrame(receive)
    if (!shared) channel.start()
  }
  function stop() {
    stopped = true
    unsubscribe?.(); unsubscribe = undefined
    cancelSendTimer()
    board.stop()
    save()
    if (!shared) channel.stop()
  }
  return { connected, canCollect, likeCount, pendingCount, queuedClicks, full, notice, like, start, stop, board }
}
