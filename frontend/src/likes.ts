import { ref, type Ref } from 'vue'
import { getLikeTicket, newRequestId } from './api/likes'
import { isRecord } from './api/http'

type Pending = { requestId: string; sessionId: string; attempts: number }

// useRoomLikes 管理单个页面的点赞连接和未确认请求；重试不改变原有事件。
export function useRoomLikes(token: string, userId: string, roomId: string) {
  const connected = ref(false)
  const likeCount: Ref<string | null> = ref(null)
  const pendingCount = ref(0)
  const notice = ref('')
  const storageKey = `livestream.likes.${userId}.${roomId}`
  let socket: WebSocket | undefined
  let controller: AbortController | undefined
  let timer: ReturnType<typeof setTimeout> | undefined
  let retryTimer: ReturnType<typeof setInterval> | undefined
  let stopped = true
  let attempts = 0
  let sessionId = ''
  let epoch = ''
  let revision = -1n
  let pending: Pending[] = []
  try {
    const saved: unknown = JSON.parse(sessionStorage.getItem(storageKey) ?? '[]')
    if (Array.isArray(saved)) pending = saved.filter(isRecord).filter((item) => typeof item.requestId === 'string' && typeof item.sessionId === 'string').slice(0,100).map((item) => ({ requestId: item.requestId as string, sessionId: item.sessionId as string, attempts: typeof item.attempts === 'number' ? item.attempts : 0 }))
  } catch { notice.value = '无法恢复此前待确认的点赞。' }
  pendingCount.value = pending.length

  // 保留原请求，使连接中断后能够使用同一个 ID 重新发送。
  function save() {
    pendingCount.value = pending.length
    try { sessionStorage.setItem(storageKey, JSON.stringify(pending)) } catch { notice.value = '浏览器无法保存待确认请求，请勿关闭页面。' }
  }

  // 重连后只发送仍属于当前场次的原请求，不能改绑到新场次。
  function flush() {
    if (!socket || socket.readyState !== WebSocket.OPEN) return
    for (const item of pending) {
      if (item.sessionId === sessionId && item.attempts < 5) {
        socket.send(JSON.stringify({ version: 1, type: 'LIKE', session_id: item.sessionId, request_id: item.requestId, data: {} }))
        item.attempts++
      }
    }
    save()
  }

  // 接受投影版本较新的计数，避免迟到的状态快照覆盖新值。
  function receive(event: MessageEvent<string>) {
    let message: unknown
    try { message = JSON.parse(event.data) } catch { return }
    if (!isRecord(message) || !isRecord(message.data)) return
    if (message.type === 'LIKES_UNAVAILABLE') { likeCount.value = null; notice.value = '实时点赞暂不可用，正在恢复。' }
    if (message.type === 'ACK' && typeof message.request_id === 'string') {
      pending = pending.filter((item) => item.requestId !== message.request_id)
      save()
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

  // 申请新票据并连接，失败时有界退避重试。
  async function connect() {
    if (stopped) return
    controller = new AbortController()
    try {
      const ticket = await getLikeTicket(token, roomId, controller.signal)
      if (stopped) return
      if (sessionId && sessionId !== ticket.session_id) { likeCount.value = null; revision = -1n; epoch = '' }
      sessionId = ticket.session_id
      const url = new URL(ticket.ws_path, window.location.href)
      url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
      socket = new WebSocket(url)
      socket.onopen = () => { connected.value = true; attempts = 0; notice.value = ''; flush(); retryTimer = setInterval(flush, 3000) }
      socket.onmessage = receive
      socket.onerror = () => { notice.value = '连接暂不可用，正在尝试恢复。' }
      socket.onclose = () => { connected.value = false; clearInterval(retryTimer); retryTimer = undefined; schedule() }
    } catch {
      if (!stopped) { notice.value = '连接暂不可用，正在尝试恢复。'; schedule() }
    }
  }

  // 限制重连次数，避免持续离线时无限请求。
  function schedule() {
    if (stopped || timer) return
    attempts++
    if (attempts > 10) { notice.value = '连接已断开，请刷新页面重试。'; return }
    timer = setTimeout(() => { timer = undefined; void connect() }, Math.min(30000, 1000 * 2 ** (attempts - 1)))
  }

  // 一个新点击只创建一个请求，不能合并之前或之后的点击。
  function like() {
    if (!connected.value || !sessionId || pending.length >= 100) return
    const item = { requestId: newRequestId(), sessionId, attempts: 0 }
    pending.push(item)
    save()
    flush()
  }

  // start 在房间开放时建立连接。
  function start() { if (!stopped) return; stopped = false; void connect() }

  // stop 在离开页面或直播结束时终止连接和计时器。
  function stop() {
    stopped = true
    connected.value = false
    controller?.abort()
    if (timer) clearTimeout(timer)
    clearInterval(retryTimer)
    retryTimer = undefined
    timer = undefined
    socket?.close()
    socket = undefined
  }

  return { connected, likeCount, pendingCount, notice, like, start, stop }
}
