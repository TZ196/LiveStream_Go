import { ref } from 'vue'
import { getLikeState, getLikeTicket } from './api/likes'
import { ApiError, isRecord } from './api/http'
import { deliveryTiming } from './deliveryTiming'

export type RoomFrame = { type: string; data: Record<string, unknown>; request_id?: string; event_id?: string }
type Listener = (frame: RoomFrame) => void

// 房间连接由页面统一拥有，业务模块只订阅消息和提交帧，不根据点赞状态关闭连接。
export function useRoomConnection(token: string, roomId: string) {
  const connected = ref(false)
  const sessionId = ref('')
  const listeners = new Set<Listener>()
  let socket: WebSocket | undefined
  let stopped = true
  let timer: ReturnType<typeof setTimeout> | undefined
  let timeout: ReturnType<typeof setTimeout> | undefined
  let stable: ReturnType<typeof setTimeout> | undefined
  let controller: AbortController | undefined
  let timing: ReturnType<typeof deliveryTiming> | undefined
  let attempts = 0
  let generation = 0

  function emit(frame: RoomFrame) { for (const listener of listeners) listener(frame) }
  function later(delay: number) {
    if (stopped || timer !== undefined) return
    timer = setTimeout(() => { timer = undefined; void connect() }, delay)
  }
  function reconnect() {
    if (stopped || timer !== undefined) return
    const base = Math.min(30000, 1000 * 2 ** attempts)
    attempts = Math.min(attempts + 1, 5)
    // 每次重试分散到略有不同的时刻，避免同一批断开的客户端同时重新握手。
    later(Math.min(30000, Math.round(base * (0.8 + Math.random() * 0.4))))
  }
  async function connect() {
    if (stopped || socket) return
    const attempt = new AbortController()
    controller = attempt
    try {
      // 此 HTTP 接口只读 MySQL，即使点赞关闭或恢复也能取得房间和场次。
      const context = await getLikeState(token, roomId, attempt.signal)
      if (stopped || attempt.signal.aborted) return
      sessionId.value = context.session_id ?? ''
      emit({ type: 'ROOM_CONTEXT', data: { ...context, session_id: context.session_id } })
      if (context.room_status === 'CLOSED' || !context.session_id) { stop(); return }
      const ticket = await getLikeTicket(token, roomId, attempt.signal)
      if (stopped || attempt.signal.aborted) return
      if (ticket.session_id !== context.session_id) { reconnect(); return }
      const url = new URL(ticket.ws_path, window.location.href)
      url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
      const ws = new WebSocket(url)
      socket = ws
      generation++
      const currentGeneration = generation
      let initialized = false
      timing = deliveryTiming(ws, context.session_id)
      timeout = setTimeout(() => { if (socket === ws) ws.close() }, 8000)
      ws.onopen = () => { if (socket === ws) connected.value = true }
      ws.onmessage = event => {
        if (socket !== ws || stopped) return
        let message: unknown
        try { message = JSON.parse(event.data) } catch { return }
        if (!isRecord(message) || typeof message.type !== 'string' || !isRecord(message.data)) return
        timing?.receive(message, performance.now())
        if (message.type === 'ROOM_STATE' && message.data.session_id === sessionId.value) {
          clearTimeout(timeout)
          // 能收到一次首屏仍可能马上再次因队列满断开，稳定三十秒才重置退避。
          if (!initialized) {
            initialized = true
            stable = setTimeout(() => {
              stable = undefined
              if (!stopped && socket === ws) attempts = 0
            }, 30000)
          }
          // generation 是连接实例，不会随同连接上的能力快照变化。
          message.data.connection_generation = currentGeneration
        }
        emit(message as RoomFrame)
        if (message.type === 'ROOM_CLOSED') stop()
      }
      ws.onclose = () => {
        if (socket !== ws) return
        socket = undefined
        connected.value = false
        clearTimeout(timeout)
        clearTimeout(stable); stable = undefined
        timing?.stop()
        timing = undefined
        emit({ type: 'CONNECTION_CLOSED', data: {} })
        reconnect()
      }
      ws.onerror = () => { if (socket === ws) emit({ type: 'CONNECTION_ERROR', data: {} }) }
    } catch (error) {
      if (stopped || attempt.signal.aborted) return
      if (error instanceof ApiError && error.status === 401) {
        emit({ type: 'AUTH_EXPIRED', data: {} })
        stop()
        return
      }
      reconnect()
    }
  }
  function start() { if (!stopped) return; stopped = false; void connect() }
  function stop() {
    stopped = true
    controller?.abort()
    clearTimeout(timer); timer = undefined
    clearTimeout(timeout)
    clearTimeout(stable); stable = undefined
    const old = socket
    socket = undefined
    old?.close()
    timing?.stop(); timing = undefined
    connected.value = false
  }
  function send(type: string, requestId: string, session: string, data: Record<string, unknown>): boolean {
    if (stopped || !socket || socket.readyState !== WebSocket.OPEN || socket.bufferedAmount > 64 * 1024 || session !== sessionId.value) return false
    try { socket.send(JSON.stringify({ version: 1, type, request_id: requestId, session_id: session, data })); return true } catch { return false }
  }
  function onFrame(listener: Listener) { listeners.add(listener); return () => { listeners.delete(listener) } }
  return { connected, sessionId, start, stop, send, onFrame }
}
export type RoomConnection = ReturnType<typeof useRoomConnection>
