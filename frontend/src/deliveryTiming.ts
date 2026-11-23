import { isRecord } from './api/http'

// 每次握手建立独立校时样本，只在服务端选中的连接上工作。浏览器全程使用单调时间。
export function deliveryTiming(ws: WebSocket, session: string) {
  let timer: ReturnType<typeof setInterval> | undefined
  let sequence = 0
  let pingAt = 0
  let nonce = ''
  let anchor: { local: number; server: number; error: number } | undefined

  function send(type: string, data: object) {
    if (ws.readyState !== WebSocket.OPEN || ws.bufferedAmount > 8192) return
    try {
      ws.send(JSON.stringify({ version: 1, type, session_id: session, data }))
    } catch {
      // 关闭中的连接可丢弃采样，业务消息仍交给页面处理。
    }
  }

  function ping() {
    nonce = String(++sequence)
    pingAt = performance.now()
    send('PING', { nonce })
  }

  function receive(message: Record<string, unknown>, at: number) {
    const data = message.data
    if (!isRecord(data)) return
    if (message.type === 'ROOM_STATE' && data.session_id === session && data.delivery_probe === true && timer === undefined) {
      ping()
      timer = setInterval(ping, 30000)
      return
    }
    if (timer === undefined) return
    if (message.type === 'PONG' && nonce && data.nonce === nonce && typeof data.server_time_ms === 'number'
      && Number.isSafeInteger(data.server_time_ms)) {
      const halfTrip = (at - pingAt) / 2
      anchor = { local: pingAt + halfTrip, server: data.server_time_ms, error: halfTrip + 1 }
      nonce = ''
      return
    }
    const sent = message.server_sent_at_ms
    if (message.type !== 'LIKE_UPDATE' || data.session_id !== session || typeof sent !== 'number' || !Number.isSafeInteger(sent)) return
    const receipt: { server_sent_at_ms: number; received_at_ms?: number; uncertainty_ms?: number } = { server_sent_at_ms: sent }
    if (anchor && at >= anchor.local && at - anchor.local <= 60000) {
      const elapsed = at - anchor.local
      receipt.received_at_ms = Math.round(anchor.server + elapsed)
      // 误差预算包含校时半往返、毫秒取整和两次校时间按 200 ppm 估计的时钟漂移。
      receipt.uncertainty_ms = Math.ceil(anchor.error + elapsed * 0.0002)
    }
    send('DELIVERY_RECEIPT', receipt)
  }

  function stop() {
    clearInterval(timer)
    timer = undefined
    anchor = undefined
    nonce = ''
  }

  return { receive, stop }
}
