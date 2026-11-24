import { isRecord, request } from './http'

export type LikeTicket = { ws_path: string; session_id: string }

export type LikeState = {
  state: 'CHECKING' | 'READY' | 'RECOVERING' | 'BLOCKED'
  session_id: string | null
  room_status: 'OPEN' | 'CLOSED'
}

// 此接口只依赖普通 HTTP 和 MySQL，恢复期间也可取得当前场次。
export async function getLikeState(token: string, roomId: string, signal: AbortSignal): Promise<LikeState> {
  const result = await request(`/api/v1/rooms/${encodeURIComponent(roomId)}/likes-state`, { token, signal })
  if (!isRecord(result) || !isRecord(result.data)) throw new Error('点赞状态无效')
  const data = result.data
  if (!['CHECKING', 'READY', 'RECOVERING', 'BLOCKED'].includes(String(data.state))
    || !['OPEN', 'CLOSED'].includes(String(data.room_status))
    || (data.room_status === 'OPEN' && (typeof data.session_id !== 'string' || !/^[1-9][0-9]*$/.test(data.session_id)))
    || (data.room_status === 'CLOSED' && data.session_id !== null)) throw new Error('点赞状态无效')
  return data as LikeState
}

// 获取绑定用户和场次的一次性 WebSocket 票据。
export async function getLikeTicket(token: string, roomId: string, signal: AbortSignal): Promise<LikeTicket> {
  const result = await request(`/api/v1/rooms/${encodeURIComponent(roomId)}/ws-ticket`, { method: 'POST', body: {}, token, signal })
  if (!isRecord(result) || !isRecord(result.data) || typeof result.data.ws_path !== 'string'
    || !result.data.ws_path.startsWith(`/ws/rooms/${roomId}?ticket=`)
    || typeof result.data.session_id !== 'string' || !/^[1-9][0-9]*$/.test(result.data.session_id)) {
    throw new Error('房间连接信息无效')
  }
  return { ws_path: result.data.ws_path, session_id: result.data.session_id }
}

// 每个新批次产生一个 UUIDv7；重试始终使用保存在待确认列表中的原值。
export function newRequestId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16))
  let timestamp = BigInt(Date.now())
  for (let index = 5; index >= 0; index--) {
    bytes[index] = Number(timestamp & 255n)
    timestamp >>= 8n
  }
  bytes[6] = (bytes[6]! & 0x0f) | 0x70
  bytes[8] = (bytes[8]! & 0x3f) | 0x80
  const hex = Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('')
  return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`
}
