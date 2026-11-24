import { isRecord, request } from './http'

export type DanmakuMessage = {
  id?: string
  event_id: string
  request_id: string
  room_id: string
  session_id: string
  user_id: string
  username: string
  content: string
  created_at: string
}

export function isDanmaku(value: unknown): value is DanmakuMessage {
  return isRecord(value) && ['event_id', 'request_id', 'room_id', 'session_id', 'user_id', 'username', 'content', 'created_at']
    .every(key => typeof value[key] === 'string')
    && Array.from(String(value.content)).length <= 200
}

export async function getDanmakuHistory(token: string, roomId: string, sessionId: string, cursor: string, signal: AbortSignal) {
  const query = new URLSearchParams({ limit: '50' })
  if (sessionId) query.set('session_id', sessionId)
  if (cursor) query.set('cursor', cursor)
  const result = await request('/api/v1/rooms/' + encodeURIComponent(roomId) + '/danmaku?' + query, { token, signal })
  if (!isRecord(result) || !isRecord(result.data) || !Array.isArray(result.data.items)
    || !result.data.items.every(isDanmaku)
    || !(result.data.next_cursor === null || typeof result.data.next_cursor === 'string')) throw new Error('弹幕历史格式无效')
  return { items: result.data.items as DanmakuMessage[], nextCursor: result.data.next_cursor as string | null }
}
