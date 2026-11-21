import { isRecord, request } from './http'

export type Room = {
  id: string
  owner_id: string
  title: string
  status: 'OPEN' | 'CLOSED'
  created_at: string
  closed_at: string | null
}

export type RoomPage = { items: Room[]; next_cursor: string | null }
export type RoomFilter = { status: 'OPEN' | 'CLOSED'; owner: '' | 'me' }

function isRoom(value: unknown): value is Room {
  if (!isRecord(value)) return false
  return typeof value.id === 'string' && /^[1-9][0-9]*$/.test(value.id)
    && typeof value.owner_id === 'string' && /^[1-9][0-9]*$/.test(value.owner_id)
    && typeof value.title === 'string'
    && typeof value.created_at === 'string' && !Number.isNaN(Date.parse(value.created_at))
    && ((value.status === 'OPEN' && value.closed_at === null)
      || (value.status === 'CLOSED' && typeof value.closed_at === 'string' && !Number.isNaN(Date.parse(value.closed_at))))
}

function readData(response: unknown): unknown {
  if (!isRecord(response) || typeof response.trace_id !== 'string' || !('data' in response)) {
    throw new Error('房间接口响应格式不正确，请刷新确认结果。')
  }
  return response.data
}

function readRoom(response: unknown): Room {
  const data = readData(response)
  if (!isRoom(data)) throw new Error('房间信息格式不正确，请刷新确认结果。')
  return data
}

export async function listRooms(token: string, filter: RoomFilter, cursor: string | null, signal: AbortSignal): Promise<RoomPage> {
  const query = new URLSearchParams({ status: filter.status, limit: '20' })
  if (filter.owner) query.set('owner', filter.owner)
  if (cursor) query.set('cursor', cursor)
  const response = await request(`/api/v1/rooms?${query}`, { token, signal })
  const data = readData(response)
  if (!isRecord(data) || !Array.isArray(data.items) || !data.items.every(isRoom)
    || (data.next_cursor !== null && (typeof data.next_cursor !== 'string' || data.next_cursor.length === 0))) {
    throw new Error('房间列表格式不正确，请稍后重试。')
  }
  return { items: data.items, next_cursor: data.next_cursor }
}

export async function createRoom(token: string, title: string, signal: AbortSignal): Promise<Room> {
  return readRoom(await request('/api/v1/rooms', { method: 'POST', body: { title }, token, signal }))
}

export async function getRoom(token: string, id: string, signal: AbortSignal): Promise<Room> {
  return readRoom(await request(`/api/v1/rooms/${encodeURIComponent(id)}`, { token, signal }))
}

export async function closeRoom(token: string, id: string, signal: AbortSignal): Promise<Room> {
  return readRoom(await request(`/api/v1/rooms/${encodeURIComponent(id)}/close`, { method: 'POST', body: {}, token, signal }))
}
