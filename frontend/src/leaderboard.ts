import { computed, ref } from 'vue'
import { isRecord, request } from './api/http'

export type BoardRow = { rank: number; user_id: string; username: string; count: string }

function unsigned(value: unknown): value is string {
  return typeof value === 'string' && /^(0|[1-9]\d*)$/.test(value)
}

function isRow(value: unknown): value is BoardRow {
  return isRecord(value) && typeof value.rank === 'number' && Number.isInteger(value.rank)
    && value.rank >= 1 && value.rank <= 100 && unsigned(value.user_id) && value.user_id !== '0'
    && typeof value.username === 'string' && unsigned(value.count) && value.count !== '0'
}

function validTop(rows: unknown): rows is BoardRow[] {
  if (!Array.isArray(rows) || rows.length > 100) return false
  const users = new Set<string>()
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]
    if (!isRow(row) || row.rank !== i + 1 || users.has(row.user_id)) return false
    users.add(row.user_id)
  }
  return true
}

// HTTP 负责首屏与补缺，WS 推送公共名次变化和只属于本人的累计数。
export function useLeaderboard(token: string, roomId: string, userId: string) {
  const rows = ref<BoardRow[] | null>(null)
  const mine = ref<string | null>(null)
  const status = ref('等待连接')
  const onBoard = computed(() => rows.value?.some(row => row.user_id === userId) ?? false)
  let session = ''
  let epoch = ''
  let version = -1n
  let personalVersion = -1n
  let wanted = -1n
  let inflight: AbortController | undefined
  let timer: ReturnType<typeof setTimeout> | undefined

  function stop(message = '等待连接') {
    inflight?.abort()
    inflight = undefined
    clearTimeout(timer)
    timer = undefined
    session = ''
    epoch = ''
    rows.value = null
    mine.value = null
    version = personalVersion = wanted = -1n
    status.value = message
  }

  function updateMine(revision: string, count: string) {
    const next = BigInt(revision)
    if (next >= personalVersion) {
      personalVersion = next
      mine.value = count
    }
  }

  async function refresh() {
    if (!session || inflight || timer !== undefined) return
    const attempt = new AbortController()
    inflight = attempt
    let retry = false
    try {
      const response = await request(`/api/v1/rooms/${roomId}/sessions/${session}/like-leaderboard`, {
        token, signal: attempt.signal,
      })
      if (attempt.signal.aborted) return
      const data = isRecord(response) ? response.data : null
      if (!isRecord(data) || data.room_id !== roomId || data.session_id !== session || data.projection_epoch !== epoch
        || !unsigned(data.revision) || !unsigned(data.likes_revision) || !validTop(data.top)
        || !isRecord(data.me) || !unsigned(data.me.count)) throw new Error('榜单快照无效')
      const next = BigInt(data.revision)
      if (next >= version) {
        version = next
        rows.value = data.top
      }
      updateMine(data.likes_revision, data.me.count)
      status.value = ''
      retry = version < wanted
    } catch {
      if (attempt.signal.aborted) return
      status.value = '本场点赞榜暂不可用，正在重试。'
      retry = true
    } finally {
      if (inflight === attempt) {
        inflight = undefined
        if (retry) timer = setTimeout(() => { timer = undefined; void refresh() }, 1500)
      }
    }
  }

  function start(id: string, projection: string) {
    stop('正在加载榜单…')
    session = id
    epoch = projection
    void refresh()
  }

  function receive(type: unknown, data: Record<string, unknown>) {
    if (!session || data.session_id !== session || data.projection_epoch !== epoch) return
    if (type === 'MY_LIKES_UPDATE' && unsigned(data.likes_revision) && unsigned(data.count)) {
      updateMine(data.likes_revision, data.count)
      return
    }
    if ((type !== 'LIKE_BOARD_REFRESH' && type !== 'LIKE_BOARD_DELTA') || !unsigned(data.revision)) return
    const next = BigInt(data.revision)
    if (next <= version) return
    if (next > wanted) wanted = next
    if (type === 'LIKE_BOARD_DELTA' && rows.value && data.base_revision === version.toString()
      && typeof data.size === 'number' && Number.isInteger(data.size) && data.size >= 0 && data.size <= 100
      && Array.isArray(data.changes) && data.changes.every(isRow)) {
      const patched = rows.value.slice(0, data.size)
      for (const row of data.changes) patched[row.rank - 1] = row
      if (patched.length === data.size && validTop(patched)) {
        rows.value = patched
        version = next
        return
      }
    }
    void refresh()
  }

  return { rows, mine, onBoard, status, start, receive, stop }
}
