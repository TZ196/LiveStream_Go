import assert from 'node:assert/strict'
import test from 'node:test'
import { setImmediate } from 'node:timers/promises'
import { createServer } from 'vite'

test('榜单首屏、个人版本、差量缺失和切换场次', async t => {
  const vite = await createServer({ server: { middlewareMode: true, ws: false, hmr: false } })
  t.after(() => vite.close())
  const { useLeaderboard } = await vite.ssrLoadModule('/src/leaderboard.ts')
  const reads = []
  t.mock.method(globalThis, 'fetch', (url, options) => new Promise(resolve => {
    reads.push({ url, signal: options.signal, finish: data => resolve(Response.json({ data })) })
  }))
  const board = useLeaderboard('demo', '1', '2')
  t.after(() => board.stop())
  const room = { room_id: '1', session_id: '11', projection_epoch: 'test' }
  const a = { rank: 1, user_id: '1', username: 'alice', count: '9' }
  const b = { rank: 2, user_id: '2', username: 'bob', count: '5' }
  const snapshot = { ...room, revision: '14', likes_revision: '14', me: { count: '5', rank: 2 }, top: [a, b] }

  board.start('11', 'test')
  assert.equal(reads[0].url, '/api/v1/rooms/1/sessions/11/like-leaderboard')
  reads[0].finish(snapshot)
  await setImmediate()
  assert.equal(board.onBoard.value, true)
  assert.equal(board.mine.value, '5')

  // 榜外的本人更新可以先于公共增量到达，较慢的快照不能还原本人旧值。
  board.receive('MY_LIKES_UPDATE', { ...room, likes_revision: '20', count: '6' })
  const other = { rank: 2, user_id: '3', username: 'carol', count: '8' }
  board.receive('LIKE_BOARD_DELTA', { ...room, revision: '18', base_revision: '14', size: 2, changes: [other] })
  assert.deepEqual(board.rows.value, [a, other])
  assert.equal(board.onBoard.value, false)
  board.receive('MY_LIKES_UPDATE', { ...room, likes_revision: '16', count: '5' })
  assert.equal(board.mine.value, '6')

  // 丢失基础版本或收到超大差量的刷新通知时，同一时刻只发一次 HTTP。
  board.receive('LIKE_BOARD_DELTA', { ...room, revision: '22', base_revision: '21', size: 2, changes: [] })
  board.receive('LIKE_BOARD_REFRESH', { ...room, revision: '22' })
  assert.equal(reads.length, 2)
  board.receive('MY_LIKES_UPDATE', { ...room, likes_revision: '25', count: '7' })
  reads[1].finish({ ...snapshot, top: [a, other], me: { count: '6' }, revision: '22', likes_revision: '23' })
  await setImmediate()
  assert.equal(board.mine.value, '7')
  assert.equal(board.onBoard.value, false)
  board.receive('LIKE_BOARD_DELTA', { ...room, revision: '21', base_revision: '18', size: 2, changes: [b] })
  assert.deepEqual(board.rows.value, [a, other])

  board.receive('LIKE_BOARD_REFRESH', { ...room, revision: '30' })
  assert.equal(reads.length, 3)
  board.start('12', 'test')
  assert.equal(reads[2].signal.aborted, true)
  assert.equal(reads.length, 4)
  reads[3].finish({ ...room, session_id: '12', top: [], me: { count: '0' }, revision: '0', likes_revision: '0' })
  reads[2].finish(snapshot)
  await setImmediate()
  board.receive('MY_LIKES_UPDATE', { ...room, likes_revision: '31', count: '8' })
  board.receive('LIKE_BOARD_REFRESH', { ...room, session_id: '12', projection_epoch: 'old', revision: '55' })
  assert.deepEqual(board.rows.value, [])
  assert.equal(board.mine.value, '0')
  assert.equal(reads.length, 4)
})
