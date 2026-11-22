import assert from 'node:assert/strict'
import test from 'node:test'
import { setImmediate } from 'node:timers/promises'
import { createServer } from 'vite'

// 使用真实的前端模块，只替换时钟、票据与 WebSocket 传输，准确覆盖窗口边界。
test('500 ms 首发、独立超时、同场恢复和旧单击兼容', async t => {
  const vite = await createServer({ server: { middlewareMode: true, hmr: false } })
  t.after(() => vite.close())
  const { useRoomLikes } = await vite.ssrLoadModule('/src/likes.ts')
  const storage = new Map()
  let socket
  let session = '11'
  class Socket {
    static OPEN = 1
    readyState = Socket.OPEN
    frames = []
    constructor() {
      socket = this
      queueMicrotask(() => this.onopen())
    }
    send(data) { this.frames.push(JSON.parse(data)) }
    ack(frame) {
      this.onmessage({ data: JSON.stringify({ type: 'ACK', request_id: frame.request_id, data: {} }) })
    }
    close() {
      this.readyState = 3
      this.onclose()
    }
  }
  const globals = {
    WebSocket: Socket,
    window: { location: { href: 'http://localhost/' } },
    sessionStorage: {
      getItem: key => storage.get(key) ?? null,
      setItem: (key, value) => storage.set(key, value),
      removeItem: key => storage.delete(key),
    },
    fetch: async () => ({
      ok: true,
      json: async () => ({ data: { ws_path: '/ws/rooms/1?ticket=fixture', session_id: session } }),
    }),
  }
  for (const [name, value] of Object.entries(globals)) {
    const original = Object.getOwnPropertyDescriptor(globalThis, name)
    Object.defineProperty(globalThis, name, { value, configurable: true })
    t.after(() => {
      if (original) Object.defineProperty(globalThis, name, original)
      else delete globalThis[name]
    })
  }
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 1800000000000 })
  const clock = t.mock.timers
  const page = useRoomLikes('demo', '1', '1')
  t.after(() => page.stop())
  page.start()
  await setImmediate()
  for (let i = 0; i < 4; i++) assert.equal(page.like(), true)
  clock.tick(499)
  assert.equal(page.like(), true)
  assert.equal(socket.frames.length, 0)
  clock.tick(1)
  assert.deepEqual(socket.frames.map(f => f.data.count), [5])
  const first = socket.frames[0]
  for (let i = 0; i < 80; i++) page.like()
  clock.tick(500)
  assert.deepEqual(socket.frames.map(f => f.data.count), [5, 80])
  assert.notEqual(first.request_id, socket.frames[1].request_id)
  socket.ack(socket.frames[1])
  clock.tick(2499)
  assert.equal(socket.frames.length, 2)
  clock.tick(1)
  assert.deepEqual(socket.frames[2], first)
  socket.close()
  clock.tick(1000)
  await setImmediate()
  clock.tick(0)
  assert.deepEqual(socket.frames, [first])
  socket.ack(first)
  clock.tick(3001)
  assert.equal(socket.frames.length, 1)
  assert.equal(page.pendingCount.value, 0)

  // 离开时尚未首发的点击已写入 sessionStorage。重开场次不发送旧批次。
  page.like()
  page.stop()
  session = '12'
  const reopened = useRoomLikes('demo', '1', '1')
  t.after(() => reopened.stop())
  reopened.start()
  await setImmediate()
  clock.tick(1000)
  assert.equal(socket.frames.length, 0)
  assert.equal(reopened.pendingCount.value, 0)
  assert.equal(reopened.like(), true)
  clock.tick(500)
  assert.equal(socket.frames[0].session_id, '12')
  assert.equal(socket.frames[0].data.count, 1)
  reopened.stop()

  // 旧页面保存的待确认请求没有 count，恢复出来仍是原 ID 的一个赞。
  session = '13'
  storage.set('livestream.likes.1.1', JSON.stringify([
    { requestId: first.request_id, sessionId: session, attempts: 1 },
  ]))
  const legacy = useRoomLikes('demo', '1', '1')
  t.after(() => legacy.stop())
  legacy.start()
  await setImmediate()
  clock.tick(0)
  assert.equal(socket.frames[0].request_id, first.request_id)
  assert.equal(socket.frames[0].data.count, 1)
})
