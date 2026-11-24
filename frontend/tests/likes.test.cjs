const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const ts = require('typescript')

// 使用项目已有 TypeScript 编译器加载真实逻辑，不向生产代码增加测试接口。
require.extensions['.ts'] = (module, filename) => {
  const source = fs.readFileSync(filename, 'utf8')
  const result = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } })
  module._compile(result.outputText, filename)
}
const { useRoomLikes } = require('../src/likes.ts')
const settle = () => new Promise(resolve => setImmediate(resolve))

function environment(t) {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: Date.now() })
  const storage = new Map()
  global.sessionStorage = { getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value), removeItem: key => storage.delete(key) }
  global.window = { location: { href: 'http://127.0.0.1:5173/' } }
  const server = { state: 'RECOVERING', session: '1', count: 0n, ack: true, snapshot: true, seen: new Set(), sent: [], sockets: [] }
  global.fetch = async url => {
    const path = String(url)
    let data
    if (path.endsWith('/likes-state')) data = { state: server.state, session_id: server.session, room_status: 'OPEN' }
    else if (path.endsWith('/ws-ticket')) data = { ws_path: '/ws/rooms/1?ticket=test', session_id: server.session }
    else if (path.includes('like-leaderboard')) data = { room_id: '1', session_id: server.session, projection_epoch: 'epoch', revision: '0', likes_revision: '0', top: [], me: { count: server.count.toString() } }
    else throw new Error('Unexpected request: ' + path)
    return new Response(JSON.stringify({ data, trace_id: 'test' }), { status: 200, headers: { 'Content-Type': 'application/json' } })
  }
  class Socket {
    static OPEN = 1
    readyState = 0
    bufferedAmount = 0
    onopen = null
    onmessage = null
    onclose = null
    onerror = null
    constructor() {
      server.sockets.push(this)
      queueMicrotask(() => {
        if (this.readyState === 3) return
        this.readyState = 1
        this.onopen?.({})
        if (server.snapshot) this.snapshot()
      })
    }
    snapshot() {
      this.frame({ type: 'ROOM_STATE', data: { session_id: server.session, projection_epoch: server.epoch ?? 'epoch', projection_generation: server.generation ?? '1', likes_available: server.state === 'READY', like_count: server.count.toString(), likes_revision: '0' } })
    }
    frame(message) { this.onmessage?.({ data: JSON.stringify(message) }) }
    send(raw) {
      if (this.readyState !== 1) throw new Error('closed')
      const message = JSON.parse(raw)
      if (message.type !== 'LIKE') return
      server.sent.push(message)
      if (!server.seen.has(message.request_id)) { server.seen.add(message.request_id); server.count += BigInt(message.data.count) }
      if (server.ack) queueMicrotask(() => this.frame({ type: 'ACK', request_id: message.request_id, data: { status: 'accepted' } }))
    }
    close() { if (this.readyState === 3) return; this.readyState = 3; queueMicrotask(() => this.onclose?.({})) }
  }
  global.WebSocket = Socket
  const hooks = []
  const create = () => { const hook = useRoomLikes('test-token', '1', '1'); hooks.push(hook); return hook }
  t.after(() => { for (const hook of hooks) hook.stop(); t.mock.timers.reset() })
  return { server, storage, create }
}

test('恢复期间持续采集，刷新后仍可自动补发', async t => {
  const { server, storage, create } = environment(t)
  let likes = create(); likes.start(); await settle()
  assert.equal(likes.canCollect.value, true)
  for (let i = 0; i < 130; i++) assert.equal(likes.like(), true)
  assert.equal(likes.queuedClicks.value, '130')
  assert.equal(server.sent.length, 0)
  assert.equal(JSON.parse(storage.get('livestream.likes.1.1.1')).collected, '130')
  likes.stop()
  likes = create(); likes.start(); await settle()
  assert.equal(likes.queuedClicks.value, '130')
  server.state = 'READY'
  server.sockets.at(-1)?.snapshot()
  t.mock.timers.tick(2000); await settle(); t.mock.timers.tick(0); await settle()
  assert.equal(server.count, 130n)
  assert.equal(likes.queuedClicks.value, '0')
  assert.equal(server.sent.length, 1)
  assert.equal(storage.has('livestream.likes.1.1.1'), false)
})

test('ACK 丢失后保留原批次，新增点击单独补发', async t => {
  const { server, create } = environment(t)
  server.state = 'READY'; server.ack = false
  let likes = create(); likes.start(); await settle()
  for (let i = 0; i < 9; i++) likes.like()
  t.mock.timers.tick(500); await settle()
  const original = server.sent[0]
  assert.equal(original.data.count, 9)
  server.state = 'RECOVERING'; server.sockets.at(-1).close(); await settle()
  for (let i = 0; i < 4; i++) likes.like()
  likes.stop()
  server.state = 'READY'; server.ack = true
  likes = create(); likes.start(); await settle(); t.mock.timers.tick(0); await settle()
  assert.equal(server.sent[1].request_id, original.request_id)
  assert.equal(server.sent[1].data.count, 9)
  assert.equal(server.sent[2].data.count, 4)
  assert.notEqual(server.sent[2].request_id, original.request_id)
  assert.equal(server.count, 13n)
  assert.equal(likes.queuedClicks.value, '0')
})

test('场次改变后保留旧缓存，不将旧点赞计入新场', async t => {
  const { server, storage, create } = environment(t)
  let likes = create(); likes.start(); await settle()
  likes.like(); likes.like(); likes.like(); likes.stop()
  server.session = '2'; server.state = 'READY'
  likes = create(); likes.start(); await settle(); t.mock.timers.tick(0); await settle()
  assert.equal(likes.queuedClicks.value, '0')
  assert.equal(server.sent.length, 0)
  assert.equal(JSON.parse(storage.get('livestream.likes.1.1.1')).collected, '3')
  likes.like(); t.mock.timers.tick(500); await settle()
  assert.equal(server.sent[0].session_id, '2')
  assert.equal(server.sent[0].data.count, 1)
})

test('WebSocket 打开后等待有效首屏，再开始发送', async t => {
  const { server, create } = environment(t)
  server.state = 'READY'; server.snapshot = false
  const likes = create(); likes.start(); await settle()
  assert.equal(likes.connected.value, true)
  likes.like(); t.mock.timers.tick(500); await settle()
  assert.equal(server.sent.length, 0)
  server.sockets.at(-1).snapshot(); t.mock.timers.tick(0); await settle()
  assert.equal(server.count, 1n)
})

test('累计数超过 JS 安全整数时拆批发送，保持总数精确', async t => {
  const { server, storage, create } = environment(t)
  const total = BigInt(Number.MAX_SAFE_INTEGER) + 4n
  storage.set('livestream.likes.1.1.1', JSON.stringify({ version: 2, collected: total.toString(), pending: [] }))
  server.state = 'READY'
  const likes = create(); likes.start(); await settle(); t.mock.timers.tick(0); await settle(); t.mock.timers.tick(500); await settle()
  assert.equal(server.sent.length, 2)
  assert.ok(server.sent.every(batch => Number.isSafeInteger(batch.data.count)))
  assert.equal(server.count, total)
  assert.equal(likes.queuedClicks.value, '0')
})

test('本地缓存无法解析时保留原记录并提示', async t => {
  const { storage, create } = environment(t)
  storage.set('livestream.likes.1.1.1', '{broken-record')
  const likes = create(); likes.start(); await settle()
  likes.like()
  assert.equal(storage.get('livestream.likes.1.1.1'), '{broken-record')
  assert.match(likes.notice.value, /无法保存/)
})

test('点赞恢复不关闭共享连接，仍然保留点击', async t => {
  const { server, create } = environment(t)
  server.state = 'READY'
  const likes = create(); likes.start(); await settle()
  const socket = server.sockets.at(-1)
  server.state = 'RECOVERING'
  socket.frame({ type: 'LIKES_UNAVAILABLE', data: {} })
  assert.equal(socket.readyState, WebSocket.OPEN)
  assert.equal(likes.like(), true)
  t.mock.timers.tick(500); await settle()
  assert.equal(server.sent.length, 0)
  server.state = 'READY'; socket.snapshot()
  t.mock.timers.tick(0); await settle()
  assert.equal(server.count, 1n)
  assert.equal(server.sockets.length, 1)
})

test('同连接切换投影后拒绝旧代次快照和点赞更新', async t => {
  const { server, create } = environment(t)
  server.state = 'READY'
  const likes = create(); likes.start(); await settle()
  const socket = server.sockets.at(-1)
  server.epoch = 'new-epoch'; server.generation = '2'; server.count = 12n
  socket.snapshot()
  socket.frame({ type: 'LIKE_UPDATE', data: { session_id: '1', projection_epoch: 'epoch', likes_revision: '999', like_count: '999' } })
  socket.frame({ type: 'ROOM_STATE', data: { session_id: '1', projection_epoch: 'epoch', projection_generation: '1', likes_available: true, likes_revision: '999', like_count: '999' } })
  assert.equal(likes.likeCount.value, '12')
})

test('同连接能力快照不重置点赞有限重试次数', async t => {
  const { server, create } = environment(t)
  server.state = 'READY'; server.ack = false
  const likes = create(); likes.start(); await settle()
  likes.like(); t.mock.timers.tick(500); await settle()
  for (let i = 0; i < 8; i++) { server.sockets.at(-1).snapshot(); t.mock.timers.tick(3000); await settle() }
  assert.equal(server.sent.length, 5)
  assert.equal(likes.queuedClicks.value, '1')
})

test('服务进程重启后新连接可以采用较小的本地投影代次', async t => {
  const { server, create } = environment(t)
  server.state = 'READY'; server.generation = '9'; server.count = 12n
  const likes = create(); likes.start(); await settle()
  assert.equal(likes.likeCount.value, '12')
  likes.stop()
  server.generation = '1'; server.epoch = 'restart-epoch'; server.count = 15n
  likes.start(); await settle()
  assert.equal(likes.likeCount.value, '15')
})
