const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const ts = require('typescript')
require.extensions['.ts'] = (module, filename) => {
  const result = ts.transpileModule(fs.readFileSync(filename, 'utf8'), { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } })
  module._compile(result.outputText, filename)
}
const { useRoomConnection } = require('../src/realtime.ts')
const { useRoomDanmaku } = require('../src/danmaku.ts')
const settle = () => new Promise(resolve => setImmediate(resolve))

function environment(t) {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: Date.now() })
  const storage = new Map()
  global.sessionStorage = { getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value), removeItem: key => storage.delete(key) }
  global.window = { location: { href: 'http://127.0.0.1:5173/' } }
  const server = { session: '1', likeState: 'BLOCKED', available: true, ack: true, broadcast: true, blocked: false, sent: [], seen: new Set(), sockets: [], history: [] }
  global.fetch = async url => {
    const path = String(url)
    let data
    if (path.endsWith('/likes-state')) data = { state: server.likeState, session_id: server.session, room_status: 'OPEN' }
    else if (path.endsWith('/ws-ticket')) data = { ws_path: '/ws/rooms/1?ticket=test', session_id: server.session }
    else if (path.includes('/danmaku?')) data = { items: server.history, next_cursor: null, consistency: 'eventual' }
    else throw new Error('Unexpected request: ' + path)
    return new Response(JSON.stringify({ data }), { status: 200, headers: { 'Content-Type': 'application/json' } })
  }
  class Socket {
    static OPEN = 1
    readyState = 0
    bufferedAmount = 0
    constructor() {
      server.sockets.push(this)
      queueMicrotask(() => { if (this.readyState === 3) return; this.readyState = 1; this.onopen?.({}); this.snapshot() })
    }
    snapshot() {
      this.frame({ type: 'ROOM_STATE', data: { session_id: server.session, likes_available: false, projection_epoch: null, danmaku_available: server.available, history_available: true } })
    }
    frame(value) { this.onmessage?.({ data: JSON.stringify(value) }) }
    send(raw) {
      const value = JSON.parse(raw)
      if (value.type !== 'DANMAKU') return
      server.sent.push(value)
      if (server.blocked) {
        queueMicrotask(() => this.frame({ type: 'ERROR', request_id: value.request_id, data: { code: 'CONTENT_BLOCKED', message: '内容未通过审核', outcome: 'rejected', retryable: false } }))
        return
      }
      server.seen.add(value.request_id)
      if (server.ack) queueMicrotask(() => this.frame({ type: 'ACK', request_id: value.request_id, event_id: 'event-' + value.request_id, data: { status: 'accepted' } }))
      if (server.broadcast) queueMicrotask(() => this.frame({
        type: 'DANMAKU_BROADCAST', request_id: value.request_id, event_id: 'event-' + value.request_id,
        data: { room_id: '1', session_id: value.session_id, user_id: '1', username: 'alice', content: value.data.content, created_at: new Date().toISOString() },
      }))
    }
    close() { if (this.readyState === 3) return; this.readyState = 3; queueMicrotask(() => this.onclose?.({})) }
  }
  global.WebSocket = Socket
  const hooks = []
  function create() {
    const channel = useRoomConnection('test-token', '1')
    const danmaku = useRoomDanmaku('test-token', '1', '1', channel)
    hooks.push({ channel, danmaku })
    danmaku.start(); channel.start()
    return { channel, danmaku }
  }
  t.after(() => { for (const hook of hooks) { hook.danmaku.stop(); hook.channel.stop() }; t.mock.timers.reset() })
  return { server, storage, create }
}

test('点赞 BLOCKED 仍能建立共享连接并发送弹幕', async t => {
  const { server, create } = environment(t)
  const { channel, danmaku } = create(); await settle()
  assert.equal(channel.connected.value, true)
  assert.equal(danmaku.available.value, true)
  danmaku.draft.value = '大家好😀'
  assert.equal(danmaku.sendDraft(), true)
  t.mock.timers.tick(0); await settle()
  assert.equal(server.sent.length, 1)
  assert.equal(danmaku.queue.value.length, 0)
  assert.equal(danmaku.messages.value.length, 1)
})

test('审核拒绝保留内容，用户修改后创建新请求', async t => {
  const { server, create } = environment(t)
  server.blocked = true
  const { danmaku } = create(); await settle()
  danmaku.draft.value = '违规测试内容'
  danmaku.sendDraft(); t.mock.timers.tick(0); await settle()
  const rejected = danmaku.queue.value[0]
  assert.equal(rejected.status, 'rejected')
  t.mock.timers.tick(10000); await settle()
  assert.equal(server.sent.length, 1)
  danmaku.editRejected(rejected.requestId)
  server.blocked = false
  danmaku.draft.value = '修改后的正常内容'
  danmaku.sendDraft(); t.mock.timers.tick(0); await settle()
  assert.notEqual(server.sent[1].request_id, rejected.requestId)
})

test('ACK 丢失后原请求重试，后续弹幕不会自动越过', async t => {
  const { server, create } = environment(t)
  server.ack = false
  const { danmaku } = create(); await settle()
  danmaku.draft.value = '第一条'; danmaku.sendDraft()
  danmaku.draft.value = '第二条'; danmaku.sendDraft()
  t.mock.timers.tick(0); await settle()
  const first = server.sent[0]
  t.mock.timers.tick(3000); await settle()
  assert.equal(server.sent[1].request_id, first.request_id)
  assert.ok(server.sent.every(value => value.data.content === '第一条'))
  assert.equal(danmaku.messages.value.length, 1)
  server.ack = true
  t.mock.timers.tick(3000); await settle(); t.mock.timers.tick(0); await settle()
  assert.equal(server.sent.at(-1).data.content, '第二条')
  assert.equal(server.seen.size, 2)
})

test('历史持久化证据可核对未确认请求，重复历史不重复展示', async t => {
  const { server, create } = environment(t)
  server.ack = false
  const { danmaku } = create(); await settle()
  danmaku.draft.value = '历史核对'; danmaku.sendDraft()
  t.mock.timers.tick(0); await settle()
  const original = server.sent[0]
  server.history = [{ id: '1', event_id: 'event-' + original.request_id, request_id: original.request_id, room_id: '1', session_id: '1', user_id: '1', username: 'alice', content: original.data.content, created_at: new Date().toISOString() }]
  await danmaku.loadHistory()
  assert.equal(danmaku.queue.value.length, 0)
  assert.equal(danmaku.messages.value.length, 1)
  await danmaku.loadHistory()
  assert.equal(danmaku.messages.value.length, 1)
})

test('刷新恢复原 ID，换场不会发送旧场请求', async t => {
  const { server, storage, create } = environment(t)
  server.ack = false
  let hook = create(); await settle()
  hook.danmaku.draft.value = '保留原请求'; hook.danmaku.sendDraft()
  t.mock.timers.tick(0); await settle()
  const original = server.sent[0]
  hook.danmaku.stop(); hook.channel.stop()
  hook = create(); await settle()
  t.mock.timers.tick(3000); await settle()
  assert.equal(server.sent.at(-1).request_id, original.request_id)
  hook.danmaku.stop(); hook.channel.stop()
  assert.equal(JSON.parse(storage.get('livestream.danmaku.1.1.1'))[0].requestId, original.request_id)
  server.session = '2'; server.ack = true
  hook = create(); await settle()
  const before = server.sent.length
  t.mock.timers.tick(3000); await settle()
  assert.equal(server.sent.length, before)
  assert.equal(hook.danmaku.queue.value.length, 0)
})

test('按 Unicode 码点校验长度，能力更新不关闭连接', async t => {
  const { server, create } = environment(t)
  const { channel, danmaku } = create(); await settle()
  danmaku.draft.value = '😀'.repeat(200)
  assert.equal(danmaku.length.value, 200)
  assert.equal(danmaku.sendDraft(), true)
  t.mock.timers.tick(0); await settle()
  server.available = false; server.sockets.at(-1).snapshot()
  assert.equal(channel.connected.value, true)
  danmaku.draft.value = '保留草稿'
  assert.equal(danmaku.sendDraft(), false)
  assert.equal(danmaku.draft.value, '保留草稿')
})

test('重连时补查历史，即使列表已有消息也不漏掉断线间隔', async t => {
  const { server, create } = environment(t)
  const { danmaku } = create(); await settle()
  danmaku.draft.value = '断线前'; danmaku.sendDraft()
  t.mock.timers.tick(0); await settle()
  assert.equal(danmaku.messages.value.length, 1)
  server.history = [{ id: '2', event_id: 'event-offline', request_id: 'request-offline', room_id: '1', session_id: '1', user_id: '2', username: 'other', content: '断线期间的消息', created_at: new Date().toISOString() }]
  server.sockets.at(-1).close(); await settle()
  t.mock.timers.tick(1000); await settle()
  assert.equal(server.sockets.length, 2)
  assert.equal(danmaku.messages.value.length, 2)
  assert.ok(danmaku.messages.value.some(value => value.content === '断线期间的消息'))
})
