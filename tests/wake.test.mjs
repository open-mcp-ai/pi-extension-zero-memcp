import { test } from 'node:test'
import assert from 'node:assert'
import { WakePolicy, shouldIgnore, parseFrame, renderPending, makeSessionTag } from '../bin/wake.js'

const ev = (session, identity = 'laptop', uri = 'memcp://daily/zero') => ({
  uri, commit: 'aaa', previous: 'bbb', origin: { session, identity },
})

test('mode off 什么都不做', () => {
  const p = new WakePolicy({ mode: 'off', session: 'S' })
  assert.equal(p.handle([ev('other')]).action, 'none')
})

test('notify 模式只累积，不唤醒', () => {
  const p = new WakePolicy({ mode: 'notify', session: 'S' })
  const r = p.handle([ev('other')])
  assert.equal(r.action, 'notify')
  assert.equal(p.status().pending, 1)
  const pend = p.takePending()
  assert.equal(pend.length, 1)
  assert.equal(p.status().pending, 0, '取走后应清空')
})

test('continue 模式忽略自己会话的事件', () => {
  const p = new WakePolicy({ mode: 'continue', session: 'S', cooldownMs: 0 })
  const r = p.handle([ev('S')])
  assert.equal(r.action, 'none')
  assert.match(r.reason, /originated from this session/)
})

test('continue 模式对他人事件唤醒', () => {
  const p = new WakePolicy({ mode: 'continue', session: 'S', cooldownMs: 0 })
  assert.equal(p.handle([ev('other')]).action, 'wake')
})

test('loop 模式不过滤自己的事件（自激是特性）', () => {
  const p = new WakePolicy({ mode: 'loop', session: 'S', cooldownMs: 0 })
  const r = p.handle([ev('S')])
  assert.equal(r.action, 'wake', 'loop 模式下自己写的也要唤醒')
})

test('冷却窗口内不重复唤醒', () => {
  const p = new WakePolicy({ mode: 'continue', session: 'S', cooldownMs: 60000 })
  const t0 = 1_000_000
  assert.equal(p.handle([ev('other')], t0).action, 'wake')
  const r = p.handle([ev('other')], t0 + 1000)
  assert.equal(r.action, 'none')
  assert.match(r.reason, /cooldown/)
  // 冷却过后恢复
  assert.equal(p.handle([ev('other')], t0 + 60001).action, 'wake')
})

test('maxChain 熔断：loop 达到上限后停止', () => {
  const p = new WakePolicy({ mode: 'loop', session: 'S', cooldownMs: 0, maxChain: 3 })
  const t0 = 1_000_000
  assert.equal(p.handle([ev('S')], t0 + 1).action, 'wake')
  assert.equal(p.handle([ev('S')], t0 + 2).action, 'wake')
  assert.equal(p.handle([ev('S')], t0 + 3).action, 'wake')
  const r = p.handle([ev('S')], t0 + 4)
  assert.equal(r.action, 'stopped', '连续 3 次后必须停')
  assert.match(r.reason, /maxChain/)
})

test('maxChain 复位后可以继续', () => {
  const p = new WakePolicy({ mode: 'loop', session: 'S', cooldownMs: 0, maxChain: 1 })
  assert.equal(p.handle([ev('S')], 1).action, 'wake')
  assert.equal(p.handle([ev('S')], 2).action, 'stopped')
  p.reset()
  assert.equal(p.handle([ev('S')], 3).action, 'wake')
})

test('非 loop 模式每轮结束清链长', () => {
  const p = new WakePolicy({ mode: 'continue', session: 'S', cooldownMs: 0, maxChain: 2 })
  assert.equal(p.handle([ev('other')], 1).action, 'wake')
  p.onTurnEnd()
  assert.equal(p.status().chain, 0)
})

test('loop 模式链长跨轮累积（这正是熔断的意义）', () => {
  const p = new WakePolicy({ mode: 'loop', session: 'S', cooldownMs: 0, maxChain: 2 })
  assert.equal(p.handle([ev('S')], 1).action, 'wake')
  p.onTurnEnd()
  assert.equal(p.status().chain, 1, 'loop 模式不清链长')
})

test('没有 session 信息时退回按 identity 过滤', () => {
  const p = new WakePolicy({ mode: 'continue', session: 'S', identity: 'laptop', cooldownMs: 0 })
  const noSession = { uri: 'memcp://daily/zero', origin: { identity: 'laptop' } }
  assert.equal(p.handle([noSession]).action, 'none')
})

test('parseFrame 解析 ack 与事件', () => {
  const frames = [
    'event: message',
    'data: {"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"notifications":{"resourceSubscriptions":["memcp://daily/zero"]}}}',
    '',
    'event: message',
    'data: {"jsonrpc":"2.0","method":"notifications/resources/updated","params":{"uri":"memcp://daily/zero","_meta":{"memcp/origin":{"identity":"laptop","session":"S1"},"memcp/commit":"c1","memcp/previous":"p1"}}}',
    '',
  ].join('\n')
  const out = parseFrame(frames)
  assert.ok(out.ack, '应解析出 ack')
  assert.equal(out.events.length, 1)
  assert.equal(out.events[0].uri, 'memcp://daily/zero')
  assert.equal(out.events[0].origin.session, 'S1')
  assert.equal(out.events[0].commit, 'c1')
})

test('parseFrame 忽略心跳与非 JSON', () => {
  const out = parseFrame(': keepalive\n\n' + 'data: not-json\n\n')
  assert.equal(out.events.length, 0)
})

test('renderPending 只给地址不给正文', () => {
  const s = renderPending([{ uri: 'memcp://BlackEggAI/zero/main.record', origin: { identity: 'desktop' } }])
  assert.match(s, /BlackEggAI@zero/)
  assert.match(s, /by desktop/)
  assert.match(s, /since/)
  // 正文绝不能出现在这里：必须经工具读回
  assert.doesNotMatch(s, /content|正文内容/)
})

test('makeSessionTag 稳定且唯一', () => {
  const a = makeSessionTag()
  const b = makeSessionTag()
  assert.notEqual(a, b)
  assert.match(a, /^pi-/)
})
