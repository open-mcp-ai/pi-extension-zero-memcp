import { test } from 'node:test'
import assert from 'node:assert'
import {
  WakePolicy, shouldIgnore, parseFrame, renderWakePrompt, renderPending, makeSessionTag,
} from '../bin/wake.js'

const ev = (session, identity = 'laptop', uri = 'memcp://daily/zero') => ({
  uri, commit: 'aaaa1111', previous: 'bbbb2222', origin: { session, identity },
})
const evAt = (uri, session = 'other') => ({ uri, commit: 'c', previous: 'p', origin: { session } })

// ── flag 语义 ────────────────────────────────────────────

test('事件到达只设 flag，绝不唤醒', () => {
  const p = new WakePolicy({ mode: 'continue', session: 'S', minIntervalMs: 0 })
  const r = p.ingest([ev('other')])
  assert.equal(r.queued, 1)
  assert.equal(p.status().backlog, 1)
  // ingest 本身不产生唤醒
  assert.equal(p.status().lastWakeAt, 0)
})

test('agent 在忙时 settle 不唤醒，积压保留', () => {
  const p = new WakePolicy({ mode: 'continue', session: 'S', minIntervalMs: 0 })
  p.markBusy()
  p.ingest([ev('other')])
  const r = p.settle()
  assert.equal(r.action, 'none')
  assert.match(r.reason, /busy/)
  assert.equal(p.status().backlog, 1, '忙的时候积压不能丢')
})

test('agent 停稳后 settle 消费积压并唤醒一次', () => {
  const p = new WakePolicy({ mode: 'continue', session: 'S', minIntervalMs: 0 })
  p.markBusy()
  p.ingest([evAt('memcp://daily/a'), evAt('memcp://daily/b'), evAt('memcp://daily/c')])
  p.markIdle()
  const r = p.settle()
  assert.equal(r.action, 'wake')
  assert.equal(r.events.length, 3, '一次唤醒消费全部积压')
  assert.equal(p.status().backlog, 0, '唤醒后积压清空')
  // 再 settle 无事可做
  assert.equal(p.settle().action, 'none')
})

test('同一 uri 多次变更只占一条积压', () => {
  const p = new WakePolicy({ mode: 'continue', session: 'S', minIntervalMs: 0 })
  p.markBusy()
  p.ingest([evAt('memcp://daily/a', 's1')])
  p.ingest([evAt('memcp://daily/a', 's2')])
  p.ingest([evAt('memcp://daily/a', 's3')])
  assert.equal(p.status().backlog, 1, '积压是「哪些树变了」不是「变了多少次」')
  p.markIdle()
  assert.equal(p.settle().events.length, 1)
})

// ── origin 过滤（自激的第一道防线）────────────────────────

test('自己会话的事件不进积压', () => {
  const p = new WakePolicy({ mode: 'continue', session: 'S', minIntervalMs: 0 })
  const r = p.ingest([ev('S')])
  assert.equal(r.queued, 0)
  assert.equal(r.ignored, 1)
  assert.equal(p.status().backlog, 0)
  p.markIdle()
  assert.equal(p.settle().action, 'none', '自己写的不能唤醒自己')
})

test('loop 模式不过滤自己的事件（自激是特性）', () => {
  const p = new WakePolicy({ mode: 'loop', session: 'S', minIntervalMs: 0 })
  const r = p.ingest([ev('S')])
  assert.equal(r.queued, 1, 'loop 模式下自己写的也要进积压')
  assert.equal(p.settle().action, 'wake')
})

test('没有 session 信息时退回按 identity 过滤', () => {
  const p = new WakePolicy({ mode: 'continue', session: 'S', identity: 'laptop', minIntervalMs: 0 })
  const noSession = { uri: 'memcp://daily/zero', origin: { identity: 'laptop' } }
  assert.equal(p.ingest([noSession]).ignored, 1)
})

// ── 节流与熔断 ───────────────────────────────────────────

test('minIntervalMs 未到则节流，积压保留', () => {
  const p = new WakePolicy({ mode: 'continue', session: 'S', minIntervalMs: 60000 })
  const t0 = 1_000_000
  p.ingest([evAt('memcp://daily/a')])
  assert.equal(p.settle(t0).action, 'wake')
  p.ingest([evAt('memcp://daily/b')])
  const r = p.settle(t0 + 5000)
  assert.equal(r.action, 'none')
  assert.match(r.reason, /throttled/)
  assert.equal(p.status().backlog, 1, '节流不丢积压')
  // 到点后恢复
  assert.equal(p.settle(t0 + 60001).action, 'wake')
})

test('maxChain 熔断：loop 达到上限后停止', () => {
  const p = new WakePolicy({ mode: 'loop', session: 'S', minIntervalMs: 0, maxChain: 3 })
  for (let i = 0; i < 3; i++) {
    p.ingest([evAt(`memcp://daily/${i}`)])
    assert.equal(p.settle().action, 'wake')
  }
  p.ingest([evAt('memcp://daily/x')])
  const r = p.settle()
  assert.equal(r.action, 'stopped')
  assert.match(r.reason, /maxChain/)
  // 熔断后新事件仍记录（数量有限：按 uri 去重），只是标记为 blocked
  const ing = p.ingest([evAt('memcp://daily/y')])
  assert.equal(ing.wakeBlocked, 1)
  assert.equal(ing.queued, 1, '熔断不该丢积压')
  assert.equal(p.settle().action, 'stopped', '熔断状态要持续可见')
})

test('复位后熔断解除', () => {
  const p = new WakePolicy({ mode: 'loop', session: 'S', minIntervalMs: 0, maxChain: 1 })
  p.ingest([evAt('memcp://daily/a')])
  assert.equal(p.settle().action, 'wake')
  p.ingest([evAt('memcp://daily/b')])
  assert.equal(p.settle().action, 'stopped')
  p.reset()
  assert.equal(p.settle().action, 'wake')
})

test('非 loop 模式每轮结束清链长', () => {
  const p = new WakePolicy({ mode: 'continue', session: 'S', minIntervalMs: 0, maxChain: 2 })
  p.ingest([evAt('memcp://daily/a')])
  assert.equal(p.settle().action, 'wake')
  p.onTurnEnd()
  assert.equal(p.status().chain, 0)
})

test('loop 模式链长跨轮累积（熔断的意义）', () => {
  const p = new WakePolicy({ mode: 'loop', session: 'S', minIntervalMs: 0, maxChain: 2 })
  p.ingest([evAt('memcp://daily/a')])
  assert.equal(p.settle().action, 'wake')
  p.onTurnEnd()
  assert.equal(p.status().chain, 1, 'loop 模式不清链长')
})

// ── notify 模式 ──────────────────────────────────────────

test('notify 模式 settle 返回 notify 而非 wake，且不消费积压', () => {
  const p = new WakePolicy({ mode: 'notify', session: 'S', minIntervalMs: 0 })
  p.ingest([ev('other')])
  const r = p.settle()
  assert.equal(r.action, 'notify')
  assert.equal(p.status().backlog, 1, 'notify 模式要等下一轮 context 取走')
  const taken = p.consume()
  assert.equal(taken.length, 1)
  assert.equal(p.status().backlog, 0)
})

test('off 模式 settle 什么都不做', () => {
  const p = new WakePolicy({ mode: 'off', session: 'S' })
  p.ingest([ev('other')])
  assert.equal(p.settle().action, 'none')
})

// ── 帧解析与渲染 ─────────────────────────────────────────

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
  assert.ok(out.ack)
  assert.equal(out.events.length, 1)
  assert.equal(out.events[0].origin.session, 'S1')
  assert.equal(out.events[0].commit, 'c1')
})

test('parseFrame 忽略心跳与非 JSON', () => {
  assert.equal(parseFrame(': keepalive\n\ndata: not-json\n\n').events.length, 0)
})

test('renderWakePrompt 只给地址与读取命令，不含正文', () => {
  const s = renderWakePrompt([{ uri: 'memcp://BlackEggAI/zero/main.record', commit: 'abcd1234ef', origin: { identity: 'desktop' } }])
  assert.match(s, /BlackEggAI@zero/)
  assert.match(s, /by desktop/)
  assert.match(s, /abcd1234/)
  // 广播点从 commit 挪到写入后，脏层没有新版本号：取差量已无意义，
  // 文案改为直接看树。
  assert.match(s, /mem\(\{ cmd: "ls/)
  assert.doesNotMatch(s, /正文内容/)
})

test('renderPending 只给地址', () => {
  const s = renderPending([{ uri: 'memcp://daily/zero', origin: { identity: 'desktop' } }])
  assert.match(s, /daily@zero/)
  assert.match(s, /<memory_updates>/)
})

test('makeSessionTag 稳定且唯一', () => {
  assert.notEqual(makeSessionTag(), makeSessionTag())
  assert.match(makeSessionTag(), /^pi-/)
})

test('项目名含斜杠时 URI 必须转义（否则服务端 403）', async () => {
  // 这段逻辑在 index.js 里（闭包内），这里复刻同样的规则做契约测试：
  // 项目名 percent-encode，branch 与 path 保持斜杠分隔。
  const toResourceURI = (addr) => {
    const at = addr.indexOf('@')
    if (at < 0) return ''
    const project = addr.slice(0, at)
    let rest = addr.slice(at + 1).replace(':', '/').replace(/\/+$/, '')
    const slash = rest.indexOf('/')
    const branch = slash < 0 ? rest : rest.slice(0, slash)
    const tail = slash < 0 ? '' : rest.slice(slash)
    return `memcp://${encodeURIComponent(project)}/${encodeURIComponent(branch)}${tail}`
  }
  assert.equal(
    toResourceURI('BlackEggAI/zero@main.record'),
    'memcp://BlackEggAI%2Fzero/main.record',
  )
  assert.equal(
    toResourceURI('BlackEggAI/zero@main.record:thought/2026-11-06'),
    'memcp://BlackEggAI%2Fzero/main.record/thought/2026-11-06',
  )
  assert.equal(toResourceURI('daily@zero'), 'memcp://daily/zero')
})

test('非 notify 模式的积压不能被 context 取走（否则唤醒永不发生）', () => {
  // 真实 pi 里踩到的 bug：context 注入无条件 consume()，把 continue 模式
  // 的 flag 在 agent_settled 之前清空，于是 settle 永远返回 no backlog。
  const p = new WakePolicy({ mode: 'continue', session: 'S', minIntervalMs: 0 })
  p.markBusy()
  p.ingest([ev('other')])
  // 模拟「一轮结束前的 context 注入」：只有 notify 才允许取
  const taken = p.mode === 'notify' ? p.consume() : []
  assert.equal(taken.length, 0, 'continue 模式不该被 context 取走积压')
  p.markIdle()
  const r = p.settle()
  assert.equal(r.action, 'wake', '停稳后仍应能唤醒')
  assert.equal(r.events.length, 1)
})
