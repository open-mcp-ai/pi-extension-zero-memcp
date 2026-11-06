// 扩展的冒烟测试：不启动真实 Pi，用最小 fake 验证「注册了就一定能跑」。
//
// 存在的理由很具体：`mem:record` 和 `mem:identity` 里的裸 `ctx` 是
// ReferenceError，`node --check` 抓不到（语法合法），真实使用中也不报
// （交互模式下 sessionId 早已赋值，那个分支进不去）。只有把命令真的
// 调一次才能发现——那正是这些测试要做的事。

import { test } from 'node:test'
import assert from 'node:assert'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

const here = dirname(fileURLToPath(import.meta.url))
const src = readFileSync(join(here, '..', 'index.js'), 'utf8')

/**
 * 构造一个最小的 Pi API 桩：只实现扩展真正用到的方法。
 *
 * 关键在于 registerCommand / on / registerTool 都要**真的记录**，
 * 否则测试无法验证「注册了什么」，也调不到 handler。
 */
function makePiStub() {
  const commands = new Map()
  const events = new Map()
  const tools = new Map()
  const notifications = []

  return {
    commands,
    events,
    tools,
    notifications,
    api: {
      registerCommand(name, opts) {
        commands.set(name, opts)
      },
      on(ev, handler) {
        if (!events.has(ev)) events.set(ev, [])
        events.get(ev).push(handler)
        return () => {}
      },
      registerTool(def) {
        tools.set(def.name, def)
      },
      events: { emit() {} },
      sendUserMessage() {},
    },
  }
}

/** 造一个最小的命令上下文（ExtensionCommandContext 的子集）。 */
function makeCmdCtx() {
  return {
    // 故意不给 sessionManager：模拟「取不到会话 id」的最坏情况，
    // 这正是裸 ctx 那个 bug 的触发条件。
    ui: { notify(msg) {} },
  }
}

async function loadExtension() {
  const { default: zeroMemcp } = await import('../index.js')
  const stub = makePiStub()
  await zeroMemcp(stub.api)
  return stub
}

test('扩展能加载，并注册全部命令与工具', async () => {
  const stub = await loadExtension()
  assert.ok(stub.tools.has('mem'), '应注册 mem 工具')
  for (const name of ['mem:sync', 'mem:record', 'mem:identity']) {
    assert.ok(stub.commands.has(name), `应注册命令 ${name}`)
  }
})

test('mem 工具的 description 覆盖服务端全部关键动词', async () => {
  const stub = await loadExtension()
  const desc = stub.tools.get('mem').description

  // 这些动词曾经全部缺失：模型看不见它们存在，就永远不会用。
  const mustMention = [
    'recall', 'read', 'grep', 'log', 'ls', 'trees', 'status',
    'put', 'write', 'batch',
    'commit', 'diff', 'branch', 'tag', 'cp', 'mv', 'delete',
    'archive', 'reset', 'lock', 'chmod',
    'remote', 'sync', 'fetch', 'push', 'merge', 'since', 'remind',
    'remote-key', 'remote-secret',
    'record', 'identity',
  ]
  const missing = mustMention.filter((v) => !desc.includes(v))
  assert.deepEqual(missing, [], `description 缺少动词: ${missing.join(', ')}`)
})

test('/mem:record 在取不到会话 id 时不抛异常', async () => {
  const stub = await loadExtension()
  const cmd = stub.commands.get('mem:record')
  // 空参：只报状态，不写盘
  await assert.doesNotReject(
    () => cmd.handler('', makeCmdCtx()),
    '空参不应抛异常',
  )
  // 设本会话模式：这里曾经用裸 ctx，会 ReferenceError
  await assert.doesNotReject(
    () => cmd.handler('on', makeCmdCtx()),
    '/mem:record on 不应抛 ReferenceError（裸 ctx 那个 bug）',
  )
})

test('/mem:identity 在取不到会话 id 时不抛异常', async () => {
  const stub = await loadExtension()
  const cmd = stub.commands.get('mem:identity')
  await assert.doesNotReject(
    () => cmd.handler('', makeCmdCtx()),
    '空参不应抛异常',
  )
  await assert.doesNotReject(
    () => cmd.handler('on', makeCmdCtx()),
    '/mem:identity on 不应抛 ReferenceError（裸 ctx 那个 bug）',
  )
})

test('/mem:sync 在服务不可达时给出通知而不崩溃', async () => {
  const stub = await loadExtension()
  const cmd = stub.commands.get('mem:sync')
  await assert.doesNotReject(() => cmd.handler('', makeCmdCtx()))
})

test('源码里不存在命令作用域内的裸 ctx 引用', () => {
  // 静态兜底：防止将来又有人把事件 handler 的写法复制到命令 handler 里。
  const lines = src.split('\n')
  const offenders = []
  let inCmd = false
  let depth = 0
  let name = ''

  lines.forEach((line, i) => {
    const m = line.match(/pi\.registerCommand\('([^']+)'/)
    if (m) {
      inCmd = true
      depth = 0
      name = m[1]
    }
    if (!inCmd) return

    depth += (line.match(/\{/g) || []).length - (line.match(/\}/g) || []).length

    // 跳过注释行：说明文字里提到 ctx 是正常的。
    const code = line.replace(/\/\/.*$/, '')
    const bare = code.match(/(?<![.\w])ctx(?![.\w:])/g)
    if (bare) {
      const isParam =
        /function[^(]*\([^)]*\bctx\b[^)]*\)/.test(code) || /\([^)]*\bctx\b[^)]*\)\s*=>/.test(code)
      if (!isParam) offenders.push(`${name} 行${i + 1}: ${line.trim()}`)
    }
    if (depth === 0 && i > 0) inCmd = false
  })

  assert.deepEqual(offenders, [], `命令作用域里出现裸 ctx：\n${offenders.join('\n')}`)
})

test('不再有自定义 SSE 通道（服务端只讲 MCP）', () => {
  // 这条是反向断言：SSE 订阅/推送已整体移除，防止有人把长连接加回来。
  // 服务端对外只有一个 /mcp 入口，多一条通道就是多一套要维护的传输。
  assert.doesNotMatch(src, /startSSEListener/, '不应再有 SSE 监听器')
  assert.doesNotMatch(src, /text\/event-stream/, '不应再消费事件流')
  assert.doesNotMatch(src, /\/events/, '不应再连 /events')
})

test('去重键用全文哈希而不是前缀截断', () => {
  // 早先用 text.slice(0,160)：同问不同答会被误判重复而丢弃。
  assert.doesNotMatch(
    src,
    /const key = text\.slice\(/,
    '去重键不应使用前缀截断',
  )
  assert.match(src, /const key = createHash\('sha256'\)\.update\(text/, '去重键应为全文哈希')
})
