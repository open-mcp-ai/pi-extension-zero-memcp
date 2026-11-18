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
  for (const name of ['mem:sync', 'mem:record', 'mem:identity', 'mem:mode']) {
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

test('事件流只走 /mcp 的标准订阅，不再有自定义端点', () => {
  // 事件流本身是合法的：subscriptions/listen 的响应体就是 SSE。
  // 要禁止的是**第二套通道**——自定义 /events 端点 + 自定义事件名。
  assert.doesNotMatch(src, /\/events/, '不应再连自定义 /events 端点')
  assert.doesNotMatch(src, /startSSEListener/, '不应有自建的 SSE 监听器')
  // 订阅必须是标准方法名
  assert.match(src, /subscriptions\/listen/, '应使用标准 subscriptions/listen')
  assert.match(src, /notifications\/resources\/updated|parseFrame/, '应解析标准资源变更通知')
})

test('唤醒策略默认关闭，且任何一种模式都要显式配置', () => {
  // 默认 off：装上就在后台唤醒 agent 是不可接受的默认值。
  assert.match(src, /mode: 'off'/, 'wake.mode 默认应为 off')
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

test('identityMode=minimal 不联网、注入短、且仍带 MARKER', async () => {
  // 这一档存在的理由：全量身份约 2KB，小模型（2b~7b）吃掉后剩下的窗口
  // 不够干活。minimal 只留入口 + 一条纪律，长度必须压到 400 字符以内；
  // endpoint 故意不可达——minimal 档不该为 2KB 跑一趟网络。
  //
  // 必须用带 query 的 import：CONFIG_FILE 是模块加载时读环境变量算出来的，
  // 前面几个 test 已经加载过 index.js，直接 import 会复用旧路径，测的就不是这一档。
  const { mkdtempSync, writeFileSync } = await import('node:fs')
  const { tmpdir } = await import('node:os')
  const { join } = await import('node:path')

  const dir = mkdtempSync(join(tmpdir(), 'zm-min-'))
  const cfgPath = join(dir, 'config.json')
  writeFileSync(cfgPath, JSON.stringify({
    endpoint: 'http://127.0.0.1:1',
    enabled: true,
    identity: 'on',
    identityMode: 'minimal',
    token: 'dummy',
  }))

  const prevCfg = process.env.ZERO_MEMCP_CONFIG
  const prevDbg = process.env.ZERO_MEM_DEBUG
  const prevPath = process.env.ZERO_MEM_DEBUG_PATH
  process.env.ZERO_MEMCP_CONFIG = cfgPath
  process.env.ZERO_MEM_DEBUG = '0'
  process.env.ZERO_MEM_DEBUG_PATH = join(dir, 'dbg.log')
  try {
    const { default: zeroMemcp } = await import('../index.js?mode=minimal')
    const stub = makePiStub()
    await zeroMemcp(stub.api)
    const handler = stub.events.get('before_provider_request')[0]
    assert.ok(handler, '应注册 before_provider_request')
    const out = await handler({ payload: { system: 'BASE' } })
    const sys = out?.system || ''
    assert.match(sys, /zero-memcp:identity/, '必须带 MARKER（幂等靠它）')
    assert.match(sys, /recall/, '必须点出 mem 入口')
    assert.ok(sys.length < 400, `minimal 档注入应短，实际 ${sys.length} 字符`)
    assert.doesNotMatch(sys, /SKILL|law\//, 'minimal 档不该带全量身份内容')
    assert.match(sys, /BASE/, '原系统提示必须保住')
  } finally {
    if (prevCfg === undefined) delete process.env.ZERO_MEMCP_CONFIG
    else process.env.ZERO_MEMCP_CONFIG = prevCfg
    if (prevDbg === undefined) delete process.env.ZERO_MEM_DEBUG
    else process.env.ZERO_MEM_DEBUG = prevDbg
    if (prevPath === undefined) delete process.env.ZERO_MEM_DEBUG_PATH
    else process.env.ZERO_MEM_DEBUG_PATH = prevPath
  }
})

test('provider payload 没有 system 字段时，注入进 messages[0]（ollama/OpenAI 兼容）', async () => {
  // 实测故障：27b 走 ollama 的 OpenAI 兼容端点，payload 只有 model/messages/stream，
  // 没有 system。旧代码在这个形态下直接 return——日志里只有 context_with_system
  // 那条「注入成功」，provider 适配层却根本不读 sections，模型一个字都没看到。
  const { mkdtempSync, writeFileSync } = await import('node:fs')
  const { tmpdir } = await import('node:os')
  const { join } = await import('node:path')

  const dir = mkdtempSync(join(tmpdir(), 'zm-oll-'))
  const cfgPath = join(dir, 'config.json')
  writeFileSync(cfgPath, JSON.stringify({
    endpoint: 'http://127.0.0.1:1', enabled: true, identity: 'on', identityMode: 'minimal', token: 'dummy',
  }))

  const prevCfg = process.env.ZERO_MEMCP_CONFIG
  const prevDbg = process.env.ZERO_MEM_DEBUG
  const prevPath = process.env.ZERO_MEM_DEBUG_PATH
  process.env.ZERO_MEMCP_CONFIG = cfgPath
  process.env.ZERO_MEM_DEBUG = '0'
  process.env.ZERO_MEM_DEBUG_PATH = join(dir, 'dbg.log')
  try {
    const { default: zeroMemcp } = await import('../index.js?shape=ollama')
    const stub = makePiStub()
    await zeroMemcp(stub.api)
    const bip = stub.events.get('before_provider_request')[0]
    assert.ok(bip, '应注册 before_provider_request')

    // 形态一：messages 里已有 system 消息
    const out = await bip({ payload: { model: 'm', stream: true, messages: [
      { role: 'system', content: 'You are pi.' }, { role: 'user', content: 'hi' },
    ] } })
    const sys = String(out.messages[0].content)
    assert.match(sys, /zero-memcp:identity/, '身份块必须真的进 messages[0]')
    assert.match(sys, /You are pi/, '原系统提示不能被覆盖')

    // 形态二：连 system 消息都没有
    const out2 = await bip({ payload: { model: 'm', messages: [{ role: 'user', content: 'hi' }] } })
    assert.equal(out2.messages[0].role, 'system', '没有 system 消息时应补一条在最前')
    assert.match(String(out2.messages[0].content), /zero-memcp:identity/)

    // 幂等：已经带 MARKER 的请求不该再叠一层
    const again = await bip({ payload: out })
    assert.equal(again, undefined, '已注入过应原样放行（返回 undefined）')
  } finally {
    if (prevCfg === undefined) delete process.env.ZERO_MEMCP_CONFIG
    else process.env.ZERO_MEMCP_CONFIG = prevCfg
    if (prevDbg === undefined) delete process.env.ZERO_MEM_DEBUG
    else process.env.ZERO_MEM_DEBUG = prevDbg
    if (prevPath === undefined) delete process.env.ZERO_MEM_DEBUG_PATH
    else process.env.ZERO_MEM_DEBUG_PATH = prevPath
  }
})
