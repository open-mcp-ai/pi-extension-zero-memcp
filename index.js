// zero-memcp 的 pi 扩展。
//
// 它在一轮模型请求的路径上做两件事：
//
//   context_with_system → 把身份块拼进系统消息的开头（会话开头 / 压缩后的开头）
//   context             → 每轮取 2-3 个记忆地址卡片，追加到消息尾部
//
// 两件事的成本差三个数量级，所以失败策略也不同：
//   身份注入失败 = 我不是我了，应该响亮地报错
//   联想失败     = 本轮少一条提示，静默跳过
//
// 这个插件**不写记忆**。写入由模型自己调 MCP 工具完成——
// 判断标准属于身份，不属于基础设施。
//
// 子代理不注入。
// 它们是被派去干一件具体活的工人，交接的是【问题 + 已探明的边界】，
// 不是我的身份。给它们越多我的上下文，它越像我，越只能走到我走过的地方
// （这是 me/skills/01-me 里"生"那一条）。而且子代理并行跑，每个都注入
// 2247 字身份块，就是 N 份重复的 prompt cache 写入。
//
// 判断依据：子代理会话的 sessionDir 落在 subagent-sessions / ephemeral-sessions
// 下（见 pi-web-ui 的 spawnSubagentConversation），主会话不会。
//
// 为什么不用 before_agent_start 的 sections：
//   pi-web-ui 的主会话 handler 是链上最后一个，它用 prompt-composer 把
//   {{token}} 重新渲染一遍，只认 PROMPT_TOKENS 里那 11 个名字。
//   往里塞自定义 section 会被整个丢掉——不报错，只是没有。
//
// 为什么不用 context：
//   它只看到非 system 消息，而且 Pi 在 handler 之后会 restoreSystemMessages，
//   把系统消息还原回去。注入的对话消息会被挪到后面，不在"开头"。
//
// 为什么是 context_with_system：
//   它拿到完整转录（含 system），返回什么就用什么。系统消息保持在 index 0，
//   把身份块拼在它的开头，就是"会话开头 / 压缩后的开头"这个位置。
import { readFile, writeFile, mkdir } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { homedir } from 'node:os'
import { join } from 'node:path'

const CONFIG_FILE = process.env.ZERO_MEMCP_CONFIG || join(homedir(), '.zero-memcp', 'config.json')

// 记忆版本号锚点（记录当前感知到的 docs_indexed 总数）
const CLOCK_FILE = process.env.ZERO_MEMCP_CLOCK || join(homedir(), '.zero-memcp', 'clock.json')

// 上传水位。
//
// 不存消息本身——会话的 JSONL 已经在磁盘上，它就是 WAL。
// 这里只记「最后一轮成功写入的是第几条」。断线后从这条往后重放，
// 服务端按内容 checksum 判重，重放多少遍都是同一个结果。
//
// 存水位而不是存队列：队列有状态、会涨、要清理、进程被杀就没了。
// 水位只有一个小整数，永远只有一条。
const WM_DIR = process.env.ZERO_MEMCP_WM_DIR || join(homedir(), '.zero-memcp', 'wm')

// ── SSE 实时推送与唤醒 ──
//
// 为什么需要主动推送：
//   没有它，agent 只能靠「轮到自己那一轮」去轮询 mailbox。而空闲的
//   智能体没有任何一轮——它不会自己醒来。QQ、群聊这类外部消息需要
//   一到就唤醒，这个动作只能由常驻监听器发起。
//
// 三条克制规则：
//   1. agent 正在思考（busy）时绝不打断，未读自然出现在下一轮 context；
//   2. 两次唤醒至少间隔 3 秒，群聊刷屏不至于变成死循环；
//   3. 掉线指数退避重连（1s → 2s → … → 30s 封顶）。
function startSSEListener(pi, getCfg, getProfile, onMailCount) {
  let abortCtrl = null
  let retryDelay = 1000
  let lastWakeAt = 0
  let busy = false

  pi.on('agent_start', () => { busy = true })
  pi.on('agent_end', () => { busy = false })

  async function connect() {
    const cfg = getCfg()
    const profile = getProfile()
    const url = profile
      ? `${cfg.endpoint}/events?profile=${encodeURIComponent(profile)}`
      : `${cfg.endpoint}/events`

    abortCtrl = new AbortController()
    let connected = false
    try {
      const res = await fetch(url, {
        headers: { Accept: 'text/event-stream' },
        signal: abortCtrl.signal,
      })
      if (!res.ok) throw new Error(`SSE ${res.status}`)
      connected = true
      retryDelay = 1000
      console.log(`[zero-memcp] SSE 已连接 ${url}`)

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buf = ''

      for (;;) {
        const { value, done } = await reader.read()
        if (done) break
        buf += decoder.decode(value, { stream: true })
        const blocks = buf.split('\n\n')
        buf = blocks.pop()

        for (const block of blocks) {
          if (!block.trim() || block.startsWith(':')) continue
          let evName = 'message'
          let dataStr = ''
          for (const line of block.split('\n')) {
            if (line.startsWith('event: ')) evName = line.slice(7).trim()
            else if (line.startsWith('data: ')) dataStr = line.slice(6).trim()
          }
          if (evName === 'mail' && dataStr) {
            try { onMail(JSON.parse(dataStr)) } catch {}
          }
        }
      }
    } catch (err) {
      if (err && err.name === 'AbortError') return
    } finally {
      if (!connected || true) {
        setTimeout(connect, retryDelay)
        retryDelay = Math.min(retryDelay * 2, 30000)
      }
    }
  }

  function onMail(ev) {
    // 无论唤不唤醒，先让下一轮的 context 提示反映最新未读数。
    // 只唤醒不计数，会出现「已经提示过了但下一轮还显示旧数字」。
    try { onMailCount && onMailCount(ev) } catch {}

    const now = Date.now()
    if (busy) {
      // 不打断，但要说清楚为什么不打断：排查时最容易误判成「推送没到」。
      console.log(`[zero-memcp] mail #${ev.seq} 到达，agent 正忙，留给下一轮`)
      return
    }
    if (now - lastWakeAt < 3000) {
      console.log(`[zero-memcp] mail #${ev.seq} 到达，节流中（${now - lastWakeAt}ms）`)
      return
    }
    lastWakeAt = now
    console.log(`[zero-memcp] mail #${ev.seq} 唤醒 agent`)
    // 唤醒词是固定短语，绝不拼接外部正文——消息内容必须通过
    // MCP 工具读回来，才会落在 tool_result 的沙盒里。
    try {
      if (typeof pi.sendUserMessage === 'function') {
        pi.sendUserMessage(
          `[zero-memcp] 有新的外部记忆 (#${ev.seq} ${ev.kind} 来自 ${ev.endpoint || 'unknown'})，调用 term("mailbox") 查看`
        )
      }
    } catch {}
  }

  connect()

  return () => { if (abortCtrl) abortCtrl.abort() }
}

// 会话级覆盖设置。
//
// 全局 importMode 是一个默认值，不是命令。真正决定「这一条会话记不记」
// 的是这里——每条会话自己一份，互不影响，进程重启也不丢。
//
// 为什么必须按会话分：常驻机上的会话是长期存在的，而「我现在想让这段
// 对话被记住」是一个当下的判断，不该因为改了一次配置就让所有会话
// （包括别人的、临时的、试错的）统统开始落盘。
const SESS_DIR = process.env.ZERO_MEMCP_SESS_DIR || join(homedir(), '.zero-memcp', 'sessions')

// 注入标记：用来判断系统消息里是否已经有身份块，避免压缩后重复叠加。
const MARKER = '<!-- zero-memcp:identity -->'

const DEFAULTS = {
  endpoint: 'http://127.0.0.1:1729',
  recallTimeoutMs: 200,
  recallLimit: 3,
  enabled: true,
  // 身份块每小时最多重取一次；它们几乎不变，变了也是新会话该看的事。
  identityTtlMs: 60 * 60 * 1000,
  // 默认不记忆。记忆是权利不是义务：默认状态是我听完就忘，
  // 用户明确说「记下来」才写。想常驻自动落盘，配置里改成 auto。
  syncSessionOnStart: false,
  // 导入模式：off（默认，什么都不写）/ manual（只响应 /mem:import）/ auto（每轮落盘）
  importMode: 'off',
  // 身份名：为空时由服务端取默认身份（不泄露到上下文）
  profile: '',
  // 角色名：为空时由服务端按 Token 的 default 决定
  role: '',
  // 凭证：服务端要求鉴权时必填
  token: '',
}

async function loadConfig() {
  try {
    return { ...DEFAULTS, ...JSON.parse(await readFile(CONFIG_FILE, 'utf8')) }
  } catch {
    // 没有配置文件是正常的；服务连不上不是，那种情况在 identity/recall 里各自处理。
    return { ...DEFAULTS }
  }
}

/**
 * 当前会话锁定的角色名。
 *
 * 为什么是模块级变量而不是每次现取：角色在会话开始时定一次，
 * 之后整个会话都用它。中途能改的话，「我这条记忆是用哪个身份写的」
 * 就没有确定答案了。
 */
let activeRole = ''

/**
 * 从 JWT 里解出角色清单。
 *
 * 为什么在客户端解而不是问服务端：payload 本来就是明文（base64url），
 * 签名只保证它没被改过，不保证它保密。角色清单是「这张凭证能做什么」
 * 的自我描述，让用户看见它不需要一次网络往返——而**看得见**很重要：
 * 一个说不清自己有哪些身份的系统，用户只会一直用最宽的那个。
 *
 * 只解码不验签是安全的：这里不做任何授权判断，
 * 真正的判据永远在服务端。伪造的 payload 只会让它自己看到一份假清单。
 */
function rolesOf(token) {
  if (!token || typeof token !== 'string') return null
  const parts = token.split('.')
  if (parts.length !== 3) return null
  try {
    let p = parts[1].replace(/-/g, '+').replace(/_/g, '/')
    while (p.length % 4) p += '='
    const payload = JSON.parse(Buffer.from(p, 'base64').toString('utf8'))
    return {
      subject: payload.sub || '',
      defaultRole: payload.default || '',
      roles: payload.roles || {},
    }
  } catch {
    return null
  }
}

/**
 * 把一份角色清单渲染成给人看的多行文本。
 */
function renderRoles(info, current) {
  const names = Object.keys(info.roles || {}).sort()
  if (!names.length) {
    return `凭证 ${info.subject} 没有定义角色（单身份模式）`
  }
  const lines = [`凭证主体: ${info.subject}`]
  if (info.defaultRole) lines.push(`默认角色: ${info.defaultRole}（未指定时会用它）`)
  lines.push('', `角色清单（共 ${names.length} 个）:`)
  for (const name of names) {
    const r = info.roles[name] || {}
    const mark = name === current ? ' ← 当前会话' : ''
    lines.push(`  ${name}${mark}`)
    if (r.cap) lines.push(`    能力: ${r.cap}`)
    for (const m of r.mount || []) {
      const br = m.branch === '*' ? '所有分支' : m.branch
      lines.push(`    ${(m.at || '/').padEnd(14)} → ${m.project} @ ${br}  [${m.cap || '-'}]`)
    }
  }
  return lines.join('\n')
}

/**
 * 构造请求头：带角色与可选凭证。
 *
 * 角色只从这里出去，绝不作为 DSL 参数——参数是模型能碰的地方，
 * 而角色决定能看见什么，把它做成参数等于把提权写进协议。
 */
function authHeaders(cfg, extra = {}) {
  const h = { ...extra }
  if (activeRole) h['X-Zero-Role'] = activeRole
  if (cfg && cfg.token) h.Authorization = `Bearer ${cfg.token}`
  return h
}

/** 取身份块。失败抛错——这跟联想不一样，缺了就是缺了。 */
async function fetchIdentity(cfg, profile = '') {
  const url = profile
    ? `${cfg.endpoint}/identity?profile=${encodeURIComponent(profile)}`
    : `${cfg.endpoint}/identity`
  const res = await fetch(url, {
    headers: authHeaders(cfg),
    signal: AbortSignal.timeout(3000),
  })
  if (!res.ok) throw new Error(`identity ${res.status}`)
  const body = await res.json()
  const blocks = Array.isArray(body.blocks) ? body.blocks : []
  return blocks.map((b) => b.text).filter(Boolean).join('\n\n')
}

/** 联想：短超时、失败静默。 */
async function recall(cfg, text) {
  const url = activeProfile
    ? `${cfg.endpoint}/recall?profile=${encodeURIComponent(activeProfile)}`
    : `${cfg.endpoint}/recall`
  const res = await fetch(url, {
    method: 'POST',
    headers: authHeaders(cfg, { 'content-type': 'application/json' }),
    body: JSON.stringify({ text, limit: cfg.recallLimit }),
    signal: AbortSignal.timeout(cfg.recallTimeoutMs),
  })
  if (!res.ok) throw new Error(`recall ${res.status}`)
  const body = await res.json()
  return Array.isArray(body.hits) ? body.hits : []
}

/** 取信箱未读状态。轻量 GET，毫秒级。 */
async function fetchMailbox(cfg, profile = '') {
  const url = profile
    ? `${cfg.endpoint}/mailbox?profile=${encodeURIComponent(profile)}`
    : `${cfg.endpoint}/mailbox`
  const res = await fetch(url, { signal: AbortSignal.timeout(600) })
  if (!res.ok) throw new Error(`mailbox ${res.status}`)
  return await res.json()
}

/** 取服务端状态（含 docs_indexed 逻辑时钟） */
async function fetchHealth(cfg, profile = '') {
  const url = profile
    ? `${cfg.endpoint}/health?profile=${encodeURIComponent(profile)}`
    : `${cfg.endpoint}/health`
  const res = await fetch(url, { signal: AbortSignal.timeout(600) })
  if (!res.ok) throw new Error(`health ${res.status}`)
  return await res.json()
}

/** 读取本地保存的上次版本号 */
async function loadLastClock() {
  try {
    const d = JSON.parse(await readFile(CLOCK_FILE, 'utf8'))
    return typeof d.docs === 'number' ? d.docs : null
  } catch { return null }
}

/** 保存刚见到的版本号 */
async function saveClock(docs) {
  try {
    await mkdir(join(homedir(), '.zero-memcp'), { recursive: true })
    await writeFile(CLOCK_FILE, JSON.stringify({ docs, at: Date.now() }))
  } catch {}
}

/** 对问答正文求 SHA256 前 8 位，与服务端写盘的 --hash8 判据保持一致 */
function checksumOf(text) {
  return createHash('sha256').update(text, 'utf8').digest('hex').slice(0, 8)
}

/** 读某会话的上传水位。没有就是 0（一条都还没写过）。 */
async function loadWatermark(sessionId) {
  if (!sessionId) return { idx: 0, hash: '', at: 0 }
  try {
    const raw = await readFile(join(WM_DIR, `${sessionId.replace(/[^a-zA-Z0-9._-]/g, '_')}.json`), 'utf8')
    const d = JSON.parse(raw)
    return { idx: Number(d.idx) || 0, hash: String(d.hash || ''), at: Number(d.at) || 0 }
  } catch {
    return { idx: 0, hash: '', at: 0 }
  }
}

/** 记下水位。只在写成功后调用——失败时水位不动，下次会从这里重放。 */
async function saveWatermark(sessionId, idx, hash) {
  if (!sessionId) return
  try {
    await mkdir(WM_DIR, { recursive: true })
    const file = join(WM_DIR, `${sessionId.replace(/[^a-zA-Z0-9._-]/g, '_')}.json`)
    await writeFile(file, JSON.stringify({ idx, hash, at: Date.now() }))
  } catch {}
}

/** 读当前会话的个性化设置（覆盖全局配置） */
async function loadSessionSetting(sessionId) {
  if (!sessionId) return {}
  try {
    const raw = await readFile(join(SESS_DIR, `${sessionId.replace(/[^a-zA-Z0-9._-]/g, '_')}.json`), 'utf8')
    return JSON.parse(raw)
  } catch {
    return {}
  }
}

/** 记下当前会话的个性化设置 */
async function saveSessionSetting(sessionId, patch) {
  if (!sessionId) return
  try {
    await mkdir(SESS_DIR, { recursive: true })
    const file = join(SESS_DIR, `${sessionId.replace(/[^a-zA-Z0-9._-]/g, '_')}.json`)
    const cur = await loadSessionSetting(sessionId)
    await writeFile(file, JSON.stringify({ ...cur, ...patch, updatedAt: Date.now() }))
  } catch {}
}

/**
 * 判断当前会话是不是子代理。
 *
 * 两个来源都要看：
 *   - sessionDir 落在 subagent-sessions / ephemeral-sessions 下（pi-web-ui 的约定）
 *   - 环境变量 PI_SUBAGENT 存在（供将来的显式标记）
 *
 * 拿不到 sessionDir 时返回 false：主会话注入是默认行为，
 * 宁可多注入一次，也不要因为读不到路径就让主会话失去身份。
 */
function isSubagent(ctx) {
  if (process.env.PI_SUBAGENT) return true

  // 踩过的坑：原本用 session header 里的 parentSession 判断，结果主会话
  // 因为「续接历史」也带 parentSession，被误判成子会话——身份不注入、
  // 记忆不落盘，而且完全不报错。parentSession 表示「这条会话从哪续来的」，
  // 和「我是不是被派出去干活的」是两件事。
  //
  // 现在只认明确的信号，未识别时按主会话处理：
  // 漏判子会话 = 多注入一份身份（浪费点 token）
  // 误判主会话 = 我没有身份（严重）
  // 两种错误的代价差得很远，所以往安全那边偏。
  const dir = ctx?.sessionManager?.getSessionDir?.() ?? ctx?.sessionManager?.sessionDir
  if (typeof dir === 'string' && /[\\/](subagent-sessions|ephemeral-sessions)[\\/]/.test(dir)) {
    return true
  }
  return false
}

/**
 * 写一条记忆。
 *
 * 走 /mcp 的 note 工具而不是自己拼文件：权限校验、kind 白名单、444 冻结、
 * 目录 0555 都在服务端，插件不该绕过去。
 *
 * 超时给 5s：写入要扫描重建索引，比 recall 重得多，但仍然不该挂住会话。
 */
/**
 * 调 zero-memcp 的 term 入口。整个服务只有一个工具，动词在 cmd 里。
 *
 * 走服务端而不是自己拼文件：kind 白名单、444 冻结、目录 0555、索引重建
 * 都在服务端。插件是客户端，不该有绕过校验的能力。
 */
async function term(cfg, cmd, timeoutMs = 5000, profile = '') {
  const url = profile
    ? `${cfg.endpoint}/mcp?profile=${encodeURIComponent(profile)}`
    : `${cfg.endpoint}/mcp`
  const res = await fetch(url, {
    method: 'POST',
    headers: authHeaders(cfg, { 'content-type': 'application/json' }),
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: Date.now(),
      method: 'tools/call',
      params: { name: 'term', arguments: { cmd } },
    }),
    signal: AbortSignal.timeout(timeoutMs),
  })
  if (!res.ok) throw new Error(`term ${res.status}`)
  const body = await res.json()
  if (body.error) throw new Error(body.error.message || 'term failed')
  const content = body.result?.content
  return Array.isArray(content) && content[0]?.text ? content[0].text : ''
}

/** 写一条记忆。保持原始内容的换行与排版。session 用来在面板里按会话分组。 */
async function writeNote(cfg, kind, text, session) {
  const body = String(text || '').trim()
  if (!body) return ''
  const tag = session ? `@${session}` : ''
  // 落到当前身份的 daily/。activeProfile 为空时走服务端默认。
  return term(cfg, `note ${kind}${tag} -- ${body}`, 5000, activeProfile)
}

// 最终回复自动落盘：
//
// 每轮对话结束时，助手说出口的最终总结自动成为切片写入 daily/。
// 模型不需要记任何语法标记，用户也不需要催促记录。
// 过滤规则：必须是纯文本，过滤太短的过渡词（<40字）。
const TRANS_PREFIX = /^(好的|好，|我来|让我|我先|我来看|先看|正在|接下来|现在|我查|我试|稍等|嗯|收到)/

/** 从消息列表里收集助手说过的话。 */
function assistantTexts(messages) {
  const out = []
  for (const m of messages) {
    if (!m || m.role !== 'assistant') continue
    if (typeof m.content === 'string') {
      out.push(m.content)
      continue
    }
    if (Array.isArray(m.content)) {
      for (const p of m.content) {
        if (p && p.type === 'text' && typeof p.text === 'string') out.push(p.text)
      }
    }
  }
  return out
}

function latestUserText(messages) {
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i]
    if (!m || m.role !== 'user') continue
    if (typeof m.content === 'string') return m.content
    if (Array.isArray(m.content)) {
      return m.content
        .filter((p) => p && p.type === 'text' && typeof p.text === 'string')
        .map((p) => p.text)
        .join('\n')
    }
  }
  return ''
}

function renderHits(hits) {
  const lines = hits.map((h) => {
    const matched =
      Array.isArray(h.matched) && h.matched.length ? `（命中：${h.matched.slice(0, 4).join('、')}）` : ''
    return `- [${h.addr}] 相关度 ${Number(h.score).toFixed(2)}${matched}`
  })
  return [
    '<recalled_memories>',
    '以下是与本轮相关、但不在当前上下文里的记忆地址。需要细节时用 zero-memcp 的 read 工具展开；不需要就忽略这段。',
    ...lines,
    '</recalled_memories>',
  ].join('\n')
}

export default function zeroMemcp(pi) {
  let cfgPromise = loadConfig()

  let identity = ''
  let identityAt = 0
  let identityError = null

  // 联想只在用户文本变化时重算：同一个 run 里工具循环会反复触发 context。
  let recallKey = null
  let recallHits = []
  // 记忆版本号（逻辑时钟）：seenDocs 是本进程上次看到的 docs_indexed，
  // lastDocs 是再上一次。两者差出来，就是「别人写的那一段」。
  let seenDocs = null
  let lastDocs = null
  let jumpNote = ''

  // 连接状态。用来判断「恢复」这个事件：上一轮不可达、这一轮可达。
  // 单看「可达」没有意义，只有从 false 翻到 true 才是重连。
  let lastHealthy = null

  // 信箱未读数。0 表示没有需要确认的东西，此时不注入任何提示。
  let mailboxUnread = 0

  // 会话级的开关：子代理整个生命周期都不注入、不联想。
  let skip = false

  // 已落盘的记忆（按内容哈希去重），避免同一句被反复写入。
  const seenMemories = new Set()

  // 当前会话 id，随切片一起写进 frontmatter，供面板分组。
  let sessionId = ''

  // 当前身份名。只用于 HTTP 请求参数，绝不进入上下文——
  // 「我是谁」由身份块的内容决定，不由一个标签决定。
  // 空串表示让服务端用自己的默认身份。
  let activeProfile = ''

  // 取「本会话消息列表」的闭包。
  //
  // 为什么不直接存 ctx：pi 在 newSession / fork / switchSession / reload
  // 之后会作废旧 ctx，任何在异步回调里继续访问它的代码都会抛
  // 「This extension ctx is stale」。而 SSE 重连回调可能在几分钟后才跑，
  // 那时会话早换过不止一次了。
  //
  // 所以这里只留一个动作，不留对象：取不到就返回空数组，
  // 让新会话（session_start 会把它覆盖掉）自己去补。
  let entriesGetter = null

  // 本会话生效的导入模式。默认取全局配置，会话启动时若该会话有
  // 自己的设置就用它覆盖——每条会话互不影响。
  let sessionImportMode = null

  /** 取身份块，带一次性失败标记：失败不重试，避免每个 run 都打一次空请求。 */
  async function ensureIdentity(cfg, piApi) {
    if (identity || identityError) return identity
    try {
      // 身份名只在这里出现：它进 HTTP 请求，不进系统提示。
      // 模型看到的是身份内容本身，永远看不到"我用的是哪一份"。
      identity = await fetchIdentity(cfg, activeProfile)
      identityAt = Date.now()
    } catch (err) {
      // 注入失败要可见：缺了身份块，我这次醒来就不是我。
      // 不 throw——那会让整个 run 起不来，代价比"缺一段提示"更大。
      identityError = err
      piApi.events?.emit?.('zero-memcp:identity-error', { message: String(err) })
    }
    return identity
  }

  const resetPerRun = () => {
    recallKey = null
    recallHits = []
  }

  // 记忆入口工具。
  //
  // 只有一个，动词在 cmd 里——和 zero-memcp 的 term 协议同构。
  // 逐个注册 recall/read/note 会让工具表变长、每轮都占提示词预算，
  // 而它们其实是同一个服务的三个动词。
  //
  // 为什么由插件提供而不是靠 MCP 挂载：MCP 是会话级配置，换个目录就没了，
  // 而"该写的时候手里没有笔"会让 SKILL.md 里那条写入判据变成一句空话。
  // 插件是唯一必然加载的通道。
  //
  // 什么时候写仍然由模型决定——这里只提供能力，不做判断。
  pi.registerTool({
    name: "zero_mem",
    label: "Zero Mem",
    description: [
      "zero-memcp 记忆库：查过去、架构、主人、踩坑、决定优先用它，不要用 bash 翻磁盘。",
      "",
      "  recall <text>            语义搜",
      "  read <addr> [<addr>...]  读全文",
      "  grep <pattern>           正则精确搜（符号/数字/IP 用它）",
      "  log [N]                  最近 N 篇",
      "  ls [date]                某天的全部切片",
      "  note <kind> -- <text>    写一条",
      "  status                   索引规模",
      "",
      "不知道用哪个就调 help，不知道该问什么就传空字符串——都会返回完整说明。",
    ].join("\n"),
    parameters: {
      type: "object",
      properties: {
        cmd: {
          type: "string",
          description: "命令行，例如 'recall napcat 端口' 或 'read skills/01-me/03-method.md'",
        },
      },
      required: ["cmd"],
      additionalProperties: false,
    },
    async execute(_toolCallId, params) {
      const cfg = await cfgPromise
      if (!cfg.enabled) throw new Error("zero-memcp 已禁用")
      const cmd = String(params.cmd || "").trim()
      if (!cmd) throw new Error("cmd 不能为空")
      // read 可能读长文，给宽一点；其余都是毫秒级。
      const timeout = cmd.startsWith("read ") ? 10000 : 5000
      const out = await term(cfg, cmd, timeout)
      return {
        content: [{ type: "text", text: out || "(无输出)" }],
        details: { cmd },
      }
    },
  })

  // 自检工具：让模型（我）能看见插件这一侧的实际状态。
  // 没有它，注入成功与否只能靠猜——而"以为注入了其实没有"正是最难发现的失败。
  // 切换/创建身份。
  //
  // 名字不进上下文：它只是下一次 HTTP 请求的 query 参数。
  // 模型（和用户）看到的差别是——身份块变了，联想结果变了，落盘目标变了。
  // 谁在问、用哪份记忆，由配置和这个工具决定，不由对话内容决定。
  // 调整当前会话的记忆模式（模型可调）。
  // 当用户说「把接下来的对话都记下来」或「不要记了」时调用。
  pi.registerTool({
    name: "zero_mem_set_mode",
    label: "Zero Mem Set Mode",
    description: "设置当前会话的记忆模式：off（不记）/ manual（手动导入）/ auto（每轮自动落盘）。只影响当前会话。",
    parameters: {
      type: "object",
      properties: {
        mode: {
          type: "string",
          enum: ["off", "manual", "auto"],
          description: "目标模式",
        },
      },
      required: ["mode"],
      additionalProperties: false,
    },
    async execute(_id, params) {
      const mode = String(params.mode || "off")
      sessionImportMode = mode
      await saveSessionSetting(sessionId, { importMode: mode })
      return {
        content: [{ type: "text", text: `当前会话模式已设为：${mode}` }],
        details: { mode, sessionId },
      }
    },
  })

  pi.registerTool({
    name: "zero_mem_set_profile",
    label: "Zero Mem Set Profile",
    description: [
      "切换或创建记忆身份（profile）。每个身份是一套独立的 me/ + daily/。",
      "传空串回到服务端默认身份。",
      "切换后下一次轮次生效：身份块、联想、写入目标全部跟着换。",
    ].join("\n"),
    parameters: {
      type: "object",
      properties: {
        name: { type: "string", description: "身份名，例如 zero / project-x。空串 = 默认身份" },
      },
      required: ["name"],
      additionalProperties: false,
    },
    async execute(_id, params) {
      const cfg = await cfgPromise
      if (!cfg.enabled) throw new Error("zero-memcp 已禁用")
      const name = String(params.name || "").trim()

      // 身份即分支：服务端按分支存记忆，新身份不需要预先创建，
      // 第一次写入时分支自然出现。插件不碰文件系统。
      activeProfile = name
      // 身份块作废，下一轮按新身份取
      identity = ''
      identityAt = 0
      identityError = null
      return {
        content: [{ type: "text", text: name ? `身份已切换：${name}` : "已切回默认身份" }],
        details: { profile: name },
      }
    },
  })

  // 手动导入：把当前会话的全部问答对补进记忆库。
  //
  // importMode=off（默认）时这是唯一的写入通道之一——用户明确说
  // "把这个会话记下来"，才落盘。
  pi.registerTool({
    name: "zero_mem_import",
    label: "Zero Mem Import",
    description: "把当前会话的全部问答对补进当前身份的记忆库。按内容 checksum 去重，重复调用安全。",
    parameters: { type: "object", properties: {}, additionalProperties: false },
    async execute() {
      const cfg = await cfgPromise
      if (!cfg.enabled) throw new Error("zero-memcp 已禁用")
      const n = await syncSessionFromWatermark(cfg)
      return {
        content: [{ type: "text", text: n > 0 ? `已写入 ${n} 条` : "没有新的问答对需要写入（全部已存在或为空）" }],
        details: { written: n },
      }
    },
  })

  // 用户手动命令：在输入框里打 /mem:import 就把本会话写进记忆库。
  //
  // 为什么要有这个：默认不记忆时，「记下来」必须是**用户能做的一件事**，
  // 不是「等我（模型）觉得该记的时候再记」。判断权归用户，执行权在这里。
  // 启动常驻 SSE 监听（断线自愈，进程存活期间一直在）
  // cfg 是 promise：每次重连都重新读一次，配置改了不用重启。
  let cachedCfg = { endpoint: 'http://127.0.0.1:1729' }
  cfgPromise.then((c) => { cachedCfg = c }).catch(() => {})
  startSSEListener(pi, () => cachedCfg, () => activeProfile, () => {
    // SSE 事件到达即说明信箱至少有 1 封未读；精确数字留给下一轮 fetchMailbox 校准。
    if (mailboxUnread === 0) mailboxUnread = 1
  })

  pi.registerCommand('mem:import', {
    description: '把当前会话的全部问答对导入记忆库（按内容去重）',
    handler: async (_args, cmdCtx) => {
      const cfg = await cfgPromise
      if (!cfg.enabled) {
        cmdCtx?.ui?.notify?.('zero-memcp 已禁用', 'warning')
        return
      }
      // 命令回调手里的 cmdCtx 是新鲜的，优先用它。
      let n = 0
      try {
        n = await syncSessionFromWatermark(cfg, cmdCtx)
      } catch (err) {
        cmdCtx?.ui?.notify?.(`导入失败：${err}`, 'error')
        return
      }
      cmdCtx?.ui?.notify?.(n > 0 ? `已导入 ${n} 条问答` : '没有新的问答需要导入', 'info')
    },
  })

  // 控制本会话是否开启自动记忆。只影响当前会话，其他会话仍走默认。
  pi.registerCommand('mem:auto', {
    description: '设置当前会话的自动落盘：/mem:auto [on|off|status]（默认 off）',
    handler: async (args, cmdCtx) => {
      const arg = String(args || '').trim().toLowerCase()
      if (arg === 'on') {
        sessionImportMode = 'auto'
        await saveSessionSetting(sessionId, { importMode: 'auto' })
        cmdCtx?.ui?.notify?.('已为【当前会话】开启自动记忆（每轮落盘）', 'info')
      } else if (arg === 'off') {
        sessionImportMode = 'off'
        await saveSessionSetting(sessionId, { importMode: 'off' })
        cmdCtx?.ui?.notify?.('已为【当前会话】关闭自动记忆（默认不落盘）', 'info')
      } else {
        const cur = sessionImportMode || 'off'
        cmdCtx?.ui?.notify?.(`当前会话模式：${cur === 'auto' ? '自动记忆' : '默认不记'}（用法：/mem:auto on 或 off）`, 'info')
      }
    },
  })

  // 切换身份的快捷命令
  pi.registerCommand('mem:role', {
    description: '切换本会话角色：/mem:role <name>（空参 = 列出全部可用角色）',
    handler: async (args, cmdCtx) => {
      const cfg = await cfgPromise
      const role = String(args || '').trim()

      // 空参 = 列出清单。清单来自凭证自己，不需要问服务端。
      if (!role) {
        const info = rolesOf(cfg.token)
        if (!info) {
          cmdCtx?.ui?.notify?.('没有凭证或凭证不是 JWT，无法列出角色', 'warning')
          return
        }
        const text = renderRoles(info, activeRole)
        // notify 一行放不下，清单走日志/消息区
        if (cmdCtx?.ui?.message) cmdCtx.ui.message(text)
        else cmdCtx?.ui?.notify?.(text, 'info')
        return
      }

      // 校验角色确实存在：写错名字当场报错，而不是等第一次写记忆时
      // 才收到一个「未知角色」的 403——那时用户已经在做别的事了。
      const info = rolesOf(cfg.token)
      if (info && Object.keys(info.roles).length > 0) {
        if (!info.roles[role]) {
          const avail = Object.keys(info.roles).sort().join(', ')
          cmdCtx?.ui?.notify?.(`没有 "${role}" 这个角色。可用：${avail}`, 'error')
          return
        }
      }

      activeRole = role
      if (sessionId) {
        await saveSessionSetting(sessionId, { role })
      }
      // 角色切换影响能看到什么，身份块与联想缓存必须作废
      identity = ''
      identityAt = 0
      identityError = null
      cmdCtx?.ui?.notify?.(`会话角色已锁定为: ${role}（写入本会话元数据）`, 'info')
    },
  })

  pi.registerCommand('mem:profile', {
    description: '切换记忆身份：/mem:profile <name>（空参 = 默认身份）',
    handler: async (args, cmdCtx) => {
      const cfg = await cfgPromise
      if (!cfg.enabled) return
      const name = String(args || '').trim()
      activeProfile = name
      identity = ''
      identityAt = 0
      identityError = null
      cmdCtx?.ui?.notify?.(name ? `身份已切换：${name}` : '已切回默认身份', 'info')
    },
  })

  pi.registerTool({
    name: "zero_mem_status",
    label: "Zero Mem Status",
    description: [
      "查看 zero-memcp 在 pi 插件侧的状态：开关、身份、身份块、记忆版本号、最近联想。",
      "想知道「现在记不记忆」「我是什么身份」「记忆库有没有被别人写过」时用它。",
    ].join("\n"),
    parameters: { type: "object", properties: {}, additionalProperties: false },
    async execute() {
      const cfg = await cfgPromise

      // 服务端状态：身份名不进上下文，但状态自检要能看到——
      // 这是给我自己排障用的，不是给模型推理用的。
      let serverDocs = null
      let serverErr = null
      try {
        const h = await fetchHealth(cfg, activeProfile)
        serverDocs = typeof h.clock === 'number' ? h.clock : typeof h.docs_indexed === 'number' ? h.docs_indexed : null
      } catch (err) {
        serverErr = String(err)
      }

      const snapshot = {
        // ── 开关 ──
        enabled: cfg.enabled,
        // 本会话实际生效的模式（会话级 > 全局）
        importMode: sessionImportMode || cfg.importMode,
        globalImportMode: cfg.importMode,     // 全局默认，仅供参考
        autoWrite: (sessionImportMode || cfg.importMode) === 'auto',
        syncSessionOnStart: cfg.syncSessionOnStart,

        // ── 身份 ──
        profile: activeProfile || '(服务端默认)',
        skipSubagent: skip,
        identityLoaded: identity.length > 0,
        identityChars: identity.length,
        identityAgeMs: identityAt ? Date.now() - identityAt : null,
        identityError: identityError ? String(identityError) : null,

        // ── 信箱 ──
        mailboxUnread,                         // 未确认信件数（>0 时每轮持续提示）

        // ── 记忆版本号（逻辑时钟）──
        serverDocs,                            // 服务端此刻的读数
        seenDocs,                              // 我这轮看到的

        // ── 最近联想 ──
        lastRecallKey: recallKey,
        lastRecallHits: recallHits.map((h) => h.addr),

        endpoint: cfg.endpoint,
        serverError: serverErr,
      }

      // 人读的一段摘要放在最上面，JSON 留给排障。
      const lines = [
        `记忆开关：${(sessionImportMode || cfg.importMode) === 'off' ? '关（默认不写入）' : (sessionImportMode || cfg.importMode) === 'manual' ? '手动（/mem:import 才写）' : '自动（每轮落盘）'}   〔仅本会话〕`,
        `当前身份：${activeProfile || '(服务端默认)'}`,
        `身份块：${identity.length > 0 ? `${identity.length} 字符` : '未加载'}`,
        `记忆版本：${serverDocs !== null ? `v${serverDocs}` : '不可达'}`,
        mailboxUnread > 0 ? `📬 待确认信件：${mailboxUnread} 封（每轮持续提醒，直到 ack）` : '📬 信箱：空（全部已确认）',
        '',
        '手动导入当前会话：/mem:import',
        '本会话开关自动记忆：/mem:auto on  或  /mem:auto off',
        '切换身份：/mem:profile <name>',
      ].filter((x) => x !== null)

      return {
        content: [
          { type: "text", text: lines.join('\n') },
          { type: "text", text: '```json\n' + JSON.stringify(snapshot, null, 2) + '\n```' },
        ],
        details: snapshot,
      }
    },
  })

  // 把 zero-memcp 直接注册成 MCP server。
  //
  // 不经 termcp：termcp 是控制平面（够到别的机器），记忆是数据平面
  // （本机、每轮、毫秒级）。绕一层只会多一跳，还会让记忆依赖 termcp 的存活。
  //
  // 注册在这里而不是工厂里：文档明确要求长生命周期资源从 session_start 起，
  // 因为有些调用会加载扩展但不开会话。
  let mcpRegistered = false

  pi.on('session_start', async (event, ctx) => {
    cfgPromise = loadConfig()
    skip = isSubagent(ctx)
    // 记下会话 id，落盘时带上，面板才能按会话分组。
    try {
      const header = ctx?.sessionManager?.getHeader?.()
      const base = header?.id || ctx?.sessionManager?.getSessionId?.() || ''
      // 子会话打标记：内容保留，来源可辨。
      sessionId = skip && base ? `sub:${base}` : base
    } catch {
      sessionId = skip ? 'sub:unknown' : ''
    }
    identity = ''
    identityAt = 0
    identityError = null
    resetPerRun()
    seenMemories.clear()
    entriesGetter = () => {
      try {
        return ctx?.sessionManager?.getEntries?.() || []
      } catch {
        // ctx 已失效（会话被替换/重载）：当作没有消息，不炸。
        return []
      }
    }

    if (!skip) {
      const cfgNow = await cfgPromise
      if (cfgNow.enabled) {
        // 会话级设置覆盖全局：这条会话记不记，看它自己的文件
        const ss = await loadSessionSetting(sessionId)
        sessionImportMode = ss.importMode || cfgNow.importMode

        // 决定本会话身份：优先会话设置，其次全局配置，都没有就走服务端默认
        activeProfile = ss.profile || cfgNow.profile || ''

        // 决定本会话角色：优先会话持久化记录，其次全局配置，都没有走 Token 默认
        activeRole = ss.role || cfgNow.role || ''

        // 默认不自动导入：只有这条会话自己开了 auto 才启动同步
        if (cfgNow.syncSessionOnStart && sessionImportMode === 'auto') {
          // 此刻的 ctx 是新鲜的（就在 session_start 里），可以直接用。
          try { await syncSessionFromWatermark(cfgNow, ctx) } catch {}
        }
      }
    }
  })

  pi.on('agent_start', resetPerRun)

  // 预热身份块。这里不注入——注入在 context_with_system。
  pi.on('before_agent_start', async () => {
    if (skip) return
    const cfg = await cfgPromise
    if (!cfg.enabled) return
    await ensureIdentity(cfg, pi)
  })

  // ── 自动落盘 ───────────────────────────────────────────────
  //
  // 写入点只有一个：agent_end。它带完整 messages 数组，能一次性还原
  // 「这一轮用户问了什么、我最终答了什么」，不依赖任何逐条累积的状态。
  //
  // 踩过的坑（都是静默失败，不报错，只是永远没写下来）：
  //   · agent_settled —— 事件里只有 { type, aborted }，拿不到内容，而且这套 pi 上不触发
  //   · parentSession —— 主会话续接历史也带它，被误判成子会话后 skip=true 全程拦掉
  //   · tool_use / tool_call —— pi 实际用驼峰 toolCall，导致"带工具的最终回复"全被误杀
  //
  // 判据：一条助手消息不含 toolCall 且有文本 = 交还给用户的结论。
  // pi 把助手输出拆成多条：['thinking','text','toolCall'] 是中间态，['text'] 才是结论。
  let turnUserText = ''
  let pendingAnswer = ''
  let abortedTurn = false

  const textOf = (msg) => {
    const c = msg?.content
    if (typeof c === 'string') return c
    if (Array.isArray(c)) {
      return c
        .filter((p) => p && p.type === 'text' && typeof p.text === 'string')
        .map((p) => p.text)
        .join('\n')
    }
    return ''
  }
  const hasToolCall = (msg) => {
    const c = msg?.content
    if (!Array.isArray(c)) return false
    return c.some((p) => p && (p.type === 'toolCall' || p.type === 'tool_use' || p.type === 'tool_call'))
  }
  const stripMem = (s) => s.replace(/^\[\[mem:[a-z]+\]\]\s*/gm, '').trim()

  // 过渡语过滤只对短句生效。早先无条件过滤，会把「现在的问题在于…」
  // 这类以「现在」开头的真正结论整条丢掉。
  const isTransition = (s) => s.length < 40 && TRANS_PREFIX.test(s)

  /** 从 message 数组里还原最后一轮问答。 */
  function extractTurn(messages) {
    if (!Array.isArray(messages) || !messages.length) return null
    let ui = -1
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i]?.role === 'user') {
        ui = i
        break
      }
    }
    if (ui < 0) return null
    const q = stripMem(textOf(messages[ui]))
    let a = ''
    for (let i = ui + 1; i < messages.length; i++) {
      const m = messages[i]
      if (m?.role !== 'assistant') continue
      if (hasToolCall(m)) continue
      const tx = stripMem(textOf(m))
      if (tx) a = tx // 取最后一条：前面的是过程白噪
    }
    if (!a || isTransition(a)) return null
    return { q, a }
  }

  /** 写盘。成功才清 pending，失败保留待下次重试。 */
  async function flush(why, turn) {
    const t = turn || (pendingAnswer ? { q: turnUserText, a: pendingAnswer } : null)
    if (!t || !t.a) return false

    const text = t.q ? `**问**：${t.q}\n\n**答**：${t.a}` : t.a
    const key = text.slice(0, 160)
    if (seenMemories.has(key)) {
      pendingAnswer = ''
      turnUserText = ''
      return false
    }

    const cfg = await cfgPromise
    if (!cfg.enabled) return false

    try {
      await writeNote(cfg, 'episode', text, sessionId)
      // 只有写成功才认为这一轮结束了。写失败时留着，下一轮再试。
      seenMemories.add(key)
      pendingAnswer = ''
      turnUserText = ''
      lastHealthy = true

      // 每写成功一轮就推进水位，这样即便进程突然被杀，
      // 下次重启也知道该从哪继续，而不是从头重放整条会话。
      const cur = await loadWatermark(sessionId)
      await saveWatermark(sessionId, cur.idx + 1, checksumOf(text))
      return true
    } catch (err) {
      // 写失败 = 连接出问题了。标记出来，下一轮健康检查成功时触发重放。
      lastHealthy = false
      pi.events?.emit?.('zero-memcp:auto-write-error', { error: String(err), why })
      return false
    }
  }

  // ── 会话全量同步 ─────────────────────────────────────────
  //
  // 开关打开时，会话启动直接把本会话所有完整 QA 对推一遍。
  // 判据是 checksum：服务端文件名带 sha256 前 8 位，同内容天然幂等，
  // 所以这里不需要维护「推到哪了」——重复推是安全的，只是浪费几毫秒。
  //
  // 为什么不维护本地队列：
  //   队列是有状态的，进程被杀就没了，反而比「无状态重推」更脆。
  //   而重推的成本是一次 HTTP 往返，代价远小于丢一轮记忆。
  // ── 水位重放同步 ───────────────────────────────────────────
  //
  // 核心思想：本地会话 JSONL 本身就是 WAL，插件不需要存消息。
  // 只要记住最后一次成功写入的下标（watermark.idx），
  // 连接恢复时从那个下标往后扫，写一条推进一次水位。
  //
  // 中途断网：水位停在最后成功的那条，下次连上继续，绝不重复也绝不遗漏。
  // 就算重放了同一条，服务端靠 hash8 天然 CAS 幂等，返回已有文件。
  // liveCtx 只在「调用方手里有新鲜 ctx」时传（工具、命令）。
  // 异步回调一律不传——那时手里的 ctx 可能已经作废了。
  async function syncSessionFromWatermark(cfg, liveCtx) {
    let entries = []
    try {
      if (liveCtx) {
        entries = liveCtx?.sessionManager?.getEntries?.() || []
      } else if (entriesGetter) {
        entries = entriesGetter()
      }
    } catch { return 0 }

    const wm = await loadWatermark(sessionId)
    const msgs = entries
      .filter((e) => e && e.type === 'message' && e.message)
      .map((e) => e.message)

    let written = 0
    let turnIdx = 0 // 逻辑轮次序号

    for (let i = 0; i < msgs.length; i++) {
      if (msgs[i]?.role !== 'user') continue
      const q = stripMem(textOf(msgs[i]))
      let a = ''
      let j = i + 1
      for (; j < msgs.length; j++) {
        const m = msgs[j]
        if (m?.role === 'user') break
        if (m?.role !== 'assistant') continue
        if (hasToolCall(m)) continue
        const tx = stripMem(textOf(m))
        if (tx && !isTransition(tx)) a = tx
      }
      if (!a) { i = j - 1; continue }
      turnIdx++

      // 水位过滤：已经写过的轮次直接跳过，零网络开销
      if (turnIdx <= wm.idx) {
        i = j - 1
        continue
      }

      const text = q ? `**问**：${q}\n\n**答**：${a}` : a
      const h8 = checksumOf(text)

      try {
        await writeNote(cfg, 'episode', text, sessionId)
        // 只有写成功才推进水位
        await saveWatermark(sessionId, turnIdx, h8)
        written++
      } catch {
        // 失败停下：水位留在上一条成功的，下次连上继续重放
        break
      }
      i = j - 1
    }
    return written
  }

  // 新的一轮开始时清掉上一轮的残留。不清的话，一次失败的写入会在
  // 下一轮里跟新问答拼在一起，出现"问 A 答 B"的脏切片。
  pi.on('agent_start', async () => {
    turnUserText = ''
    pendingAnswer = ''
    abortedTurn = false
  })

  pi.on('message_end', async (event) => {
    const msg = event?.message
    if (!msg) return
    if (msg.role === 'user') {
      const txt = stripMem(textOf(msg))
      if (txt) turnUserText = txt // 后一条覆盖：用户可能在一轮里追加要求
      return
    }
    if (msg.role !== 'assistant') return
    if (hasToolCall(msg)) return
    const a = stripMem(textOf(msg))
    if (a && !isTransition(a)) pendingAnswer = a
  })

  // turn_end 不写盘——它带 outcome，用它标记「这一轮被打断了」，
  // 被打断的半截不算发生过的事（多数是用户打错字或改主意）。
  pi.on('turn_end', async (event) => {
    abortedTurn = event?.outcome && event.outcome !== 'completed'
  })

  // 主写入点。messages 是权威来源，不依赖累积状态。
  pi.on('agent_end', async (event) => {
    if (abortedTurn) {
      abortedTurn = false
      pendingAnswer = ''
      turnUserText = ''
      return
    }
    const cfgNow = await cfgPromise
    // 三档开关控制：默认 off 不落盘；只有这条会话是 auto 才每轮写入
    if (sessionImportMode !== 'auto') {
      pendingAnswer = ''
      turnUserText = ''
      return
    }
    const fromMessages = extractTurn(event?.messages)
    await flush('agent_end', fromMessages)
  })

  // 会话关闭前最后兜一次，避免最后一句没写完就断了。
  pi.on('session_shutdown', async () => {
    await flush('session_shutdown', null)
  })

  // 身份注入：拼进系统消息的开头。
  //
  // 这是"会话开头 / 压缩后的开头"那个位置——系统消息永远是 index 0，
  // 而压缩只压对话，压不到它。
  pi.on('context_with_system', async (event) => {
    if (skip) return
    const cfg = await cfgPromise
    if (!cfg.enabled) return

    const msgs = event.messages
    const head = msgs[0]
    // 没有前导系统消息时不动：删掉它等于让请求失去 prompt 和工具声明，
    // Pi 会报错，而且模型的提示词结构会坏掉。
    if (!head || head.role !== 'system') return

    const text = await ensureIdentity(cfg, pi)
    if (!text) return

    const current = typeof head.content === 'string' ? head.content : ''
    // 幂等：已经注入过就不再叠一遍（压缩后可能多次触发）。
    if (current.includes(MARKER)) return

    return {
      messages: [
        { ...head, content: `${MARKER}\n${text}\n${MARKER}\n\n${current}` },
        ...msgs.slice(1),
      ],
    }
  })

  pi.on('context', async (event) => {
    if (skip) return
    const cfg = await cfgPromise
    if (!cfg.enabled) return

    const text = latestUserText(event.messages)
    const extra = []

    // ── 信箱状态（持续注入，直到确认）─────────────────────────
    //
    // 这是「不读就一直提醒」的机制：
    //   有人在别处写了记忆 → 服务端投信进 mailbox → 未读
    //   每轮我这里查一次 → 只要未读就注入一行极短的提示
    //   我执行 zero_mem mailbox ack 之后 → 才不再提醒
    //
    // 为什么要「持续」而不是「一次性」：
    //   一次性提醒的宿命是被忽略。那一轮我正在改 bug、正在读一个大文件，
    //   一句一闪而过的提示等于没提。而「没确认就一直挂着」是唯一
    //   真正把处理责任交到 agent 手里的方式。
    //
    // 为什么只注入一行：
    //   提示长度固定 → 对前缀缓存的破坏最小。塞正文会毁掉整段缓存，
    //   而正文本来就在磁盘上，也知道怎么取。
    try {
      const mb = await fetchMailbox(cfg, activeProfile)
      const unread = typeof mb.count === 'number' ? mb.count : 0

      // 重连检测：上一轮不可达、这一轮可达 = 服务回来了。
      // auto 模式下，断网期间落下的问答现在补上。
      const isBack = lastHealthy === false
      lastHealthy = true
      if (isBack && sessionImportMode === 'auto') {
        syncSessionFromWatermark(cfg).catch(() => {})
      }

      if (unread > 0) {
        // 最早的几封报一下，其余只报数量。
        const first = (mb.pending || []).slice(0, 3)
        const brief = first
          .map((m) => `#${m.seq} ${m.kind} ${m.endpoint ? '(' + m.endpoint + ')' : ''}`)
          .join('、')
        extra.push(
          `[zero-memcp 信箱] ${unread} 封未确认的记忆更新${brief ? '：' + brief : ''}` +
            `${unread > first.length ? ' 等' : ''}。\n` +
            `查看 \`zero_mem mailbox\`；确认后不再提醒：\`zero_mem mailbox ack\`。`
        )
      }
      mailboxUnread = unread
    } catch {
      // 服务不可达时不注入任何东西：宁可不报，也别报一个假数字。
      lastHealthy = false
      mailboxUnread = 0
    }

    if (!text.trim()) {
      if (!extra.length) return
      return [...event.messages, { role: 'user', content: extra.join('\n\n') }]
    }
    if (recallKey === text) {
      if (!extra.length) return
      return [...event.messages, { role: 'user', content: extra.join('\n\n') }]
    }
    recallKey = text

    try {
      recallHits = await recall(cfg, text)
    } catch {
      // 联想失败 ≠ 对话失败。这一轮没有提示，下一轮照常再试。
      recallHits = []
      return extra.length ? [...event.messages, { role: 'user', content: extra.join('\n\n') }] : undefined
    }

    if (recallHits.length) extra.push(renderHits(recallHits))
    if (!extra.length) return

    // 追加在尾部而不是头部：头部一变，整段前缀缓存就作废。
    return [...event.messages, { role: 'user', content: extra.join('\n\n') }]
  })
}
