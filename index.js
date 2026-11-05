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

/** 插件版本号。由 status 命令报出，排查版本时唯一不需要猜的依据。 */
const VERSION = '0.2.0'

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

// ── SSE 实时推送：clock 与 tree ──
//
// 服务端只广播两类事件（见 internal/server/sse.go 的 eventAllowed）：
//   clock  逻辑时钟跳变：有人在别处写了东西
//   tree   子树提交推进：带 previous / commit / tree / paths
//
// 没有 mail、没有 mailbox。记忆库是 Git 树，不是消息队列；
// 变更感知靠 tree 事件与 remind 锚定，不靠轮询未读数。
//
// 三条克制规则：
//   1. agent 正在思考（busy）时绝不打断，变化留给下一轮 context；
//   2. 两次唤醒至少间隔 3 秒；
//   3. 掉线指数退避重连（1s → 2s → … → 30s 封顶）。
// isBusy 由调用方提供，监听器本身不注册任何 pi 事件。
//
// 踩过的坑：这里原本在函数内 pi.on('agent_start'/'agent_end')。
// 而 restartSSE() 每次换身份都会重新调用本函数，于是每换一次身份
// 就多注册一对 handler，agent_end 的处理链越攒越长——
// 表现是同一轮问答被写多遍、以及最后一个 handler 抢走了本该落盘的调用。
// 注册事件必须只发生一次，所以挪到外面。
function startSSEListener(pi, getCfg, onTreeEvent, isBusy) {
  let abortCtrl = null
  let retryDelay = 1000
  let closed = false

  async function connect() {
    if (closed) return
    // getCfg 返回 Promise：不 await 会拿到一个没有 enabled 字段的 Promise，
    // 结果是永远不连、也不报错。
    let cfg = null
    try { cfg = await getCfg() } catch { cfg = null }
    if (!cfg || !cfg.enabled) {
      setTimeout(connect, 5000)
      return
    }
    const url = `${cfg.endpoint}/events`

    abortCtrl = new AbortController()
    let connected = false
    try {
      const res = await fetch(url, {
        headers: authHeaders(cfg, { Accept: 'text/event-stream' }),
        signal: abortCtrl.signal,
      })
      if (!res.ok) throw new Error(`SSE ${res.status}`)
      connected = true
      retryDelay = 1000
      // 默认静默：SSE 连上是常态，打到 stdout 会在 TUI 输入框上方
      // 刷一行噪音。需要排障时设 ZERO_MEMCP_DEBUG=1，日志走 stderr。
      if (process.env.ZERO_MEMCP_DEBUG) {
        process.stderr.write(`[zero-memcp] SSE 已连接 ${url}\n`)
      }

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
          if (evName === 'tree' && dataStr) {
            try { onTreeEvent(JSON.parse(dataStr), isBusy()) } catch {}
          }
        }
      }
    } catch (err) {
      if (err && err.name === 'AbortError') return
    }
    // 连接结束（无论成功与否）都要重连；但 abort 与 closed 不再重连，
    // 否则一个插件实例会越攒越多条 SSE 连接。
    if (!closed && abortCtrl && !abortCtrl.signal.aborted) {
      setTimeout(connect, retryDelay)
      retryDelay = Math.min(retryDelay * 2, 30000)
    }
  }

  connect()

  return () => {
    closed = true
    if (abortCtrl) abortCtrl.abort()
  }
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
  // 分层记忆：按优先级排列的作用域列表，例如
  //   ["zero-agi@fact", "zero-agi@record", "zero-agi@thought"]
  //
  // 为什么是列表而不是一个 scope：记忆有层，层的价值不同。事实层是已经
  // 提炼过的结论，记录层是发生过什么，想法层是推理过程。一次查询不该
  // 平等地翻三层——上层有答案时，下层的正文只是噪声，还会把 top-N 名额
  // 从真正的结论上挤走。
  //
  // 顺序就是优先级，用 '||' 连成一条回落链：上层查到了就停，
  // 查不到才付下一层的代价。空列表表示不分层，走单次全局检索。
  recallScopes: [],
  // 分层回落时每层最多取几条。比 recallLimit 略大，因为落空是常态。
  recallScopeLimit: 3,
  // 分层落点：不同性质的内容写进不同的分支。
  //
  // 为什么写入也要分层：读的时候按可信度分了三层，写的时候却全塞进一个
  // 平铺目录，那这套分层就只对历史数据成立，新对话立刻退化成一锅粥。
  // 读取的价值取决于写入时有没有保住「这是结论 / 这是原话 / 这是推演」
  // 这个区别。
  //
  //   recordScope：一问一答（默认落点）
  //   factScope  ：compaction 摘要——Pi 压缩时产出的提炼结论
  //
  // 留空表示不分层，走 put 的默认落点（由服务端的 stream 策略决定）。
  recordScope: '',
  factScope: '',
  // thoughtScope：推理链落点。留空表示不记录思考。
  //
  // 体积是问答的 4~5 倍（历史数据里 thinking 占全部正文的 71.7%），
  // 所以它排在回落链最后一环：只有事实层和记录层都没答案时才会被翻出来，
  // 不占日常召回的成本。
  thoughtScope: '',
  // 分层回落的总超时。比单次 recall 宽松：一次三层回落最多要做三次
  // 检索，200ms 会把正常查询误判成超时（超时=静默丢弃，看起来像没记忆）。
  recallLayerTimeoutMs: 2000,
  enabled: true,
  // 身份块每小时最多重取一次；它们几乎不变，变了也是新会话该看的事。
  identityTtlMs: 60 * 60 * 1000,
  // 默认不记忆。记忆是权利不是义务：默认状态是我听完就忘，
  // 用户明确说「记下来」才写。想常驻自动落盘，配置里改成 auto。
  syncSessionOnStart: false,
  // 记录模式：off（默认，不自动写）/ auto（每轮自动写）
  //
  // 只有两档。曾经有过第三档 manual（只手动同步），但它和 off 在代码里
  // 没有区别——/mem:sync 根本不检查模式。留着只会让人问「这俩有啥不同」。
  // 想临时记一次，任何时候敲 /mem:sync 就行，不需要一个专门档位。
  importMode: 'off',
  // 身份挂载：off（默认关闭，保持普通编码助手）/ on（新会话默认挂载身份）
  //
  // 为什么默认关：机器上有很多普通会话，不是每个都属于某个身份。
  // 默认挂载会把所有对话都当成那个身份的，既污染上下文也污染记忆库。
  //
  // 可以通过 /mem:identity on 随时为本会话挂载，
  // 也可以通过 /mem:identity global on 开启全局默认。
  identity: 'off',
  // 身份（凭证名）：指向 tokens 表里的哪一张凭证。
  //
  // 身份由凭证决定，不由请求头决定：服务端一张凭证就是一个身份，
  // 它的能力与挂载范围在签发时定死。想换视角就换一张凭证——
  // 就像换一个数据库连接，而不是给同一个连接发一个"我现在是谁"的 header。
  //
  // 空表示用下面这张默认 token。
  identityName: '',
  // 默认凭证：服务端要求鉴权时必填。
  token: '',
  // 具名凭证表：{ "名字": "JWT" }。
  //
  // 一个身份一张凭证。服务端签发时就把能力与挂载范围写死了，
  // 这里只是把它们收在一处，方便按名字切换。
  tokens: {},
  // 锚定订阅：逗号分隔的树地址，例如 "daily@zero,notes@zero/team"。
  //
  // 服务端的 tree 事件只推给 remind 锚定过的人（eventAllowed 里
  // treeWatch.Matches 就是授权本身）。不锚定 = 收不到，且不报错。
  // 所以这里必须显式声明要看哪几棵树，而不是"全都推给我"。
  watch: '',
}

async function loadConfig() {
  try {
    return { ...DEFAULTS, ...JSON.parse(await readFile(CONFIG_FILE, 'utf8')) }
  } catch {
    return { ...DEFAULTS }
  }
}

/** 修改并写回全局 config.json */
async function saveGlobalConfig(patch) {
  try {
    let cur = {}
    try {
      cur = JSON.parse(await readFile(CONFIG_FILE, 'utf8'))
    } catch {}
    const updated = { ...cur, ...patch }
    await mkdir(join(homedir(), '.zero-memcp'), { recursive: true })
    await writeFile(CONFIG_FILE, JSON.stringify(updated, null, 2) + String.fromCharCode(10), 'utf8')
    return updated
  } catch (err) {
    throw new Error(`保存全局配置失败: ${err.message}`)
  }
}

/**
 * 当前会话锁定的身份名（对应 tokens 表里的键）。
 *
 * 为什么是模块级变量而不是每次现取：身份在会话开始时定一次，
 * 之后整个会话都用它。中途能改的话，「我这条记忆是用哪个身份写的」
 * 就没有确定答案了。
 */
let activeIdentityName = ''

/**
 * 取当前身份对应的分层落点与召回范围。
 *
 * 优先级：
 *   1. cfg.personas[activeName]  显式分身配置（权威）
 *   2. cfg.recordScope 等全局字段（单身份时代的旧配置兼容）
 *
 * 为什么不能全放全局字段：全局字段只有一份，而会话会换身份（/mem:identity zero）。
 * 切了身份但落点没切，结果就是 zero 拿着 zero 的凭证往 weft@main.record 写——
 * 服务端挂载表当场以 missing w permission 拒绝，写入静默失败，
 * 表现是「简的对话一篇都没记下来」。
 *
 * 每个分身必须自成一套完整的认知闭环：
 *   - 自己的一问一答落到自己的 record
 *   - 自己的思维链落到自己的 thought
 *   - 自己的召回链路优先翻自己的记忆，其次才是跨分身只读层
 */
function scopesFor(cfg, identityName) {
  const name = identityName || activeIdentityName || cfg?.identityName || ''
  const p = (cfg?.personas && name && cfg.personas[name]) || null
  if (p) {
    return {
      persona: name,
      recordScope: p.recordScope || '',
      thoughtScope: p.thoughtScope || '',
      factScope: p.factScope || '',
      recallScopes: Array.isArray(p.recallScopes) ? p.recallScopes.filter(Boolean) : [],
    }
  }
  // 旧单身份配置兼容
  return {
    persona: name || 'default',
    recordScope: cfg?.recordScope || '',
    thoughtScope: cfg?.thoughtScope || '',
    factScope: cfg?.factScope || '',
    recallScopes: Array.isArray(cfg?.recallScopes) ? cfg.recallScopes.filter(Boolean) : [],
  }
}

/** 取当前生效的凭证：具名凭证优先，否则默认凭证。 */
function tokenOf(cfg) {
  if (!cfg) return ''
  const name = activeIdentityName || cfg.identityName || ''
  if (name && cfg.tokens && cfg.tokens[name]) return cfg.tokens[name]
  return cfg.token || ''
}

/**
 * 从 JWT 里解出这张凭证自述的能力与挂载范围。
 *
 * 为什么在客户端解而不是问服务端：payload 本来就是明文（base64url），
 * 签名只保证它没被改过，不保证它保密。它是「这张凭证能做什么」的
 * 自我描述，让用户看见它不需要一次网络往返——而**看得见**很重要：
 * 一个说不清自己权限的系统，用户只会一直用最宽的那个。
 *
 * 只解码不验签是安全的：这里不做任何授权判断，
 * 真正的判据永远在服务端。伪造的 payload 只会让它自己看到一份假描述。
 */
function credentialInfo(token) {
  if (!token || typeof token !== 'string') return null
  const parts = token.split('.')
  if (parts.length !== 3) return null
  try {
    let p = parts[1].replace(/-/g, '+').replace(/_/g, '/')
    while (p.length % 4) p += '='
    const payload = JSON.parse(Buffer.from(p, 'base64').toString('utf8'))
    return {
      subject: payload.sub || '',
      cap: payload.cap || '',
      mount: payload.mount || [],
    }
  } catch {
    return null
  }
}

/**
 * 校验一张凭证能不能写进一个 scope。
 *
 * 为什么要交叉校验：personas 表和凭证挂载表是同一件事的两个说法——
 * 「这个人格的记忆存在哪」与「这张票能写哪」。它们不一致时，
 * 写入会在服务端被拒，而且是被 catch 吃掉、不报错的静默失败。
 * 与其等写入失败后再去查日志，不如挂载身份的那一刻就说清楚。
 *
 * 返回空数组表示通过，否则每个元素是一条人话错误。
 */
function validatePersonaScope(cfg, name) {
  const problems = []
  const token = (cfg.tokens && cfg.tokens[name]) || ''
  const info = credentialInfo(token)
  if (!info) {
    if (token) problems.push(`tokens.${name} 不是可解析的 JWT`)
    else problems.push(`tokens 里没有 ${name}`)
    return problems
  }

  const p = cfg.personas && cfg.personas[name]
  if (!p) {
    problems.push(`personas 里没有 ${name}：这个身份没有声明自己的记忆落在哪，写入会被服务端拒绝`)
    return problems
  }

  // 挂载表可能为空（root 凭证 = 全命名空间），此时不做限制。
  const mounts = Array.isArray(info.mount) ? info.mount : []
  if (!mounts.length) return problems

  const canWrite = (scope) => {
    if (!scope) return true
    // scope 形如 project@branch:path 或 project@branch/path
    const at = scope.indexOf('@')
    if (at < 0) return true
    const project = scope.slice(0, at)
    const rest = scope.slice(at + 1)
    const cut = rest.search(/[:/]/)
    const branch = cut < 0 ? rest : rest.slice(0, cut)
    return mounts.some((m) => {
      if (m.project !== project) return false
      // 挂载点没写分支 = 全分支
      if (m.branch && m.branch !== branch) return false
      const cap = m.cap || info.cap || ''
      return cap === '' || cap.includes('w')
    })
  }

  for (const key of ['recordScope', 'thoughtScope', 'factScope']) {
    const scope = p[key]
    if (!scope) continue
    if (!canWrite(scope)) {
      problems.push(`${key} = ${scope} 超出凭证挂载表的写权限（服务端会拒写）`)
    }
  }
  return problems
}

/**
 * 把「有哪些身份可用」渲染成给人看的多行文本。
 *
 * 身份 = 一张凭证。服务端不接受请求头换身份，
 * 所以这里列出的是本地配好的凭证，不是某张凭证里的子选项。
 */
function renderIdentities(cfg, current) {
  const names = Object.keys(cfg.tokens || {}).sort()
  const lines = []

  const activeName = current || cfg.identityName || ''
  const activeTok = tokenOf(cfg)
  const info = credentialInfo(activeTok)

  lines.push(`当前身份: ${activeName || '（默认凭证）'}`)
  if (info) {
    lines.push(`  主体: ${info.subject}`)
    if (info.cap) lines.push(`  能力: ${info.cap}`)
    if (info.mount && info.mount.length) {
      lines.push('  挂载:')
      for (const m of info.mount) {
        const br = m.branch === '*' ? '所有分支' : m.branch || '默认分支'
        lines.push(`    ${(m.at || '/').padEnd(14)} → ${m.project} @ ${br}  [${m.cap || '-'}]`)
      }
    } else {
      lines.push('  挂载: 全命名空间')
    }
  } else {
    lines.push('  （没有凭证，或凭证不是 JWT）')
  }

  if (names.length) {
    lines.push('', `已配置的具名凭证（共 ${names.length} 个）:`)
    for (const name of names) {
      const mark = name === activeName ? ' ← 当前会话' : ''
      lines.push(`  ${name}${mark}`)
    }
    lines.push('', '切换：/mem:identity <name>')
  } else {
    lines.push('', '只配了默认凭证。想多身份，在 config.json 的 tokens 里加名字 → JWT。')
  }
  return lines.join('\n')
}

/**
 * 构造请求头：只带凭证，不带任何身份声明头。
 *
 * 服务端一张凭证就是一个身份，没有任何 header 能改变它。
 * 想换视角就换凭证（/mem:identity <name>），而不是在同一张凭证上
 * 声明"我现在是谁"——那种设计把提权入口交给了任何能发 header 的东西。
 */
function authHeaders(cfg, extra = {}) {
  const h = { ...extra }
  const tok = tokenOf(cfg)
  if (tok) h.Authorization = `Bearer ${tok}`
  return h
}

/** 取身份块。失败抛错——这跟联想不一样，缺了就是缺了。 */
async function fetchIdentity(cfg) {
  const url = `${cfg.endpoint}/identity`
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
  const url = `${cfg.endpoint}/recall`
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

/** 取服务端状态（含 docs_indexed 逻辑时钟） */
async function fetchHealth(cfg) {
  const url = `${cfg.endpoint}/health`
  const res = await fetch(url, {
    headers: authHeaders(cfg),
    signal: AbortSignal.timeout(800),
  })
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

/**
 * 对问答正文求 SHA256 前 8 位。
 *
 * 这是插件侧的水位指纹，只用来判断「本会话的这一轮写过没有」，
 * 不发给服务端。
 *
 * 曾经这里写着「与服务端写盘的 --hash8 判据保持一致」——那是错的：
 * 服务端从来没有 --hash8 参数，它的去重靠语义指纹（kind + session
 * + 正文 的 SHA-256，不含时间戳），与这个 8 位前缀无关。
 *
 * 换句话说：插件的水位是性能优化（少发几个请求），
 * 服务端的判重才是正确性保证。水位丢了只会重发，不会重复落盘。
 */
function checksumOf(text) {
  return createHash('sha256').update(text, 'utf8').digest('hex').slice(0, 8)
}

/** 读某会话的上传水位。没有就是 0（一条都还没写过）。 */
async function loadWatermark(sessionId) {
  if (!sessionId) return { idx: 0, thinkIdx: 0, hash: '', at: 0 }
  try {
    const raw = await readFile(join(WM_DIR, `${sessionId.replace(/[^a-zA-Z0-9._-]/g, '_')}.json`), 'utf8')
    const d = JSON.parse(raw)
    return {
      idx: Number(d.idx) || 0,
      thinkIdx: Number(d.thinkIdx) || 0,
      hash: String(d.hash || ''),
      at: Number(d.at) || 0,
    }
  } catch {
    return { idx: 0, thinkIdx: 0, hash: '', at: 0 }
  }
}

/**
 * 记下水位。只在写成功后调用——失败时水位不动，下次会从这里重放。
 *
 * 必须是合并写入，不能整体覆盖：记录层与想法层各有一个独立游标
 * （idx / thinkIdx）。整体覆盖会让「补思考」这一步把 record 的进度
 * 抹回 0，下一轮 sync 就把整场会话重写一遍——虽然有服务端判重兜底，
 * 但那意味着每次 sync 都白发几百个请求。
 */
async function saveWatermark(sessionId, patch) {
  if (!sessionId) return
  try {
    await mkdir(WM_DIR, { recursive: true })
    const file = join(WM_DIR, `${sessionId.replace(/[^a-zA-Z0-9._-]/g, '_')}.json`)
    const cur = await loadWatermark(sessionId)
    const next = { ...cur, ...patch, at: Date.now() }
    await writeFile(file, JSON.stringify(next))
  } catch {}
}

/** 读当前会话的个性化设置（覆盖全局配置） */
/**
 * deriveSessionId 从 ExtensionContext 里取出稳定的会话标识。
 *
 * Pi 的 ctx 上没有 sessionId 字段，真实位置是 ctx.sessionManager：
 *   - sessionId      ：字符串，最直接；
 *   - sessionFile    ：<ISO 时间戳>_<uuid>.jsonl，文件名里带 uuid；
 *   - getHeader()    ：老版本 API，保留作兼容。
 *
 * 三者按可靠性排序，逐个试。全都没有时返回空串——调用方会提示，
 * 而不是静默把设置写到 `.json` 这种共用文件名上。
 */
function deriveSessionId(ctx, isSub) {
  const sm = ctx?.sessionManager
  let base = ''

  const direct = typeof sm?.sessionId === 'string' ? sm.sessionId : ''
  if (direct) {
    base = direct
  } else {
    const headerId = (() => {
      try {
        const h = sm?.getHeader?.()
        return typeof h?.id === 'string' ? h.id : ''
      } catch {
        return ''
      }
    })()
    if (headerId) {
      base = headerId
    } else if (typeof sm?.sessionFile === 'string' && sm.sessionFile) {
      // 退路：从文件名里抠 uuid。
      const m = /([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$/i.exec(sm.sessionFile)
      if (m) base = m[1]
    }
  }

  if (!base) return ''
  return isSub ? `sub:${base}` : base
}

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
 * 调 zero-memcp 的 term 入口。唯一工具名为 term，正文通过 vars/args 传递。
 *
 * 服务端的 /mcp 接口不接受 profile 参数；权限与视角完全来自 Bearer Token。
 */
async function term(cfg, cmd, timeoutMs = 5000, vars = {}, args = []) {
  const rpcArgs = { cmd }
  if (vars && Object.keys(vars).length) rpcArgs.vars = vars
  if (args && args.length) rpcArgs.args = args

  const res = await fetch(`${cfg.endpoint}/mcp`, {
    method: 'POST',
    headers: authHeaders(cfg, { 'content-type': 'application/json' }),
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: Date.now(),
      method: 'tools/call',
      params: { name: 'term', arguments: rpcArgs },
    }),
    signal: AbortSignal.timeout(timeoutMs),
  })
  if (!res.ok) throw new Error(`term ${res.status}`)
  const body = await res.json()
  if (body.error) throw new Error(body.error.message || 'term failed')
  if (body.result?.isError) {
    const content = body.result?.content
    throw new Error(Array.isArray(content) && content[0]?.text ? content[0].text : 'term failed')
  }
  const content = body.result?.content
  return Array.isArray(content) && content[0]?.text ? content[0].text : ''
}

/** 写一条记忆。使用 put，正文通过 vars 安全传递。 */
async function putRecord(cfg, text, session, target) {
  const body = String(text || '').trim()
  if (!body) return ''
  // 落点与 @session 两个 token 之间必须有空格。
  //
  // 踩过的坑：早先写成 `${where}${tag}`，而 where 以 "/" 结尾，于是拼出
  // "zero-agi@record/2026-11-03/@<uuid>" —— 会话 id 被粘进路径，
  // 而 put 的会话识别要求 token 以 "@" 开头且不含 "/"，因此没被识别，
  // 结果是目录里多出一层 "@<uuid>"，且面板上无法按会话分组。
  // 用数组 join 拼，从形状上杜绝这类漏空格。
  const parts = ['put']
  if (target) parts.push(`${target}/`)
  if (session) parts.push(`@${session}`)
  parts.push('--', '$TEXT')

  // put 的 kind 由落点项目的写入策略决定；插件不再发旧版 note kind。
  // 服务端命令为 put [<tree>] -- $TEXT，正文走 vars，不参与 DSL 切分。
  return term(cfg, parts.join(' '), 8000, { TEXT: body })
}

/** 当天日期段（YYYY-MM-DD）。用记录自身时间，不用写入时间。 */
function daySegment(d) {
  const t = d instanceof Date && !Number.isNaN(d.getTime()) ? d : new Date()
  const p = (n) => String(n).padStart(2, '0')
  return `${t.getFullYear()}-${p(t.getMonth() + 1)}-${p(t.getDate())}`
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

/**
 * 按优先级分层召回：一条 DSL 链，上层查到就停。
 *
 * 为什么不是「查三次、拼起来」：那会把想法层的正文也塞进 prompt，
 * 而想法层是 2695 条推理过程，它靠体量就能把事实层的结论挤出 top-N。
 * 用 '||' 表达「上一层没答案才问下一层」，服务端执行时短路，
 * 既省 token 也省检索——层级的语义落在查询语言里，而不是插件里。
 *
 * 查询词走 vars，不拼进命令行：正文里可能有分号、换行、'--scope'，
 * 拼进去就成了语法的一部分。
 */
async function recallLayered(cfg, text) {
  const scopes = scopesFor(cfg).recallScopes
  if (!scopes.length) return null

  const cmd = scopes
    .map((sc) => `recall $Q --scope ${sc}`)
    .join(' || ')

  const out = await term(cfg, cmd, cfg.recallLayerTimeoutMs, { Q: String(text || '').trim() })

  // 输出是一串块，每块形如：
  //   [statement] recall <query> --scope zero-agi@fact
  //   <hit list>
  // 或者 no matches。
  const sections = String(out || '')
    .split(/\n\n(?=\[statement\] )/)
    .map((part) => {
      const nl = part.indexOf('\n')
      const head = (nl < 0 ? part : part.slice(0, nl)).trim()
      const body = (nl < 0 ? '' : part.slice(nl + 1)).trim()
      const m = /^\[statement\]\s+(.*)$/.exec(head)
      if (!m) return null
      return { statement: m[1], body }
    })
    .filter(Boolean)

  // 短路链里排在最后的那块就是真正给出答案的层——
  // 前面的都是 no matches，后面的根本没执行。
  for (let i = sections.length - 1; i >= 0; i--) {
    const body = sections[i].body
    if (!body || body === 'no matches') continue
    const scope = scopes.find((sc) => sections[i].statement.includes(`--scope ${sc}`)) || scopes[0]
    const tried = sections.map((sec) => {
      const sc = scopes.find((x) => sec.statement.includes(`--scope ${x}`)) || ''
      return { scope: sc, hit: sec.body !== 'no matches' && !!sec.body }
    })
    return { scope, body, tried }
  }
  return { scope: '', body: '', tried: sections.map((sec) => {
    const sc = scopes.find((x) => sec.statement.includes(`--scope ${x}`)) || ''
    return { scope: sc, hit: false }
  }) }
}

/**
 * 渲染分层召回结果。带上「来自哪一层」，因为层的可信度不同：
 * 事实层是提炼过的结论，记录层是当时说过的话，想法层是推演过程。
 * 不说清来源，模型会把一段推演当成结论用。
 */
function renderLayered(res, cfg) {
  if (!res || !res.body) {
    if (!res || !res.tried?.length) return ''
    const order = res.tried.map((t) => t.scope).filter(Boolean).join(' → ')
    return [
      '<recalled_memories>',
      `分层记忆（${order}）都已查过，没有命中。不要假设这些记忆里存在答案。`,
      '</recalled_memories>',
    ].join('\n')
  }
  const order = res.tried.map((t) => t.scope).filter(Boolean).join(' → ')
  return [
    '<recalled_memories>',
    `按优先级依次查了：${order}。下面是第一个命中的层（${res.scope}）的原文摘要。`,
    '事实层是提炼过的结论，记录层是当时的原话，想法层是推理过程——可信度不同，别混用。',
    '需要完整内容时用 read 展开地址；与本轮无关就忽略。',
    '',
    res.body,
    '</recalled_memories>',
  ].join('\n')
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

// 思考层的切片与渲染直接复用导入工具的同一份实现。
//
// 为什么不自带一份：切片边界和 frontmatter 一旦两边写法不同，历史数据
// 与新对话就变成两个语料库——同一个 recall 里它们的 docLen、字段、
// 甚至能否被 parseSliceMeta 识别都会不一致。一条规则只应存在一处。
import { chunkThinking, renderThought } from './bin/layer-import-lib.js'

export default function zeroMemcp(pi) {
  let cfgPromise = loadConfig()

  let identity = ''
  let identityAt = 0
  let identityError = null

  // 联想只在用户文本变化时重算：同一个 run 里工具循环会反复触发 context。
  let recallKey = null
  let recallHits = []
  // 连接状态。用来判断「恢复」这个事件：上一轮不可达、这一轮可达。
  // 单看「可达」没有意义，只有从 false 翻到 true 才是重连。
  let lastHealthy = null

  // 会话级的开关：子代理整个生命周期都不注入、不联想。
  let skip = false

  // 本条会话的身份挂载状态。null 表示跟随全局 config.identity。
  // 'on' = 这场对话认领身份（挂载规则块、联想记忆、允许落盘）
  // 'off' = 普通会话（纯净助手，不挂载、不联想、不自动落盘）
  let sessionIdentity = null

  /** 当前会话是否挂载了身份。会话级设置优先于全局。 */
  const identityActive = (cfg) => {
    const v = sessionIdentity ?? cfg.identity ?? 'off'
    return v === 'on' || v === 'auto' || v === true
  }

  // 已落盘的记忆（按内容哈希去重），避免同一句被反复写入。
  const seenMemories = new Set()

  // 当前会话 id，随切片一起写进 frontmatter，供面板分组。
  let sessionId = ''

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
      // 身份视角完全由凭证决定：换身份是换一张凭证，不是换一个请求头。
      identity = await fetchIdentity(cfg)
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

  // 后台订阅树变更。
  //
  // 注意这里是唯一的启动点：只定义不调用等于"写了监听器但没接上"，
  // 而那种故障不报错，只是永远收不到推送。
  //
  // 它必须能在身份变化后重启：SSE 连接的身份是建立时那一刻的凭证，
  // 切换凭证后不重连就等于用旧身份继续收推送。
  let lastWakeAt = 0
  let stopSSE = null

  // agent 是否正在思考。只在这里注册一次，SSE 重启不再重复注册。
  let agentBusy = false
  pi.on('agent_start', () => { agentBusy = true })
  pi.on('agent_end', () => { agentBusy = false })

  function restartSSE() {
    if (stopSSE) {
      try { stopSSE() } catch {}
      stopSSE = null
    }
    stopSSE = startSSEListener(pi, () => cfgPromise, (ev, busy) => {
    // agent 正忙时不打断：变化自然会在下一轮 context 里体现。
    if (busy) return
    const now = Date.now()
    if (now - lastWakeAt < 3000) return
    lastWakeAt = now
    if (typeof pi.sendUserMessage !== 'function') return
    const paths = Array.isArray(ev?.paths) ? ev.paths : []
    const brief = paths.slice(0, 3).join('、')
    const proj = ev?.project || ''
    const br = ev?.branch || ''
    const commit = String(ev?.commit || '').slice(0, 8)
    // 唤醒词是固定短语，绝不拼接外部正文：
    // 正文必须经 MCP 工具读回来，才会落在 tool_result 的沙盒里。
    pi.sendUserMessage(
      `[zero-memcp] 记忆树更新：${proj}@${br} 推进到 ${commit}` +
        `${brief ? `（${brief}${paths.length > 3 ? ' 等' : ''}）` : ''}`
    )
    }, () => agentBusy)
  }

  // 锚定 watch 里声明的树。服务端只把 tree 事件推给锚定过的人。
  async function anchorWatchedTrees(cfg) {
    const list = String(cfg.watch || '')
      .split(',')
      .map((x) => x.trim())
      .filter(Boolean)
    if (!list.length) return
    for (const addr of list) {
      try {
        await term(cfg, `remind ${addr}`, 3000)
      } catch (err) {
        pi.events?.emit?.('zero-memcp:anchor-error', { addr, message: String(err) })
      }
    }
  }

  // 记忆入口工具。
  //
  // 只有一个，动词在 cmd 里——和 zero-memcp 的 term 协议同构。
  // ── 用户斜杠命令 ──────────────────────────────────────────

  pi.registerCommand('mem:sync', {
    description: '同步：把当前会话全部问答存入 mem（按内容去重）',
    handler: async (_args, cmdCtx) => {
      const cfg = await cfgPromise
      if (!cfg.enabled) {
        cmdCtx?.ui?.notify?.('zero-memcp 已禁用', 'warning')
        return
      }
      let n = 0
      try {
        n = await syncSessionFromWatermark(cfg, cmdCtx)
      } catch (err) {
        cmdCtx?.ui?.notify?.(`同步失败：${err}`, 'error')
        return
      }
      cmdCtx?.ui?.notify?.(n > 0 ? `已同步 ${n} 条问答到 mem` : '已是最新，无新内容需要同步', 'info')
    },
  })

  pi.registerCommand('mem:record', {
    description: '自动记录：/mem:record [on|off] [global]（空参 = 看状态）',
    handler: async (args, cmdCtx) => {
      const cfg = await cfgPromise
      const raw = String(args || '').trim().toLowerCase()
      const parts = raw ? raw.split(/\s+/) : []

      const globalVal = cfg.importMode || 'off'
      const sessionVal = sessionImportMode
      const describe = { auto: 'on', off: 'off' }
      const show = (v) => describe[v] || v

      // 空参：两条都报，学 /mem:identity 的格式
      if (!parts.length) {
        cmdCtx?.ui?.notify?.(
          [
            `本会话: ${sessionVal === null ? `跟随全局（${globalVal}）` : `${sessionVal}，全局默认 ${globalVal}`}`,
            '',
            '用法：',
            '  /mem:record on            本会话每轮自动存',
            '  /mem:record off           本会话不自动存',
            '  /mem:record global on     以后新会话默认自动存',
            '  /mem:record global off    以后新会话默认不自动存',
            '',
            '任何时候想立刻存一次，敲 /mem:sync（与模式无关）。',
          ].join('\n'),
          'info',
        )
        return
      }

      const isGlobal = parts.includes('global')
      const action = parts.find((x) => ['on', 'off', 'auto'].includes(x))

      if (!action) {
        cmdCtx?.ui?.notify?.('参数只支持 on / off，可加 global 改全局默认', 'warning')
        return
      }
      const target = action === 'on' ? 'auto' : action

      if (isGlobal) {
        try {
          await saveGlobalConfig({ importMode: target })
          cfg.importMode = target
        } catch (err) {
          cmdCtx?.ui?.notify?.(String(err), 'error')
          return
        }
        // 全局改动同时清掉本会话覆盖，让新设置立刻可见——与 /mem:identity 一致
        if (sessionImportMode !== null && sessionImportMode !== target) {
          sessionImportMode = null
          if (sessionId) await saveSessionSetting(sessionId, { importMode: null })
        }
        cmdCtx?.ui?.notify?.(
          `全局默认已改为: ${show(target)}（对以后新会话生效，本会话${sessionImportMode === null ? '已跟随' : '仍用本会话设置'}）`,
          'info',
        )
        return
      }

      sessionImportMode = target
      if (!sessionId) sessionId = deriveSessionId(ctx, skip)
      if (sessionId) await saveSessionSetting(sessionId, { importMode: target })
      cmdCtx?.ui?.notify?.(`本会话记录模式已设为: ${show(target)}`, 'info')
    },
  })

  pi.registerCommand('mem:identity', {
    description: '挂载/卸下身份：/mem:identity [on|off] [global]（空参 = 看状态）',
    handler: async (args, cmdCtx) => {
      const cfg = await cfgPromise
      const raw = String(args || '').trim().toLowerCase()
      const parts = raw ? raw.split(/\s+/) : []

      const label = (v) => (v === 'on' || v === 'auto' ? '已挂载' : '未挂载')
      const globalVal = cfg.identity === 'on' || cfg.identity === 'auto' ? 'on' : 'off'
      const sessionVal = sessionIdentity === null ? null : sessionIdentity

      // 空参：报状态 + 列出可用凭证 + 说清怎么改
      if (!parts.length) {
        const head = [
          `本会话: ${sessionVal === null ? `跟随全局（${label(globalVal)}）` : label(sessionVal)}`,
          `全局默认: ${label(globalVal)}`,
          '',
        ].join('\n')
        const body = renderIdentities(cfg, activeIdentityName)
        const tail = [
          '',
          '用法：',
          '  /mem:identity <name>      切换到某张具名凭证（= 换一个身份）',
          '  /mem:identity on          本会话挂载身份',
          '  /mem:identity off         本会话卸下，变回普通助手',
          '  /mem:identity global on   以后新会话默认挂载',
          '  /mem:identity global off  以后新会话默认不挂载',
        ].join('\n')
        cmdCtx?.ui?.notify?.([head, body, tail].join('\n'), 'info')
        return
      }

      const isGlobal = parts.includes('global')
      const action = parts.find((x) => x === 'on' || x === 'off' || x === 'auto')

      // 不是 on/off/global 时，把它当作凭证名：切换身份。
      if (!action && !isGlobal) {
        const name = parts[0]
        const tokens = cfg.tokens || {}
        if (!tokens[name]) {
          const avail = Object.keys(tokens).sort().join(', ')
          cmdCtx?.ui?.notify?.(
            avail
              ? `没有 "${name}" 这张凭证。可用：${avail}`
              : `没有 "${name}" 这张凭证。config.json 里还没有配 tokens。`,
            'error',
          )
          return
        }
    const sc = scopesFor(cfgNow)
    const problems = validatePersonaScope(cfgNow, name)
    if (problems.length > 0) {
      cmdCtx?.ui?.notify?.(
        `身份 "${name}" 配置有风险：\n${problems.map((p) => `  - ${p}`).join('\n')}`,
        'warning',
      )
    }
    activeIdentityName = name
    if (sessionId) await saveSessionSetting(sessionId, { identityName: name })
    // 换了凭证就是换了一个世界：身份块、SSE 订阅、锚定全部重来。
    identity = ''
    identityAt = 0
    identityError = null
    restartSSE()
    anchorWatchedTrees(cfgNow).catch(() => {})
    cmdCtx?.ui?.notify?.(
      `本会话身份已切换为: ${name}（落点: ${sc.recordScope || '默认'}）`,
      'info',
    )
        return
      }

      if (!action) {
        cmdCtx?.ui?.notify?.('参数支持 <name> / on / off，可加 global 改全局默认', 'warning')
        return
      }
      const want = action === 'off' ? 'off' : 'on'

      if (isGlobal) {
        try {
          await saveGlobalConfig({ identity: want })
          cfg.identity = want
        } catch (err) {
          cmdCtx?.ui?.notify?.(String(err), 'error')
          return
        }
        // 全局改动同时清掉本会话的覆盖，让新设置立刻可见
        if (sessionIdentity !== null && sessionIdentity !== want) {
          sessionIdentity = null
          if (sessionId) await saveSessionSetting(sessionId, { identity: null })
        }
        cmdCtx?.ui?.notify?.(
          `全局默认已改为: ${label(want)}（对以后新开的会话生效，本会话${sessionIdentity === null ? '已跟随' : '仍用本会话设置'}）`,
          'info',
        )
        return
      }

      // 本会话
      sessionIdentity = want
      if (!sessionId) sessionId = deriveSessionId(ctx, skip)
      if (sessionId) await saveSessionSetting(sessionId, { identity: want })

      if (want === 'on') {
        // 重取一次：上一次可能是失败的，别让"我已经没有身份了"变成永久状态
        identity = ''
        identityAt = 0
        identityError = null
        try {
          const text = await ensureIdentity(cfg, pi)
          if (text) {
            cmdCtx?.ui?.notify?.('本会话已挂载身份，下一轮对话生效', 'info')
          } else {
            cmdCtx?.ui?.notify?.(
              `挂载失败（拿不到身份块）：${identityError || '服务未返回内容'}。检查 endpoint 与 token。`,
              'warning',
            )
          }
        } catch (err) {
          cmdCtx?.ui?.notify?.(`挂载失败: ${err}`, 'error')
        }
      } else {
        cmdCtx?.ui?.notify?.(
          '本会话已卸下身份：不再联想记忆、不再自动落盘。\n' +
            '已注入的规则块还在系统消息里，压缩后自动消失；想立刻清理请重开会话。',
          'info',
        )
      }
    },
  })

  // ── 工具：只有一个 mem ────────────────────────────────────
  //
  // 声明面恒定为 1，能力增长在命令空间，不在 tools/list。
  //
  // 五个工具（zero_mem / _set_mode / _set_identity / _import / _status）
  // 拆开注册的代价是每轮都多几百字符的 schema，而模型真正需要的只是
  // 「说一句话就能查/写/查状态」。把动词做成参数，schema 只写一次。
  //
  // 本地动词（mode/sync/status/identity）由插件处理，其余整行透传服务端——
  // 插件的职责是「在合适的时机加上下文和写入」，不是替服务端实现记忆。
  pi.registerTool({
    name: "mem",
    label: "Context",
    description: [
      "mem —— 跨会话、跨机器的长期上下文。事实、决定、踩坑、偏好都在这里，按需取用。",
      "",
      "当前窗口之外的东西不在你的上下文里，但在这里。需要时取，不需要时它不占位。",
      "",
      "  put [<tree>] -- $TEXT    写入记忆（正文通过 vars/args 传，不拼进 cmd）",
      "  put <tree> --ref <addr>  在树中追加已有记忆的引用",
      "  recall <text>            语义联想（返回地址与摘要）",
      "  read <addr>...           读取全文，可一次读多篇",
      "  grep <regex>             正则搜索全文",
      "  log [N]                  最近 N 篇",
      "  ls [date]                某天的切片",
      "  trees <project>[@<br>]   查看树目录",
      "  remind [<tree>]          锚定子树并接收 SSE 通知",
      "  since <tree> <commit>    获取提交差分",
      "  commit [<p>[@<br>]]      显式封箱（put 只入队，不提交）",
      "  diff <p>@<br>            分支比对",
      "",
      "会话（插件就地处理）：",
      "  status                   当前凭证、身份、自动记录与索引规模",
      "  record [on|off]          本会话是否每轮自动存",
      "  identity [<name>]        查看身份或换一张凭证",
      "",
      "put 入脏层后立即返回；折叠为 Git 提交由显式 commit 或服务端后台策略决定。",
      "正文必须通过 vars/args 传递，例如 cmd='put notes -- $TEXT', vars={TEXT:'正文'}。",
    ].join("\n"),
    parameters: {
      type: "object",
      properties: {
        cmd: {
          type: "string",
          description: "DSL 命令行，例如 'recall 数据库选型' 或 'put notes -- $TEXT'。正文走 vars/args，引用必须独占一个词。",
        },
        vars: {
          type: "object",
          additionalProperties: { type: "string" },
          description: "命名正文参数。cmd 中使用独占词引用，例如 cmd='put -- $TEXT', vars={TEXT:'多行正文'}。",
        },
        args: {
          type: "array",
          items: { type: "string" },
          description: "位置正文参数。cmd 中使用 $1、$2 引用。",
        },
      },
      required: ["cmd"],
      additionalProperties: false,
    },
    async execute(_toolCallId, params) {
      const cfg = await cfgPromise
      const line = String(params.cmd || "").trim()
      if (!line) {
        return { content: [{ type: "text", text: "空命令。试 'status' 或 'recall <关键词>'" }] }
      }

      const verb = line.split(/\s+/)[0]
      const arg = line.slice(verb.length).trim()

      // ── 本地动词 ──
      switch (verb) {
        case 'record': {
          if (!arg) {
            const cur = sessionImportMode || cfg.importMode
            return { content: [{ type: "text", text: `自动记录: ${cur === "auto" ? "on" : cur}（全局默认: ${cfg.importMode === "auto" ? "on" : cfg.importMode}）` }] }
          }
          const m = arg === 'on' ? 'auto' : arg
          if (!['auto', 'off'].includes(m)) {
            return { content: [{ type: "text", text: "模式仅支持 on / off" }], isError: true }
          }
          sessionImportMode = m
          if (sessionId) await saveSessionSetting(sessionId, { importMode: m })
          return { content: [{ type: "text", text: `本会话自动记录已设为: ${m === "auto" ? "on" : m}` }] }
        }

        case 'identity': {
          if (!arg) {
            return { content: [{ type: "text", text: renderIdentities(cfg, activeIdentityName) }] }
          }
          const tokens = cfg.tokens || {}
          if (!tokens[arg]) {
            const avail = Object.keys(tokens).sort().join(', ')
            return {
              content: [{
                type: "text",
                text: avail
                  ? `没有 "${arg}" 这张凭证。可用：${avail}`
                  : `没有 "${arg}" 这张凭证。config.json 里还没有配 tokens。`,
              }],
              isError: true,
            }
          }
          activeIdentityName = arg
          if (sessionId) await saveSessionSetting(sessionId, { identityName: arg })
          identity = ''
          identityAt = 0
          identityError = null
          return { content: [{ type: "text", text: `本会话身份已切换为 ${arg}` }] }
        }

        case 'status': {
          let health = null
          let err = null
          try {
            health = await fetchHealth(cfg)
          } catch (e) { err = String(e) }

          let wm = { idx: 0 }
          try { wm = await loadWatermark(sessionId) } catch {}

          // 报规模，不报计数器。
          //
          // clock 是「有人写过东西」的增量通知，重启归零、读数没有
          // 稳定含义，对用户是无意义数字。真正有用的是「这里有多少」。
          const docs = health && typeof health.docs_indexed === 'number' ? health.docs_indexed : null
          const reachable = health !== null

          // 会话值与全局默认分开写：只报一个数字的话，用户没法判断
          // 「这是这场会话的设置，还是以后新会话也会这样」。
          const globalIdentity = cfg.identity === 'on' || cfg.identity === 'auto' ? 'on' : 'off'
          const globalMode = cfg.importMode || 'off'
          const mark = (sessionVal, globalVal) =>
            sessionVal === null ? `跟随全局（${globalVal}）` : `${sessionVal}，全局默认 ${globalVal}`

          const lines = [
            `凭证: ${activeIdentityName || cfg.identityName || '（默认凭证）'}`,
            `端点: ${cfg.endpoint}${reachable ? '' : `（不可达：${err}）`}`,
            `记忆挂载: ${mark(sessionIdentity, globalIdentity)}`,
            `自动记录: ${mark(sessionImportMode, globalMode)}`,
            `本会话: ${wm.idx} 轮已入库`,
            docs === null ? null : `已索引切片: ${docs} 篇`,
          ].filter(Boolean)

          return { content: [{ type: "text", text: lines.join("\n") }] }
        }
      }

      // ── 其余动词透传服务端 ──
      if (!cfg.enabled) {
        return { content: [{ type: "text", text: 'zero-memcp 已禁用' }], isError: true }
      }
      const out = await term(cfg, line, 8000, params.vars, params.args)
      return { content: [{ type: "text", text: out || '(无输出)' }] }
    },
  })

  pi.on('session_start', async (event, ctx) => {
    cfgPromise = loadConfig()
    skip = isSubagent(ctx)
    // 记下会话 id，落盘时带上，面板才能按会话分组。
    //
    // 踩过的坑：这里原先只试 sessionManager.getHeader() 和 getSessionId()。
    // 这两个方法在 Pi 的 sessionManager 上都不存在（实测字段是
    // sessionId / sessionFile / sessionDir ...），于是 sessionId 恒为空串。
    // 连锁后果是静默的、且很难查：
    //   - saveSessionSetting('') 第一行就 return，/mem:identity on 存不下来；
    //   - loadSessionSetting('') 返回 {}，sessionIdentity 恒为 null，
    //     回落成全局 identity=off，于是身份块和召回都不注入；
    //   - 水位同步同理失效。
    // 用户看到的现象是「敲了 /mem:identity on，但身份没挂上」，
    // 而代码里没有任何一处报错。
    try {
      sessionId = deriveSessionId(ctx, skip)
    } catch {
      sessionId = skip ? 'sub:unknown' : ''
    }
    if (!sessionId) {
      // 宁可显式说出来，也不要让「每会话设置」静默失效。
      try {
        ctx?.ui?.notify?.('zero-memcp: 取不到会话 id，本会话的身份/记录设置不会持久化', 'warning')
      } catch {}
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
        sessionIdentity = ss.identity || null

        // 决定本会话身份：优先会话持久化记录，其次全局配置，都没有就用默认凭证
        activeIdentityName = ss.identityName || cfgNow.identityName || ''

        // 身份确定后启动 SSE 连接，并把 watch 声明的树锚定到服务端
        restartSSE()
        anchorWatchedTrees(cfgNow).catch(() => {})

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

  /**
   * 从本轮消息里抽出推理链（thinking 块）。
   *
   * 只取本轮（最后一条 user 之后）的 assistant 消息，避免把上几轮的推理
   * 重复归到这一轮——thinking 在会话里是逐轮累积的，全量抽取会让同一段
   * 推理在每一轮都落一次盘。
   */
  function extractThinking(messages) {
    if (!Array.isArray(messages) || !messages.length) return ''
    let ui = -1
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i]?.role === 'user') { ui = i; break }
    }
    if (ui < 0) return ''
    const parts = []
    for (let i = ui + 1; i < messages.length; i++) {
      const m = messages[i]
      if (m?.role !== 'assistant') continue
      const c = m.content
      if (!Array.isArray(c)) continue
      for (const p of c) {
        if (p && p.type === 'thinking' && typeof p.thinking === 'string' && p.thinking.trim()) {
          parts.push(p.thinking)
        }
      }
    }
    return parts.join('\n\n').trim()
  }

  /** 写盘。成功才清 pending，失败保留待下次重试。 */
  async function flush(why, turn, thinkingText) {
    const t = turn || (pendingAnswer ? { q: turnUserText, a: pendingAnswer } : null)
    if (!t || !t.a) return false

    // 非交互模式（pi -p）不发 session_start，sessionId 还是空串。
    if (!sessionId) sessionId = 'oneshot'

    const text = t.q ? `**问**：${t.q}\n\n**答**：${t.a}` : t.a
    const key = text.slice(0, 160)
    if (seenMemories.has(key)) {
      pendingAnswer = ''
      turnUserText = ''
      return false
    }

    const cfg = await cfgPromise
    if (!cfg.enabled) return false

    const now = new Date()
    const day = daySegment(now)
    const sc = scopesFor(cfg)

    try {
      // 1) 问答对写进记录层（按当前分身的人格落点）
      const whereRecord = sc.recordScope ? `${sc.recordScope}/${day}` : ''
      await putRecord(cfg, text, sessionId, whereRecord)

      // 2) 推理链写进想法层（配置了 thoughtScope 且当前轮有 thinking）
      if (sc.thoughtScope && thinkingText && thinkingText.trim()) {
        const chunks = chunkThinking(thinkingText)
        const whereThought = `${sc.thoughtScope}/${day}`
        for (let i = 0; i < chunks.length; i++) {
          const body = renderThought(
            { ask: t.q, thinking: chunks[i], at: now },
            sessionId,
            i + 1,
            chunks.length,
          )
          try {
            await putRecord(cfg, body, sessionId, whereThought)
          } catch (terr) {
            // thought 写入失败不阻断 record：记录比想法优先
            pi.events?.emit?.('zero-memcp:thought-write-error', { error: String(terr), session: sessionId })
          }
        }
      }

      // 只有写成功才认为这一轮结束了。写失败时留着，下一轮再试。
      seenMemories.add(key)
      pendingAnswer = ''
      turnUserText = ''
      lastHealthy = true

      const cur = await loadWatermark(sessionId)
      await saveWatermark(sessionId, { idx: cur.idx + 1, hash: checksumOf(text) })
      return true
    } catch (err) {
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
  // 就算重放了同一条，服务端也会按语义指纹判重、返回已有路径，
  // 不会再落一份。水位不是正确性机制——它是「少发几个请求」的优化，
  // 所以这个文件丢了、两台机器各存一份、或者手动清零，都不会产生重复。
  // liveCtx 只在「调用方手里有新鲜 ctx」时传（工具、命令）。
  // 异步回调一律不传——那时手里的 ctx 可能已经作废了。
  async function syncSessionFromWatermark(cfg, liveCtx) {
    const sc = scopesFor(cfg)
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
        const where = sc.recordScope ? `${sc.recordScope}/${daySegment(new Date())}` : ''
        await putRecord(cfg, text, sessionId, where)
        // 只有写成功才推进水位
        await saveWatermark(sessionId, { idx: turnIdx, hash: h8 })
        written++
      } catch {
        // 失败停下：水位留在上一条成功的，下次连上继续重放
        break
      }
      i = j - 1
    }

    // 补思考层：用独立游标 thinkIdx。
    //
    // 为什么要独立：record 的水位早就到顶了（历史会话都同步过），
    // 复用同一个游标意味着 thinking 永远补不回来——「已是最新」会
    // 把它挡在门外。两个游标各管一层，互不牵连。
    if (sc.thoughtScope) {
      const thinkTurns = []
      let ti = 0
      for (let i = 0; i < msgs.length; i++) {
        if (msgs[i]?.role !== 'user') continue
        let j = i + 1
        const parts = []
        for (; j < msgs.length; j++) {
          const m = msgs[j]
          if (m?.role === 'user') break
          if (m?.role !== 'assistant') continue
          const c = m.content
          if (!Array.isArray(c)) continue
          for (const p of c) {
            if (p && p.type === 'thinking' && typeof p.thinking === 'string' && p.thinking.trim()) {
              parts.push(p.thinking)
            }
          }
        }
        const think = parts.join('\n\n').trim()
        ti++
        if (think) thinkTurns.push({ idx: ti, q: stripMem(textOf(msgs[i])), think })
        i = j - 1
      }

      for (const t of thinkTurns) {
        if (t.idx <= wm.thinkIdx) continue
        const now = new Date()
        const chunks = chunkThinking(t.think)
        const whereThought = `${sc.thoughtScope}/${daySegment(now)}`
        let ok = true
        for (let k = 0; k < chunks.length; k++) {
          const body = renderThought({ ask: t.q, thinking: chunks[k], at: now }, sessionId, k + 1, chunks.length)
          try {
            await putRecord(cfg, body, sessionId, whereThought)
          } catch {
            ok = false
            break
          }
        }
        if (!ok) break
        await saveWatermark(sessionId, { thinkIdx: t.idx })
        written++
      }
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
    // 未挂载身份的会话不落盘
    if (!identityActive(cfgNow)) {
      pendingAnswer = ''
      turnUserText = ''
      return
    }
    // 三档开关控制：默认 off 不落盘；只有当前会话/全局是 auto 才每轮写入。
    //
    // 踩过的坑：早先只查 sessionImportMode（初始为 null，靠 session_start
    // 赋值）。而 pi -p 模式压根不触发 session_start 事件，导致
    // sessionImportMode 恒为 null，非交互模式整段落盘被静默短路。
    const effectiveMode = sessionImportMode || cfgNow.importMode || 'off'
    if (effectiveMode !== 'auto') {
      pendingAnswer = ''
      turnUserText = ''
      return
    }
    const fromMessages = extractTurn(event?.messages)
    const thinkText = extractThinking(event?.messages)
    await flush('agent_end', fromMessages, thinkText)
  })

  // 非交互模式的落盘口。
  //
  // pi -p（--print，脚本/CI/定时任务用的模式）根本不发 agent_end——
  // 它发完 turn_end 就直接退出进程。挂在 agent_end 上的写入逻辑
  // 在那种模式下一次都不会执行，表现是「聊了一整轮，记忆库里什么都没有」，
  // 而且不报错。
  //
  // turn_end 在两种模式下都发，所以它是兜底口。重复触发无害：
  // 服务端按语义指纹判重，同一条写两次只落一份。
  pi.on('turn_end', async (event) => {
    if (skip) return
    const cfgNow = await cfgPromise
    // 未挂载身份的普通会话绝不自动写盘，避免污染记忆库
    if (!identityActive(cfgNow)) return
    const effectiveMode = sessionImportMode || cfgNow.importMode || 'off'
    if (effectiveMode !== 'auto') return
    // 非交互模式下 session_start 不触发，sessionId 还是空串。
    // 空 session 会让写入丢掉「这条属于哪个会话」这个信息，
    // 面板上全都归到「未分组」。给一个可辨识的兜底名。
    if (!sessionId) sessionId = 'oneshot'
    const fromMessages = extractTurn(event?.messages)
    const thinkText = extractThinking(event?.messages)
    await flush('turn_end', fromMessages, thinkText)
  })

  // 捕获上下文压缩摘要：写入事实层（factScope）。
  //
  // 为什么在这里接：模型刚把之前的对话压成一份结构化摘要（Goal / Decisions
  // / Progress）。这是三层里信息密度最高的内容——平常问答记了十几万字，
  // 提炼出来的结论就这几百字。写进 factScope 之后，未来的查询第一跳
  // 就能直接拿到这份结论，不必在数千条问答细节里大海捞针。
  pi.on('session_compact', async (event) => {
    if (skip) return
    const cfgNow = await cfgPromise
    if (!cfgNow.enabled) return
    // 同样受 identity 约束：未挂载身份的会话不往事实层落盘
    if (!identityActive(cfgNow)) return

    const summary = String(event?.compactionEntry?.summary || '').trim()
    if (!summary) return
    const target = scopesFor(cfgNow).factScope || ''
    if (!target) return

    const at = event?.compactionEntry?.timestamp ? new Date(event.compactionEntry.timestamp) : new Date()
    const day = daySegment(at)
    const sid = sessionId || 'unknown'
    const tokens = event?.compactionEntry?.tokensBefore || 0

    // 格式与历史导入保持同构：带标准 frontmatter，未来可统一审计。
    // 注意别用 filter 去空行——那会把 frontmatter 与正文之间的空行一起
    // 删掉，格式立刻变形。这里按需拼行。
    const fm = ['---', 'layer: fact', `session: ${sid}`, `at: ${at.toISOString()}`]
    if (tokens) fm.push(`tokens_before: ${tokens}`)
    fm.push('---', '', '# Compaction summary', '', summary, '')
    const frontmatter = fm.join('\n')

    try {
      const where = `${target}/${day}`
      await putRecord(cfgNow, frontmatter, sid, where)
    } catch (err) {
      // 事实层写入失败不阻断对话，但记事件供排查
      pi.events?.emit?.('zero-memcp:fact-write-error', { error: String(err), session: sid })
    }
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
    if (!identityActive(cfg)) return

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
    // 未挂载身份的会话是普通会话：不往提问尾部塞记忆卡片
    if (!identityActive(cfg)) return

    const text = latestUserText(event.messages)
    const extra = []

    // 重连检测：上一轮不可达、这一轮可达 = 服务回来了。
    // auto 模式下，断网期间落下的问答现在补上。
    try {
      await fetchHealth(cfg)
      const isBack = lastHealthy === false
      lastHealthy = true
      if (isBack && sessionImportMode === 'auto') {
        syncSessionFromWatermark(cfg).catch(() => {})
      }
    } catch {
      // 服务不可达时不注入任何东西：宁可不报，也别报一个假数字。
      lastHealthy = false
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

    // 已配置分层：走一条 '||' 回落链，只把真正答上来的那一层放进上下文。
    // 没配置分层：保持老的单次全局检索（返回地址列表）。
    const sc = scopesFor(cfg)
    const layered = sc.recallScopes.length > 0

    if (layered) {
      try {
        recallHits = [await recallLayered(cfg, text)]
      } catch {
        recallHits = []
        return extra.length ? [...event.messages, { role: 'user', content: extra.join('\n\n') }] : undefined
      }
      const rendered = renderLayered(recallHits[0], cfg)
      if (rendered) extra.push(rendered)
      if (!extra.length) return
      return [...event.messages, { role: 'user', content: extra.join('\n\n') }]
    }

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
