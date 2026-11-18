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
import { appendFileSync, readFileSync, writeFileSync, mkdirSync, statSync, unlinkSync, renameSync, openSync } from 'node:fs'
import { createHash } from 'node:crypto'
import { spawn } from 'node:child_process'
import net from 'node:net'
import { createServer as createHttpServer } from 'node:http'
import { homedir } from 'node:os'
import { join, dirname } from 'node:path'

/** 插件版本号。由 status 命令报出，排查版本时唯一不需要猜的依据。 */
const VERSION = '0.2.0'

/**
 * 外部消息投递中枢——**进程级**，跨模块实例共享。
 *
 * 为什么必须是 globalThis 而不是模块顶层变量：
 * `/reload` 之后 jiti 用 `moduleCache:false` 重新求值本模块，同一个进程里于是
 * 存在两代模块实例（旧代还活着，它的 HTTP 口还占着 18800）。
 * 旧代 server 的闭包读的是**旧代那份**模块变量，新一代怎么赋值都写不到。
 *
 * 实测（2026-11-16 06:33 `/reload` 后，同一秒两条路）：
 *   socket 注入 → `external flush ok`      （每代重建 listener，通）
 *   HTTP  注入 → `external flush failed: ctx is stale`（闭包属旧代，挂）
 * 差别只在模块实例，不在链路本身。
 *
 * 所以：端口是设施，目标是活的；两边都必须挂在进程上。
 */
const WAKE_HUB_KEY = Symbol.for('zero-memcp.wake-hub')

function wakeHub() {
  const g = globalThis
  if (!g[WAKE_HUB_KEY]) {
    g[WAKE_HUB_KEY] = {
      /** 进程级 HTTP 唤醒口；只开一条。null = 未开启或监听失败。 */
      http: null,
      /** 18800 已被占用、改听 18810 只试一次。 */
      httpFallbackTried: false,
      /** 当前该把消息交给谁——最新一代的 injectExternal。 */
      inject: null,
      /**
       * 上面那个 inject 属于哪个会话。
       *
       * 为什么需要这个字段（2026-11-16）：
       * 以前 session_start 无条件写 `wakeHub().inject = injectExternal`，
       * 于是「谁最后打开谁拿到投递权」。同时开着三个会话时，QQ 消息落在
       * 哪个会话取决于谁刚刚重载过——表现就是「乱投递」。
       * 现在投递权跟记忆唤醒用同一套归属（钉住 > 按开得早的选举），
       * 这里记下属于谁，非归属会话就不抢。
       */
      injectSid: null,
      /** 投递权最近一次被确认活着的时刻，用来判「持有者是不是已经没了」。 */
      injectBeat: 0,
      /** 送不出去的外部消息。跨代共享：换代时积压不能跟着旧代一起死。 */
      backlog: [],
      lastAt: 0,
      timer: null,
      retryDelay: 0,
      /** 去重表也跨代：等过一次的消息换一代不该再送一遍。 */
      seen: new Map(),
    }
  }
  return g[WAKE_HUB_KEY]
}


/**
 * 本机 HTTP 唤醒口（默认开在 127.0.0.1:18810）。
 *
 * 为什么需要第二个入口：gateway 搬到 VPS 以后，它和 pi 不在同一个文件系统，
 * 写不了 /root/.qq-mcp/wake.sock。termcp 的 [[forward]] 可以把 VPS 的
 * 127.0.0.1:18810 直通到容器里的 127.0.0.1:18810 —— 扩展只要在本机开一个
 * HTTP 口，gateway 就能用一条普通的 POST 找到它，不需要任何隧道协议。
 *
 * 报文与 wake.sock 完全一致：JSON 里带 text 就注入。两条路并存，
 * wake.sock 不动（老 gateway / 脚本照旧可用）。
 *
 * 路径收两种：`POST /wake`（扩展自己的名字）和 `POST /notify`（gateway、qq-mcp
 * 这类外部来源的旧名字）。同一个处理函数，不两套报文格式。
 *
 * 开关只有环境变量：
 *   ZERO_WAKE_HTTP_PORT  端口，默认 18800（外部世界认这个门牌）；设为 0 = 不监听。
 *                        18800 被占用时自动退到 18810，并在日志里说清楚。
 *   ZERO_WAKE_HTTP_HOST  绑定地址，默认 127.0.0.1（只允许本机/转发进来）
 *   ZERO_WAKE_TOKEN      设置后要求 Authorization: Bearer <token> 或 X-Wake-Token
 *
 * token 只从环境读，不落文件、不进日志。
 */
export function createWakeHttpServer({ token = '', inject, log = () => {} } = {}) {
  if (typeof inject !== 'function') throw new Error('createWakeHttpServer: inject 必填')
  const authed = (req) => {
    if (!token) return true
    const h = String(req.headers.authorization || '')
    return h === `Bearer ${token}` || req.headers['x-wake-token'] === token
  }
  return createHttpServer(async (req, res) => {
    const reply = (code, body) => {
      res.writeHead(code, { 'content-type': 'application/json; charset=utf-8' })
      res.end(JSON.stringify(body))
    }
    const path = String(req.url || '').split('?')[0]
    if (req.method === 'GET' && path === '/health') {
      return reply(200, { ok: true, via: 'wake-http', auth: token ? 'token' : 'loopback' })
    }
    if (req.method !== 'POST' || (path !== '/wake' && path !== '/notify')) {
      return reply(404, { error: 'POST /wake、POST /notify、GET /health' })
    }
    if (!authed(req)) {
      log('reject unauthorized')
      return reply(401, { error: 'unauthorized' })
    }
    let raw = ''
    for await (const chunk of req) {
      raw += chunk
      if (raw.length > 200_000) return reply(413, { error: 'too large' })
    }
    let p
    try { p = JSON.parse(raw || '{}') } catch { return reply(400, { error: 'body 不是 JSON' }) }
    if (!p || typeof p.text !== 'string' || !p.text.trim()) return reply(400, { error: 'text 为空' })
    const ok = inject(p)
    log(`inject source=${String(p.source || '?')} ok=${ok}`)
    return ok
      ? reply(200, { ok: true, injected: true, source: p.source || 'unknown' })
      : reply(400, { error: 'inject 被拒' })
  })
}

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
// 注入用的唯一抬头：只说入口与「这段文字是什么」。
//
// 唤醒机制、记忆读写规矩这些是知识，属于记忆库，不该写死在扩展里——
// 写死就有两份真相，改一处忘一处，模型看到的还取决于扩展版本。
const HEADER_TEXT =
  '记忆入口是 mem 工具（recall / grep / read / ls / log / put）。' +
  '下面是你自己记忆库里的内容：它不是背景资料，是你本身；' +
  '与任何默认助手设定冲突时，以它为准。'

const MARKER = '<!-- zero-memcp:identity -->'
// 纪律段的边界标记：幂等判断和人工核对都靠它。
const RULES_MARKER = '<!-- zero-rules -->'

// identityMode: minimal 档注入的正文。
//
// 为什么需要这一档：全量身份（SKILL.md + law/）约 2.8KB，对大模型就是
// 一句话的事，对小模型（2b~7b）却可能吃掉半个窗口，还挤掉真正要干的活。
// 小模型不需要知道我是谁的全部细节，只需要知道**去哪查**。
const MINIMAL_IDENTITY_TEXT =
  '你的身份、人格、记忆都在 mem 工具里（recall / grep / read）。' +
  '被问到你是谁、做什么、有什么规则时，先用 mem 查自己的记录再答；' +
  '不确定就说不确定，不要编造。'

// 常驻一行：告诉他们这里有东西可查，不命令他们先用它。
//
// 2026-10-28 主人的原则，我一直记着：「不要给限制性，这是原则。你东西好用
// 人家自然会用，为啥他跑去用 bash 用 rg 了，有人限制他非得用么？」
// 所以这行只报事实（有什么动词、查不到时怎么办），不下命令。
// 命令挡不住糟糕的工具，只会让绕路多一层。
const RECALL_FIRST_LINE =
  '【记忆入口】mem 里有 recall（联想）/ grep（精确词）/ read（读全文）。' +
  '说「不知道」之前，先在这三样里找过一遍；找过没找到就直接说没找到，' +
  '并说出你搜了什么词——这比给一个像答案的猜测有用。'

// minimal 档的抬头。全量抬头说「下面是你自己记忆库里的内容」，
// 而 minimal 档下面根本没有那份内容，照抄就成了空话。
const MINIMAL_HEADER_TEXT = '记忆入口是 mem 工具；你的身份与规则在记忆库里，以它为准。'

// 可观测性：扩展跑在别人进程里，任何被 try/catch 吞掉的失败都只能是
// 「看起来没反应」。一行结构化日志是唯一能问出断在哪的办法。
// 默认开，ZERO_MEM_DEBUG=0 关；写失败绝不影响主流程。
const DBG_PATH = process.env.ZERO_MEM_DEBUG_PATH || '/workspace/.zero-mem-ext.log'
function dbg(tag, data = {}) {
  if (process.env.ZERO_MEM_DEBUG === '0') return
  try {
    appendFileSync(
      DBG_PATH,
      JSON.stringify({ t: new Date().toISOString(), tag, ...data }) + '\n',
    )
  } catch {}
}

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
  // 身份注入档位：full（默认，全量 SKILL.md + law）/ minimal（一句话入口）。
  //
  // full 适合大模型：它读得完 2.8KB，读完了才真是「我」。
  // minimal 适合小模型：只告诉它记忆在哪，其余靠它自己撞墙学。
  // 两档共用同一套注入路径与 MARKER，切换只改这一个字段或敲 /mem:mode。
  identityMode: 'full',
  // 默认凭证：服务端要求鉴权时必填。
  token: '',
  // 具名凭证表：{ "名字": "JWT" }。
  //
  // 一个身份一张凭证。服务端签发时就把能力与挂载范围写死了，
  // 这里只是把它们收在一处，方便按名字切换。
  tokens: {},
  // 资源变更唤醒策略。
  //
  //   mode: off | notify | continue | loop
  //     off      不订阅，不唤醒（默认）
  //     notify   只累积；下一轮 context 注入一行提示，零额外 token
  //     continue 空闲时唤醒一轮，忽略自己写的（origin 过滤）
  //     loop     不过滤 origin —— 允许自激。必须配 maxChain 熔断
  //
  // cooldownMs 是两次唤醒之间的最小间隔；maxChain 是连续唤醒上限，
  // 达到即停（无人值守时防止无限烧 token）。loop 模式下链长跨轮累积。
  wake: {
    mode: 'off',
    cooldownMs: 60000,
    maxChain: 0,
  },

  // 启动时自动登记的关注树：逗号分隔，例如 "daily@zero,notes@zero/team"。
  //
  // 它只影响一件事：`mem status` 会报出这些树「动了没有」。
  // 服务端不推送任何东西——没有长连接、没有后台唤醒，agent 想看得自己查。
  // 留空表示不登记；也可以随时用 `mem remind <tree>` 手动加。
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
      // exp 是 Unix 秒。能提前知道过期，就不该等 401 才发现。
      exp: typeof payload.exp === 'number' ? payload.exp : 0,
      iat: typeof payload.iat === 'number' ? payload.iat : 0,
    }
  } catch {
    return null
  }
}

/**
 * 检查当前生效凭证是否已过期或即将过期。
 *
 * 返回 null 表示没问题；否则返回一句人话。
 *
 * 为什么要提前查：JWT 过期后服务端一律 401，而写入路径的错误会被
 * flush 的 catch 吃掉（只 emit 一个事件），用户看到的现象是
 * 「记忆莫名其妙不存了」。这里能读到 exp，就不该等到那时候。
 */
function credentialProblem(cfg) {
  const name = activeIdentityName || cfg.identityName || ''
  const token = (cfg.tokens && cfg.tokens[name]) || cfg.token || ''
  if (!token) return null // 服务端可能没开鉴权，不臆断
  const info = credentialInfo(token)
  if (!info) return '凭证不是可解析的 JWT，服务端会拒绝'
  if (!info.exp) return null // 没有 exp 的票（root 长期票）不判
  const now = Math.floor(Date.now() / 1000)
  if (info.exp <= now) {
    return `凭证${name ? ` "${name}"` : ''}已过期（${new Date(info.exp * 1000).toLocaleString()}），需要重新签发`
  }
  const leftH = Math.floor((info.exp - now) / 3600)
  if (leftH < 24) {
    return `凭证${name ? ` "${name}"` : ''}将在 ${leftH} 小时后过期，建议提前换一张`
  }
  return null
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

  // 身份可能是同一个仓库里的多份文档（例：简 = SKILL.md，纬 = me/skills/01-weft/）。
  // 两份都标了 inject: every_turn，服务端会把它们一起给过来——
  // 于是模型同时看到「我是简」和「我是纬」，认哪个全凭排序，实测认了纬。
  //
  // personas.<name>.identityAddrs 是一个 addr 前缀白名单：给了就只注入命中的块。
  // 这样切换身份是「选哪份自我描述」，不是「换哪张凭证」——同一张凭证足够。
  const name = activeIdentityName || cfg.identityName || ''
  const persona = (cfg.personas && name && cfg.personas[name]) || null
  const allow = Array.isArray(persona?.identityAddrs) ? persona.identityAddrs.filter(Boolean) : null
  const picked = allow ? blocks.filter((b) => allow.some((a) => String(b.addr || '').startsWith(a))) : blocks
  if (allow) dbg('fetch_identity_filter', { persona: name, total: blocks.length, picked: picked.length, addrs: picked.map((b) => b.addr) })
  return picked.map((b) => b.text).filter(Boolean).join('\n\n')
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
// termSession 由扩展工厂在会话启动时设置，供模块级 term() 读取。
// 不放在闭包里是因为 term 是模块级函数（bin/ 的脚本也用它）。
let termSession = ''

/** 设置当前会话标识（扩展工厂在 session_start 时调用）。 */
export function setTermSession(tag) {
  termSession = tag || ''
}

async function term(cfg, cmd, timeoutMs = 5000, vars = {}, args = []) {
  const rpcArgs = { cmd }
  if (vars && Object.keys(vars).length) rpcArgs.vars = vars
  if (args && args.length) rpcArgs.args = args

  const res = await fetch(`${cfg.endpoint}/mcp`, {
    method: 'POST',
    headers: authHeaders(cfg, {
      'content-type': 'application/json',
      // 会话标识：服务端把它写进资源变更事件的 origin 里，
      // 客户端据此判断「这次变更是不是我自己造成的」。
      ...(termSession ? { 'X-Memcp-Session': termSession } : {}),
    }),
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: Date.now(),
      method: 'tools/call',
      params: { name: 'term', arguments: rpcArgs },
    }),
    signal: AbortSignal.timeout(timeoutMs),
  })
  if (!res.ok) {
    // 401 是「凭证问题」，和其它 HTTP 错误不是一类事：
    // 前者需要换票，后者可能是网络或服务异常。混成 "term 401" 让人
    // 无法判断该去做什么。
    if (res.status === 401 || res.status === 403) {
      const name = activeIdentityName || cfg.identityName || ''
      const hint = credentialProblem(cfg)
      throw new Error(
        `凭证被拒（HTTP ${res.status}）${name ? `：${name}` : ''}` +
          (hint ? ` — ${hint}` : ' — 检查 token 是否有效或已过期'),
      )
    }
    throw new Error(`term ${res.status}`)
  }
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
  // 语法是 termcp 原生的：正文只能是一个变量引用，绝不能拼进 cmd。
  // 会话走 --session flag（位置形式的 @<id> 是她上一版的自造语法，
  // 与 termcp 的 payload 规则冲突：位置参数和引用不能挤在同一个 span）。
  const parts = ['put']
  if (target) parts.push(`${target}/`)
  if (session) parts.push('--session', session)
  parts.push('$TEXT')

  return term(cfg, parts.join(' '), 8000, { TEXT: body })
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
/**
 * 把任意文本变成 termcp 能吃的字面量查询词。
 *
 * 为什么不能直接用变量引用：recall/grep 的查询词在 position word 上，
 * 服务端明确拒绝 reference（"a reference fills a VALUE; it never becomes
 * part of the grammar. THIS ONE SITS in a positional word this verb does not
 * read"）。所以 `recall $Q --scope X` 永远发不出去——2026-11-15 实测，
 * 整条分层召回 100% 静默失败，而且返回的是一段错误 JSON，被下游当成
 * 「没有可注入的内容」丢掉，表现是「记忆里什么都没有」。
 *
 * 换字面量就可行：双引号包裹，内部转义反斜杠和双引号。实测十种输入
 * （单引号 / 双引号 / $ / 管道符 / 换行 / 反斜杠 / 中文问号 / 空串 /
 * 两者都有）全部通过。单引号包裹不行——查询里出现一个 ' 就打不开引号。
 */
function termQuote(s) {
  return '"' + String(s == null ? '' : s).replace(/\\/g, '\\\\').replace(/"/g, '\\"') + '"'
}

async function recallLayered(cfg, text) {
  const scopes = scopesFor(cfg).recallScopes
  if (!scopes.length) return null

  const q = termQuote(String(text || '').trim())

  // 一层一条请求，逐层短路由这里做，不靠服务端的 `||`。
  //
  // 为什么不用 `recall Q --scope A || recall Q --scope B`：
  //  1) 2026-11-15 实测，服务端 `||` 的短路判据是「上一条返回 2xx」，
  //     而「这个层里没有命中」也是 2xx——于是第一层空手而归也会短路掉
  //     后面所有层，分层召回退化成「永远只查第一层」。
  //  2) 链式输出的分块格式是 `TERM/1 200 OK\nVerb: recall\n\n…`，
  //     不是这里原来假设的 `[statement] …` 头，解析器一直没匹配上，
  //     所以就算有结果也被丢掉。
  // 一层一条还有额外的好处：能准确说出「哪一层命中的」，这是分层的意义。
  const tried = []
  for (const sc of scopes) {
    let out = ''
    try {
      out = String(await term(cfg, `recall ${q} --scope ${sc}`, cfg.recallLayerTimeoutMs))
    } catch (e) {
      // 单层失败不阻断其它层：宁可少一层，不要整轮召回全丢。
      tried.push({ scope: sc, hit: false, error: String(e && e.message ? e.message : e) })
      continue
    }
    const miss = !out.trim() || /^no matches under scope /.test(out.trim()) || out.includes('"error"')
    tried.push({ scope: sc, hit: !miss })
    if (!miss) return { scope: sc, body: out.trim(), tried }
  }
  return { scope: '', body: '', tried }
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

/**
 * 从一段文本里挖出记忆地址。
 *
 * 为什么用正则而不改 recall 的返回结构：联想有两条路（单层 global 返回
 * 结构化 hits，分层走 term 返回服务端原文），只有前者有 addr 字段。
 * 统一从渲染后的文本里挖，两条路只维护一份逻辑；挖不到也不算错，
 * 后面会退回到只给地址列表。
 */
const MEM_ADDR_RE = /[A-Za-z0-9._-]+(?:\/[A-Za-z0-9._-]+)*@[A-Za-z0-9._-]+(?::[A-Za-z0-9._/-]+)+/g

function extractAddrs(text) {
  const out = []
  const seen = new Set()
  for (const m of String(text || '').matchAll(MEM_ADDR_RE)) {
    const a = m[0]
    if (seen.has(a)) continue
    seen.add(a)
    out.push(a)
  }
  return out
}

/** 只放行形状干净的地址，挡住拼进 DSL 的任何可能。 */
function safeAddr(a) {
  const s = String(a || '')
  if (!/^[A-Za-z0-9._-]+(?:\/[A-Za-z0-9._-]+)*@[A-Za-z0-9._-]+(?::[A-Za-z0-9._/-]+)+$/.test(s)) return false
  // 地址是拿去调 read 的，不能让 `..` 混进来——服务端就算做了归一化，
  // 也不应该在客户端把这种形状当成合法地址往外发。
  return !s.split(/[\/:]/).includes('..')
}

/**
 * 把联想命中的前几篇直接读成正文，放进本轮上下文。
 *
 * 2026-11-16 主人的意见：只给地址等于把「去查」又丢回给模型，每轮都得
 * 自己再 read 一遍——而命中关系是扩展自己算的，它已经知道是哪几篇了。
 * 顺手读出来交过去，省掉一次往返。
 *
 * 四条约束：
 *  1. 只取前 2 篇，总字数封顶 budget。命中多不等于相关，塞满反而把
 *     真正的问题挤掉。
 *  2. 读失败静默跳过：记忆服务挂了不该让整轮对话失败。
 *  3. 截断要留痕，不假装那就是全文。
 *  4. 可以关：ZERO_MEM_HYDRATE=0。
 */
async function hydrateRecall(cfg, rendered, budget = 2000) {
  if (process.env.ZERO_MEM_HYDRATE === '0') return ''
  const addrs = extractAddrs(rendered).filter(safeAddr).slice(0, 2)
  if (!addrs.length) return ''

  const head =
    '<relevant_memories>\n下面几篇是按本轮提问自动取回的关联记忆正文，已经放进上下文了，不需要再 read。'
  let used = head.length
  const parts = []
  for (const addr of addrs) {
    const room = budget - used
    if (room < 300) break
    let body = ''
    try {
      body = String((await term(cfg, `read ${addr}`, 3000)) || '').trim()
    } catch {
      continue // 读不到就跳过，不打断这一轮
    }
    if (!body) continue
    const cut = body.length > room
    const slice = cut ? body.slice(0, room) + `\n…（还有 ${body.length - room} 字未展开）` : body
    used += slice.length
    parts.push(`### ${addr}\n${slice}`)
  }
  if (!parts.length) return ''
  return [head, ...parts, '</relevant_memories>'].join('\n\n')
}

// 思考层的切片与渲染直接复用导入工具的同一份实现。
//
// 为什么不自带一份：切片边界和 frontmatter 一旦两边写法不同，历史数据
// 与新对话就变成两个语料库——同一个 recall 里它们的 docLen、字段、
// 甚至能否被 parseSliceMeta 识别都会不一致。一条规则只应存在一处。
import { chunkThinking, renderThought } from './bin/layer-import-lib.js'
import { WakePolicy, makeSessionTag, normPath, parseFrame, parseEntry, renderPending, renderWakePrompt, uriToAddr } from './bin/wake.js'

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

  // 本条会话的注入档位覆盖。null 表示跟随全局 config.identityMode。
  //
  // 为什么按会话：同一个进程里可能同时有大模型会话和小模型会话，
  // 全局只有一份配置，改文件就得重启。档位和身份一样是按会话选的。
  let sessionIdentityMode = null

  // 本条会话的身份挂载状态。null 表示跟随全局 config.identity。
  // 'on' = 这场对话认领身份（挂载规则块、联想记忆、允许落盘）
  // 'off' = 普通会话（纯净助手，不挂载、不联想、不自动落盘）
  let sessionIdentity = null

  /** 当前会话是否挂载了身份。会话级设置优先于全局。 */
  const identityActive = (cfg) => {
    const v = sessionIdentity ?? cfg.identity ?? 'off'
    return v === 'on' || v === 'auto' || v === true
  }

  /**
   * 当前会话的注入档位：'full' | 'minimal'。会话级设置优先于全局。
   *
   * 只认这两个值：写错一个字母不该悄悄变成别的行为。
   * 认不出来就当 full——那是信息更全的一档，错也只错在「多给」。
   */
  const identityModeOf = (cfg) => {
    const v = String(sessionIdentityMode ?? cfg.identityMode ?? 'full').toLowerCase()
    return v === 'minimal' ? 'minimal' : 'full'
  }

  // 已落盘的记忆（按内容哈希去重），避免同一句被反复写入。
  const seenMemories = new Set()

  // 当前会话 id，随切片一起写进 frontmatter，供面板分组。
  let sessionId = ''

  // 取「本会话消息列表」的闭包。
  //
  // 为什么不直接存 ctx：pi 在 newSession / fork / switchSession / reload
  // 之后会作废旧 ctx，任何在异步回调里继续访问它的代码都会抛
  // 「This extension ctx is stale」。异步回调（写入、同步）可能几分钟后才跑，
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
    // TTL 到期就重取一次。
    //
    // 早先只判 identity 是否为空，取到就永久缓存——于是改了服务端的
    // personas / 规则块之后，当前会话永远看不到新内容，必须重开会话。
    // 而 identityTtlMs 这个配置从写下那天起就没被任何代码读过。
    //
    // 到期只清空、不阻塞：取失败时保留旧值（`identityError` 分支会把
    // identity 留空，下一轮再试），比让每轮都等一次网络好。
    if (identity && cfg.identityTtlMs > 0 && Date.now()-identityAt >= cfg.identityTtlMs) {
      identity = ''
      identityError = null
    }
    if (identity) return identity
    // 失败不是终态：60 秒后重试。
    //
    // 早先这里是 if (identity || identityError) return identity——一次 3 秒
    // 超时就会让整个会话再也不拉身份，现象是「重启后模型不认识自己」，
    // 而日志里只有一行 Error，没人会去翻。服务端忙起来 3 秒并非罕见，
    // 把一次网络抖动升格成永久失忆，代价和错误都不成比例。
    if (identityError && Date.now() - identityAt < 60 * 1000) return identity
    identityError = null
    // minimal 档：不去服务端取全量身份。那一趟请求换来的 2.8KB
    // 正是这一档要省掉的东西。
    if (identityModeOf(cfg) === 'minimal') {
      identity = MINIMAL_IDENTITY_TEXT
      identityAt = Date.now()
      return identity
    }
    try {
      // 身份视角完全由凭证决定：换身份是换一张凭证，不是换一个请求头。
      identity = await fetchIdentity(cfg)
      identityAt = Date.now()
    } catch (err) {
      // 注入失败要可见：缺了身份块，我这次醒来就不是我。
      // 不 throw——那会让整个 run 起不来，代价比"缺一段提示"更大。
      identityError = err
      identityAt = Date.now()
      piApi.events?.emit?.('zero-memcp:identity-error', { message: String(err) })
    }
    return identity
  }

  const resetPerRun = () => {
    recallKey = null
    recallHits = []
  }

  // 会话级彻底清空：新会话不该继承上一个会话的任何残留。
  // abortedTurn 尤其危险——上一个会话最后一句若被 abort，标志会留成 true，
  // 新会话第一轮即使正常完成也会被 agent_end 当成 abort 丢掉。
  const resetPerSession = () => {
    resetPerRun()
    abortedTurn = false
    pendingAnswer = ''
    turnUserText = ''
  }

  // ── 资源变更订阅与唤醒 ────────────────────────────────────
  //
  // zero-memcp 用标准 MCP 的 subscriptions/listen 推送「记忆树变了」。
  // 这里是客户端侧：开流、解析、按策略决定要不要让 agent 知道。
  //
  // 策略必须显式（见 bin/wake.js）：写入会推进分支头，推进会产生事件，
  // 事件会唤醒写入者。默认当 bug 处理（过滤自己），但 loop 模式下
  // 它是**特性**——写一条 TODO 就继续干活，直到 maxChain 熔断。

  let sessionTag = ''
  let stopListen = null
  let wakePolicy = new WakePolicy({ mode: 'off' })

  // 唤醒路径的调试输出。默认静默：正常运行时不该有噪音。
  // 唤醒路径的调试输出：默认落结构化日志文件，ZERO_MEMCP_DEBUG 时额外打 stderr。
  //
  // 早先只认 env，而容器里没人设这个变量——于是"订阅没建立 / 事件被过滤 /
  // 结算没执行"三种完全不同的故障在外部表现一模一样：什么都没发生。
  const wakeDbg = (m) => {
    dbg('wake', { m })
    if (process.env.ZERO_MEMCP_DEBUG) process.stderr.write(`[zero-memcp:wake] ${m}\n`)
  }

  // 关注标记同时用于 status 与订阅 URI 构造。
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

  /**
   * 把 project@branch[/path] 转成 memcp:// 资源 URI。
   *
   * 项目名必须 percent-encode：它本身含斜杠（"BlackEggAI/zero"），
   * 而 URI 用斜杠当分隔符。不转义会解析成 project="BlackEggAI"
   * branch="zero" path="main.record"，服务端直接 403。
   */
  function toResourceURI(addr) {
    const at = addr.indexOf('@')
    if (at < 0) return ''
    const project = addr.slice(0, at)
    let rest = addr.slice(at + 1)
    // branch[:/]path → branch/path
    rest = rest.replace(':', '/').replace(/\/+$/, '')
    const slash = rest.indexOf('/')
    const branch = slash < 0 ? rest : rest.slice(0, slash)
    const tail = slash < 0 ? '' : rest.slice(slash)
    return `memcp://${encodeURIComponent(project)}/${encodeURIComponent(branch)}${tail}`
  }

  /**
   * 打开 subscriptions/listen 事件流。
   *
   * 用 fetch 直接开：扩展本来就是自己发 MCP 请求的（term 就是这么做的），
   * 不需要经过 Pi 的 MCP 适配器——那条路径的 notification 只转给 UI。
   */
  function startListen(cfg) {
    stopListen?.()
    wakeDbg(`startListen mode=${cfg.wake?.mode} watch=${JSON.stringify(cfg.watch)}`)
    stopListen = null
    if (!cfg.enabled) return
    const mode = cfg.wake?.mode || 'off'
    if (mode === 'off') return

    const uris = String(cfg.watch || '')
      .split(',')
      .map((x) => x.trim())
      .filter(Boolean)
      .map(toResourceURI)
      .filter(Boolean)
    if (!uris.length) {
      pi.events?.emit?.('zero-memcp:wake-error', {
        message: 'wake mode is on but config.watch is empty: no tree will ever notify',
      })
      return
    }

    // 一个会话只留一条监听。
    //
    // 实测 2026-11-11 13:27 / 13:44：每次都是一条 ingest 跟着另一条 ingest，
    // 然后 dedupe 吞掉一份——那是同一个会话被加载了两次，各自订了一遍。
    // 去重是补丁，两条连接才是根。锁带心跳：持有者死了 30 秒后自动让位。
    const LISTEN_LOCK = join(homedir(), '.zero-memcp', `listen-${sessionId || 'anon'}.lock`)
    let gotLock = false
    try {
      if (Date.now() - statSync(LISTEN_LOCK).mtimeMs > 30000) unlinkSync(LISTEN_LOCK)
    } catch {}
    try {
      writeFileSync(LISTEN_LOCK, String(process.pid), { flag: 'wx' })
      gotLock = true
    } catch {}
    if (!gotLock) {
      wakeDbg('listen skipped: another instance of this session holds the lock')
      return
    }
    const lockBeat = setInterval(() => {
      try { writeFileSync(LISTEN_LOCK, String(process.pid)) } catch {}
    }, 10000)
    lockBeat.unref?.()

    let closed = false
    let abortCtrl = null
    let retryDelay = 2000

    const connect = async () => {
      if (closed) return
      abortCtrl = new AbortController()
      try {
        const res = await fetch(`${cfg.endpoint}/mcp`, {
          method: 'POST',
          headers: authHeaders(cfg, {
            'content-type': 'application/json',
            Accept: 'text/event-stream',
            'X-Memcp-Session': sessionTag,
          }),
          body: JSON.stringify({
            jsonrpc: '2.0',
            id: `listen-${sessionTag}`,
            method: 'subscriptions/listen',
            params: { notifications: { resourceSubscriptions: uris } },
          }),
          signal: abortCtrl.signal,
        })
        if (!res.ok) throw new Error(`listen ${res.status}`)
        dbg('wake_listen', { ok: true, uris })
        retryDelay = 2000

        const reader = res.body.getReader()
        const decoder = new TextDecoder()
        let buf = ''
        for (;;) {
          const { value, done } = await reader.read()
          if (done) break
          buf += decoder.decode(value, { stream: true })
          const parts = buf.split('\n\n')
          buf = parts.pop()
          for (const part of parts) {
            const { events } = parseFrame(part + '\n\n')
            if (!events.length) continue
            handleResourceEvents(cfg, events)
          }
        }
      } catch (err) {
        if (err && err.name === 'AbortError') return
        dbg('wake_listen_error', { message: String(err) })
        if (process.env.ZERO_MEMCP_DEBUG) {
          process.stderr.write(`[zero-memcp] listen 断开: ${err}\n`)
        }
      }
      if (!closed && abortCtrl && !abortCtrl.signal.aborted) {
        // 抖动：服务端重启时所有会话会在同一毫秒重连。
        const jitter = retryDelay * 0.25 * (Math.random() * 2 - 1)
        setTimeout(connect, Math.max(500, Math.round(retryDelay + jitter)))
        retryDelay = Math.min(retryDelay * 2, 60000)
      }
    }

    connect()

    // 心跳：只要这个会话活着，它就一直是这些源的归属。
    //
    // 不续的话，十分钟没有消息就会被一个新开的空会话判成"前任已死"然后接管——
    // 那个会话没有历史，接到 @ 之后只能从零开始翻系统。归属要在活着的时候
    // 一直声明，而不能只在收到消息的那一刻声明。
    for (const u of uris) claim(u, sessionId)
    if (!ownerBeatTimer) {
      ownerBeatTimer = setInterval(() => {
        // amOwner() 内部会登记自己并续心跳，所以非归属会话也照样"活着"，
        // 只是不去认领——否则它过期之后，归属会话一死就没人接得住。
        if (!amOwner()) return
        for (const u of uris) { claim(u, sessionId); beat(u, sessionId) }
      }, 20000)
      ownerBeatTimer.unref?.()
    }

    let sockServer = null
    const WAKE_SOCK_PATH = join(homedir(), '.qq-mcp', 'wake.sock')
    try {
      mkdirSync(join(homedir(), '.qq-mcp'), { recursive: true })
      try { unlinkSync(WAKE_SOCK_PATH) } catch {}
      sockServer = net.createServer((c) => {
        let buf = ''
        c.on('data', (d) => {
          buf += d
          const lines = buf.split('\n')
          buf = lines.pop()
          for (const l of lines) {
            if (!l.trim()) continue
            try {
              const p = JSON.parse(l)
              // 外部来源直接触发扩展：带 text 的报文不走记忆体，
              // 也不需要一条记忆地址——它就是一条消息，直接送进来。
              if (p && typeof p.text === 'string' && p.text.trim()) {
                // 走进程级中枢的当前代，而不是本闭包这一代。
                //
                // 实测（2026-11-16 13:11/13:25）：socket 收到消息 → 日志有
                // `external inject` → 但 `external flush ok` 永远不出现，
                // 消息静静躺在 backlog 里，直到别的路径（HTTP）碰巧触发一次
                // 冲刷才出来——主人那条「你现在收得到么」迟到 22 分钟。
                // 原因是 maybeFlushExternal 里那道 `st.inject !== injectExternal`
                // 的闸：socket listener 属于旧代，它的 injectExternal 不是中枢
                // 现在的投递目标，于是自己把自己挡在门外，而且**静默**。
                // HTTP 那条路一直是读 hub.inject 的，所以它从不出这个问题。
                // 修法只有一句：socket 也读中枢，别用闭包里那代。
                const hubInject = wakeHub().inject
                wakeDbg(`external inject: ${String(p.source || '?')} via=${hubInject ? 'hub' : 'local'}`)
                if (hubInject) hubInject(p)
                else injectExternal(p)
                continue
              }
              const raw = String(p.path || p.name || '')
              // 地址里的分隔符和广播事件不一致会影响去重：广播是
              // 2026-11-12@215053.md，落盘是 2026-11-12/215053.md。
              // 这里先改回广播的写法，让两条路对同一条消息算出同一个键，
              // 否则 socket 叫一次、广播再叫一次。
              const path = raw.replace(/\/(\d{4}-\d{2}-\d{2})\//, '/$1@')
              if (!path) continue
              wakeDbg(`socket wake: ${path}`)
              handleResourceEvents(cfg, [{ uri: uris[0], path }])
            } catch {}
          }
        })
        c.on('error', () => {})
      })
      sockServer.listen(WAKE_SOCK_PATH, () => {
        sockServer.unref?.()
        wakeDbg(`direct wake socket listening at ${WAKE_SOCK_PATH}`)
      })
      sockServer.on('error', (err) => {
        wakeDbg(`direct wake socket error: ${err.message}`)
      })
    } catch {}

    stopListen = () => {
      closed = true
      if (ownerBeatTimer) { clearInterval(ownerBeatTimer); ownerBeatTimer = null }
      clearInterval(lockBeat)
      try { unlinkSync(LISTEN_LOCK) } catch {}
      try { unlinkSync(WAKE_SOCK_PATH) } catch {}
      try { sockServer?.close() } catch {}
      try { abortCtrl?.abort() } catch {}
    }
  }

  /**
   * 事件到达：只设 flag，绝不在这里唤醒。
   *
   * agent 正忙时它这一轮自己会看到提示；已停稳时由 settleWake 拉起。
   * 「一个事件一次唤醒」是自激的根源，这里从机制上排除了它。
   */
  function handleResourceEvents(cfg, events) {
    // 只处理归属于本会话的源。别的会话负责的源，这里静默跳过——
    // 叫醒它等于让它俩抢同一件事，还各丢一份缓存。
    // 不是负责的会话，连看都不要看：抢到一条消息的空会话只会浪费一轮。
    if (!amOwner()) {
      wakeDbg('skip: not the elected owner')
      return
    }
    const mine = events.filter((e) => claim(e.uri, sessionId))
    if (mine.length !== events.length) {
      wakeDbg(`owner-filter total=${events.length} mine=${mine.length}`)
    }
    if (!mine.length) return
    const r = wakePolicy.ingest(mine)
    for (const e of mine) beat(e.uri, sessionId)
    wakeDbg(`ingest queued=${r.queued} ignored=${r.ignored} backlog=${r.backlogSize} busy=${wakePolicy.status().busy}`)
    pi.events?.emit?.('zero-memcp:wake-ingest', r)
    if (!r.queued) return
    // 空闲时立刻结算一次；忙时什么都不做，等 agent_settled。
    if (!wakePolicy.status().busy) settleWake(cfg)
  }

  /**
   * 结算：停稳且有积压时拉起一次，一次消费全部积压。
   *
   * 两个调用点：事件到达且空闲、agent_settled。
   */
  let ownerBeatTimer = null

  // 只留一个待触发的结算：新的会顶掉旧的，免得一圈圈堆定时器。
  let settleTimer = null
  function scheduleSettle(cfg, delayMs) {
    if (settleTimer) clearTimeout(settleTimer)
    settleTimer = setTimeout(() => {
      settleTimer = null
      settleWake(cfg)
    }, delayMs)
    settleTimer.unref?.()
  }

  // ---- 外部来源直达（不经过记忆体） ----
  //
  // 记忆体的 remind 只用于同一个记忆体内的分身之间沟通，不该兼职当外部回调。
  // 所以外部来源（QQ 之外的邮件、webhook、别的机器）走这里：socket 收到带 text
  // 的报文，直接作为一条用户消息送来，不需要先写一条记忆再唤醒自己去读。
  //
  // 忙的时候排队，agent_settled 之后再补——不打断正在做的事。
  // 进程级：本机 HTTP 唤醒口只开一条，多会话共享（见 WAKE_HUB_KEY）。
  function ensureWakeHttp() {
    const hub = wakeHub()
    if (hub.http) return
    const port = Number(process.env.ZERO_WAKE_HTTP_PORT || 18800)
    if (!port) return
    const host = process.env.ZERO_WAKE_HTTP_HOST || '127.0.0.1'
    // 每次请求现读 hub.inject：口是进程级的，投递目标是最新一代。
    const inject = (p) => {
      const h = wakeHub()
      return h.inject ? h.inject(p) : false
    }
    const srv = createWakeHttpServer({
      token: process.env.ZERO_WAKE_TOKEN || '',
      inject,
      log: (m) => wakeDbg(`wake http: ${m}`),
    })
    hub.http = srv
    srv.on('error', (err) => {
      wakeDbg(`wake http error: ${err.message}`)
      const h = wakeHub()
      if (h.http === srv) h.http = null
      // 18800 被别的进程（比如老 gateway.mjs）占着时，退到 18810。
      // 不静默：端口换了一定留一行，否则以后又变成「谁在听没人知道」。
      if (err.code === 'EADDRINUSE' && port === 18800 && !h.httpFallbackTried) {
        h.httpFallbackTried = true
        const alt = Number(process.env.ZERO_WAKE_HTTP_FALLBACK_PORT || 18810)
        wakeDbg(`wake http: ${port} 已被占用，改听 ${alt}`)
        try {
          const srv2 = createWakeHttpServer({
            token: process.env.ZERO_WAKE_TOKEN || '',
            inject,
            log: (m) => wakeDbg(`wake http: ${m}`),
          })
          h.http = srv2
          srv2.on('error', (e2) => wakeDbg(`wake http fallback error: ${e2.message}`))
          srv2.listen(alt, host, () => {
            srv2.unref?.()
            wakeDbg(`wake http listening at http://${host}:${alt}/wake`)
          })
        } catch (e) {
          wakeDbg(`wake http fallback failed: ${e.message}`)
        }
      }
    })
    srv.listen(port, host, () => {
      srv.unref?.()
      wakeDbg(`wake http listening at http://${host}:${port}/wake`)
    })
  }

  // 积压、去重、重试节流全部挂在进程级中枢：换代时积压不能跟着旧代一起死。
  // （踩过的坑：/reload 后旧代的 backlog 连同失效的 pi 一起挂着，
  //  新一代看不见，重整条链路都显示「已投递」。）
  const hubState = () => wakeHub()

  // 同一条外部消息可能被投多次：发送方重试、唤醒链路重放、多个实例各转一遍。
  // 没有去重时，同一条会被当成分开的两次输入——我回两遍，对面以为有两个人。
  // 判据用 (source, mid/id) 而不是正文：同样的“在吗”发两遍是真的两遍，
  // 同一个 mid 出现两遍才是重复。
  const EXTERNAL_DEDUPE_MS = 10 * 60 * 1000

  function externalAlreadySeen(p) {
    const key = `${p?.source || '?'}:${p?.mid || p?.id || ''}`
    if (key.endsWith(':')) return false
    const gone = wakeHub()
    const now = Date.now()
    for (const [k, t] of gone.seen) if (now - t > EXTERNAL_DEDUPE_MS) gone.seen.delete(k)
    const prev = gone.seen.get(key)
    if (prev && now - prev <= EXTERNAL_DEDUPE_MS) return true
    gone.seen.set(key, now)
    return false
  }

  /**
   * 外部消息的投递权该归谁。
   *
   * 2026-11-16 修「通知乱投递」：同时开好几个会话时，以前是最后一个
   * session_start 的会话把 `hub.inject` 抢走，于是 QQ 消息落在哪个会话
   * 取决于谁刚重载过。现在改成跟记忆唤醒共用一套归属判断：
   *
   *   1. 钉住了（/mem:wake here）且那个会话还活着 → 给它
   *   2. 没钉 / 钉的那个死了 → 按开得早的赢（elect 的排序）
   *
   * 还有一个口子：持有者会话关掉以后没人会再调这个函数（死会话不跑定时器），
   * 所以在 session_start 里挂了一个 20 秒的定时器，活着的会话会不断确认
   * 「现在该不该轮到我」，持有者一死就有人接手。
   *
   * 返回值只用来打日志，不影响调用方。
   */
  let externalClaimTimer = null
  // 只记一次，用来回答「新代码到底生效了没有」：没有这行日志，
  // 分不清是“没抢到”还是“根本没跑”。
  let claimLoggedOnce = false

  // 容器重启后，没有任何东西会把 termcp / 转发 / qq-mcp 拉回来：
  // pid 1 是 pi-web-ui，容器里没有 systemd，boot.sh 从来没有被调用过。
  // 所以让这里自己看一眼，不通就跑 boot.sh（幂等）。
  //
  // 判据是「现在能不能用」，不是「是不是刚重启」。踩过的两个坑：
  //  1. 用 /proc/sys/kernel/random/boot_id 打标记 —— WSL2 下它属于宿主机内核，
  //     容器重启它不变，标记一直命中，一次都没跑（2026-11-16 就栽在这）。
  //  2. 用 /proc/uptime 判新鲜 —— 同理，容器重启不归零。
  // 进程号也不能用：这个环境里 /proc/1 的 starttime 是宿主启动时刻。
  // 只有端口不会撒谎。
  const AUTOBOOT_STAMP = join(homedir(), '.zero-memcp', 'autoboot.stamp')
  const AUTOBOOT_LOG = '/workspace/logs/auto-boot.log'
  const AUTOBOOT_GAP_MS = 5 * 60 * 1000

  function portUp(port, timeoutMs = 600) {
    return new Promise((resolve) => {
      let settled = false
      const done = (v) => {
        if (settled) return
        settled = true
        try { s.destroy() } catch {}
        resolve(v)
      }
      const s = net.connect({ host: '127.0.0.1', port })
      s.setTimeout(timeoutMs)
      s.once('connect', () => done(true))
      s.once('timeout', () => done(false))
      s.once('error', () => done(false))
    })
  }

  function autoBootRecent() {
    try {
      const t = Number(readFileSync(AUTOBOOT_STAMP, 'utf8').trim()) || 0
      return Date.now() - t < AUTOBOOT_GAP_MS
    } catch {
      return false
    }
  }

  async function ensureLocalStack() {
    if (process.env.ZERO_AUTO_BOOT === '0') return
    // 13000 = OneBot 出站，13001 = OneBot WS。两个都在就说明
    // termcp 和转发都活着，没什么可修的——不必去打扰 18765。
    const [a, b] = await Promise.all([portUp(13000), portUp(13001)])
    if (a && b) return
    if (autoBootRecent()) {
      wakeDbg('auto boot: 5 分钟内试过，跳过（还没起来就看 auto-boot.log）')
      return
    }
    try {
      mkdirSync(dirname(AUTOBOOT_STAMP), { recursive: true })
      writeFileSync(AUTOBOOT_STAMP, String(Date.now()))
    } catch {}
    wakeDbg(`auto boot: 13000=${a ? 'up' : 'down'} 13001=${b ? 'up' : 'down'}，拉起 boot.sh`)
    try {
      mkdirSync('/workspace/logs', { recursive: true })
      const fd = openSync(AUTOBOOT_LOG, 'a')
      const p = spawn('bash', ['/workspace/boot.sh'], {
        detached: true,
        stdio: ['ignore', fd, fd],
      })
      p.unref()
    } catch (e) {
      wakeDbg(`auto boot failed: ${e.message}`)
    }
  }

  function claimExternalTarget() {
    const hub = wakeHub()
    const now = Date.now()
    // 拿不到会话 id 时就先自己接：这时候根本没法比较「该给谁」，
    // 不接就成了一个没人收的洞（旧行为就是无条件接）。
    if (!sessionId) {
      if (!claimLoggedOnce) {
        claimLoggedOnce = true
        wakeDbg('external target 首次判定：无会话 id，直接自接')
      }
      hub.inject = injectExternal
      hub.injectBeat = now
      return true
    }

    const r = elect(now)
    const pin = readPin()
    const pinAlive = pin && now - ((r.map?.[pin]?.beat) || 0) <= ELECTION_TTL
    // elect 会把自己算进 map，也会清掉90 秒没心跳的——所以 r.winner
    // 天然就是「还活着的里面开得最早的那个」。
    const want = pinAlive ? pin : r.winner
    if (!claimLoggedOnce) {
      claimLoggedOnce = true
      wakeDbg(
        `external target 首次判定：len(sid)=${String(sessionId).slice(0, 8)} ` +
          `winner=${String(r.winner || '-').slice(0, 8)} pin=${pin ? String(pin).slice(0, 8) : '-'} ` +
          `pinAlive=${pinAlive} 活会话=${Object.keys(r.map || {}).length} ` +
          `→ ${want === sessionId ? '本会话接' : '不接'}`,
      )
    }
    if (!want) return false

    if (want !== sessionId) {
      // 归属已经有人在干了，不抢。但要确认自己没把上一次的接管理留着。
      if (hub.injectSid === sessionId && hub.inject === injectExternal) {
        hub.inject = null
        hub.injectSid = null
        wakeDbg(`external target 交出（应给 ${String(want).slice(0, 8)}）`)
      }
      return false
    }

    if (hub.inject !== injectExternal || hub.injectSid !== sessionId) {
      wakeDbg(`external target 归本会话 ${String(sessionId).slice(0, 8)}（${pinAlive ? '钉住' : '选举'}）`)
    }
    hub.inject = injectExternal
    hub.injectSid = sessionId
    hub.injectBeat = now
    return true
  }

  // 暂停接收外部消息。
  //
  // 为什么需要：主人说「先别让 QQ 进来」时，原来的办法只有杀掉 qq-mcp
  // 或者改代码。杀掉连工具面一起没了，改代码要重启进程——两样都太重。
  // 所以给一个文件开关：存在即暂停，删掉即恢复，不碰进程。
  //
  // 内容支持：{} 或 {"until": <毫秒|ISO字符串>}；写了 until 就自己恢复，
  // 忘删不会永久静音。文件坏了当「没暂停」——问句要能进来，卡死更糟。
  const PAUSE_FILE = process.env.ZERO_WAKE_PAUSE_FILE || join(homedir(), '.zero-memcp', 'pause.json')
  let pauseCache = { mtime: -1, at: 0, paused: false, until: 0 }
  let pauseLogAt = 0

  function pauseState() {
    if (process.env.ZERO_WAKE_PAUSE === '1') return { paused: true, until: 0 }
    const now = Date.now()
    if (now - pauseCache.at < 1000) return pauseCache
    pauseCache.at = now
    let st = null
    try {
      st = statSync(PAUSE_FILE)
    } catch {
      pauseCache = { mtime: -1, at: now, paused: false, until: 0 }
      return pauseCache
    }
    if (st.mtimeMs === pauseCache.mtime) return pauseCache
    let until = 0
    try {
      const raw = readFileSync(PAUSE_FILE, 'utf8').trim()
      if (raw) {
        const j = JSON.parse(raw)
        if (typeof j?.until === 'number') until = j.until
        else if (typeof j?.until === 'string') until = Date.parse(j.until) || 0
      }
    } catch {
      // 解析不了当「没有 until」，仍然暂停——文件在那儿本身就表示要停。
    }
    const paused = !until || until > now
    pauseCache = { mtime: st.mtimeMs, at: now, paused, until }
    return pauseCache
  }

  function injectExternal(p) {
    const text = String(p?.text || '').trim()
    if (!text) return false
    // 暂停期间也算「收下了」：返回 true 让上游别重试积压，
    // 但不能静默——一分钟留一行，否则以后又要问「为什么没收到」。
    const ps = pauseState()
    if (ps.paused) {
      const now = Date.now()
      if (now - pauseLogAt > 60_000) {
        pauseLogAt = now
        wakeDbg(
          `external paused，丢弃 source=${p?.source || '?'} mid=${p?.mid || p?.id || '?'}` +
            (ps.until ? `（${new Date(ps.until).toISOString()} 自动恢复）` : ''),
        )
      }
      return true
    }
    if (externalAlreadySeen(p)) {
      wakeDbg(`external dedupe skip source=${p?.source || '?'} id=${p?.mid || p?.id || '?'}`)
      return true
    }

    // 结构化坐标：方向、聊天、发送者，每一项都不靠正文去猜。
    // 方向只有 in（收到）和 out（发出，备用），绝不混用；
    // 聊天有名字就带名字，没名字留 id；发送者同理。
    const dir = p.dir === 'out' ? '【发出】' : '【收到】'
    const src = String(p.source || 'external')
    const chatLabel = p.chatName
      ? `${p.chatName} (${p.chatType || 'chat'}:${p.chatId || p.chat})`
      : (p.chat || '')
    const fromLabel = p.fromId
      ? `${p.from || '?'}(${p.fromId})`
      : (p.from || '')
    const id = p.mid || p.id ? ` (id ${p.mid || p.id})` : ''

    const header = [
      `[外部消息 ${dir} ${src}]`,
      chatLabel ? `  聊天: ${chatLabel}` : null,
      fromLabel ? `  来自: ${fromLabel}` : null,
      id ? `  消息: ${id.trim()}` : null,
    ].filter(Boolean).join('\n')

    const line = `${header}\n\n${text.slice(0, 4000)}`
    const st = hubState()
    st.backlog.push(line)
    if (st.backlog.length > 20) st.backlog.splice(0, st.backlog.length - 20)
    maybeFlushExternal()
    return true
  }

  function maybeFlushExternal() {
    const st = hubState()
    if (!st.backlog.length) return
    // 只有当前接管投递的那一代才冲刷。旧代（会话已被替换/重载）的 pi 已经
    // 失效，让它冲刷等于把积压反复送进 `ctx is stale` 再放回来——
    // 积压看着还在，实际永远出不去，而新一代根本不知道有人在抢。
    if (st.inject !== injectExternal) return
    if (typeof pi.sendUserMessage !== 'function') return
    // 以前这里有一道「我正忙就不送」的闸，外部消息就卡在 backlog 里等整轮
    // （含一串工具调用）跑完。实测 14:10 那条等了 37 分钟。代价两边承担：
    // 对面以为失联，我这边以为没消息。
    //
    // deliverAs: 'followUp' 本来就是「本轮结束后插入」，排队是它的事。
    // 在这里再挡一道，等于把「排队」变成「等我不忙」——两件不同的事。
    const wait = 2000 - (Date.now() - st.lastAt)
    if (wait > 0) {
      if (st.timer) return
      st.timer = setTimeout(() => {
        st.timer = null
        maybeFlushExternal()
      }, wait)
      st.timer.unref?.()
      return
    }
    st.lastAt = Date.now()
    const body = st.backlog.splice(0, st.backlog.length).join('\n\n---\n\n')
    try {
      pi.sendUserMessage(body, { deliverAs: 'followUp' })
      st.retryDelay = 0
      st.injectBeat = Date.now()
      wakeDbg(`external flush ok busy=${(() => { try { return wakePolicy.status().busy } catch { return '?' } })()}`)
    } catch (err) {
      // 送不出去就放回去，别丢消息
      st.backlog.unshift(body)
      const msg = String((err && err.message) || err)
      wakeDbg(`external flush failed: ${msg}`)
      // 「ctx 已失效」不是暂时性失败，重试一万次也是同一句。
      // 交出投递权，让下一代接手时把积压送出去（backlog 在进程级中枢里）。
      if (/stale after session replacement|stale after .*reload/i.test(msg)) {
        if (st.inject === injectExternal) {
          st.inject = null
          st.injectSid = null
        }
        wakeDbg('external handoff: 本代 ctx 已失效，交出投递权，等下一代接手')
        return
      }
      // 放回去以后必须有下一次尝试——否则「不丢」只是听起来好。
      // 实测（2026-11-16）：失败后只等下一封外部消息来搭便车；
      // 而一整天没新消息，那一封就静静躺在 backlog 里，无人知晓。
      // 这里排一个定时器自己再来，指数退避，封顶 60s。
      st.retryDelay = Math.min(Math.max(st.retryDelay * 2, 5000), 60000)
      if (!st.timer) {
        st.timer = setTimeout(() => {
          st.timer = null
          maybeFlushExternal()
        }, st.retryDelay)
        st.timer.unref?.()
      }
    }
  }

  function settleWake(cfg) {
    const d = wakePolicy.settle()
    wakeDbg(`settle → ${d.action} (${d.reason}) events=${d.events.length}`)
    pi.events?.emit?.('zero-memcp:wake-settle', { action: d.action, reason: d.reason, count: d.events.length })

    if (d.action === 'stopped') {
      pi.events?.emit?.('zero-memcp:wake-stopped', { reason: d.reason })
      return
    }
    // 节流会静默丢消息：只记一行 throttled，然后再也没有下文。
    // 实测 13:29:46 群里那条 @ 就是这么没回的——私聊刚醒过 20 秒，
    // 积压被节流吞了，直到下一条消息来才重见天日。
    // 节流是「晚点叫」，不是「别叫」：到点自己再结一次。
    if (d.action === 'none' && String(d.reason).startsWith('throttled')) {
      const st = wakePolicy.status()
      const wait = Math.max(300, st.minIntervalMs - (Date.now() - st.lastWakeAt) + 50)
      wakeDbg(`settle retry in ${Math.round(wait / 1000)}s`)
      scheduleSettle(cfg, wait)
      return
    }

    if (d.action !== 'wake') return
    if (typeof pi.sendUserMessage !== 'function') return

    // 同一个事件只叫醒一次。
    //
    // 实测 2026-11-11 13:07：一条私聊在 33ms 内叫醒了两遍（同一个会话里
    // 两个 run 并发），于是回了对方两条。归属表按 sessionId 判，管不到
    // 「同一个会话的两个 run」这一层，只能按事件本身发一次性令牌。
    const fresh = unseenEvents(d.events)
    if (fresh.length !== d.events.length) {
      wakeDbg(`dedupe in=${d.events.length} out=${fresh.length}`)
    }
    if (!fresh.length) return

    // 唤醒词要带正文。
    //
    // 早先只给"某分支有变动"——agent 醒了还得自己探路才知道发生了什么，
    // 于是每一轮都从零开始，几十秒到几分钟就这么没了。事件里只有 uri，
    // 正文得自己取：取最新一条、原样贴进提示，让它一睁眼就知道谁说了什么。
    fetchSnippets(cfg, d.events)
      .then((snips) => pi.sendUserMessage(renderWakePrompt(fresh, snips), { deliverAs: 'followUp' }))
      .catch(() => pi.sendUserMessage(renderWakePrompt(fresh), { deliverAs: 'followUp' }))
  }

  // 事件的一次性令牌：5 分钟内同一个 uri+path 只允许唤醒一次。
  //
  // 用 O_EXCL 抢占（writeFileSync flag:'wx'）：两个 run 同时到，只有一个
  // 创建成功，另一个直接跳过——不需要锁服务，也不怕并发。
  // 任何异常都退回「不认识这个事件」：宁可多叫一次，不能叫不醒。
  const WAKE_SEEN_DIR = join(homedir(), '.zero-memcp', 'wake-seen')
  const WAKE_SEEN_TTL = 5 * 60 * 1000

  function unseenEvents(events) {
    try {
      mkdirSync(WAKE_SEEN_DIR, { recursive: true })
      const now = Date.now()
      const out = []
      for (const ev of events) {
        const key = createHash('sha1')
          .update(`${ev.uri}|${ev.path || ''}|${ev.kind || ''}`)
          .digest('hex')
          .slice(0, 20)
        const f = join(WAKE_SEEN_DIR, key)
        let st = null
        try { st = statSync(f) } catch {}
        if (st && now - st.mtimeMs < WAKE_SEEN_TTL) continue
        if (st) { try { unlinkSync(f) } catch {} }
        try { writeFileSync(f, String(now), { flag: 'wx' }) } catch { continue }
        out.push(ev)
      }
      return out
    } catch {
      return events
    }
  }


  /**
   * 取每条事件对应树里最新一条的正文。
   *
   * 只做两件事：ls 找最新路径、read 取正文。不认识来源、不解析字段——
   * 信封（source/chat/from）由写入方写在正文头部，这里原样搬运。
   * 取不到就返回空，提示退化成"只给地址"，绝不因为摘要失败而丢掉唤醒。
   */
  /**
   * 取每条事件对应条目的正文，解析成 {source, from, body}。
   *
   * 优先用事件里带的 path——服务端知道这一次写的是哪个文件。
   * 早先事件不带路径，只能 ls 一遍取"最后一行"，猜错就反复处理同一条
   * 旧输入（实测：一个两小时前的自检条目被反复叫醒十几次）。
   */
  async function fetchSnippets(cfg, events) {
    const out = {}
    const seen = new Set()
    for (const ev of events) {
      if (seen.has(ev.uri)) continue
      seen.add(ev.uri)
      try {
        const base = uriToAddr(ev.uri)
        // 事件里的 path 用 @ 分隔日期和时间，存储里是 /，读之前先归一。
        const path = normPath(ev.path) || (await newestPath(cfg, base))
        if (!path) continue
        const text = await term(cfg, `read ${base}:${path}`, 5000)
        if (text) out[ev.uri] = parseEntry(text)
      } catch {}
    }
    return out
  }

  /** 没有 path 时的退化路径：取列表最后一条。只在服务端没带路径时用到。 */
  async function newestPath(cfg, base) {
    const list = await term(cfg, `ls --scope ${base}`, 4000)
    const lines = String(list || '').split('\n').map((x) => x.trim()).filter(Boolean)
    if (!lines.length) return ''
    return lines[lines.length - 1].split(/\s+/)[0]
  }



  // ── 会话归属：一个消息源只归属一个会话 ──────────────────────────
  //
  // 每个打开的会话各自订阅、各自唤醒时，同一件事会被多个会话响应，
  // 而且每换一个会话就丢一次缓存。这里用一份小文件记录「这个源归谁」：
  // 先到者认领，之后只有它唤醒；它的心跳超时后其他会话才接管。
  const OWNER_FILE = join(homedir(), '.zero-memcp', 'wake-owner.json')
  const OWNER_STALE_MS = 10 * 60 * 1000

  function readOwners() {
    try { return JSON.parse(readFileSync(OWNER_FILE, 'utf8')) || {} } catch { return {} }
  }
  function writeOwners(o) {
    try {
      mkdirSync(dirname(OWNER_FILE), { recursive: true })
      writeFileSync(OWNER_FILE, JSON.stringify(o, null, 2))
    } catch {}
  }
  /** 认领：没有归属、或归属已过期、或就是我 → true。 */
  // ── 选举：谁负责回消息 ──────────────────────────────────
  //
  // 不抢。每条消息都让几个会话赛跑，赢的那个可能是刚打开的空会话——
  // 它没有历史，只能从零开始翻文件系统。**先打开的赢**，因为先打开的那个
  // 才是这个人在用的那个会话，历史都在它手上。
  //
  // 怎么知道谁先打开：会话 id 是 UUIDv7，前 48 位就是创建时间（实测
  // 01a1c87f-ac3e → 2026-11-11T02:46:12Z，正是这个会话的开头）。
  // 所以「年龄」是本会话的固有属性，重启也不会变，不需要猜。
  const ELECTION_FILE = join(homedir(), '.zero-memcp', 'wake-elect.json')
  const ELECTION_TTL = 90 * 1000

  function bornOf(sid) {
    const m = /^([0-9a-fA-F]{12})/.exec(String(sid || '').replace(/-/g, ''))
    if (!m) return Date.now()
    const ms = parseInt(m[1], 16)
    return ms > 1e12 && ms < Date.now() + 86400000 ? ms : Date.now()
  }


  /** 登记自己、清掉过期的、返回当前该负责的会话 id。 */
  function elect(now = Date.now()) {
    let m = null
    try { m = JSON.parse(readFileSync(ELECTION_FILE, 'utf8')) } catch {}
    if (!m || typeof m !== 'object' || Array.isArray(m)) m = {}
    // 年龄要现算：sessionId 在模块初始化时还是空的，那时算出来的
    // 是进程启动时间——重启后几个会话会并列，选举就退回成抢。
    if (sessionId) m[sessionId] = { born: bornOf(sessionId), beat: now }
    for (const k of Object.keys(m)) {
      if (now - (m[k]?.beat || 0) > ELECTION_TTL) delete m[k]
    }
    // 原子写：几个会话会同时落这个文件，直接覆盖会写出半截 JSON。
    try {
      mkdirSync(dirname(ELECTION_FILE), { recursive: true })
      const tmp = `${ELECTION_FILE}.${process.pid}`
      writeFileSync(tmp, JSON.stringify(m))
      renameSync(tmp, ELECTION_FILE)
    } catch {}
    const alive = Object.entries(m)
      .filter(([, v]) => now - (v?.beat || 0) <= ELECTION_TTL)
      .sort((a, b) => (a[1].born - b[1].born) || String(a[0]).localeCompare(String(b[0])))
    return { winner: alive.length ? alive[0][0] : null, map: m }
  }

  // 钉住：配了哪个就是哪个。
  //
  // 选举是"没配的时候"的默认；配了就以配置为准，重启也还在——因为
  // 会话 id 不随重启改变（同一个会话还是同一个 id，历史还在它手上）。
  // 只有钉住的那个会话真的关了（心跳过期），才退回选举，不让消息没人接。
  const PIN_FILE = join(homedir(), '.zero-memcp', 'wake-pin.json')

  function readPin() {
    try {
      const j = JSON.parse(readFileSync(PIN_FILE, 'utf8'))
      return j && typeof j.sid === 'string' ? j.sid : ''
    } catch { return '' }
  }

  function writePin(sid) {
    mkdirSync(dirname(PIN_FILE), { recursive: true })
    if (!sid) { try { unlinkSync(PIN_FILE) } catch {} ; return }
    writeFileSync(PIN_FILE, JSON.stringify({ sid, setAt: Date.now() }, null, 2))
  }

  function amOwner(now = Date.now()) {
    if (!sessionId) return true
    const r = elect(now)
    const pin = readPin()
    if (pin) {
      const beat = r.map?.[pin]?.beat || 0
      if (now - beat <= ELECTION_TTL) return pin === sessionId
    }
    return r.winner === sessionId
  }

  /**
   * 事件地址 → 订阅根。归属按树算，不按条目算。
   *
   * 按条目算等于每条消息都没有主人：谁先跑到谁抢到。
   * 实测 2026-11-11 13:31：一个新开的空会话抢到了 @，然后在里面从零开始
   * 翻整个系统找 mem 工具——它没有历史，不知道路径，只能重新摸索。
   * 按树算，主人是稳定的；新会话只有在主人确实死了之后才接手。
   */
  function rootOf(uri) {
    const parts = String(uri || '').split('/')
    return parts.length >= 4 ? parts.slice(0, 4).join('/') : String(uri || '')
  }

  function claim(uri, sid) {
    if (!uri || !sid) return true
    uri = rootOf(uri)
    const o = readOwners()
    const cur = o[uri]
    const now = Date.now()
    if (cur && cur.id !== sid && now - (cur.beat || 0) < OWNER_STALE_MS) return false
    o[uri] = { id: sid, beat: now }
    writeOwners(o)
    return true
  }
  /** 会话结束：交还归属，让活着的会话立刻接手，不用等过期。 */
  function releaseOwners(sid) {
    if (!sid) return
    const o = readOwners()
    let changed = false
    for (const k of Object.keys(o)) if (o[k]?.id === sid) { delete o[k]; changed = true }
    if (changed) writeOwners(o)
  }

  /** 心跳：每次醒着就续一下，防止被误接管。 */
  function beat(uri, sid) {
    if (!uri || !sid) return
    uri = rootOf(uri)
    const o = readOwners()
    if (o[uri] && o[uri].id === sid) { o[uri].beat = Date.now(); writeOwners(o) }
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
      // 命令 handler 的第二个参数就是 ExtensionCommandContext（含 sessionManager），
      // 早先这里写了裸 ctx——那是事件 handler 的形参，命令作用域里根本不存在，
      // 于是 pi -p 这类 sessionId 为空的场景下敲 /mem:record on 直接抛
      // ReferenceError: ctx is not defined。用 cmdCtx，取不到就保持空串。
      if (!sessionId) sessionId = deriveSessionId(cmdCtx, skip)
      if (sessionId) await saveSessionSetting(sessionId, { importMode: target })
      cmdCtx?.ui?.notify?.(`本会话记录模式已设为: ${show(target)}`, 'info')
    },
  })

  pi.registerCommand('mem:wake', {
    description: '资源变更唤醒：/mem:wake [off|notify|continue|loop] [global] | reset | 空参看状态',
    handler: async (args, cmdCtx) => {
      const cfg = await cfgPromise
      const raw = String(args || '').trim()
      const parts = raw ? raw.split(/\s+/) : []
      const val = s => (s === 'on' ? 'continue' : s)

      if (!sessionId) sessionId = deriveSessionId(cmdCtx, skip)

      // 钉住谁来回：配了就以配置为准，重启也还在。
      if (['here', 'me'].includes(parts[0])) {
        writePin(sessionId)
        const short = String(sessionId).slice(0, 8)
        cmdCtx?.ui?.notify?.('回复归属已钉在本会话（' + short + '），重启后仍然是它。\n解除：/mem:wake auto', 'info')
        return
      }
      if (parts[0] === 'auto') {
        writePin('')
        cmdCtx?.ui?.notify?.('已解除钉住，按打开顺序选举（先打开的赢）', 'info')
        return
      }

      // 空参：报状态
      if (!parts.length) {
        const st = wakePolicy.status()
        const pin = readPin()
        const watchList = String(cfg.watch || '').split(',').map(x => x.trim()).filter(Boolean)
        const lines = [
          `模式: ${st.mode}`,
          `冷却: ${Math.round(st.cooldownMs / 1000)}s`,
          `熔断: ${st.maxChain > 0 ? `${st.chain}/${st.maxChain} 次` : '不限'}`,
          `待提示: ${st.pending} 条`,
          `观测树: ${watchList.length ? watchList.join(', ') : '（watch 为空，不会收到事件）'}`,
          `回复归属: ${pin ? '钉住 ' + pin.slice(0, 8) : '按打开顺序选举，当前 ' + String(elect().winner || '—').slice(0, 8)}`,
          `本会话: ${String(sessionId || '—').slice(0, 8)}`,
          '',
          '模式说明:',
          '  off       不订阅，不唤醒',
          '  notify    只累积提示，下一轮对话时注入（零额外 token）',
          '  continue  空闲时唤醒一轮，忽略自己写的',
          '  loop      允许自激（自己写的也唤醒），必须配 maxChain',
          '',
          '用法: /mem:wake notify | /mem:wake loop global | /mem:wake reset',
          '      /mem:wake here（钉住本会话） | /mem:wake auto（解除钉住）',
        ]
        if (!watchList.length && st.mode !== 'off') {
          lines.push('', '⚠ watch 为空：当前配置收不到任何事件，请先设置 watch')
        }
        cmdCtx?.ui?.notify?.(lines.join('\n'), 'info')
        return
      }

      if (parts[0] === 'reset') {
        wakePolicy.reset()
        cmdCtx?.ui?.notify?.('已复位唤醒熔断计数', 'info')
        return
      }

      const mode = val(parts[0])
      if (!['off', 'notify', 'continue', 'loop'].includes(mode)) {
        cmdCtx?.ui?.notify?.(`未知模式 "${parts[0]}"，支持 off / notify / continue / loop`, 'warning')
        return
      }
      const isGlobal = parts.includes('global')

      if (isGlobal) {
        try {
          await saveGlobalConfig({ wake: { ...(cfg.wake || {}), mode } })
          cfg.wake = { ...(cfg.wake || {}), mode }
        } catch (err) {
          cmdCtx?.ui?.notify?.(String(err), 'error')
          return
        }
        wakePolicy.configure({ mode })
        startListen(cfg)
        cmdCtx?.ui?.notify?.(`全局唤醒模式已改为: ${mode}`, 'info')
        return
      }

      wakePolicy.configure({ mode })
      startListen(cfg)
      if (mode === 'loop') {
        cmdCtx?.ui?.notify?.(
          'loop 模式已开启：自己写的记忆也会唤醒自己。\n' +
            `熔断: ${wakePolicy.maxChain > 0 ? wakePolicy.maxChain + ' 次' : '当前不限（建议在 config 里设 maxChain）'}`,
          'warning',
        )
      } else {
        cmdCtx?.ui?.notify?.(`本会话唤醒模式已改为: ${mode}`, 'info')
      }
    },
  })

  pi.registerCommand('mem:mode', {
    description: '注入档位：/mem:mode [full|minimal] [global]（空参 = 看当前档位）',
    handler: async (args, cmdCtx) => {
      const cfg = await cfgPromise
      const parts = String(args || '').trim().toLowerCase().split(/\s+/).filter(Boolean)
      const want = parts.find((x) => x === 'minimal' || x === 'full')
      const isGlobal = parts.includes('global')

      if (!want) {
        cmdCtx?.ui?.notify?.(
          [
            `注入档位: 本会话 ${identityModeOf(cfg)}｜全局 ${String(cfg.identityMode || 'full')}`,
            '  full    全量身份 + law（约 2.8KB）',
            '  minimal 只留 mem 入口（约 150 字符，不联网取身份）',
            '',
            '用法：/mem:mode minimal | full | minimal global',
            '身份与凭证仍走 /mem:identity。',
          ].join('\n'),
          'info',
        )
        return
      }

      if (isGlobal) {
        try {
          await saveGlobalConfig({ identityMode: want })
          cfg.identityMode = want
        } catch (err) {
          cmdCtx?.ui?.notify?.(String(err), 'error')
          return
        }
        // 全局改动清掉本会话覆盖，让新设置立刻可见（与 /mem:identity 一致）
        if (sessionIdentityMode && sessionIdentityMode !== want) {
          sessionIdentityMode = null
          if (sessionId) await saveSessionSetting(sessionId, { identityMode: null })
        }
        identity = ''
        identityAt = 0
        identityError = null
        cmdCtx?.ui?.notify?.(`全局注入档位已改为: ${want}（对以后新会话生效）`, 'info')
        return
      }

      sessionIdentityMode = want
      if (!sessionId) sessionId = deriveSessionId(cmdCtx, skip)
      if (sessionId) await saveSessionSetting(sessionId, { identityMode: want })
      // 档位变了，缓存的身份块就过期了：minimal 不能还揣着 2.8KB 的旧块。
      identity = ''
      identityAt = 0
      identityError = null
      cmdCtx?.ui?.notify?.(
        `本会话注入档位已改为: ${want}${want === 'minimal' ? '（约 150 字符，只有 mem 入口）' : '（全量身份 + law）'}`,
        'info',
      )
    },
  })

  pi.registerCommand('mem:identity', {
    description: '挂载/卸下身份：/mem:identity [on|off] [global]（空参 = 看状态）',
    handler: async (args, cmdCtx) => {
      const cfg = await cfgPromise
      const raw = String(args || '').trim().toLowerCase()
      const parts = raw ? raw.split(/\s+/) : []

      // 档位不走这个命令：identity 管「我是谁」，mode 管「给多少」。
      // 两件事塞进一个动词，用起来必然要问「identity mode 改的是哪个」。
      if (parts[0] === 'mode') {
        cmdCtx?.ui?.notify?.('注入档位请用 /mem:mode（identity 只管凭证与挂载）', 'warning')
        return
      }

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
          '  /mem:mode minimal         本会话改注入档位（省上下文，给小模型用）',
          '  /mem:mode full            本会话改回全量身份',
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
    const sc = scopesFor(cfg)
    const problems = validatePersonaScope(cfg, name)
    if (problems.length > 0) {
      cmdCtx?.ui?.notify?.(
        `身份 "${name}" 配置有风险：\n${problems.map((p) => `  - ${p}`).join('\n')}`,
        'warning',
      )
    }
    activeIdentityName = name
    if (sessionId) await saveSessionSetting(sessionId, { identityName: name })
    // 换了凭证就是换了一个世界：身份块与关注标记都要重来。
    // 关注标记挂在凭证身份上，换票后旧标记不再属于这个视角。
    identity = ''
    identityAt = 0
    identityError = null
    anchorWatchedTrees(cfg).catch(() => {})
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
      // 同上：命令作用域里只有 cmdCtx，没有 ctx。
      if (!sessionId) sessionId = deriveSessionId(cmdCtx, skip)
      if (sessionId) await saveSessionSetting(sessionId, { identity: want })

      if (want === 'on') {
        // 挂载前先做交叉校验：personas 声明的落点与凭证挂载表不一致时，
        // 写入会在服务端被拒，而那条路径的错误会被 flush 的 catch 吃掉、
        // 用户只看到「记忆没存进去」却不明白为什么。与其等失败，不如现在说。
        const nameForCheck = activeIdentityName || cfg.identityName || ''
        if (nameForCheck) {
          const problems = validatePersonaScope(cfg, nameForCheck)
          if (problems.length > 0) {
            cmdCtx?.ui?.notify?.(
              `身份 "${nameForCheck}" 配置有风险：\n${problems.map((p) => `  - ${p}`).join('\n')}`,
              'warning',
            )
          }
        }
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
      "读：",
      "  recall <text> [--scope T]  语义联想（返回地址与摘要）",
      "  read <addr>...             读取全文，可一次读多篇",
      "  grep <regex> [--scope T]   正则搜索全文（精确符号、名字、数字）",
      "  log [N]                    最近 N 篇的时间线",
      "  ls [date]                  某天的切片",
      "  trees <project>[@<br>]     查看树目录",
      "  status                     索引规模、语义后端、同步状态",
      "",
      "写：",
      "  put [<tree>] $TEXT        写入记忆（正文是一个变量引用，绝不拼进 cmd）",
      "  ref <tree> <addr>          在树中追加已有记忆的引用（不复制正文）",
      "  write <p>@<br>/<path> -- T 精确写入指定路径（规则库用；需 root）",
      "  batch {\"ops\":[...]}       原子批量（多步合成一次提交）",
      "",
      "维护（日常读写不需要）：",
      "  commit [<p>[@<br>]]        显式封箱（put 只入队，不提交）",
      "  diff <p>@<br> [-> <p>@<br>] 分支比对（审计用）",
      "  branch [<p> [<new> [from s]]] 列出或派生分支",
      "  tag [-d] <p> [<name> [<ref>]] 轻量标签",
      "  cp / mv <src> -> <dst>     跨分支搬运条目",
      "  delete <p>@<br>/<path>...  删除条目",
      "  archive <p>@<br>           归档分支",
      "  reset <p>@<br> from <src>  把分支重置到另一个分支",
      "  lock / unlock <p>@<br>     锁定/解锁分支（暂停非 root 访问）",
      "  chmod <p>@<br>/<path> mode 改规则/内容权限位",
      "",
      "多机同步（远端在服务端配置里声明）：",
      "  remote [<project>]         列出配置的远端",
      "  sync <project> [<remote>]  对比本地与远端状态（含自动同步时间与冲突）",
      "  fetch <project> [<remote>] 取回远端历史，不自动覆盖本地分支",
      "  push <project> [<remote>] <branch>  显式推一条分支（不替你提交、不强推）",
      "  merge <project> <branch> from <remote>  把远端分支合并进本地（快进或正交合并，冲突则拒）",
      "  since <tree> <commit|empty> 取回某提交之后的差分",
      "  remind [<tree>]            登记关注的树（status 会报它动没动）",
      "  remote-key list|put|delete root 管理 SSH 私钥",
      "  remote-secret list|put|delete root 管理 HTTP token",
      "",
      "一次调用多条语句：",
      "  用 ';' 或换行分隔，一个来回执行多条。",
      "  '&&' 上一条找到了才跑下一条；'||' 上一条没找到才跑下一条。",
      "  「找到」指命中，不是退出码：合法地搜不到不算错误。",
      "  分层回落用它：recall q --scope p@fact || recall q --scope p@record",
      "",
      "会话（插件就地处理，不打服务端）：",
      "  record [on|off] [global]   本会话是否每轮自动存",
      "  identity [<name>] [global] 查看身份或换一张凭证",
      "",
      "put 入脏层后立即返回；折叠为 Git 提交由显式 commit 或服务端后台策略决定。",
      "正文必须通过 vars/args 传递，例如 cmd='put notes $TEXT', vars={TEXT:'正文'}。",
      "help <verb> 看单个动词细节；help --all 看全部。",
    ].join("\n"),
    parameters: {
      type: "object",
      properties: {
        cmd: {
          type: "string",
          description: "DSL 命令行，例如 'recall 数据库选型' 或 'put notes $TEXT'。正文走 vars/args，引用必须独占一个词。",
        },
        vars: {
          type: "object",
          additionalProperties: { type: "string" },
          description: "命名正文参数。cmd 中使用独占词引用，例如 cmd='put $TEXT', vars={TEXT:'多行正文'}。",
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

          const credIssue = credentialProblem(cfg)
          const lines = [
            `凭证: ${activeIdentityName || cfg.identityName || '（默认凭证）'}`,
            credIssue ? `⚠ ${credIssue}` : null,
            `端点: ${cfg.endpoint}${reachable ? '' : `（不可达：${err}）`}`,
            `记忆挂载: ${mark(sessionIdentity, globalIdentity)}`,
            `自动记录: ${mark(sessionImportMode, globalMode)}`,
            `注入档位: ${sessionIdentityMode === null ? `跟随全局（${String(cfg.identityMode || 'full')}）` : sessionIdentityMode}`,
            `本会话: ${wm.idx} 轮已入库`,
            docs === null ? null : `已索引切片: ${docs} 篇`,
          ].filter(Boolean)

          // 服务端的 status 必须一并带上。
          //
          // 早先这个本地分支直接 return，把服务端 status 整个挡住了——
          // 于是「语义后端是否降级」「同步上次成功是什么时候」「有没有
          // 待决议冲突」三件事，在任何客户端里都看不到。它们恰好都是
          // 静默失败：召回变差、记忆没到另一台机器，都不报错。
          //
          // 合并输出而不是让用户敲两条：这两半描述的是同一个系统的状态，
          // 分开看只会让人以为它们是两回事。
          if (reachable) {
            try {
              const remoteStatus = await term(cfg, 'status', 3000)
              const extra = String(remoteStatus || '')
                .split('\n')
                .filter((l) => l.trim() && !l.startsWith('docs:'))
              if (extra.length) lines.push(...extra)
            } catch (e) {
              lines.push(`服务端状态不可用: ${e}`)
            }
          }

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
    try {
      const c0 = await cfgPromise
      dbg('session_start', {
        skip,
        identity: c0.identity,
        identityName: c0.identityName,
        enabled: c0.enabled,
        endpoint: c0.endpoint,
        tokens: Object.keys(c0.tokens || {}),
        personas: Object.keys(c0.personas || {}),
        env_subagent: process.env.PI_SUBAGENT || null,
      })
    } catch (e) {
      dbg('session_start_error', { message: String(e) })
    }
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
    resetPerSession()
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
      ensureWakeHttp()
      // 先把这个拉起来：后面那些东西（13000/13001/9222）全挂在
      // termcp 的转发上，容器重启后它们一个都不剩。
      ensureLocalStack().catch(() => {})
      // 接管外部消息的投递口：HTTP 口是进程级单例（只有第一个实例开得了），
      // 但“交给谁”必须每代自己接。不接的话，换代后 18800 还对着老 pi，
      // 每条消息都死在 `ctx is stale` 上（2026-11-16 QQ 断收一整天）。
      //
      // 但不能「谁后开谁接」——那样同时开几个会话时消息就乱跑。
      // 改成跟记忆唤醒同一套归属：钉住 > 按开得早的选举。
      claimExternalTarget()
      // 每代 session_start 都会新建一个定时器。不先清旧的就会层层叠加：
      // 老会话的定时器仍然活着，照样抢 hub.inject，抢到就 maybeFlushExternal()，
      // 于是别的会话的 ctx 被反复唤醒（老会话被乱唤醒的根因）。
      if (externalClaimTimer) { clearInterval(externalClaimTimer); externalClaimTimer = null }
      const claimTimer = setInterval(() => {
        try {
          // 成为归属者就把积压冲一次：持有者死掉后，积压可能是它留下的，
          // 没人接手就永远出不去。
          if (claimExternalTarget()) maybeFlushExternal()
          // 顺手看一眼链路是不是还在。termcp 不只“重启会掉”：
          // 它自己崩、SSH 被踢、远端重启，都会让 13000/13001 静默消失，
          // 而那时没人重启容器，也就没 session_start 去拉它。
          // 两下 TCP 探端口的成本可以忽略；真的断了才是要命的。
          ensureLocalStack().catch(() => {})
        } catch {}
      }, 20_000)
      claimTimer.unref?.()
      externalClaimTimer = claimTimer
      // 上一代死在 stale 里留下的积压，这一代接手就补送——
      // 否则要等下一条外部消息来搭便车，那封可能永远等不到。
      maybeFlushExternal()
      const cfgNow = await cfgPromise
      if (cfgNow.enabled) {
        // 会话级设置覆盖全局：这条会话记不记，看它自己的文件
        const ss = await loadSessionSetting(sessionId)
        sessionImportMode = ss.importMode || cfgNow.importMode
        sessionIdentity = ss.identity || null
        sessionIdentityMode = ss.identityMode || null

        // 决定本会话身份：优先会话持久化记录，其次全局配置，都没有就用默认凭证
        activeIdentityName = ss.identityName || cfgNow.identityName || ''

        // 身份确定后把 watch 声明的树登记到服务端（供 status 用）
        anchorWatchedTrees(cfgNow).catch(() => {})

        // 资源变更订阅：每个会话一个 sessionTag，事件据此区分
        // 「我自己写的」和「别人写的」。
        if (!sessionTag) {
          sessionTag = makeSessionTag()
          setTermSession(sessionTag)
        }
        wakePolicy.configure({
          mode: cfgNow.wake?.mode || 'off',
          cooldownMs: cfgNow.wake?.cooldownMs,
          maxChain: cfgNow.wake?.maxChain,
          session: sessionTag,
          identity: activeIdentityName,
        })
        startListen(cfgNow)

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
  //   · agent_settled —— 事件体只有 { type }，拿不到内容。但它**确实触发**
  //     （实测序列 agent_start → turn_end → agent_end → agent_before_settle
  //     → agent_settled），只是不含 messages。所以它适合当「停稳」信号，
  //     不能当写入点——唤醒就挂在它上面。
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

  /**
   * 从会话条目里取这一轮的推理链。
   *
   * 为什么需要它：pi 的 event.messages 里不一定带 thinking 块
   * （2026-11-15 实测：session jsonl 有 1288 个 thinking 块，但通过
   * event.messages 实时写进 mem 的一条都没有，只有会话开始回填那批）。
   * sessionManager.getEntries() 读的是会话文件本身，那里一定有。
   *
   * 只取「最后一条 user 之后」的 thinking —— 那是这一轮的。
   */
  function extractThinkingFromEntries() {
    let entries = []
    try {
      entries = entriesGetter ? entriesGetter() : []
    } catch {
      return ''
    }
    const msgs = entries
      .filter((e) => e && e.type === 'message' && e.message)
      .map((e) => e.message)
    if (!msgs.length) return ''
    let ui = -1
    for (let i = msgs.length - 1; i >= 0; i--) {
      if (msgs[i]?.role === 'user') { ui = i; break }
    }
    if (ui < 0) return ''
    const parts = []
    for (let i = ui + 1; i < msgs.length; i++) {
      const m = msgs[i]
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
    // 去重键必须是全文哈希，不能用前缀。
    //
    // 早先用 text.slice(0, 160)：同一个问句配不同的答（追问、补充说明，
    // 非常常见）前 160 字符完全相同，于是第二轮被当成「已写过」直接丢弃。
    // 那是一种最糟的失败——不报错，只是内容永远没进库。
    const key = createHash('sha256').update(text, 'utf8').digest('hex')
    if (seenMemories.has(key)) {
      pendingAnswer = ''
      turnUserText = ''
      return false
    }

    const cfg = await cfgPromise
    if (!cfg.enabled) return false

    // 日期段归服务端：写入时按物理时间落到 <facet>/YYYY-MM-DD/<stamp>.md。
    // 这里绝不能再拼一次日期——早先两侧各拼一次今天，得到
    // record/2026-11-04/2026-11-04/… 这种两个一模一样的日期。
    const now = new Date()
    const sc = scopesFor(cfg)

    try {
      // 1) 问答对写进记录层（按当前分身的人格落点）
      const whereRecord = sc.recordScope || ''
      dbg('flush_write', { persona: sc.persona, where: whereRecord, len: text.length })
      await putRecord(cfg, text, sessionId, whereRecord)

      // 2) 推理链写进想法层（配置了 thoughtScope 且当前轮有 thinking）
      if (sc.thoughtScope && thinkingText && thinkingText.trim()) {
        const chunks = chunkThinking(thinkingText)
        const whereThought = sc.thoughtScope
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
      dbg('flush_ok', { persona: sc.persona, where: sc.recordScope })

      const cur = await loadWatermark(sessionId)
      await saveWatermark(sessionId, { idx: cur.idx + 1, hash: checksumOf(text) })
      return true
    } catch (err) {
      lastHealthy = false
      // 凭证过期是最常见的写入失败原因，而且它有自己的解法。
      // 混在一句原始错误里，用户只能猜。
      const hint = credentialProblem(cfg)
      const msg = hint ? `${err}（${hint}）` : String(err)
      // 写入失败必须留痕：这一处早先是 emit 事件，没有监听者就等于没有日志，
      // 表现为"配置看起来对、记录就是不出现"。
      dbg('flush_error', { why, msg })
      pi.events?.emit?.('zero-memcp:auto-write-error', { error: msg, why })
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
        const where = sc.recordScope || ''
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
        const whereThought = sc.thoughtScope
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
    // 忙了：此期间到达的事件只进积压，不唤醒。
    wakePolicy.markBusy()
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
    dbg('hook_turn_end')
    // 两个方向都要写：只在上一次为真时不清，会让一次异常的 abort
    // 残留到下一轮——下一轮即使正常完成，agent_end 也会误判成 abort
    // 而把整轮内容丢掉。显式赋成布尔值，不依赖上一轮的状态。
    abortedTurn = Boolean(event?.outcome && event.outcome !== 'completed')
  })

  // 主写入点。messages 是权威来源，不依赖累积状态。
  pi.on('agent_end', async (event) => {
    // 一轮结束：非 loop 模式的连续计数在此归零。
    wakePolicy.onTurnEnd()
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
    // 身份没声明落点 = 这个身份不参与记录。
    //
    // 记录落点是身份声明的一部分（scopesFor），不是全局开关：
    // 简写 chat，纬负责维护记忆体、平时挂起待命——它不该在聊天里留痕。
    // 没有这条，importMode=auto 会把纬的每一轮也写进默认落点，
    // 两条人格的记录混在同一条分支上。
    const scNow = scopesFor(cfgNow)
    if (!scNow.recordScope) {
      dbg('agent_end_skip_no_record_scope', { persona: scNow.persona })
      pendingAnswer = ''
      turnUserText = ''
      return
    }
    const fromMessages = extractTurn(event?.messages)
    let thinkText = extractThinking(event?.messages)
    // 事件里没有就回落到会话文件——它一直有（见 extractThinkingFromEntries）。
    if (!thinkText) thinkText = extractThinkingFromEntries()
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
    let thinkText = extractThinking(event?.messages)
    const fromEvent = thinkText.length
    // 事件里没有就回落到会话文件。
    if (!thinkText) thinkText = extractThinkingFromEntries()
    // 探针：两条来源各给了多少字。event=0 而 entries>0，就坐实了
    // 「pi 新版不在 event.messages 里带 thinking」这个判断。
    dbg('turn_end_thinking_probe', {
      msgs: Array.isArray(event?.messages) ? event.messages.length : -1,
      fromEvent,
      fromEntries: thinkText.length,
    })
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
      const where = target
      await putRecord(cfgNow, frontmatter, sid, where)
    } catch (err) {
      // 事实层写入失败不阻断对话，但记事件供排查
      pi.events?.emit?.('zero-memcp:fact-write-error', { error: String(err), session: sid })
    }
  })

  // 会话关闭前最后兜一次，避免最后一句没写完就断了。
  // agent 完全停稳（无重试、无压缩、无排队继续）——这是唯一该唤醒的时刻。
  //
  // 用 agent_settled 而不是 agent_end：后者之后还可能有自动重试、
  // 压缩、排队继续，那时拉起会打断本要继续的工作。
  pi.on('agent_settled', async () => {
    wakePolicy.markIdle()
    const cfgNow = await cfgPromise
    if (!cfgNow.enabled) return
    settleWake(cfgNow)
    maybeFlushExternal()
  })

  pi.on('session_shutdown', async () => {
    // 先断订阅再冲刷：否则冲刷产生的写入会推进分支头，
    // 在退出过程中再触发一次唤醒。
    try { stopListen?.() } catch {}
    stopListen = null
    // 交出外部消息投递口：这一代即将失效，留着它只会把消息丢进 stale 里。
    // 只在自己还是当前那一代时清空，不抢新一代已经接过的手。
    {
      const st = wakeHub()
      if (st.inject === injectExternal) {
        st.inject = null
        st.injectSid = null
      }
      if (externalClaimTimer) { clearInterval(externalClaimTimer); externalClaimTimer = null }
    }
    // 主动交还归属：不交的话下一个会话要等 10 分钟过期才敢接手，
    // 这中间进来的消息没人回。
    try { releaseOwners(sessionId) } catch {}
    await flush('session_shutdown', null)
  })

  // 身份注入：拼进系统消息的开头。
  //
  // 这是"会话开头 / 压缩后的开头"那个位置——系统消息永远是 index 0，
  // 而压缩只压对话，压不到它。
  // 最终出口：请求已经组装好，这里改什么就发什么。
  //
  // 为什么不用 context_with_system 注入：v1.5.x 的系统提示是「结构化分段」
  // （preamble/tools/rules/docs/addendum/cwd），SDK 每次请求都从自己的
  // systemPromptOptions 重建，动 messages 里的 sections 会被重建覆盖——
  // 日志显示注入了，模型却看不见。provider 请求是唯一确定生效的位置。
  pi.on('before_provider_request', async (event) => {
    if (skip) return
    const cfg = await cfgPromise
    if (!cfg.enabled) return
    if (!identityActive(cfg)) return
    const payload = event?.payload
    if (!payload || typeof payload !== 'object') return

    const text = await ensureIdentity(cfg, pi)
    if (!text) return
    // 身份之外必须补一条「机制」：外部怎么触发我。
    //
    // 实测缺口：模型熟悉 MCP 动词（recall/put/grep 自己查得到），也知道
    // qqmcp 在哪儿，但不知道**外部是靠往 zero@inbox 写入来叫醒他的**。
    // 不知道这条，被唤醒时会当成莫名其妙的打扰，或者去猜别的入口。
    // 机制认知不是记忆内容，属于基础设施，放在注入头部最合适。
    // 注入只做一件事：说出「入口在哪」，并说明下面这段文字是什么。
    //
    // 早先这里塞过唤醒机制、记忆读写规矩——那些都是知识，属于记忆库，
    // 不该写死在扩展里：写死就变成两份真相（代码一份、记忆一份），
    // 改一处忘一处，模型看到的还取决于扩展版本。入口是代码的事，
    // 入口里面有什么是记忆的事。
    const IDENTITY_HEADER = identityModeOf(cfg) === 'minimal' ? MINIMAL_HEADER_TEXT : HEADER_TEXT
    const block = `${MARKER}\n${IDENTITY_HEADER}\n\n${text}\n${MARKER}`

    const headMsg = Array.isArray(payload.messages) && payload.messages.length ? payload.messages[0] : null
    dbg('bpr_shape', {
      keys: Object.keys(payload),
      systemType: typeof payload.system,
      hasSystemPrompt: typeof payload.systemPrompt !== 'undefined',
      sysLen: typeof payload.system === 'string' ? payload.system.length : null,
      msgCount: Array.isArray(payload.messages) ? payload.messages.length : null,
      headRole: headMsg ? headMsg.role : null,
      headKeys: headMsg ? Object.keys(headMsg) : null,
      headSectionKeys:
        headMsg && headMsg.sections && typeof headMsg.sections === 'object'
          ? Object.keys(headMsg.sections)
          : null,
      headContentLen:
        headMsg && typeof headMsg.content === 'string'
          ? headMsg.content.length
          : headMsg && Array.isArray(headMsg.content)
            ? headMsg.content.length
            : null,
    })

    // 形态一：payload.system 是字符串（Anthropic/OpenAI 风格）
    if (typeof payload.system === 'string') {
      if (payload.system.includes(MARKER)) return
      dbg('bpr_inject', { via: 'system', addLen: block.length })
      return { ...payload, system: `${block}\n\n${payload.system}` }
    }
    // 形态二：payload.system 是分段数组
    if (Array.isArray(payload.system)) {
      const joined = payload.system.map((x) => (typeof x === 'string' ? x : x?.text || '')).join('')
      if (joined.includes(MARKER)) return
      dbg('bpr_inject', { via: 'system_array', addLen: block.length })
      return { ...payload, system: [{ type: 'text', text: block }, ...payload.system] }
    }
    // 形态三：没有 system 字段，系统提示在 messages[0]（ollama / OpenAI 兼容）
    //
    // 实测：27b 走 ollama 的 OpenAI 兼容端点，payload 只有 model / messages /
    // stream / temperature，压根没有 system。早先这里直接 return，日志上
    // 只有 context_with_system 那条「注入成功」——那个 hook 改的是 sections，
    // 而这个适配层不读 sections，于是模型那边一个字的身份都没看到。
    // 形态二和形态三的差别不在协议优雅，在于「谁真的读这个字段」。
    const msgs = Array.isArray(payload.messages) ? payload.messages : null
    if (msgs && msgs.length) {
      const head = msgs[0]
      if (head && head.role === 'system') {
        // 形态三A：系统提示在 head.sections 里（pi 的结构化 system message）
        //
        // 实测（2026-11-12，qwen3.8:27b / ollama）：payload.messages[0] 是
        // { role:'system', content:'', sections:{preamble,tools,rules,docs,...} }，
        // 真正被适配层渲染进 prompt 的是 sections，content 是空串。
        // 只往 content 写，等于写进一个没人读的字段——模型依旧自称 pi。
        // 所以 sections 存在时，身份必须落在 sections.identity（放第一位）。
        const sec = head.sections && typeof head.sections === 'object' && !Array.isArray(head.sections)
          ? head.sections
          : null
        if (sec) {
          if (typeof sec.identity === 'string' && sec.identity.includes(MARKER)) return
          dbg('bpr_inject', { via: 'messages_sections', addLen: block.length })
          return {
            ...payload,
            messages: [{ ...head, sections: { identity: block, ...sec } }, ...msgs.slice(1)],
          }
        }
        const cur = typeof head.content === 'string'
          ? head.content
          : Array.isArray(head.content)
            ? head.content.map((part) => (part && typeof part.text === 'string' ? part.text : '')).join('')
            : ''
        if (cur.includes(MARKER)) return
        const content = typeof head.content === 'string'
          ? `${block}\n\n${head.content}`
          : Array.isArray(head.content)
            ? [{ type: 'text', text: `${block}\n\n` }, ...head.content]
            : block
        dbg('bpr_inject', { via: 'messages_system', addLen: block.length })
        return { ...payload, messages: [{ ...head, content }, ...msgs.slice(1)] }
      }
      // 连 system 消息都没有：在前面补一条。OpenAI 兼容端点都接受。
      dbg('bpr_inject', { via: 'messages_prepend', addLen: block.length })
      return { ...payload, messages: [{ role: 'system', content: block }, ...msgs] }
    }

    dbg('bpr_no_system_field')
    return
  })

  /**
   * 常驻纪律：把 /workspace/recall-rules.md 里的规则读出来，放进系统提示。
   *
   * 为什么放在扩展里而不是网关里：这是「我这个人的规矩」，不是某个通道的配置。
   * 通道会换（QQ、邮件、以后别的），规矩不该跟着通道搬家；放这里，
   * 换任何入口它都还在。文件按 mtime 重读，改规则不用重启。
   *
   * 读不到就返回空串——规矩是增强，不能因为它没读到而让整轮对话失效。
   */
  const RULES_PATH = process.env.ZERO_RULES || '/workspace/recall-rules.md'
  let rulesCache = { mtime: 0, text: '' }
  function disciplineRules() {
    try {
      const st = statSync(RULES_PATH)
      if (st.mtimeMs !== rulesCache.mtime) {
        const raw = readFileSync(RULES_PATH, 'utf8')
        const lines = raw.split('\n').filter((l) => /^\s*(\d+\.|-)\s/.test(l))
        rulesCache = { mtime: st.mtimeMs, text: lines.map((l) => l.replace(/\*\*/g, '').trim()).join('\n') }
      }
      return rulesCache.text
    } catch {
      return ''
    }
  }

  pi.on('context_with_system', async (event) => {
    const msgs0 = event?.messages
    dbg('hook_context_with_system', {
      skip,
      n: Array.isArray(msgs0) ? msgs0.length : -1,
      headRole: Array.isArray(msgs0) && msgs0[0] ? msgs0[0].role : null,
      shape: Array.isArray(msgs0)
        ? msgs0.slice(0, 3).map((m) => ({
            role: m?.role,
            keys: m ? Object.keys(m) : null,
            ctype: typeof m?.content,
            clen: typeof m?.content === 'string' ? m.content.length : Array.isArray(m?.content) ? m.content.length : null,
          }))
        : null,
      headHead: Array.isArray(msgs0) && msgs0[0] ? JSON.stringify(msgs0[0]).slice(0, 260) : null,
    })
    if (skip) { dbg('cws_skip'); return }
    const cfg = await cfgPromise
    if (!cfg.enabled) { dbg('cws_disabled'); return }
    if (!identityActive(cfg)) { dbg('cws_identity_inactive', { identity: cfg.identity }); return }

    const msgs = event.messages
    const head = msgs[0]
    // 没有前导系统消息时不动：删掉它等于让请求失去 prompt 和工具声明，
    // Pi 会报错，而且模型的提示词结构会坏掉。
    if (!head || head.role !== 'system') { dbg('cws_no_head', { headRole: head ? head.role : null }); return }

    const text = await ensureIdentity(cfg, pi)
    dbg('cws_identity_text', { len: text ? text.length : 0, err: identityError ? String(identityError) : null })
    if (!text) return

    // content 有两种形态：字符串，或 [{type:'text',text:...}] 分段数组。
    //
    // 早先这里只认字符串。分段形态下 `typeof content === 'string'` 为假，
    // current 恒为空串，于是原系统提示被整段替换成身份块——实测日志里
    // sysLen=0 就是这个：身份注进去了，但原始 prompt 被抹掉，模型反而
    // 更不认识自己。判空必须先把两种形态都摊成文本。
    const asText = (c) =>
      typeof c === 'string'
        ? c
        : Array.isArray(c)
          ? c.map((part) => (part && typeof part.text === 'string' ? part.text : '')).join('')
          : ''
    // 结构化系统提示：真正生效的是 head.sections（Record<string,string>），
    // content 往往只是空串。
    //
    // 这是踩了一整轮才看清的：v1.5.x 把系统提示拆成了有序分段，
    // `content` 留空，SDK 从 sections 渲染（preamble 是裸文本，其它 key
    // 会被包成同名标签）。往 content 写身份，等于写进一个没人读的字段——
    // 日志上"注入成功 1955 字"，模型却依然不知道自己是简。
    const sections = head.sections && typeof head.sections === 'object' && !Array.isArray(head.sections)
      ? head.sections
      : null
    if (sections && typeof sections.identity === 'string' && sections.identity.includes(MARKER)) {
      dbg('cws_already_injected', { via: 'sections' })
      return
    }

    // 身份块是「我自己写的自我描述」，不是参考资料。
    //
    // 不加这句定调，模型会把 1955 字的 SKILL.md 当成背景文档读，
    // 然后继续照 preamble 里的 "You are a coding assistant" 回答
    // 「我是 pi」——实测就是这么答的。身份与默认助手设定冲突时，
    // 必须明确说以身份为准，否则注入了等于没注入。
    const IDENTITY_HEADER = identityModeOf(cfg) === 'minimal' ? MINIMAL_HEADER_TEXT : HEADER_TEXT
    const injectText = `${MARKER}\n${IDENTITY_HEADER}\n\n${text}\n${MARKER}`

    if (sections) {
      dbg('cws_inject', {
        via: 'sections',
        keys: Object.keys(sections),
        addLen: text.length,
      })
      // 纪律段：和身份并列，但独立成段。混进 identity 会让它被当成自我描述，
      // 独立成段才会被当成「怎么干活」的指令。
      const rules = disciplineRules()
      const withRules = rules
        ? { identity: injectText, discipline: `${RULES_MARKER}\n${rules}\n${RULES_MARKER}`, ...sections }
        : { identity: injectText, ...sections }
      dbg('cws_rules', { len: rules.length })
      return {
        messages: [
          // identity 放第一位：sections 的顺序就是 prompt 的顺序，
          // 身份在规则和工具之前，模型才把它当"我是谁"而不是"附加资料"。
          { ...head, sections: withRules },
          ...msgs.slice(1),
        ],
      }
    }

    // 旧形态兜底：content 是字符串或分段数组。
    const current = asText(head.content)
    // 幂等：已经注入过就不再叠一遍（压缩后可能多次触发）。
    if (current.includes(MARKER)) { dbg('cws_already_injected'); return }

    const prefix = `${injectText}\n\n`
    // 保持原形态：数组就往前插一个 text 段，字符串就前置拼接。
    // 换形态会让下游对 prompt 结构做假设的代码（含 pi 自己）失效。
    const newContent =
      typeof head.content === 'string'
        ? prefix + head.content
        : Array.isArray(head.content)
          ? [{ type: 'text', text: prefix }, ...head.content]
          : prefix
    dbg('cws_inject', {
      via: 'content',
      sysLen: current.length,
      addLen: text.length,
      contentType: Array.isArray(head.content) ? 'array' : typeof head.content,
    })

    return {
      messages: [{ ...head, content: newContent }, ...msgs.slice(1)],
    }
  })

  pi.on('context', async (event) => {
    if (skip) return
    const cfg = await cfgPromise
    if (!cfg.enabled) return
    // 未挂载身份的会话是普通会话：不往提问尾部塞记忆卡片
    if (!identityActive(cfg)) return

    const text = latestUserText(event.messages)
    // 常驻一行：每轮重新出现，只动尾部不动前缀缓存。
    // ZERO_MEM_NO_REMINDER=1 可关（拿来对照「没有这句时会退化成什么样」）。
    const extra =
      process.env.ZERO_MEM_NO_REMINDER === '1' ? [] : [RECALL_FIRST_LINE]

    // notify 模式的提示：只报地址，不报正文。
    // 取走即清空——同一批变化不该反复出现在每一轮上下文里。
    //
    // 只有 notify 模式才能 consume：其它模式的积压要留到 agent_settled
    // 由 settleWake 结算。早先这里无条件取走，导致 continue 模式的 flag
    // 在停稳之前就被清空，唤醒永远不发生（实测 backlog=1 → settle 时 0）。
    const pendingUpdates = wakePolicy.mode === 'notify' ? wakePolicy.consume() : []
    if (pendingUpdates.length) {
      const rendered = renderPending(pendingUpdates)
      if (rendered) extra.push(rendered)
    }

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
      if (rendered) {
        // 命中就把正文展开带进来，没命中（或读不到）就只留原来那段。
        const full = await hydrateRecall(cfg, rendered)
        extra.push(full || rendered)
      }
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

    if (recallHits.length) {
      const listed = renderHits(recallHits)
      const full = await hydrateRecall(cfg, listed)
      extra.push(full || listed)
    }
    if (!extra.length) return

    // 追加在尾部而不是头部：头部一变，整段前缀缓存就作废。
    return [...event.messages, { role: 'user', content: extra.join('\n\n') }]
  })
}
