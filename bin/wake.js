// 资源变更订阅 + 唤醒策略（flag 模式，不是边沿触发）。
//
// # 核心语义
//
// 唤醒不是一个「动作」，而是一个**状态标记**。
//
//   事件到达  → 只设 flag。绝不碰 agent。
//   agent 停稳（agent_settled）→ 检查 flag，有积压就拉起一次并清空。
//   事件到达时 agent 正空闲 → 立即拉起一次。
//
// 这样：
//   - agent 在忙：事件只入队，不打断。它自己这一轮就会看到积压提示。
//   - agent 已停：积压把它拉起来，一次性处理完所有积压。
//   - 一次唤醒消费全部积压 —— 一个事件一次唤醒是自激的根源。
//
// # 为什么不用边沿触发
//
// 「每来一个事件就 sendUserMessage」有两个死法：
//   ① 打断正在工作的 agent（它本来就在处理，不需要被叫）
//   ② 一次突发写 20 条 → 唤醒 20 次 → 每次都落盘 → 20 批新事件……
//
// flag 模式天然免疫两者：写入端只置位，唤醒端只在「停稳」这一个时刻
// 结算一次。
//
// # 自激仍然是可配置的
//
// agent 自己落盘的写入，其 origin.session 等于自己 → 被过滤 → 不进 flag。
// 因此正常配置下不存在自激。
//
// loop 模式关掉这个过滤 —— 那时自激是**特性**（写一条 TODO 就继续干活
// 直到办完）。这种模式必须配 maxChain 熔断，否则会无人值守地烧下去。
//
// # 三种模式
//
//   off       不订阅，不唤醒
//   notify    只累积；下一轮 context 注入一行提示，绝不主动起轮
//   continue  停稳且有积压时拉起一轮，忽略自己写的
//   loop      同 continue 但不过滤 origin（允许自激，靠 maxChain 熔断）

/** 生成一个会话标识：同一进程内稳定，用于事件溯源。 */
export function makeSessionTag() {
  const rand = Math.random().toString(36).slice(2, 10)
  return `pi-${Date.now().toString(36)}-${rand}`
}

/** 解析服务端推送的事件帧。 */
export function parseFrame(text) {
  const out = { ack: null, events: [] }
  for (const block of String(text).split('\n\n')) {
    if (!block.trim() || block.startsWith(':')) continue
    let data = ''
    for (const line of block.split('\n')) {
      if (line.startsWith('data: ')) data += line.slice(6)
    }
    if (!data) continue
    let msg
    try {
      msg = JSON.parse(data)
    } catch {
      continue
    }
    const method = msg?.method || ''
    if (method === 'notifications/subscriptions/acknowledged') {
      out.ack = msg.params || {}
    } else if (method === 'notifications/resources/updated') {
      const p = msg.params || {}
      const meta = p._meta || {}
      out.events.push({
        uri: p.uri || '',
        origin: meta['memcp/origin'] || {},
        commit: meta['memcp/commit'] || '',
        previous: meta['memcp/previous'] || '',
      })
    }
  }
  return out
}

/** 判断一条事件该不该被忽略（是不是自己造成的）。 */
export function shouldIgnore(ev, opts) {
  if (opts.mode === 'loop') return false
  const o = ev.origin || {}
  if (opts.session && o.session && o.session === opts.session) return true
  // 没有 session 信息时退回按身份判断
  if (!o.session && opts.identity && o.identity && o.identity === opts.identity) return true
  return false
}

/**
 * 唤醒策略：flag + 停稳结算。
 *
 * 所有方法都是纯状态变更，不碰 Pi 的 API —— 便于测试，也让
 * 「何时唤醒」与「如何唤醒」解耦。
 */
export class WakePolicy {
  constructor(opts = {}) {
    this.mode = opts.mode || 'off'
    // 两次唤醒之间的最小间隔。用 flag 模式后它不只是「冷却」，
    // 更是「让循环慢到人能观察和干预」的节拍器。
    this.minIntervalMs = Number.isFinite(opts.minIntervalMs)
      ? opts.minIntervalMs
      : Number.isFinite(opts.cooldownMs)
        ? opts.cooldownMs
        : 60000
    this.maxChain = Number.isFinite(opts.maxChain) ? opts.maxChain : 0
    this.session = opts.session || ''
    this.identity = opts.identity || ''

    /** 积压：uri → 最近一次事件。同一个 uri 多次变更只算一条。 */
    this.backlog = new Map()
    this.busy = false
    this.lastWakeAt = 0
    this.chain = 0
    this.stoppedReason = ''
  }

  configure(opts = {}) {
    if (opts.mode !== undefined) this.mode = opts.mode
    if (opts.minIntervalMs !== undefined) this.minIntervalMs = opts.minIntervalMs
    else if (opts.cooldownMs !== undefined) this.minIntervalMs = opts.cooldownMs
    if (opts.maxChain !== undefined) {
      this.maxChain = opts.maxChain
      if (this.maxChain === 0 || this.chain < this.maxChain) this.stoppedReason = ''
    }
    if (opts.session !== undefined) this.session = opts.session
    if (opts.identity !== undefined) this.identity = opts.identity
  }

  /**
   * 事件到达：**只设 flag**。
   *
   * 绝不在这里唤醒 agent。返回有多少条进入积压/被忽略，
   * 供调用方决定要不要在「当前已空闲」时立刻结算一次。
   */
  ingest(events) {
    let queued = 0
    let ignored = 0
    let wakeBlocked = 0
    for (const ev of events) {
      if (shouldIgnore(ev, this)) {
        ignored += 1
        continue
      }
      // 熔断状态下仍然记录积压，只是不唤醒。
      //
      // 早先这里直接丢弃，结果是 settle() 因「没有积压」返回 none，
      // 调用方永远看不到 stopped —— 而熔断的全部意义就是让人知道它停了。
      // 而且积压本身是 Map（按 uri 去重），上限是树的棵数，不会失控。
      if (this.maxChain > 0 && this.chain >= this.maxChain) wakeBlocked += 1
      // 同一 uri 只保留最新一条：积压是「哪些树变了」，
      // 不是「发生了多少次变更」。
      this.backlog.set(ev.uri, ev)
      queued += 1
    }
    return { queued, ignored, wakeBlocked, backlogSize: this.backlog.size }
  }

  /** agent 开始工作。 */
  markBusy() {
    this.busy = true
  }

  /** agent 停稳（agent_settled）。 */
  markIdle() {
    this.busy = false
  }

  /**
   * 结算：当前该不该唤醒。
   *
   * 调用时机只有两个：
   *   ① 事件到达且 `busy === false`
   *   ② agent_settled（busy 刚变 false）
   *
   * 返回 { action, reason, events }
   *   none      不唤醒（off / 无积压 / 冷却中 / 仍在忙）
   *   notify    只把积压留给下一轮 context（notify 模式）
   *   wake      应当唤醒，events 是本次消费掉的积压
   *   stopped   熔断
   *
   * action === 'wake' 时积压已被清空并计入链长；调用方只需负责
   * 实际发出唤醒消息。
   */
  settle(now = Date.now()) {
    if (this.mode === 'off') return { action: 'none', reason: 'mode off', events: [] }
    if (this.busy) return { action: 'none', reason: 'agent is busy; backlog will be settled when it stops', events: [] }
    if (this.backlog.size === 0) return { action: 'none', reason: 'no backlog', events: [] }

    if (this.mode === 'notify') {
      // notify 不主动起轮：积压留给下一轮 context 提示。
      return { action: 'notify', reason: 'notify mode; backlog waits for next turn', events: [...this.backlog.values()] }
    }

    if (this.maxChain > 0 && this.chain >= this.maxChain) {
      const reason = this.stoppedReason || `maxChain ${this.maxChain} reached; stopping to avoid runaway`
      this.stoppedReason = reason
      return { action: 'stopped', reason, events: [...this.backlog.values()] }
    }

    const wait = this.minIntervalMs - (now - this.lastWakeAt)
    if (this.lastWakeAt > 0 && wait > 0) {
      return { action: 'none', reason: `throttled: ${Math.ceil(wait / 1000)}s until next wake`, events: [...this.backlog.values()] }
    }

    const events = [...this.backlog.values()]
    this.backlog.clear()
    this.lastWakeAt = now
    this.chain += 1
    return { action: 'wake', reason: `settling ${events.length} pending change(s)`, events }
  }

  /** 取走积压但不唤醒（notify 模式取提示用）。 */
  consume() {
    const out = [...this.backlog.values()]
    this.backlog.clear()
    return out
  }

  /** 一轮结束：非 loop 模式的链长归零。 */
  onTurnEnd() {
    if (this.mode !== 'loop') this.chain = 0
  }

  /** 人工复位熔断与节流。 */
  reset() {
    this.chain = 0
    this.lastWakeAt = 0
    this.stoppedReason = ''
  }

  status() {
    return {
      mode: this.mode,
      minIntervalMs: this.minIntervalMs,
      maxChain: this.maxChain,
      chain: this.chain,
      backlog: this.backlog.size,
      busy: this.busy,
      stoppedReason: this.stoppedReason,
      lastWakeAt: this.lastWakeAt,
    }
  }
}

/**
 * 事件里的路径分隔符和存储里的不一致，读之前得先归一。
 *
 * 实测（2026-11-12，重启后第一轮）：服务端资源事件的 path 形如
 *   2026-11-12@215053.484870-5490-3060.md
 * 而真正存进去的路径是
 *   2026-11-12/215053.484870-5490-3060.md
 * 只差 `@` 和 `/`。read 用 `@` 直接 not found，fetchSnippets 又静默吞异常，
 * 于是唤醒词永远退化成「[外部] 有新消息」——看起来像「推送不带正文」，
 * 实际是取正文那一步没找到文件。
 */
export function normPath(p) {
  return String(p || '').replace(/(\d{4}-\d{2}-\d{2})@(?=\d{6}\.)/, '$1/')
}

/**
 * 把积压事件渲染成唤醒词。
 *
 * 刻意不渲染正文：正文必须经 mem 工具读回才会落进 tool_result 沙盒。
 */
export function renderWakePrompt(events, snippets) {
  if (!events.length) return ''
  const seen = new Set()
  const parts = []

  for (const ev of events) {
    if (seen.has(ev.uri)) continue
    seen.add(ev.uri)
    const path = normPath(ev.path)
    const addr = path ? `${uriToAddr(ev.uri)}:${path}` : uriToAddr(ev.uri)
    const snip = snippets?.[ev.uri]

    // 越短越好：一条消息 = 一行标题 + 正文 + 地址。
    //
    // 早先把信封全部字段（source/endpoint/created/session/evt/at/src/from/mid）
    // 原样倒出来，agent 得先读一堆元数据才看到谁说了什么。信封是给机器看的，
    // 提示是给人看的：只留来源名、发送者、正文。
    //
    // 群里说的「不同群的消息混一起」是这条引起的：只写 [QQ] 名字，
    // 同一拨人在两个群说话时分不清哪句出自哪个群。来源必须带会话 id。
    const srcRaw = snip?.source ? String(snip.source).toUpperCase() : '外部'
    const chat = snip?.chat ? String(snip.chat) : ''
    const src = chat ? `${srcRaw}:${chat}` : srcRaw
    const who = snip?.from ? String(snip.from) : ''
    const head = who ? `[${src}] ${who}` : `[${src}]`
    const body = snip?.body ? String(snip.body).trim() : ''
    if (body) {
      parts.push(`${head}：${body}`, `（${addr}）`)
    } else {
      parts.push(`${head} 有新消息`, `（${addr}）`)
    }
  }
  return parts.join('\n\n')
}

/** memcp://BlackEggAI%2Fzero/inbox → BlackEggAI/zero@inbox */
export function uriToAddr(uri) {
  const rest = decodeURIComponent(String(uri || '').replace(/^memcp:\/\//, ''))
  const i = rest.lastIndexOf('/')
  if (i < 0) return rest
  return `${rest.slice(0, i)}@${rest.slice(i + 1)}`
}

function indent(text) {
  return String(text)
    .split('\n')
    .slice(0, 12)
    .map((l) => `  ${l}`)
    .join('\n')
}


/**
 * 把积压渲染成 context 提示行（notify 模式）。
 *
 * 不主动起轮，所以措辞是「供参考」而不是「去做」。
 */
export function renderPending(events) {
  if (!events.length) return ''
  const lines = []
  const seen = new Set()
  for (const ev of events) {
    if (seen.has(ev.uri)) continue
    seen.add(ev.uri)
    const addr = ev.uri.replace(/^memcp:\/\//, '').replace('/', '@')
    const from = ev.origin?.identity ? ` by ${ev.origin.identity}` : ''
    lines.push(`- ${addr} 有更新${from}`)
  }
  return [
    '<memory_updates>',
    '以下记忆树在你上次查看之后有变化。需要时用 mem 取差量：',
    '  mem({ cmd: "since <tree> <commit>" })',
    ...lines,
    '</memory_updates>',
  ].join('\n')
}


/**
 * 把一条记忆拆成「信封 + 正文」。
 *
 * 信封是写入方写在正文开头的 key: value 行（source/chat/from/id），
 * 它和 zero-memcp 自带的元数据（created/session/endpoint）混在一起，
 * 所以先按顺序收 kv，遇到第一个不匹配的行就算正文开始。
 */
export function parseEntry(text) {
  const raw = String(text || '')
  // 形态一：qqmcp 投递格式（带 === 新消息开始 === 标记）
  //
  // 实测（2026-11-12）：qqmcp 写入的信封头部是 frontmatter（--- source: qqmcp-inbox ... ---），
  // 真正的正文包在 === 新消息开始 === 与 === 新消息结束 === 之间。
  // 旧代码遇到第一个 --- 就以为正文开始了，把整个 inbox/qq ... 连带
  // 上下文全当成 body，甚至在第一道空行前只认出 source: qqmcp-inbox，
  // 发送者和正文全空——于是提示词只能退化成「[外部] 有新消息」，
  // 看起来像「推送没带内容」。
  const mNews = /=== 新消息开始 ===\s*\n([\s\S]*?)\n\s*=== 新消息结束 ===/.exec(raw)
  const mFrom = /\bfrom=([^\s\n]+)/.exec(raw)
  const mSrc = /\bsrc=([a-z]+):/.exec(raw)
  if (mNews) {
    // src 的形状是 group:<gid> / private:<uid>，来源渠道是 qq 而不是「group」。
    // 名字后缀的 (uin) 对人间对话没用，提示词里只留名字。
    const kind = mSrc ? mSrc[1] : ''
    const mChat = /\bsrc=(?:group|private):([\w-]+)/.exec(raw)
    const source = kind === 'group' || kind === 'private' ? 'qq' : (kind || 'qq')
    const who = mFrom ? mFrom[1].replace(/\s*\(\d+\)\s*$/, '') : ''
    return {
      source,
      chat: mChat ? mChat[1] : '',
      from: who,
      body: mNews[1].trim().slice(0, 400),
    }
  }

  // 形态二：标准信封格式（以 --- 分割，或 key: value 抬头）
  const lines = raw.split('\n')
  const kv = {}
  let bodyAt = 0
  let inFm = false
  for (let i = 0; i < lines.length && i < 20; i++) {
    const line = lines[i].trim()
    if (line === '---') {
      if (!inFm) { inFm = true; continue }
      bodyAt = i + 1
      break
    }
    if (!line) continue
    const m = /^([A-Za-z_][\w-]*)\s*:\s*(.*)$/.exec(line)
    if (m) kv[m[1].toLowerCase()] = m[2].trim()
    else if (!inFm) break
    bodyAt = i + 1
  }
  const srcRaw = (kv.src || kv.source || '').toLowerCase()
  const known = ['qq', 'wechat', 'tieba', 'mail', 'rss', 'telegram']
  const source = known.find((x) => srcRaw.includes(x)) || (kv.src ? srcRaw : '外部')
  return {
    source,
    chat: kv.chat || kv.thread || kv.room || '',
    from: kv.from || kv.user || kv.sender || '',
    body: lines.slice(bodyAt).join('\n').trim().slice(0, 600),
  }
}
