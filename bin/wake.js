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
 * 把积压事件渲染成唤醒词。
 *
 * 刻意不渲染正文：正文必须经 mem 工具读回才会落进 tool_result 沙盒。
 */
export function renderWakePrompt(events) {
  if (!events.length) return ''
  const lines = []
  const seen = new Set()
  for (const ev of events) {
    if (seen.has(ev.uri)) continue
    seen.add(ev.uri)
    const addr = ev.uri.replace(/^memcp:\/\//, '').replace('/', '@')
    const from = ev.origin?.identity ? ` by ${ev.origin.identity}` : ''
    const at = ev.commit ? ` @${ev.commit.slice(0, 8)}` : ''
    lines.push(`- ${addr}${at}${from}`)
  }
  // 事件不再携带 commit：写入即通知，脏层里没有新版本号。
  //
  // 早先文案让人 `since <tree> <commit>` 取差量。广播点从 commit 挪到写入后，
  // 那个 commit 是「写入前的头」——拿它取差量只会得到空。
  // 正确的动作是直接去看那棵树，所以文案改成让人去读。
  return [
    '[zero-memcp] 有新的写入需要处理：',
    ...lines,
    '',
    '这些树刚被写过（可能还没折叠成提交）。用 mem 工具直接看：',
    '  mem({ cmd: "ls <日期>" })      看某天有什么',
    '  mem({ cmd: "grep <关键词> --scope <树>" })',
    '  mem({ cmd: "recall <问题>" })   语义检索',
    '若这些变更不需要动作，直接回复即可。',
  ].join('\n')
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
