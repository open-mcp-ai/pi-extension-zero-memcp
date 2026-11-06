// 资源变更订阅 + 唤醒策略。
//
// # 这是什么
//
// zero-memcp 用标准 MCP 的 subscriptions/listen 推送「记忆树变了」。
// 本模块接收那些事件，并决定要不要让 agent 知道。
//
// # 为什么唤醒必须策略化
//
// 写入会推进分支头，推进会产生事件，事件会唤醒写入者——这构成一个
// 可以自我维持的循环。它有两种面貌：
//
//   - 默认是 bug：agent 每轮落盘，服务端 30 秒后提交并推事件，
//     于是 agent 醒来、又落盘、又被唤醒……无人参与地烧 token。
//   - 也可以是特性：写一条 TODO 就继续干活，直到办完为止。
//
// 二者在服务端看来完全一样。区别只在客户端策略，所以策略必须显式。
//
// # 四种模式
//
//   off       不订阅，不唤醒（默认）
//   notify    收到事件只标记；下一轮 context 注入一行提示（零额外 token）
//   continue  空闲时唤醒一轮，但忽略自己写的（origin 过滤）
//   loop      不过滤 origin —— 允许自激。必须配 maxChain 熔断
//
// maxChain 是真正的护栏：无人值守时连续 N 次后自动停。

/** 生成一个会话标识：同一进程内稳定，用于事件溯源。 */
export function makeSessionTag() {
  const rand = Math.random().toString(36).slice(2, 10)
  return `pi-${Date.now().toString(36)}-${rand}`
}

/**
 * 解析服务端推送的事件帧。
 *
 * 只关心 notifications/resources/updated。其他（含 ack）原样返回，
 * 由调用方决定要不要用——ack 对「连接已就绪」这个判断有用。
 */
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

/**
 * 判断一条事件该不该被忽略。
 *
 * @param ev       事件
 * @param opts     { mode, session, identity }
 *
 * loop 模式不过滤——那正是「自激」的定义。
 * 其余模式忽略自己这次会话造成的事件：自己写的记忆唤醒自己没有意义。
 */
export function shouldIgnore(ev, opts) {
  if (opts.mode === 'loop') return false
  const o = ev.origin || {}
  // 会话相同 = 我自己这一轮写的
  if (opts.session && o.session && o.session === opts.session) return true
  // 没有 session 信息时退回按身份判断（同一张凭证的写入）
  if (!o.session && opts.identity && o.identity && o.identity === opts.identity) return true
  return false
}

/**
 * 唤醒策略引擎：冷却 + 熔断 + 模式判定。
 *
 * 状态很少（上次唤醒时间、连续链长），但每一项都对应一个真实的失控场景，
 * 所以单独抽出来便于测试。
 */
export class WakePolicy {
  constructor(opts = {}) {
    this.mode = opts.mode || 'off'
    this.cooldownMs = Number.isFinite(opts.cooldownMs) ? opts.cooldownMs : 60000
    this.maxChain = Number.isFinite(opts.maxChain) ? opts.maxChain : 0
    this.session = opts.session || ''
    this.identity = opts.identity || ''

    this.lastWakeAt = 0
    this.chain = 0
    this.pending = []      // notify 模式下的待提示事件
  }

  /** 更新策略（配置热变、换身份时调用）。 */
  configure(opts = {}) {
    if (opts.mode !== undefined) this.mode = opts.mode
    if (opts.cooldownMs !== undefined) this.cooldownMs = opts.cooldownMs
    if (opts.maxChain !== undefined) this.maxChain = opts.maxChain
    if (opts.session !== undefined) this.session = opts.session
    if (opts.identity !== undefined) this.identity = opts.identity
  }

  /**
   * 处理一批事件，返回要采取的动作。
   *
   * 返回 { action: 'none'|'notify'|'wake'|'stopped', reason, events }
   *   none     什么都不做（关闭、被忽略、冷却中、熔断）
   *   notify   只记录，等下一轮 context 提示
   *   wake     应当唤醒 agent
   *   stopped  因熔断停止
   */
  handle(events, now = Date.now()) {
    if (this.mode === 'off' || !events.length) {
      return { action: 'none', reason: this.mode === 'off' ? 'mode off' : 'no events', events: [] }
    }

    const fresh = events.filter((ev) => !shouldIgnore(ev, this))
    if (!fresh.length) {
      return { action: 'none', reason: 'all events originated from this session', events: [] }
    }

    if (this.mode === 'notify') {
      // 累积到下一轮 context，不主动起轮。
      this.pending.push(...fresh)
      return { action: 'notify', reason: 'notify mode', events: fresh }
    }

    // continue / loop
    if (this.maxChain > 0 && this.chain >= this.maxChain) {
      return {
        action: 'stopped',
        reason: `maxChain ${this.maxChain} reached; stopping to avoid runaway`,
        events: fresh,
      }
    }

    if (now - this.lastWakeAt < this.cooldownMs) {
      return {
        action: 'none',
        reason: `cooldown ${this.cooldownMs}ms not elapsed`,
        events: fresh,
      }
    }

    this.lastWakeAt = now
    this.chain += 1
    return { action: 'wake', reason: 'wake condition met', events: fresh }
  }

  /** agent 完成一轮后调用：给了终止条件的循环在此归零。 */
  onTurnEnd() {
    // 只有 loop 模式靠 maxChain 熔断，链长跨轮累积；
    // 其它模式每轮结束即清（它们本来就不该连续自我触发）。
    if (this.mode !== 'loop') this.chain = 0
  }

  /** 取走并清空待提示事件（notify 模式用）。 */
  takePending() {
    const out = this.pending
    this.pending = []
    return out
  }

  /** 人工复位熔断（/mem:wake reset）。 */
  reset() {
    this.chain = 0
    this.lastWakeAt = 0
    this.pending = []
  }

  /** 当前状态，供 /mem:wake 显示。 */
  status() {
    return {
      mode: this.mode,
      cooldownMs: this.cooldownMs,
      maxChain: this.maxChain,
      chain: this.chain,
      pending: this.pending.length,
      lastWakeAt: this.lastWakeAt,
    }
  }
}

/**
 * 把 notify 模式的待提示事件渲染成一行上下文。
 *
 * 刻意不渲染正文：正文必须经 mem 工具读回才会落进 tool_result 沙盒。
 * 这里只给地址和下一步命令。
 */
export function renderPending(events) {
  if (!events.length) return ''
  const seen = new Set()
  const lines = []
  for (const ev of events) {
    if (seen.has(ev.uri)) continue
    seen.add(ev.uri)
    const addr = ev.uri.replace(/^memcp:\/\//, '').replace('/', '@')
    const from = ev.origin?.identity ? ` by ${ev.origin.identity}` : ''
    lines.push(`- ${addr} 有更新${from}`)
  }
  return [
    '<memory_updates>',
    '以下记忆树在你上次查看之后有变化。需要时用 mem 工具取差量：',
    '  mem({ cmd: "since <tree> <commit>" })',
    ...lines,
    '</memory_updates>',
  ].join('\n')
}
