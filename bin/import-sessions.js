#!/usr/bin/env node
// 把 pi 的历史会话（~/.pi/agent/sessions/*.jsonl）搬进 zero-memcp。
//
// 这是 pi 生态的工具，不是记忆体的功能：zero-memcp 只认 put/commit 这些
// 通用动词，完全不知道 pi 的会话文件长什么样。读取 pi 私有格式的知识
// 属于这里。
//
// ── 三件容易做错的事 ──────────────────────────────────────────
//
// 1. 会话文件会叠加。压缩（compaction）会写出一个新文件，里面是旧对话的
//    副本加上新增部分。逐个文件读会重复导入，只读最新的会丢历史。
//    这里按 session id 归并，同一个 id 只取最大的那份快照。
//
// 2. 正在写的文件读不得。助手还在回答时，jsonl 的最后一行可能只写了一半，
//    直接解析会失败并把这一轮导入成截断的半句。判据不靠文件时间——那要靠猜——
//    而是靠消息自带的 stopReason：只有 stop 才算说完，toolUse 是中间态，
//    aborted 是用户打断（不回填），error 是失败。
//
// 3. 重复导入不能产生重复切片。写盘时间戳每次都不同，文件名也就不同，
//    比文件名判重必然失败。服务端的 dedup 是内存 Map，进程重启就清空，
//    也指望不上。所以这里维护一份本地账本，按 endpoint 分区记录
//    已经导入过的轮次指纹；重跑零网络开销，且幂等。
import { readFile, writeFile, mkdir, stat } from 'node:fs/promises'
import { createReadStream, statSync } from 'node:fs'
import { createHash } from 'node:crypto'
import { createInterface } from 'node:readline'
import { homedir } from 'node:os'
import { join, resolve, basename } from 'node:path'

const VERSION = '1.0.0'

const SESSION_ID_RE = /_([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$/i
const TRANSITION_RE = /^(好的|好，|我来|让我|我先|我来看|先看|正在|接下来|现在|我查|我试|稍等|嗯|收到)/
const MEM_MARK_RE = /^\[\[mem:[a-z]+\]\]\s*/gm

const stripMem = (s) => s.replace(MEM_MARK_RE, '').trim()

const isTransition = (s) => {
  const t = s.trim()
  return [...t].length < 40 && TRANSITION_RE.test(t)
}

function extractText(content) {
  if (!content) return ''
  if (typeof content === 'string') return content
  if (!Array.isArray(content)) return ''
  const parts = []
  for (const p of content) {
    if (p && p.type === 'text' && typeof p.text === 'string' && p.text) {
      parts.push(p.text)
    }
  }
  return parts.join('\n')
}

async function* eachLine(path) {
  const rl = createInterface({
    input: createReadStream(path, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  })
  for await (const line of rl) {
    if (!line.trim()) continue
    let obj
    try {
      obj = JSON.parse(line)
    } catch {
      continue
    }
    yield obj
  }
}

async function startedAt(path) {
  let n = 0
  for await (const line of eachLine(path)) {
    if (++n > 5) break
    if (line?.type !== 'session') continue
    const v = line.timestamp || line.created
    if (!v) continue
    const t = new Date(v)
    if (!Number.isNaN(t.getTime())) return t
  }
  return null
}

async function extractTurns(path, sessionId) {
  const out = []
  let ask = ''
  let answer = ''
  let haveQ = false
  let askAt = null
  let skipFirstUser = true

  for await (const line of eachLine(path)) {
    if (line?.type !== 'message' || !line.message) continue
    const msg = line.message
    const text = stripMem(extractText(msg.content))

    if (msg.role === 'user') {
      if (skipFirstUser) {
        skipFirstUser = false
        if (text.startsWith('The conversation history before this point') || text.startsWith('<')) {
          continue
        }
      }
      if (haveQ && answer) {
        out.push({ session: sessionId, ask, answer, at: askAt })
      }
      ask = text
      answer = ''
      haveQ = true
      const t = line.timestamp ? new Date(line.timestamp) : null
      askAt = t && !Number.isNaN(t.getTime()) ? t : null
      continue
    }

    if (msg.role === 'assistant') {
      if (msg.stopReason === 'aborted') {
        answer = ''
        continue
      }
      if (msg.stopReason !== 'stop') continue
      if (text) answer = text
    }
  }

  if (haveQ && answer) {
    out.push({ session: sessionId, ask, answer, at: askAt })
  }
  return out
}

const renderTurn = (t) => (t.ask.trim() ? `Q: ${t.ask}\n\nA: ${t.answer}` : t.answer)

async function collectSessions(targets) {
  const byId = new Map()
  const files = []

  const walk = async (p) => {
    let st
    try {
      st = await stat(p)
    } catch {
      return
    }
    if (st.isDirectory()) {
      const { readdir } = await import('node:fs/promises')
      const entries = await readdir(p, { withFileTypes: true })
      for (const e of entries) {
        await walk(join(p, e.name))
      }
      return
    }
    if (!p.endsWith('.jsonl')) return
    files.push(p)
  }

  for (const t of targets) await walk(t)

  for (const f of files) {
    const m = SESSION_ID_RE.exec(basename(f))
    if (!m) continue
    const id = m[1].toLowerCase()
    let size = 0
    try {
      size = statSync(f).size
    } catch {
      continue
    }
    const prev = byId.get(id)
    if (!prev || size > prev.size) byId.set(id, { id, path: f, size })
  }

  return { sessions: [...byId.values()], files }
}

const fingerprint = (session, answer) =>
  createHash('sha256').update(`${session}\n${answer}`, 'utf8').digest('hex').slice(0, 16)

async function loadState(path, endpoint) {
  try {
    const raw = JSON.parse(await readFile(path, 'utf8'))
    const seen = raw?.[endpoint]?.seen
    if (Array.isArray(seen)) return new Set(seen)
  } catch {}
  return new Set()
}

async function saveState(path, endpoint, seen) {
  let all = {}
  try {
    all = JSON.parse(await readFile(path, 'utf8')) || {}
  } catch {}
  all[endpoint] = { seen: [...seen], updatedAt: Date.now() }
  await mkdir(join(path, '..'), { recursive: true })
  await writeFile(path, JSON.stringify(all, null, 2) + '\n', 'utf8')
}

function makeClient(endpoint, token, role) {
  const headers = { 'content-type': 'application/json' }
  if (token) headers.Authorization = `Bearer ${token}`
  if (role) headers['X-Zero-Role'] = role

  return async function callTerm(cmd, vars, timeoutMs = 30000) {
    const args = { cmd }
    if (vars && Object.keys(vars).length) args.vars = vars

    const res = await fetch(`${endpoint}/mcp`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: Date.now(),
        method: 'tools/call',
        params: { name: 'term', arguments: args },
      }),
      signal: AbortSignal.timeout(timeoutMs),
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const body = await res.json()
    if (body.error) throw new Error(body.error.message || 'term failed')
    const content = body.result?.content
    const text = Array.isArray(content) && content[0]?.text ? content[0].text : ''
    if (body.result?.isError) throw new Error(text || 'term failed')
    return text
  }
}

function parseArgs(argv) {
  const opts = {
    targets: [],
    endpoint: process.env.ZERO_MEMCP_ENDPOINT || 'http://127.0.0.1:1729',
    token: process.env.ZERO_MEMCP_TOKEN || '',
    role: process.env.ZERO_MEMCP_ROLE || '',
    since: '',
    dryRun: false,
    statePath: process.env.ZERO_MEMCP_IMPORT_STATE || join(homedir(), '.zero-memcp', 'import-state.json'),
    useState: true,
    commitEvery: 50,
    autoCommit: true,
    json: false,
    quiet: false,
    limit: 0,
  }

  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    const next = () => argv[++i]
    switch (a) {
      case '-h':
      case '--help':
        opts.help = true
        break
      case '--endpoint':
        opts.endpoint = next()
        break
      case '--token':
        opts.token = next()
        break
      case '--role':
        opts.role = next()
        break
      case '--since':
        opts.since = next()
        break
      case '--dry-run':
        opts.dryRun = true
        break
      case '--state':
        opts.statePath = next()
        break
      case '--no-state':
        opts.useState = false
        break
      case '--commit-every':
        opts.commitEvery = Number(next()) || 0
        break
      case '--no-commit':
        opts.autoCommit = false
        break
      case '--limit':
        opts.limit = Number(next()) || 0
        break
      case '--json':
        opts.json = true
        break
      case '--quiet':
        opts.quiet = true
        break
      case '-V':
      case '--version':
        opts.version = true
        break
      default:
        if (a.startsWith('-')) throw new Error(`未知参数: ${a}`)
        opts.targets.push(a)
    }
  }
  return opts
}

function usage() {
  return `zero-memcp-import ${VERSION} —— 把 pi 的历史会话导入 zero-memcp

用法：
  zero-memcp-import [选项] [路径...]

路径可以是 .jsonl 文件，也可以是目录（递归扫描）。可以给多个。
不给路径时默认扫描 ~/.pi/agent/sessions。

选项：
  --endpoint URL      zero-memcp 地址（缺省 http://127.0.0.1:1729）
  --token JWT         凭证（服务端要求鉴权时必填）
  --role NAME         以某个角色身份写入（X-Zero-Role）
  --since YYYY-MM-DD  只导入该日期之后创建的会话
  --limit N           最多导入 N 轮（调试用）
  --dry-run           只统计，不写盘
  --state FILE        判重账本路径（缺省 ~/.zero-memcp/import-state.json）
  --no-state          不用账本（每次都全量尝试，靠服务端去重）
  --commit-every N    每写 N 条提交一次（缺省 50；0 = 只在结束时提交）
  --no-commit         完全不提交（脏层留给服务端后台策略）
  --json              以 JSON 输出统计
  --quiet             不打印逐条进度
  -h, --help          显示本帮助
  -V, --version       显示版本

例子：
  # 先统计有多少能导入（默认扫 ~/.pi/agent/sessions）
  zero-memcp-import --dry-run

  # 导入单个会话文件
  zero-memcp-import /path/to/xxx.jsonl

  # 导入指定目录且只导入 8 月之后的
  zero-memcp-import --since 2026-08-01 ~/my-sessions/
`
}

async function main() {
  const opts = parseArgs(process.argv.slice(2))

  if (opts.help) {
    process.stdout.write(usage())
    return 0
  }
  if (opts.version) {
    console.log(VERSION)
    return 0
  }

  const targets = opts.targets.length
    ? opts.targets.map((p) => resolve(p.replace(/^~(?=$|\/)/, homedir())))
    : [join(homedir(), '.pi', 'agent', 'sessions')]

  let cutoff = null
  if (opts.since) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(opts.since)) {
      console.error(`--since 需要 YYYY-MM-DD，得到 ${opts.since}`)
      return 2
    }
    cutoff = new Date(`${opts.since}T00:00:00`)
  }

  const { sessions, files } = await collectSessions(targets)
  if (!files.length) {
    console.error(`没有找到 .jsonl 会话文件：${targets.join(', ')}`)
    return 1
  }

  let selected = sessions
  if (cutoff) {
    const kept = []
    for (const s of sessions) {
      const at = await startedAt(s.path)
      if (at && at >= cutoff) kept.push(s)
    }
    selected = kept
  }

  const seen = opts.useState ? await loadState(opts.statePath, opts.endpoint) : new Set()
  const callTerm = makeClient(opts.endpoint, opts.token, opts.role)

  const result = {
    endpoint: opts.endpoint,
    files: files.length,
    sessions: selected.length,
    turns: 0,
    written: 0,
    present: 0,
    empty: 0,
    committed: 0,
    errors: [],
  }

  let pendingCommit = 0
  const written = new Set(seen)

  const doCommit = async () => {
    if (opts.dryRun || !opts.autoCommit) return
    try {
      await callTerm('commit', null, 120000)
      result.committed++
      pendingCommit = 0
    } catch (err) {
      result.errors.push(`commit: ${err.message}`)
    }
  }

  for (const s of selected) {
    let turns
    try {
      turns = await extractTurns(s.path, s.id)
    } catch (err) {
      result.errors.push(`${s.id}: ${err.message}`)
      continue
    }

    for (const t of turns) {
      result.turns++
      if (opts.limit && result.turns > opts.limit) break

      if (!t.answer.trim() || isTransition(t.answer)) {
        result.empty++
        continue
      }

      const fp = fingerprint(t.session, t.answer)
      if (written.has(fp)) {
        result.present++
        continue
      }

      const text = renderTurn(t)

      if (opts.dryRun) {
        result.written++
        written.add(fp)
        continue
      }

      try {
        await callTerm(`put @${t.session} -- $TEXT`, { TEXT: text })
        written.add(fp)
        result.written++
        pendingCommit++
        if (opts.commitEvery > 0 && pendingCommit >= opts.commitEvery) {
          await doCommit()
        }
      } catch (err) {
        result.errors.push(`${s.id}: ${err.message}`)
      }
    }
  }

  if (pendingCommit > 0 || (result.written > 0 && opts.commitEvery === 0)) {
    await doCommit()
  }

  if (opts.useState && !opts.dryRun) {
    try {
      await saveState(opts.statePath, opts.endpoint, written)
    } catch (err) {
      result.errors.push(`保存账本: ${err.message}`)
    }
  }

  if (opts.json) {
    console.log(JSON.stringify(result, null, 2))
  } else {
    const verb = opts.dryRun ? '可写入' : '写入'
    console.log(`扫描 ${result.files} 个文件，归并出 ${result.sessions} 个会话`)
    console.log(`完整轮次 ${result.turns}：已存在 ${result.present}，${verb} ${result.written}，跳过空回答 ${result.empty}`)
    if (result.committed) console.log(`提交 ${result.committed} 次`)
    if (result.errors.length) {
      console.log(`失败 ${result.errors.length} 条：`)
      for (const e of result.errors.slice(0, 20)) console.log(`  ! ${e}`)
      if (result.errors.length > 20) console.log(`  ... 另有 ${result.errors.length - 20} 条`)
    }
  }

  return result.errors.length && !result.written ? 1 : 0
}

main()
  .then((code) => process.exit(code))
  .catch((err) => {
    console.error(`zero-memcp-import: ${err.message}`)
    process.exit(1)
  })
