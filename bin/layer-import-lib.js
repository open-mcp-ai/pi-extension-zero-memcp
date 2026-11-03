#!/usr/bin/env node
// zero-memcp-layer-import —— 把 pi 历史会话按认知层级导入 zero-memcp。
//
// 为什么按层分：一次会话里混着三种性质完全不同的东西——
//   事实（compaction summary）：模型对历史的压缩，密度最高、最值得先看；
//   记录（一问一答）：实际发生过什么，可信但没有提炼；
//   想法（thinking）：推理过程，量大、噪声多，只在需要时才该被翻出来。
// 把三者塞进同一个分支，recall 的 top-3 名额会被 thinking 抢走大半，
// 因为 thinking 的文本量是事实层的十倍以上。分成三个分支之后，
// 查询可以用 `recall q --scope p@fact || recall q --scope p@record`
// 表达「先看事实，没有再看记录」——层级变成了查询语言里的东西。
//
// 这个脚本属于 pi 生态，不属于记忆体：zero-memcp 只认 put/commit，
// 它不知道 pi 的 jsonl 长什么样。读私有格式的知识放在这里。
//
// ── 四件容易做错的事 ──────────────────────────────────────────
//
// 1. 会话文件会叠加。压缩会写出一个新文件，里面是旧对话的副本加新增部分。
//    逐文件读会重复导入，只读最新会丢历史。这里按 session id 归并，
//    同一个 id 只取最大的那份快照。
//
// 2. 正在写的文件读不得。jsonl 最后一行可能只写了一半，解析会失败并
//    导入半句。判据不靠文件时间（那要靠猜），而是靠 stopReason：
//    只有 stop 才算说完，toolUse 是中间态，aborted 是用户打断。
//
// 3. 重复导入不能产生重复切片。写盘时间戳每次都不同，文件名也就不同，
//    比文件名判重必然失败。服务端的 dedup 是内存 Map，重启就清空。
//    所以这里维护本地账本，按 endpoint 分区记录已导入的切片指纹。
//
// 4. 过渡句不是记录。"好的"、"我来看下"这类句子单独成篇只会稀释索引。
//    判据要保守：宁可漏掉一句废话，也不要把真实结论当成废话扔掉。
import { createReadStream } from 'node:fs'
import { readFile, writeFile, mkdir, stat, readdir } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { createInterface } from 'node:readline'
import { homedir } from 'node:os'
import { join, resolve, basename } from 'node:path'

const VERSION = '1.0.0'

const SESSION_ID_RE = /_([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$/i

// 过渡句判据：短 + 以常见的起手式开头。两条同时满足才算。
// 只看长度会把"同意"这种真实表态也吃掉；只看前缀会误伤
// "现在我把它改成 X，因为 Y"这类长句。
const TRANSITION_RE = /^(好的|好，|好。|我来|让我|我先|我来看|先看|正在|接下来|现在|我查|我试|稍等|嗯|收到|OK|Okay|Sure|Let me|I'll|Now I)/i

// 记忆标记：扩展会往正文里插 [[mem:...]]，导入时剥掉，
// 否则这些标记会变成索引里的高频噪声。
const MEM_MARK_RE = /^\[\[mem:[a-z]+\]\]\s*/gm

const LAYERS = ['fact', 'record', 'thought']

const stripMem = (s) => s.replace(MEM_MARK_RE, '').trim()

const isTransition = (s) => {
  const t = s.trim()
  if (!t) return true
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

function extractThinking(content) {
  if (!Array.isArray(content)) return ''
  const parts = []
  for (const p of content) {
    if (p && p.type === 'thinking' && typeof p.thinking === 'string' && p.thinking) {
      parts.push(p.thinking)
    }
  }
  return parts.join('\n\n')
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
      // 半行：正在写的文件。跳过而不是猜测补全。
      continue
    }
    yield obj
  }
}

function parseTime(v) {
  if (!v) return null
  const t = new Date(v)
  return Number.isNaN(t.getTime()) ? null : t
}

const pad = (n) => String(n).padStart(2, '0')

// dayStamp 用本地时间：'哪天'对人是本地概念，用 UTC 会让同一天的
// 记录按机器时区裂到两个日期目录里。
function dayStamp(d) {
  if (!d) return 'unknown-date'
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
}

function timeStamp(d) {
  if (!d) return '000000'
  return `${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`
}

// ── 从会话里抽取三层内容 ──────────────────────────────────────

// extractLayers 读一个会话文件，返回 { fact: [], record: [], thought: [] }。
//
// 三层在同一次遍历里抽取：文件动辄十几 MB，走三遍就是把 I/O 也乘三。
async function extractLayers(path, sessionId) {
  const fact = []
  const record = []
  const thought = []

  let ask = ''
  let askAt = null
  let answers = []
  let thinkings = []
  let skipFirstUser = true
  let compactionSeq = 0

  const flushTurn = () => {
    if (!ask && answers.length === 0 && thinkings.length === 0) return
    const answer = answers.join('\n\n').trim()
    const think = thinkings.join('\n\n').trim()

    // 记录层：一问一答。没有实质回答的轮次不算记录。
    if (ask.trim() && answer && !isTransition(answer)) {
      record.push({ ask: ask.trim(), answer, at: askAt })
    }

    // 想法层：这一轮里产生的推理。它挂在同一个问题上，
    // 因为脱离问题单独看一段 thinking，读者不知道它在推演什么。
    if (think && !isTransition(think)) {
      thought.push({ ask: ask.trim(), thinking: think, at: askAt })
    }

    ask = ''
    answers = []
    thinkings = []
    askAt = null
  }

  for await (const line of eachLine(path)) {
    const type = line?.type

    // 事实层：压缩摘要。三层里唯一已经过提炼的内容。
    if (type === 'compaction') {
      const summary = typeof line.summary === 'string' ? line.summary.trim() : ''
      if (summary) {
        compactionSeq++
        fact.push({
          summary,
          at: parseTime(line.timestamp),
          tokensBefore: line.tokensBefore || 0,
          seq: compactionSeq,
        })
      }
      continue
    }

    if (type !== 'message' || !line.message) continue
    const msg = line.message
    const at = parseTime(line.timestamp)

    if (msg.role === 'user') {
      const text = stripMem(extractText(msg.content))
      // 第一条 user 消息常常是压缩注入的摘要块，不是真正的提问。
      if (skipFirstUser) {
        skipFirstUser = false
        if (text.startsWith('The conversation history before this point') || text.startsWith('<')) {
          continue
        }
      }
      flushTurn()
      ask = text
      askAt = at
      continue
    }

    if (msg.role === 'assistant') {
      // 被用户打断的轮次不留回答：那是半句话，
      // 当成结论存进记忆比丢掉更危险。
      if (msg.stopReason === 'aborted') {
        answers = []
        thinkings = []
        continue
      }
      const think = extractThinking(msg.content)
      if (think) thinkings.push(think)
      // 只有 stop 才算说完。toolUse 是中间态，它的 text 是
      // "我接下来要调用某工具"这类过程叙述，不是结论。
      if (msg.stopReason !== 'stop') continue
      const text = stripMem(extractText(msg.content))
      if (text) answers.push(text)
    }
  }
  flushTurn()

  return { fact, record, thought, sessionId }
}

// chunkThinking 把一轮的推理切成若干片。
//
// 为什么必须切：thinking 单轮 p50 6KB、p90 25KB、最大 138KB（实测 55 个
// 会话）。整轮成一篇会撞上两个问题——BM25 的长度归一化会让超长文档在
// 关键词命中时也拿不到好分数；而 recall 返回的摘要是"关键词附近 240 字节"，
// 一篇 138KB 的文档里那 240 字节几乎必然是断章取义。
//
// 切点优先选空行（推理里的自然段落边界），其次换行，最后硬切。
// 每片都带同样的 front matter 和"这是第 i/n 片"标记，读者能看出它是
// 一段连续推理的一部分。
const THOUGHT_CHUNK_BYTES = 4000

function chunkThinking(text) {
  const t = text.trim()
  if (!t) return []
  if (t.length <= THOUGHT_CHUNK_BYTES) return [t]

  const out = []
  let rest = t
  while (rest.length > THOUGHT_CHUNK_BYTES) {
    // 在窗口内找一个自然的断点，从后往前找，尽量填满。
    const window = rest.slice(0, THOUGHT_CHUNK_BYTES)
    let cut = window.lastIndexOf('\n\n')
    if (cut < THOUGHT_CHUNK_BYTES / 2) cut = window.lastIndexOf('\n')
    if (cut < THOUGHT_CHUNK_BYTES / 2) cut = THOUGHT_CHUNK_BYTES
    out.push(rest.slice(0, cut).trim())
    rest = rest.slice(cut).trim()
  }
  if (rest) out.push(rest)
  return out
}

// ── 渲染成 markdown ────────────────────────────────────────────

const frontMatter = (kv) => {
  const lines = ['---']
  for (const [k, v] of Object.entries(kv)) {
    if (v === undefined || v === null || v === '') continue
    lines.push(`${k}: ${v}`)
  }
  lines.push('---', '')
  return lines.join('\n')
}

const renderFact = (f, sessionId) => {
  const head = frontMatter({
    layer: 'fact',
    session: sessionId,
    at: f.at ? f.at.toISOString() : '',
    tokens_before: f.tokensBefore || '',
  })
  return `${head}# Compaction summary\n\n${f.summary}\n`
}

const renderRecord = (t, sessionId) => {
  const head = frontMatter({
    layer: 'record',
    session: sessionId,
    at: t.at ? t.at.toISOString() : '',
  })
  const q = t.ask ? `## User\n\n${t.ask}\n\n` : ''
  return `${head}${q}## Assistant\n\n${t.answer}\n`
}

const renderThought = (t, sessionId, chunk, total) => {
  const head = frontMatter({
    layer: 'thought',
    session: sessionId,
    at: t.at ? t.at.toISOString() : '',
    part: total > 1 ? `${chunk}/${total}` : '',
  })
  const q = t.ask ? `> Asked: ${t.ask.replace(/\n+/g, ' ').slice(0, 300)}\n\n` : ''
  const cont = total > 1 ? `_part ${chunk} of ${total}_\n\n` : ''
  return `${head}# Reasoning\n\n${cont}${q}${t.thinking}\n`
}

// ── 会话归并 ──────────────────────────────────────────────────

async function collectSessions(targets) {
  const byId = new Map()
  let fileCount = 0

  const walk = async (p) => {
    let st
    try {
      st = await stat(p)
    } catch {
      return
    }
    if (st.isDirectory()) {
      for (const e of await readdir(p, { withFileTypes: true })) {
        await walk(join(p, e.name))
      }
      return
    }
    if (!p.endsWith('.jsonl')) return
    fileCount++
    const m = SESSION_ID_RE.exec(basename(p))
    if (!m) return
    const id = m[1].toLowerCase()
    // 同一个 session 的多个快照里取最大的：压缩只会让文件变大。
    const prev = byId.get(id)
    if (!prev || st.size > prev.size) byId.set(id, { id, path: p, size: st.size })
  }

  for (const t of targets) await walk(t)
  return { sessions: [...byId.values()], fileCount }
}

// ── 账本 ──────────────────────────────────────────────────────

const fingerprint = (layer, sessionId, text) =>
  createHash('sha256').update(`${layer}\n${sessionId}\n${text}`, 'utf8').digest('hex').slice(0, 16)

// contentFingerprint 只按内容判重，不掺会话号。
//
// 为什么必须如此：pi 的会话文件是「累积快照」。每次压缩都写一个新文件，
// 里面是旧历史的全量副本加上新增部分。所以同一份 compaction 摘要、
// 同一段问答会出现在多个 session 里——实测 765 篇 fact 里只有 36 篇
// 内容真正唯一（重复率 95.3%），19490 篇 record 里只有 988 篇唯一。
//
// 指纹里带 sessionId 就等于「每条切片在每个会话里都是新的」，跨会话
// 去重完全失效，于是把同一份历史按会话个数复制了十几遍入库。
const contentFingerprint = (layer, text) =>
  createHash('sha256').update(`${layer}\n${text}`, 'utf8').digest('hex').slice(0, 20)

async function loadState(path, endpoint) {
  try {
    const raw = JSON.parse(await readFile(path, 'utf8'))
    const seen = raw?.[endpoint]?.seen
    if (Array.isArray(seen)) return new Set(seen)
  } catch {}
  return new Set()
}

async function saveState(path, endpoint, seen, stats) {
  let all = {}
  try {
    all = JSON.parse(await readFile(path, 'utf8')) || {}
  } catch {}
  all[endpoint] = { seen: [...seen], stats, updatedAt: Date.now() }
  await mkdir(join(path, '..'), { recursive: true })
  await writeFile(path, JSON.stringify(all, null, 2) + '\n', 'utf8')
}

// ── MCP 客户端 ────────────────────────────────────────────────

function makeClient(endpoint, token) {
  const headers = { 'content-type': 'application/json' }
  if (token) headers.Authorization = `Bearer ${token}`

  return async function callTerm(cmd, vars, timeoutMs = 60000) {
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

export {
  extractLayers,
  collectSessions,
  fingerprint,
  contentFingerprint,
  renderFact,
  renderRecord,
  renderThought,
  makeClient,
  loadState,
  saveState,
  dayStamp,
  timeStamp,
  chunkThinking,
  THOUGHT_CHUNK_BYTES,
  LAYERS,
  isTransition,
  stripMem,
  VERSION,
}
