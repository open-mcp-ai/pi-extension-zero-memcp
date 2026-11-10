#!/usr/bin/env node
// zero-memcp-doctor —— 一次性体检：记忆到底有没有在工作。
//
// 为什么需要它：插件对失败是静默降级的（联想超时就走空，写入被拒也不拦对话），
// 这对正在干活的会话是对的，但对刚装好的人来说等于「配了半天，什么都看不出来」。
// 这个命令把每一环单独验证一遍并打印证据，让「没生效」变成一个具体的原因。
//
// 它只读：不写记忆、不改配置、不动水位（唯一例外是 --probe-write 显式要求时，
// 会往一个诊断专用路径写一条极短的内容）。
import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { homedir } from 'node:os'
import {
  normEndpoint,
  probeHealth,
  probeTerm,
  probeRecall,
  parseWhoami,
  scopeCovered,
} from './probe-lib.js'

const CONFIG_FILE = process.env.ZERO_MEMCP_CONFIG || join(homedir(), '.zero-memcp', 'config.json')
const CLOCK_FILE = process.env.ZERO_MEMCP_CLOCK || join(homedir(), '.zero-memcp', 'clock.json')
const WM_DIR = process.env.ZERO_MEMCP_WM_DIR || join(homedir(), '.zero-memcp', 'wm')

const OK = '✓'
const BAD = '✗'
const WARN = '!'

function parseArgs(argv) {
  const opts = { config: CONFIG_FILE, probeWrite: false, text: '记忆服务器', json: false, help: false }
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    const next = () => argv[++i]
    switch (a) {
      case '--config':
      case '-c':
        opts.config = next()
        break
      case '--probe-write':
        opts.probeWrite = true
        break
      case '--text':
        opts.text = next()
        break
      case '--json':
        opts.json = true
        break
      case '--help':
      case '-h':
        opts.help = true
        break
    }
  }
  return opts
}

/** 从配置里取出本会话实际会用的 token（与插件同一条优先级）。 */
export function resolveToken(cfg) {
  const name = cfg.identityName || ''
  if (name && cfg.tokens && cfg.tokens[name]) return cfg.tokens[name]
  return cfg.token || ''
}

/** 收集配置里声明的所有 scope，标注用途与读写需求。 */
export function declaredScopes(cfg) {
  const out = []
  const push = (scope, label, need) => {
    if (scope) out.push({ scope, label, need })
  }
  const persona = (cfg.personas && cfg.identityName && cfg.personas[cfg.identityName]) || null
  const src = persona || cfg

  push(src.recordScope, 'recordScope（问答落点）', 'w')
  push(src.factScope, 'factScope（摘要落点）', 'w')
  push(src.thoughtScope, 'thoughtScope（思维链落点）', 'w')
  const scopes = Array.isArray(src.recallScopes) ? src.recallScopes : []
  for (const s of scopes) push(s, 'recallScopes（召回链）', 'r')
  return out
}

async function readJson(path) {
  try {
    return JSON.parse(await readFile(path, 'utf8'))
  } catch {
    return null
  }
}

async function main() {
  const opts = parseArgs(process.argv.slice(2))
  if (opts.help) {
    console.log(`zero-memcp-doctor —— 诊断 zero-memcp 记忆链路

用法:
  zero-memcp-doctor [选项]

选项:
  -c, --config <path>   配置文件路径（默认 ~/.zero-memcp/config.json）
      --text <text>     联想探针用的话题（默认「记忆服务器」）
      --probe-write     额外验证写路径（会写一条诊断内容到 recordScope）
      --json            输出机器可读结果
  -h, --help            查看帮助

检查项:
  1. 配置文件是否存在、能否解析
  2. 服务端是否在线（/health）
  3. 凭证是否有效、身份与权限是什么（whoami）
  4. 配置声明的 scope 是否被凭证挂载表覆盖（越权写入会静默失败）
  5. 读路径端到端是否可用（/recall）
  6. 本地状态：逻辑时钟、会话水位

退出码: 0 全部通过；1 有致命项；2 只有警告
`)
    process.exit(0)
  }

  const report = { configPath: opts.config, checks: [] }
  const add = (level, name, detail, hint) => {
    report.checks.push({ level, name, detail, hint })
    if (!opts.json) {
      const mark = level === 'ok' ? OK : level === 'bad' ? BAD : level === 'warn' ? WARN : '·'
      console.log(`${mark} ${name}${detail ? ': ' + detail : ''}`)
      if (hint) console.log(`    → ${hint}`)
    }
  }
  const section = (t) => {
    if (!opts.json) console.log(`\n${t}`)
  }

  if (!opts.json) console.log('zero-memcp 记忆链路体检\n' + '='.repeat(40))

  // ── 1. 配置 ──
  section('[1] 配置')
  const cfg = await readJson(opts.config)
  if (!cfg) {
    add('bad', '配置文件', `读不到或不是合法 JSON: ${opts.config}`, '先运行 zero-memcp-init 生成配置')
    finish(report, opts)
    return
  }
  add('ok', '配置文件', opts.config)

  const endpoint = normEndpoint(cfg.endpoint || '')
  if (!endpoint) {
    add('bad', 'endpoint', '未配置', '在配置里填 endpoint，例如 http://127.0.0.1:1729')
    finish(report, opts)
    return
  }
  add('ok', 'endpoint', endpoint)
  add(cfg.enabled === false ? 'warn' : 'ok', 'enabled', String(cfg.enabled !== false))

  const token = resolveToken(cfg)
  add(token ? 'ok' : 'warn', '凭证', token ? `已配置（${token.slice(0, 10)}…，身份名 ${cfg.identityName || '(空)'}）` : '未配置 token：私有仓库的读写都会 401')

  // ── 2. 服务端 ──
  section('[2] 服务端')
  const health = await probeHealth(endpoint)
  if (!health.ok) {
    add('bad', '/health', health.error, `服务端没起来，或地址不可达。试: curl ${endpoint}/health`)
    finish(report, opts)
    return
  }
  add('ok', '/health', `${health.data.service || 'zero-memcp'} v${health.data.version || '?'}，逻辑时钟 ${health.data.clock ?? '?'}`)

  // ── 3. 身份 ──
  section('[3] 凭证身份')
  const who = await probeTerm(endpoint, token, 'whoami')
  let mounts = []
  if (!who.ok) {
    add('bad', 'whoami', who.error, 'token 可能过期。让管理员重新签发，或检查是否配错了 tokens 表里的键')
  } else {
    const parsed = parseWhoami(who.text)
    mounts = parsed.mounts
    add('ok', 'whoami', `${parsed.user}｜${parsed.privilege}`)
    if (parsed.unrestricted) {
      add('warn', '挂载表', '不受限的 root 凭证', '生产环境建议改用收窄凭证，避免误写别处')
    } else if (mounts.length) {
      for (const m of mounts) {
        add('ok', '挂载', `${m.project}@${m.branch || '(默认)'}${m.path ? ':' + m.path : ''} 权限 ${m.cap || '-'}`)
      }
    } else {
      add('warn', '挂载表', '解析不出挂载项', '用 whoami 原文核对格式是否变化')
    }
  }

  // ── 4. scope 覆盖 ──
  section('[4] 配置的 scope 是否被凭证覆盖')
  const scopes = declaredScopes(cfg)
  if (!scopes.length) {
    add('warn', 'scope', '配置里没有声明任何 scope', '插件会走服务端默认落点；想分层读写请配 recordScope/factScope/thoughtScope/recallScopes')
  } else if (!mounts.length) {
    add('warn', 'scope', '没有挂载表可比对（凭证可能是 root 或未取到身份）', '跳过此项')
  } else {
    for (const s of scopes) {
      const r = scopeCovered(mounts, s.scope, s.need)
      if (r.ok) {
        add('ok', s.label, s.scope)
      } else {
        add(
          'bad',
          s.label,
          `${s.scope} —— ${r.reason}`,
          s.need === 'w'
            ? '写入会被服务端拒绝且静默失败。要么改 scope，要么让管理员把该分支加入凭证挂载表'
            : '检索这一层会被拒，回落链继续往下找'
        )
      }
    }
  }

  // ── 5. 读路径 ──
  section('[5] 读路径（recall）')
  const rec = await probeRecall(endpoint, token, opts.text, cfg.recallLimit || 3)
  if (!rec.ok) {
    add('bad', '/recall', rec.error, '服务端可达但检索失败：看服务端日志，确认索引是否已建')
  } else if (!rec.hits.length) {
    add('warn', 'recall', `「${opts.text}」没有命中`, '库里可能还没有相关内容，或索引尚未建立（试 status 看 docs 数）')
  } else {
    add('ok', 'recall', `「${opts.text}」命中 ${rec.hits.length} 条`)
    for (const h of rec.hits.slice(0, 3)) {
      const addr = h.address || h.addr || '?'
      const score = typeof h.score === 'number' ? h.score.toFixed(2) : '?'
      add('·', '  命中', `${addr}（${score}）`)
    }
  }

  // ── 6. 本地状态 ──
  section('[6] 本地状态')
  const clock = await readJson(CLOCK_FILE)
  if (clock && typeof clock.docs === 'number') {
    // clock.json 是全局的，不区分 endpoint：换过服务端（比如从本机切到 hub）
    // 时它记的是**另一个实例**的文档数。此时比对毫无意义，而且差值为负
    // 会读成「服务端比本地少」，把人引向错误方向。
    // 只在服务端确实报告了时钟、且不倒退时才比对。
    const serverClock = typeof health.data.clock === 'number' ? health.data.clock : null
    const comparable = serverClock !== null && serverClock > 0 && serverClock >= clock.docs
    if (comparable) {
      const lag = serverClock - clock.docs
      add('ok', '逻辑时钟', `本地记到 ${clock.docs}，服务端 ${serverClock}（差 ${lag}）`)
    } else {
      add(
        '·',
        '逻辑时钟',
        `本地记到 ${clock.docs}；服务端报 ${serverClock ?? '?'}，不可比`, 
        'clock.json 不区分 endpoint，切换服务端后这个数字会失真；不影响功能'
      )
    }
  } else {
    add('·', '逻辑时钟', '还没有记录（插件首次连上后会自动写）')
  }

  let wmCount = 0
  try {
    const { readdir } = await import('node:fs/promises')
    wmCount = (await readdir(WM_DIR)).filter((f) => f.endsWith('.json')).length
  } catch {}
  add(wmCount ? 'ok' : '·', '会话水位', `${wmCount} 个文件（${WM_DIR}）`)

  // ── 可选：写路径 ──
  if (opts.probeWrite) {
    section('[7] 写路径（--probe-write）')
    const w = scopes.find((s) => s.need === 'w')
    if (!w) {
      add('warn', '写探针', '没有声明写 scope，跳过', '先配 recordScope')
    } else {
      const stamp = new Date().toISOString()
      const target = `${w.scope}/doctor-probe.md`
      const res = await probeTerm(endpoint, token, `write ${target} --data "doctor probe ${stamp}"`, 8000)
      if (res.ok) {
        add('ok', '写入成功', target)
      } else {
        add('bad', '写入失败', res.error, '这就是「记忆没记下来」的真实原因；对照第 4 节看权限')
      }
    }
  }

  finish(report, opts)
}

function finish(report, opts) {
  const bad = report.checks.filter((c) => c.level === 'bad').length
  const warn = report.checks.filter((c) => c.level === 'warn').length

  if (opts.json) {
    console.log(JSON.stringify({ ...report, summary: { bad, warn } }, null, 2))
  } else {
    console.log('\n' + '='.repeat(40))
    if (bad === 0 && warn === 0) {
      console.log(`${OK} 全部通过：记忆链路可用`)
    } else if (bad === 0) {
      console.log(`${WARN} 可用，但有 ${warn} 项需要注意`)
    } else {
      console.log(`${BAD} ${bad} 项致命问题${warn ? `，${warn} 项警告` : ''}——见上面每条的 → 提示`)
    }
  }
  process.exit(bad > 0 ? 1 : warn > 0 ? 2 : 0)
}

if (process.argv[1] && import.meta.url.endsWith(process.argv[1].split('/').pop())) {
  main().catch((err) => {
    console.error('体检异常:', err)
    process.exit(1)
  })
}
