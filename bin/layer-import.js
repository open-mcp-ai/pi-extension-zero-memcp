#!/usr/bin/env node
// zero-memcp-layer-import —— 把 pi 历史会话按认知层级导入 zero-memcp。
//
// 三层各一个分支，一次调用一条链：
//   fact    ← compaction 摘要（已提炼，密度最高）
//   record  ← 一问一答（实际发生过什么）
//   thought ← thinking 推理链（按 4KB 切片）
//
// 用法：
//   zero-memcp-layer-import --endpoint URL --project zero-agi \
//       ~/workdir/zero-agi/record
//
//   # 并发 8，每 2000 条提交一次分支
//   zero-memcp-layer-import --endpoint http://127.0.0.1:1729 --concurrency 8 --commit-every 2000 ...
import { homedir } from 'node:os'
import { join, resolve } from 'node:path'
import {
  collectSessions,
  extractLayers,
  fingerprint,
  contentFingerprint,
  loadState,
  saveState,
  makeClient,
  renderFact,
  renderRecord,
  renderThought,
  chunkThinking,
  dayStamp,
  LAYERS,
  VERSION,
} from './layer-import-lib.js'

function parseArgs(argv) {
  const opts = {
    targets: [],
    endpoint: process.env.ZERO_MEMCP_ENDPOINT || 'http://127.0.0.1:1729',
    token: process.env.ZERO_MEMCP_TOKEN || '',
    project: 'zero-agi',
    branch: 'zero',
    statePath:
      process.env.ZERO_MEMCP_IMPORT_STATE || join(homedir(), '.zero-memcp', 'layer-import-state.json'),
    useState: true,
    dryRun: false,
    limit: 0,
    commitEvery: 2000,
    concurrency: 6,
    layers: LAYERS,
    quiet: false,
    json: false,
  }
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    const next = () => argv[++i]
    switch (a) {
      case '-h':
      case '--help':
        opts.help = true
        break
      case '-V':
      case '--version':
        opts.version = true
        break
      case '--endpoint':
        opts.endpoint = next()
        break
      case '--token':
        opts.token = next()
        break
      case '--project':
        opts.project = next()
        break
      case '--branch':
        opts.branch = next()
        break
      case '--state':
        opts.statePath = next()
        break
      case '--no-state':
        opts.useState = false
        break
      case '--dry-run':
        opts.dryRun = true
        break
      case '--limit':
        opts.limit = Number(next()) || 0
        break
      case '--concurrency':
        opts.concurrency = Math.max(1, Number(next()) || 1)
        break
      case '--commit-every':
        opts.commitEvery = Number(next()) || 0
        break
      case '--layers':
        opts.layers = next()
          .split(',')
          .map((s) => s.trim())
          .filter((s) => LAYERS.includes(s))
        break
      case '--quiet':
        opts.quiet = true
        break
      case '--json':
        opts.json = true
        break
      default:
        if (a.startsWith('-')) throw new Error(`unknown option: ${a}`)
        opts.targets.push(a)
    }
  }
  return opts
}

function usage() {
  return `zero-memcp-layer-import ${VERSION} - import pi session history into zero-memcp by cognitive layer

Three branches, one per layer:
  fact    compaction summaries (already distilled; highest density)
  record  question/answer turns (what actually happened)
  thought thinking chains, chunked at ~4KB (reasoning, high volume)

Query them as a fallback chain so the cheap layer answers first:
  recall q --scope <project>@fact || recall q --scope <project>@record || recall q --scope <project>@thought

Usage:
  zero-memcp-layer-import [options] [path...]

Paths may be .jsonl files or directories (scanned recursively).
Default: ~/.pi/agent/sessions

Options:
  --endpoint URL      zero-memcp address (default http://127.0.0.1:1729)
  --token JWT         credential (required when the server enforces auth)
  --project NAME      target project (default zero-agi)
  --branch NAME       base branch name (default zero)
  --layers a,b,c      import only these layers (default fact,record,thought)
  --limit N           stop after N sessions (debugging)
  --concurrency N     parallel writes in flight (default 6)
  --commit-every N    commit every N writes (default 2000; 0 = only at the end)
  --dry-run           report what would be written, write nothing
  --state FILE        dedup ledger (default ~/.zero-memcp/layer-import-state.json)
  --no-state          ignore the ledger (relies on server-side dedup)
  --json              machine-readable summary
  --quiet             no per-session progress
  -h, --help          this help
  -V, --version       version
`
}

function targetAddr(project, layer, day) {
  return `${project}@${layer}/${day}`
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
  if (!opts.layers.length) {
    console.error(`--layers must name at least one of: ${LAYERS.join(', ')}`)
    return 2
  }

  const targets = opts.targets.length
    ? opts.targets.map((p) => resolve(p.replace(/^~(?=$|\/)/, homedir())))
    : [join(homedir(), '.pi', 'agent', 'sessions')]

  const { sessions, fileCount } = await collectSessions(targets)
  if (!sessions.length) {
    console.error(`no .jsonl session files found in: ${targets.join(', ')}`)
    return 1
  }

  const seen = opts.useState ? await loadState(opts.statePath, opts.endpoint) : new Set()
  const callTerm = makeClient(opts.endpoint, opts.token)

  const result = {
    endpoint: opts.endpoint,
    project: opts.project,
    files: fileCount,
    sessions: sessions.length,
    fact: 0,
    record: 0,
    thought: 0,
    thoughtChunks: 0,
    written: 0,
    present: 0,
    skipped: 0,
    committed: 0,
    errors: [],
  }

  let pending = 0
  const written = new Set(seen)

  const commitAll = async () => {
    if (opts.dryRun) return
    for (const layer of opts.layers) {
      try {
        await callTerm(`commit ${opts.project}@${layer} -m "import: ${layer}"`, null, 180000)
        result.committed++
      } catch (err) {
        result.errors.push(`commit ${layer}: ${err.message}`)
      }
    }
    pending = 0
  }

  const putOne = async (target, text, session) => {
    if (opts.dryRun) return
    const s = session ? `@${session.slice(0, 8)} ` : ''
    await callTerm(`put ${s}${target} $BODY`, { BODY: text }, 120000)
  }

  // 并发写工作池
  const runParallel = async (jobs) => {
    let nextIdx = 0
    const worker = async () => {
      while (nextIdx < jobs.length) {
        const j = jobs[nextIdx++]
        if (written.has(j.fp)) {
          result.present++
          continue
        }
        try {
          await putOne(j.target, j.text, j.sessionId)
          written.add(j.fp)
          result.written++
          pending++
          if (opts.commitEvery > 0 && pending >= opts.commitEvery) {
            await commitAll()
          }
        } catch (err) {
          result.errors.push(`${j.sessionId} ${j.layer}: ${err.message}`)
        }
      }
    }
    const workers = Array.from({ length: opts.concurrency }, worker)
    await Promise.all(workers)
  }

  let sessionCount = 0
  for (const s of sessions) {
    if (opts.limit && ++sessionCount > opts.limit) break

    let L
    try {
      L = await extractLayers(s.path, s.id)
    } catch (err) {
      result.errors.push(`${s.id}: ${err.message}`)
      continue
    }

    result.fact += L.fact.length
    result.record += L.record.length
    result.thought += L.thought.length
    result.thoughtChunks += L.thought.reduce((n, t) => n + chunkThinking(t.thinking).length, 0)

    const jobs = []

    if (opts.layers.includes('fact')) {
      for (const f of L.fact) {
        const day = dayStamp(f.at)
        jobs.push({
          layer: 'fact',
          target: targetAddr(opts.project, 'fact', day),
          text: renderFact(f, s.id),
          fp: contentFingerprint('fact', f.summary),
          sessionId: s.id,
        })
      }
    }

    if (opts.layers.includes('record')) {
      for (const r of L.record) {
        const day = dayStamp(r.at)
        jobs.push({
          layer: 'record',
          target: targetAddr(opts.project, 'record', day),
          text: renderRecord(r, s.id),
          fp: contentFingerprint('record', r.ask + '\n' + r.answer),
          sessionId: s.id,
        })
      }
    }

    if (opts.layers.includes('thought')) {
      for (const t of L.thought) {
        const chunks = chunkThinking(t.thinking)
        chunks.forEach((chunk, i) => {
          const day = dayStamp(t.at)
          jobs.push({
            layer: 'thought',
            target: targetAddr(opts.project, 'thought', day),
            text: renderThought({ ...t, thinking: chunk }, s.id, i + 1, chunks.length),
            fp: contentFingerprint('thought', chunk),
            sessionId: s.id,
          })
        })
      }
    }

    await runParallel(jobs)

    if (!opts.quiet) {
      process.stderr.write(
        `  ${s.id.slice(0, 8)} fact=${L.fact.length} record=${L.record.length} thought=${L.thought.length} written=${result.written} present=${result.present}\n`,
      )
    }

    // 每会话存一次账本：即使中途断电，重启后秒级跳过已写切片
    if (opts.useState && !opts.dryRun && sessionCount % 5 === 0) {
      try {
        await saveState(opts.statePath, opts.endpoint, written, {
          written: result.written,
          present: result.present,
        })
      } catch {}
    }
  }

  if (pending > 0) await commitAll()

  if (opts.useState && !opts.dryRun) {
    try {
      await saveState(opts.statePath, opts.endpoint, written, {
        fact: result.fact,
        record: result.record,
        thought: result.thought,
        thoughtChunks: result.thoughtChunks,
        written: result.written,
        present: result.present,
      })
    } catch (err) {
      result.errors.push(`save ledger: ${err.message}`)
    }
  }

  if (opts.json) {
    console.log(JSON.stringify(result, null, 2))
  } else {
    const verb = opts.dryRun ? 'would write' : 'wrote'
    console.log(`scanned ${result.files} files -> ${result.sessions} sessions`)
    console.log(
      `extracted: fact=${result.fact} record=${result.record} ` +
        `thought=${result.thought} turns -> ${result.thoughtChunks} chunks (at ~4KB each)`,
    )
    console.log(`${verb} ${result.written}, already present ${result.present}`)
    if (result.committed) console.log(`committed ${result.committed} branch-commits`)
    if (result.errors.length) {
      console.log(`errors: ${result.errors.length}`)
      for (const e of result.errors.slice(0, 20)) console.log(`  ! ${e}`)
      if (result.errors.length > 20) console.log(`  ... ${result.errors.length - 20} more`)
    }
  }
  return result.errors.length && !result.written ? 1 : 0
}

main()
  .then((code) => process.exit(code))
  .catch((err) => {
    console.error(`zero-memcp-layer-import: ${err.message}`)
    process.exit(1)
  })
