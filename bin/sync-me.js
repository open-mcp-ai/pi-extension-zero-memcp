#!/usr/bin/env node
// zero-memcp-sync-me —— 把 BlackEggAI/me 规划分层导入 zero-memcp。
//
// 分层规则：
//   me 分支（默认）：
//     - 根 README.md, SKILL.md
//     - skills/01-me/*（自我形状、工作方法、禁忌）
//     - skills/02-law/*（硬性约束，违背即出局）
//     保留原有的 Markdown 路径与 frontmatter（inject: every_turn 会被
//     zero-memcp 识别并常驻每轮上下文）。
//
//   ideas 分支：
//     - ideas/*（想过的点子，带 inject: never，需要时才通过 recall 翻出）
//
// 用 write 而不用 put：这里是规则库，路径必须精确稳定（skills/01-me/...），
// 不能带自动生成的日期时间戳。write 是 root 专用的精确写入动词。
import { readFile, readdir, stat } from 'node:fs/promises'
import { join, resolve, relative } from 'node:path'
import { homedir } from 'node:os'
import { makeClient } from './layer-import-lib.js'

function parseArgs(argv) {
  const opts = {
    source: resolve(homedir(), 'workdir', 'BlackEggAI', 'me'),
    endpoint: process.env.ZERO_MEMCP_ENDPOINT || 'http://127.0.0.1:1729',
    token: process.env.ZERO_MEMCP_TOKEN || '',
    project: 'blackegg/me',
    dryRun: false,
  }
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    const next = () => argv[++i]
    switch (a) {
      case '--source':
        opts.source = resolve(next().replace(/^~(?=$|\/)/, homedir()))
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
      case '--dry-run':
        opts.dryRun = true
        break
    }
  }
  return opts
}

async function collectFiles(dir) {
  const out = []
  const walk = async (p) => {
    for (const e of await readdir(p, { withFileTypes: true })) {
      const full = join(p, e.name)
      if (e.name.startsWith('.')) continue
      if (e.isDirectory()) await walk(full)
      else if (e.isFile() && e.name.endsWith('.md')) out.push(full)
    }
  }
  await walk(dir)
  return out
}

async function main() {
  const opts = parseArgs(process.argv.slice(2))
  const files = await collectFiles(opts.source)
  if (!files.length) {
    console.error(`no markdown files in: ${opts.source}`)
    return 1
  }

  const callTerm = makeClient(opts.endpoint, opts.token)

  let wroteMe = 0
  let wroteIdeas = 0

  for (const f of files) {
    const rel = relative(opts.source, f)
    const branch = rel.startsWith('ideas/') ? 'ideas' : 'me'
    const addr = `${opts.project}@${branch}/${rel}`
    const text = await readFile(f, 'utf8')

    if (opts.dryRun) {
      console.log(`[dry-run] ${addr} (${text.length} bytes)`)
      if (branch === 'ideas') wroteIdeas++
      else wroteMe++
      continue
    }

    await callTerm(`write ${addr} -- $BODY`, { BODY: text }, 60000)
    console.log(`wrote ${addr}`)
    if (branch === 'ideas') wroteIdeas++
    else wroteMe++
  }

  if (!opts.dryRun) {
    await callTerm(`commit ${opts.project}@me -m "sync me skills"`, null, 60000)
    await callTerm(`commit ${opts.project}@ideas -m "sync ideas"`, null, 60000)
    console.log(`committed ${opts.project}@me and ${opts.project}@ideas`)
  }

  console.log(`done: me branch=${wroteMe} files, ideas branch=${wroteIdeas} files`)
}

main().catch((err) => {
  console.error(err.message)
  process.exit(1)
})
