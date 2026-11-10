#!/usr/bin/env node
// zero-memcp-init —— 配置向导：为 pi-extension-zero-memcp 生成并检验配置。
//
// 模式：
//   1. 交互式（默认在 TTY 下）：探测、确认、校验、写盘
//   2. 非交互式（--yes 或非 TTY）：适合脚本与容器自动化
//
// 所有的探测复用 probe-lib.js，不自建 fetch 逻辑。
import { readFile, writeFile, mkdir } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { homedir } from 'node:os'
import { createInterface } from 'node:readline/promises'
import {
  normEndpoint,
  probeHealth,
  probeTerm,
  parseWhoami,
} from './probe-lib.js'

const DEFAULT_CONFIG_PATH = process.env.ZERO_MEMCP_CONFIG || join(homedir(), '.zero-memcp', 'config.json')

export function parseArgs(argv) {
  const opts = {
    endpoint: '',
    token: '',
    identity: '',
    config: DEFAULT_CONFIG_PATH,
    yes: false,
    help: false,
  }
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    const next = () => argv[++i]
    switch (a) {
      case '--endpoint':
      case '-e':
        opts.endpoint = next()
        break
      case '--token':
      case '-t':
        opts.token = next()
        break
      case '--identity':
      case '-i':
        opts.identity = next()
        break
      case '--config':
      case '-c':
        opts.config = resolve(next().replace(/^~(?=$|\/)/, homedir()))
        break
      case '--yes':
      case '-y':
        opts.yes = true
        break
      case '--help':
      case '-h':
        opts.help = true
        break
    }
  }
  return opts
}

export function buildConfig({ endpoint, token, identity, existing = {} }) {
  const ep = normEndpoint(endpoint || existing.endpoint || 'http://127.0.0.1:1729')
  const tok = token !== undefined ? token : (existing.token || '')
  const idName = identity || existing.identityName || (tok ? 'default' : '')
  const idMode = idName ? 'auto' : (existing.identity || 'off')

  const cfg = {
    endpoint: ep,
    enabled: true,
    importMode: existing.importMode || 'off',
    syncSessionOnStart: existing.syncSessionOnStart ?? false,
    recallLimit: existing.recallLimit || 3,
    recallTimeoutMs: existing.recallTimeoutMs || 200,
    recallLayerTimeoutMs: existing.recallLayerTimeoutMs || 2000,
    identity: idMode,
    identityName: idName,
    identityTtlMs: existing.identityTtlMs || 3600000,
    token: tok,
    tokens: { ...existing.tokens },
    watch: existing.watch || '',
    recallScopes: existing.recallScopes || [],
    recallScopeLimit: existing.recallScopeLimit || 3,
    recordScope: existing.recordScope || '',
    factScope: existing.factScope || '',
    thoughtScope: existing.thoughtScope || '',
    personas: { ...existing.personas },
    wake: existing.wake || { mode: 'off', cooldownMs: 60000, maxChain: 0 },
  }

  if (idName && tok) {
    cfg.tokens[idName] = tok
  }
  return cfg
}

async function main() {
  const opts = parseArgs(process.argv.slice(2))
  if (opts.help) {
    console.log(`zero-memcp-init —— 配置向导

用法:
  zero-memcp-init [选项]

选项:
  -e, --endpoint <url>   zero-memcp 地址（默认 http://127.0.0.1:1729）
  -t, --token <token>    JWT 访问凭证
  -i, --identity <name>  分身标识名（如 laptop / qq / work）
  -c, --config <path>    目标配置文件路径
  -y, --yes              非交互模式，遇到未指定项使用默认值
  -h, --help             查看帮助

示例:
  # 本机快速接入（连 127.0.0.1:1729）
  zero-memcp-init

  # 接入 3060 Hub（带凭证与分身）
  zero-memcp-init -e http://10.10.10.12:1729 -t eyJhbGci... -i laptop -y
`)
    process.exit(0)
  }

  let existing = {}
  try {
    existing = JSON.parse(await readFile(opts.config, 'utf8'))
  } catch {}

  let endpoint = opts.endpoint
  let token = opts.token
  let identity = opts.identity

  if (!opts.yes && process.stdin.isTTY) {
    const rl = createInterface({ input: process.stdin, output: process.stdout })
    const ask = async (q, dflt) => {
      const ans = (await rl.question(`${q}${dflt ? ` [${dflt}]` : ''}: `)).trim()
      return ans || dflt || ''
    }

    try {
      endpoint = await ask('zero-memcp 服务端地址', endpoint || existing.endpoint || 'http://127.0.0.1:1729')
      console.log(`正在探测 ${normEndpoint(endpoint)}/health ...`)
      const hp = await probeHealth(endpoint)
      if (hp.ok) {
        console.log(`✓ 服务端响应正常: ${hp.data.service || 'zero-memcp'} (clock=${hp.data.clock})`)
      } else {
        console.log(`! 警告: 无法连通服务端 (${hp.error})，后续可检查服务是否启动`)
      }

      token = await ask('JWT 凭证 (token，留空表示匿名访问)', token || existing.token || '')
      if (token) {
        console.log(`正在校验凭证身份 ...`)
        const wp = await probeTerm(endpoint, token, 'whoami')
        if (wp.ok) {
          const parsed = parseWhoami(wp.text)
          console.log(`✓ 凭证有效: ${parsed.user}｜${parsed.privilege}`)
          for (const m of parsed.mounts) {
            console.log(`   - 挂载 ${m.project}@${m.branch || '(默认)'} (${m.cap || '-'})`)
          }
        } else {
          console.log(`! 校验失败: ${wp.error}`)
        }
      }

      identity = await ask('分身标识名 (用于区分当前机器/场景)', identity || existing.identityName || (token ? 'laptop' : ''))
    } finally {
      rl.close()
    }
  } else {
    endpoint = endpoint || existing.endpoint || 'http://127.0.0.1:1729'
    token = token !== undefined ? token : (existing.token || '')
    identity = identity || existing.identityName || ''
  }

  const finalCfg = buildConfig({ endpoint, token, identity, existing })
  const dir = opts.config.slice(0, Math.max(0, opts.config.lastIndexOf('/')))
  if (dir) await mkdir(dir, { recursive: true })
  await writeFile(opts.config, JSON.stringify(finalCfg, null, 2) + '\n', 'utf8')

  console.log(`\n✓ 配置已写入 ${opts.config}`)
  console.log(`  - 服务端: ${finalCfg.endpoint}`)
  console.log(`  - 分身:   ${finalCfg.identityName || '(未命名)'}`)
  console.log(`  - 凭证:   ${finalCfg.token ? '已配置 (' + finalCfg.token.slice(0, 10) + '...)' : '(无)'}`)
  console.log(`\n下一步:`)
  console.log(`  1. 运行 npx zero-memcp-doctor 进行端到端体检`)
  console.log(`  2. 在 pi 会话中即可使用长期记忆`)
}

if (process.argv[1] && import.meta.url.endsWith(process.argv[1].split('/').pop())) {
  main().catch((err) => {
    console.error('初始化失败:', err)
    process.exit(1)
  })
}
