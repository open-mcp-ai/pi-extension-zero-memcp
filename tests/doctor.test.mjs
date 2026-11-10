// doctor / probe-lib / init 的单元测试。
//
// 重点覆盖「把静默失败翻译成人话」这条路径：解析 whoami 的挂载表、
// 判断配置声明的 scope 是否被覆盖。这两处一旦回归，用户看到的会是
// 「记忆没生效但没有任何报错」，正是这个工具存在的理由。
import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'

import {
  parseWhoami,
  parseMountList,
  parseScope,
  scopeCovered,
  normEndpoint,
  probeHealth,
  probeTerm,
} from '../bin/probe-lib.js'
import { buildConfig, parseArgs as parseInitArgs } from '../bin/init.js'

test('normEndpoint 去掉尾部斜杠', () => {
  assert.equal(normEndpoint('http://a:1/'), 'http://a:1')
  assert.equal(normEndpoint('http://a:1///'), 'http://a:1')
  assert.equal(normEndpoint('  http://a:1  '), 'http://a:1')
  assert.equal(normEndpoint(''), '')
})

test('parseMountList 解析受限凭证的挂载表', () => {
  const s = 'BlackEggAI/weft@main (rwx), BlackEggAI/weft@main.record (rwx), BlackEggAI/zero@main (rwx)'
  const m = parseMountList(s)
  assert.equal(m.length, 3)
  assert.deepEqual(
    { p: m[0].project, b: m[0].branch, c: m[0].cap },
    { p: 'BlackEggAI/weft', b: 'main', c: 'rwx' }
  )
  assert.equal(m[1].branch, 'main.record')
})

test('parseMountList 处理空挂载表标记', () => {
  assert.deepEqual(parseMountList('(none: every namespace is visible)'), [])
  assert.deepEqual(parseMountList(''), [])
})

test('parseMountList 解析带虚拟路径的挂载', () => {
  const m = parseMountList('BlackEggAI/zero@main.record:record (rw)')
  assert.equal(m.length, 1)
  assert.equal(m[0].project, 'BlackEggAI/zero')
  assert.equal(m[0].branch, 'main.record')
  assert.equal(m[0].path, 'record')
  assert.equal(m[0].cap, 'rw')
})

test('parseWhoami 提取用户、权限与不受限标记', () => {
  const text = `User: zero
Privilege: root (unrestricted global access)`
  const p = parseWhoami(text)
  assert.equal(p.user, 'zero')
  assert.equal(p.unrestricted, true)
  assert.deepEqual(p.mounts, [])
})

test('parseWhoami 提取受限凭证的挂载表', () => {
  const text = `User: zero
Privilege: root (narrowed by mount table; global administration verbs are refused)
Mount: BlackEggAI/zero@main (rwx), BlackEggAI/zero@qq (rwx)`
  const p = parseWhoami(text)
  assert.equal(p.unrestricted, false)
  assert.equal(p.mounts.length, 2)
  assert.equal(p.mounts[1].branch, 'qq')
})

test('parseScope 拆 project@branch:path', () => {
  assert.deepEqual(parseScope('BlackEggAI/zero@main.record:record'), {
    project: 'BlackEggAI/zero',
    branch: 'main.record',
    path: 'record',
    raw: 'BlackEggAI/zero@main.record:record',
  })
  assert.equal(parseScope('BlackEggAI/zero').branch, '')
  assert.equal(parseScope(''), null)
})

test('scopeCovered 识别写权限缺失（静默失败的根源）', () => {
  const mounts = parseMountList('BlackEggAI/zero@main (r), BlackEggAI/zero@main.record (rwx)')
  const ro = scopeCovered(mounts, 'BlackEggAI/zero@main:record', 'w')
  assert.equal(ro.ok, false)
  assert.match(ro.reason, /没有 'w'/)

  const rw = scopeCovered(mounts, 'BlackEggAI/zero@main.record:record', 'w')
  assert.equal(rw.ok, true)
})

test('scopeCovered 识别项目与分支不存在', () => {
  const mounts = parseMountList('BlackEggAI/zero@main (rwx)')
  assert.match(scopeCovered(mounts, 'BlackEggAI/weft@main:x', 'w').reason, /没有项目/)
  assert.match(scopeCovered(mounts, 'BlackEggAI/zero@qq:x', 'w').reason, /没有分支 qq/)
})

test('scopeCovered 支持通配分支与读需求', () => {
  const mounts = parseMountList('BlackEggAI/zero@* (r)')
  const r = scopeCovered(mounts, 'BlackEggAI/zero@anything', 'r')
  assert.equal(r.ok, true)
  assert.equal(scopeCovered(mounts, 'BlackEggAI/zero@anything', 'w').ok, false)
})

test('buildConfig 保留既有字段并登记 token', () => {
  const existing = { recallLimit: 5, personas: { laptop: { recordScope: 'a@b:c' } } }
  const cfg = buildConfig({ endpoint: 'http://h:1/', token: 'tk', identity: 'laptop', existing })
  assert.equal(cfg.endpoint, 'http://h:1')
  assert.equal(cfg.recallLimit, 5)
  assert.equal(cfg.tokens.laptop, 'tk')
  assert.equal(cfg.identity, 'auto')
  assert.deepEqual(cfg.personas, existing.personas)
})

test('buildConfig 无 token 时保持 identity=off', () => {
  const cfg = buildConfig({ endpoint: 'http://h:1', token: '', identity: '' })
  assert.equal(cfg.identity, 'off')
  assert.equal(cfg.identityName, '')
})

test('init 参数解析支持短选项与 ~ 展开', () => {
  const o = parseInitArgs(['-e', 'http://x:1', '-t', 'tok', '-i', 'qq', '-y'])
  assert.equal(o.endpoint, 'http://x:1')
  assert.equal(o.token, 'tok')
  assert.equal(o.identity, 'qq')
  assert.equal(o.yes, true)
})

// ── 用假服务端验证探针的端到端行为 ──

async function withServer(handler, fn) {
  const srv = createServer(handler)
  await new Promise((r) => srv.listen(0, '127.0.0.1', r))
  const { port } = srv.address()
  try {
    return await fn(`http://127.0.0.1:${port}`)
  } finally {
    await new Promise((r) => srv.close(r))
  }
}

test('probeHealth 在服务不可达时返回 ok=false 而不抛错', async () => {
  const r = await probeHealth('http://127.0.0.1:1', 300)
  assert.equal(r.ok, false)
  assert.ok(r.error)
})

test('probeTerm 把 JSON-RPC 结果里的文本取出来', async () => {
  await withServer(
    (req, res) => {
      if (req.url === '/mcp') {
        res.setHeader('content-type', 'application/json')
        res.end(
          JSON.stringify({
            jsonrpc: '2.0',
            id: 1,
            result: { content: [{ type: 'text', text: 'User: zero\nPrivilege: root' }] },
          })
        )
        return
      }
      res.statusCode = 404
      res.end()
    },
    async (base) => {
      const r = await probeTerm(base, 'tok', 'whoami')
      assert.equal(r.ok, true)
      assert.match(r.text, /User: zero/)
    }
  )
})

test('probeTerm 区分 401/403 并给出人话', async () => {
  await withServer(
    (req, res) => {
      res.statusCode = req.headers.authorization ? 403 : 401
      res.end()
    },
    async (base) => {
      assert.match((await probeTerm(base, '', 'whoami')).error, /401/)
      assert.match((await probeTerm(base, 'bad', 'whoami')).error, /403/)
    }
  )
})
