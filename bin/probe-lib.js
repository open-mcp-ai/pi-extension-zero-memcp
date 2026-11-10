// probe-lib —— 与服务端对话的只读探测原语。
//
// 为什么单独一层：init 要验活、doctor 要诊断、将来 dsh 适配器也要同一套探针。
// 三处各写一份 fetch 的话，认证头与超时语义迟早会分叉，而分叉的那一天
// 表现是「同一个 token 在一个工具里能用、在另一个里 401」——查起来极贵。

/** 规范化 endpoint：去尾斜杠，避免拼出 `//health`。 */
export function normEndpoint(ep) {
  return String(ep || '').trim().replace(/\/+$/, '')
}

/** 最小认证头。token 为空时纯匿名请求（公开仓库场景）。 */
export function authHeaders(token, extra = {}) {
  const h = { ...extra }
  if (token) h.Authorization = `Bearer ${token}`
  return h
}

/** 健康检查。永不抛错——探测失败本身就是答案。 */
export async function probeHealth(endpoint, timeoutMs = 1500) {
  const ep = normEndpoint(endpoint)
  if (!ep) return { ok: false, error: 'endpoint 为空' }
  try {
    const res = await fetch(`${ep}/health`, { signal: AbortSignal.timeout(timeoutMs) })
    if (!res.ok) return { ok: false, error: `HTTP ${res.status}` }
    return { ok: true, data: await res.json() }
  } catch (err) {
    return { ok: false, error: err.message }
  }
}

/**
 * 调用服务端 MCP 上的 term 工具，返回纯文本结果。
 *
 * zero-memcp 把动词暴露成单个 `term` 工具（cmd 是一个 DSL 行），
 * 所以 whoami/status/ls 这些诊断都走这一条通道，不需要各自的 HTTP 路由。
 */
export async function probeTerm(endpoint, token, cmd, timeoutMs = 5000) {
  const ep = normEndpoint(endpoint)
  try {
    const res = await fetch(`${ep}/mcp`, {
      method: 'POST',
      headers: authHeaders(token, { 'content-type': 'application/json' }),
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: { name: 'term', arguments: { cmd } },
      }),
      signal: AbortSignal.timeout(timeoutMs),
    })
    if (res.status === 401) return { ok: false, error: '凭证被拒绝 (401)：token 无效或已过期' }
    if (res.status === 403) return { ok: false, error: '凭证无权访问 (403)：检查挂载表' }
    if (!res.ok) return { ok: false, error: `HTTP ${res.status}` }
    const body = await res.json()
    if (body.error) return { ok: false, error: body.error.message || JSON.stringify(body.error) }
    const text = body.result?.content?.[0]?.text ?? ''
    return { ok: true, text }
  } catch (err) {
    return { ok: false, error: err.message }
  }
}

/** whoami -> 结构化身份信息。 */
export function parseWhoami(text) {
  const out = { user: '', privilege: '', unrestricted: false, mounts: [] }
  for (const raw of String(text || '').split('\n')) {
    const line = raw.trim()
    if (line.startsWith('User:')) {
      out.user = line.slice(5).trim()
    } else if (line.startsWith('Privilege:')) {
      out.privilege = line.slice(10).trim()
      out.unrestricted = /unrestricted/i.test(out.privilege)
    } else if (line.startsWith('Mount:')) {
      out.mounts = parseMountList(line.slice(6).trim())
    }
  }
  return out
}

/**
 * 解析挂载表字符串。
 *
 * 服务端格式（internal/server/mount.go 的 Describe）：
 *   project@branch:虚拟路径 (cap), project@branch (cap), ...
 * 空挂载表会给出 "(none: every namespace is visible)"。
 */
export function parseMountList(s) {
  const out = []
  for (const part of String(s || '').split(',')) {
    const item = part.trim()
    if (!item || item.startsWith('(none')) continue
    const m = item.match(/^([^\s@]+)(?:@([^\s:(]+))?(?::([^\s(]+))?\s*(?:\(([a-z]+)\))?$/)
    if (!m) {
      out.push({ project: item, branch: '', path: '', cap: '', raw: item })
      continue
    }
    out.push({
      project: m[1],
      branch: m[2] || '',
      path: m[3] || '',
      cap: m[4] || '',
      raw: item,
    })
  }
  return out
}

/** 把 `project@branch:path` 拆开。也接受省略 branch/path 的短写。 */
export function parseScope(scope) {
  const s = String(scope || '').trim()
  if (!s) return null
  const at = s.indexOf('@')
  const project = at === -1 ? s : s.slice(0, at)
  let rest = at === -1 ? '' : s.slice(at + 1)
  const colon = rest.indexOf(':')
  const branch = colon === -1 ? rest : rest.slice(0, colon)
  const path = colon === -1 ? '' : rest.slice(colon + 1)
  return { project, branch, path, raw: s }
}

/**
 * 检查一个 scope 是否被挂载表覆盖。
 *
 * need: 'r' 读 / 'w' 写。
 *
 * 这是把「写入静默失败」提前变成一句人话的地方：服务端对越权写入会拒绝，
 * 而插件的写入路径是后台的，用户只看得到「好像没记下来」。
 */
export function scopeCovered(mounts, scope, need = 'w') {
  const sc = parseScope(scope)
  if (!sc) return { ok: false, reason: '空的 scope' }
  if (!sc.project) return { ok: false, reason: `无法解析 scope: ${sc.raw}` }

  const sameProject = mounts.filter((m) => m.project === sc.project)
  if (!sameProject.length) {
    return { ok: false, reason: `凭证里没有项目 ${sc.project}` }
  }
  // 分支为空表示继承服务端默认分支，无法静态判定，按可用处理。
  const candidates = sc.branch
    ? sameProject.filter((m) => m.branch === sc.branch || m.branch === '*')
    : sameProject
  if (!candidates.length) {
    const have = sameProject.map((m) => m.branch || '(默认)').join(', ')
    return { ok: false, reason: `${sc.project} 上没有分支 ${sc.branch}；凭证只有 ${have}` }
  }
  for (const m of candidates) {
    if (!m.cap) return { ok: true, mount: m }
    if (m.cap.includes(need)) return { ok: true, mount: m }
  }
  const caps = candidates.map((m) => `${m.branch || '(默认)'}=${m.cap || '-'}`).join(', ')
  return { ok: false, reason: `${sc.project}@${sc.branch} 权限里没有 '${need}'（现有 ${caps}）` }
}

/** 联想探针：验证读路径端到端可用。 */
export async function probeRecall(endpoint, token, text, limit = 3, timeoutMs = 3000) {
  const ep = normEndpoint(endpoint)
  try {
    const res = await fetch(`${ep}/recall`, {
      method: 'POST',
      headers: authHeaders(token, { 'content-type': 'application/json' }),
      body: JSON.stringify({ text, limit }),
      signal: AbortSignal.timeout(timeoutMs),
    })
    if (!res.ok) return { ok: false, error: `HTTP ${res.status}` }
    const body = await res.json()
    const hits = Array.isArray(body.hits) ? body.hits : []
    return { ok: true, hits }
  } catch (err) {
    return { ok: false, error: err.message }
  }
}
