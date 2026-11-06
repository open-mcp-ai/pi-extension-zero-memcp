# pi-extension-zero-memcp

[zero-memcp](https://github.com/open-mcp-ai/zero-memcp) 记忆服务的 [Pi](https://github.com/badlogic/pi) 智能体扩展。

它在模型生命周期的关键点介入，完成**身份注入、语义联想、逻辑时钟感知与问答对落盘**：

| Pi 钩子 | 触发时机 | 动作 | 容错策略 |
|---|---|---|---|
| `context_with_system` | 构造系统提示词 | 将当前身份的规则块（`inject: every_turn`）拼入开头 | **失败显式报错**（身份缺失不能假装正常） |
| `context` | 每次用户发问前 | 按优先级分层召回（配置 `recallScopes`），把第一个命中的层连同正文摘要追加到上下文尾部 | 失败静默（少一次联想不阻断会话） |
| `context` | 每次用户发问前 | 检查服务端可达性，恢复时补发断线期间未入库的问答 | 失败静默 |
| `agent_end` | 当轮问答结束 | 通过 `put` 将当轮问答对写入脏层（仅在 `auto` 模式） | 幂等写入，防重复 |
| SSE 监听器 | 后台常驻 | 接收服务端 `tree` 树变更与 `clock` 时钟广播 | 断线指数退避重连 |

---

## 安装

在 Pi 扩展目录下建软链接：

```bash
mkdir -p ~/.pi/agent/extensions
ln -s /path/to/pi-extension-zero-memcp ~/.pi/agent/extensions/zero-memcp
```

### 配置文件

扩展按如下优先级寻找配置：

1. 环境变量 `ZERO_MEMCP_CONFIG` 指向的文件；
2. `~/.zero-memcp/config.json`；
3. 本扩展目录下的 `config.json`；
4. 默认值：`http://127.0.0.1:1729`，`importMode: "off"`。

身份和授权完全由 Bearer Token 决定，不存在可逐请求切换身份的 header。
一张凭证代表一个身份，服务端在签发时将能力与挂载范围定死；需要另一种视角就使用另一张凭证。

创建 `~/.zero-memcp/config.json`：

```json
{
  "endpoint": "http://127.0.0.1:1729",
  "enabled": true,

  "importMode": "off",
  "syncSessionOnStart": false,

  "identity": "off",
  "identityName": "",
  "identityTtlMs": 3600000,

  "token": "",
  "tokens": {},

  "watch": "BlackEggAI/zero@main.record",

  "recallLimit": 3,
  "recallTimeoutMs": 200,
  "recallLayerTimeoutMs": 2000,

  "recallScopes": [
    "BlackEggAI/zero@main.record:fact",
    "BlackEggAI/zero@main.record:record"
  ],
  "recallScopeLimit": 3,
  "recordScope": "BlackEggAI/zero@main.record:record",
  "factScope": "BlackEggAI/zero@main.record:fact",
  "thoughtScope": "BlackEggAI/zero@main.record:thought",

  "personas": {
    "laptop": {
      "recordScope": "BlackEggAI/zero@main.record:record",
      "factScope": "BlackEggAI/zero@main.record:fact",
      "thoughtScope": "BlackEggAI/zero@main.record:thought",
      "recallScopes": [
        "BlackEggAI/zero@main.record:fact",
        "BlackEggAI/zero@main.record:record"
      ]
    }
  }
}
```

`identityTtlMs` 控制身份块的重取周期（默认 1 小时）。改过服务端的规则块或
personas 之后，最多等一个周期就能在当前会话里看到新内容，不必重开会话。

`watch` 是 SSE 订阅的树地址（逗号分隔）。服务端的 tree 事件**只推给
remind 锚定过的人**——不填 `watch` 就收不到变更通知，而且不会报错。

### 分层召回（`recallScopes`）

记忆可以按认知层级分到不同分支，`recallScopes` 决定查询顺序：

```json
"recallScopes": ["zero-agi@fact", "zero-agi@record", "zero-agi@thought"]
```

扩展把它编译成一条服务端回落链：

```
recall $Q --scope zero-agi@fact || recall $Q --scope zero-agi@record || recall $Q --scope zero-agi@thought
```

`||` 表示「上一层没查到才问下一层」。事实层命中时，后面两层**根本不执行**，
既省一次检索也省一段本该塞进 prompt 的无关正文。

为什么不用一个 `scope` 查全部：想法层（thinking）的文本量是事实层的十几倍，
混在一起查时它靠体量就能把提炼过的结论挤出 top-N——于是「有结论」和
「有过一段推演」在结果里分不出来。层级不是排序偏好，是可信度差异：
事实层是结论，记录层是当时的原话，想法层是推理过程。注入的文本会标明来源层，
避免模型把推演当结论用。

留空 `[]` 则退回单次全局检索（返回地址列表，不带正文摘要）。

`recallLayerTimeoutMs` 比 `recallTimeoutMs` 宽松（默认 2000ms）：一次回落链
最多做三次检索，200ms 会把正常查询误判成超时，而超时是静默丢弃——
看起来就像「记忆库里没有」。

### 分层写入（`recordScope` / `factScope` / `thoughtScope`）

读的时候按可信度分了三层，写的时候也必须分层——否则这套分层只对历史数据
成立，新对话立刻退化成一锅粥。

| 配置 | 内容 | 特点 |
|---|---|---|
| `recordScope` | 一问一答 | 默认落点，量最大 |
| `factScope` | compaction 摘要 | 信息密度最高（几十万字压成几百字） |
| `thoughtScope` | thinking 推理链 | 体积是问答的 4~5 倍，留空即不记录 |

`factScope` 接在 `session_compact` 事件上：Pi 压缩对话时产出的结构化摘要
（Goal / Decisions / Progress）直接写进事实层。这样未来的查询第一跳就能
拿到结论，不必在数千条问答细节里大海捞针。

### 人格映射（`personas`）

一个身份一张凭证，每张凭证有自己的挂载表。`personas` 把「这个人格的记忆
存在哪」写下来：

```json
"personas": {
  "laptop": {
    "recordScope": "BlackEggAI/zero@main.record:record",
    "recallScopes": ["BlackEggAI/zero@main.record:fact"]
  }
}
```

`personas` 声明的 scope 必须落在对应凭证的挂载范围内。不一致时写入会在
服务端被拒——插件在挂载身份的那一刻就会把这个问题报出来，而不是等写入
失败后让人对着日志猜。

---

## 命令

用户在 TUI 输入框中可使用的 3 个斜杠命令：

- `/mem:identity` — **身份 / 挂载**：空参显示当前身份、凭证能力与挂载范围；`/mem:identity <name>` 切换到一张已配置的凭证（等同换一个数据库连接），当前会话立即使用它；`on|off [global]` 控制本会话或全局默认是否启用记忆注入。
- `/mem:sync` — **同步到记忆体**：手动把当前会话全部问答导入记忆库（按内容 checksum CAS 幂等，无重复开销）；
- `/mem:record` — **自动记录**：空参查看状态；`/mem:record [on|off] [global]` 切换本会话或全局默认。

---

## 历史会话导入 CLI (`zero-memcp-import`)

把安装插件之前的旧会话（`~/.pi/agent/sessions/*.jsonl`）批量导入到 zero-memcp 中：

```bash
# 1. 先统计有哪些可以导入（只读扫描，不写盘）
npx zero-memcp-import --dry-run

# 2. 导入单个指定的 JSONL 文件
npx zero-memcp-import ~/.pi/agent/sessions/--Users-me--/2026-08-29T...jsonl

# 3. 导入指定目录且只导入 2026-08-01 之后的会话
npx zero-memcp-import --since 2026-08-01 ~/.pi/agent/sessions/
```

- **会话快照归并**：按 session UUID 归并压缩版本，同会话只取最大的最新快照；
- **智能截断与过渡语过滤**：只取 `stopReason === 'stop'` 的最终结论，排除中间态、用户打断与无意义过渡语；
- **本地账本幂等**：维护 `~/.zero-memcp/import-state.json`，重跑零网络开销，不重复写入；
- **纯通用协议交互**：通过 MCP `put` 原语入队，正文走 `vars.TEXT`，分批自动 `commit`，服务端不侵入任何 Pi 专属知识。

## 暴露给模型的工具（只有一个）

遵循 termcp 规范：**声明面恒定为 1，能力增长在命令空间，不在 `tools/list` 堆工具**。

模型在整个会话中只能看到一个工具：

```
mem({ cmd: "recall 数据库选型" })
```

### 命令语法

| 类别 | 动词 | 说明 |
|---|---|---|
| **会话管理**（插件处理） | `mem status` | 当前端点、身份、自动记录模式、本会话已入库轮次与索引规模 |
| | `mem record [on\|off]` | 模型自主调整本会话自动记录 |
| | `mem identity [<name>]` | 查看身份信息或选择另一张凭证 |

> **整场会话同步不在工具里暴露。** 只有 `/mem:sync` 这一个入口，
> 因为全量写入是重度且难以撤回的操作（实测 757 条约 176 秒、108 万字符），
> 决定权属于人类。
| **记忆检索**（透传服务） | `mem recall <text>` | BM25 语义搜（返回地址 + 摘要片段） |
| | `mem read <addr>...` | 读全文，可一次多篇 |
| | `mem grep <regex>` | 正则精确搜符号/数字/函数名 |
| | `mem log [N]` | 最近 N 篇 |
| | `mem ls [date]` | 列某天的切片 |
| | `mem trees <project>[@<br>]` | 查看树目录 |
| | `mem since <tree> <commit>` | 取某提交之后的差分 |
| | `mem remind [<tree>]` | 锚定子树并接收 SSE 变更通知 |
| | `mem status`（服务端） | 索引规模、**语义后端与降级状态**、**同步现场与冲突** |
| **记忆写入**（透传服务） | `mem put [<tree>] -- $TEXT` | 写入脏层（正文必须走 `vars` / `args`，不拼进命令） |
| | `mem put <tree> --ref <addr>` | 在树里追加已有记忆的引用 |
| | `mem write <p>@<br>/<path> -- T` | 精确写入指定路径（规则库用，需 root） |
| | `mem batch {"ops":[...]}` | 原子批量：多步合成一次提交 |
| **结构维护**（透传服务） | `mem commit [<p>[@<br>]]` | 显式折叠脏层为 Git 提交 |
| | `mem diff <p>@<br>` | 分支比对 / 提取未审脏流 |
| | `mem branch [<p> [<new>]]` | 列出或派生分支 |
| | `mem tag [-d] <p> [<name>]` | 轻量标签 |
| | `mem cp / mv <src> -> <dst>` | 跨分支搬运条目 |
| | `mem delete <p>@<br>/<path>...` | 删除条目 |
| | `mem archive <p>@<br>` | 归档分支 |
| | `mem reset <p>@<br> from <src>` | 把分支重置到另一分支 |
| | `mem lock / unlock <p>@<br>` | 锁定/解锁分支（暂停非 root 访问） |
| | `mem chmod <p>@<br>/<path> mode` | 改规则/内容权限位 |
| **多机同步**（透传服务） | `mem remote [<project>]` | 列出配置的远端 |
| | `mem sync <project> [<remote>]` | 对比本地与远端状态（含自动同步时间与待决议冲突） |
| | `mem fetch <project> [<remote>]` | 取回远端历史，不自动覆盖本地分支 |
| | `mem push <project> [<remote>] <branch>` | 显式推一条分支（不替你提交、不强推） |
| | `mem merge <project> <branch> from <remote>` | 把远端分支合并进本地（快进或正交合并；冲突则拒绝） |
| | `mem remote-key list\|put\|delete` | root 管理 SSH 私钥 |
| | `mem remote-secret list\|put\|delete` | root 管理 HTTP token |

> 服务端实际有 33 个动词。上面这张表就是模型看到的全部能力——**漏一个
> 就等于那个能力不存在**，因为模型不会去猜没被声明的动词。
> 完整清单随时可用 `mem({ cmd: "help" })` 取回；单动词细节用 `help <verb>`。

## 记忆写入与模式说明

- **`put` 与 `commit` 分离**：`put` 将内容投递进目录级脏层并立即返回，不代表 Git 历史已经推进；显式 `commit` 或服务端 `[commit]` 后台策略负责折叠成提交；
- **正文走 vars/args**：例如 `mem({ cmd: "put notes -- $TEXT", vars: { TEXT: "正文可含空格、换行与 --" } })`，引用必须独占一个词，避免正文被解析成 DSL；
- **`off`（缺省）**：只读，不自动写入。每轮可做身份注入和语义联想；
- **`auto`**：每轮问答结束时由扩展调用 `put` 自动记录问答对；
- `/mem:sync` 可手动把当前会话完整问答导入记忆库（按水位与内容去重）。

在 `~/.zero-memcp/config.json` 中配置多张凭证：

```json
{
  "identityName": "reader",
  "token": "<默认凭证>",
  "tokens": {
    "reader": "<只读凭证 JWT>",
    "writer": "<写入凭证 JWT>"
  }
}
```

`/mem:identity writer` 会让当前 Pi 会话改用 `writer` 这张凭证发出的 Bearer Token；服务端不接收身份切换 header，也不允许在同一凭证上换身份。会话重启后会恢复该会话上次选的凭证名。

树变更通知基于服务端 SSE `tree` 事件，只有先执行 `mem remind <project>@<branch>[/<path>]` 锚定后，才会收到对应变更事件。服务端当前不提供 mailbox，也不使用身份切换 header。

---

## 许可证

MIT License. 见 [LICENSE](LICENSE)。
