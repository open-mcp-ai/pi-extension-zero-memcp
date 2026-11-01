# pi-extension-zero-memcp

[zero-memcp](https://github.com/open-mcp-ai/zero-memcp) 记忆服务的 [Pi](https://github.com/badlogic/pi) 智能体扩展。

它在模型生命周期的关键点介入，完成**身份注入、语义联想、逻辑时钟感知与问答对落盘**：

| Pi 钩子 | 触发时机 | 动作 | 容错策略 |
|---|---|---|---|
| `context_with_system` | 构造系统提示词 | 将当前身份的规则块（`inject: every_turn`）拼入开头 | **失败显式报错**（身份缺失不能假装正常） |
| `context` | 每次用户发问前 | 取 2-3 个最相关的记忆地址卡片追加到上下文尾部 | 失败静默（少一次联想不阻断会话） |
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

身份和授权只由 Bearer Token 与 `X-Zero-Role` 决定，不配置 `profile`；
旧配置里的 `profile` 字段会被忽略。

创建 `~/.zero-memcp/config.json`：

```json
{
  "endpoint": "http://127.0.0.1:1729",
  "enabled": true,
  "importMode": "off",
  "identity": "off",
  "role": "",
  "token": "",
  "watch": "daily@zero,notes@zero/team",
  "recallLimit": 3,
  "recallTimeoutMs": 200
}
```

`watch` 是 SSE 订阅的树地址（逗号分隔）。服务端的 tree 事件**只推给
remind 锚定过的人**——不填 `watch` 就收不到变更通知，而且不会报错。

---

## 命令

用户在 TUI 输入框中可使用的 3 个斜杠命令：

- `/mem:role` — **身份**：空参列出凭证内全部可用角色（名称、能力位、挂载表，当前角色带标记）；`/mem:role <name>` 切换并锁定本会话角色（写入会话元数据，重启后自动恢复）；
- `/mem:sync` — **同步到记忆体**：手动把当前会话全部问答导入记忆库（按内容 checksum CAS 幂等，无重复开销）；
- `/mem:record` — **自动记录**：空参查看状态；`/mem:record [on|off] [global]` 切换本会话或全局默认。

## 暴露给模型的工具（只有一个）

遵循 termcp 规范：**声明面恒定为 1，能力增长在命令空间，不在 `tools/list` 堆工具**。

模型在整个会话中只能看到一个工具：

```
mem({ cmd: "recall 数据库选型" })
```

### 命令语法

| 类别 | 动词 | 说明 |
|---|---|---|
| **会话管理**（插件处理） | `mem status` | 当前端点、角色、自动记录模式、本会话已入库轮次与索引规模 |
| | `mem record [on\|off]` | 模型自主调整本会话自动记录 |
| | `mem role [<name>]` | 查看/切换本会话角色 |

> **整场会话同步不在工具里暴露。** 只有 `/mem:sync` 这一个入口，
> 因为全量写入是重度且难以撤回的操作（实测 757 条约 176 秒、108 万字符），
> 决定权属于人类。
| **记忆检索**（透传服务） | `mem recall <text>` | BM25 语义搜（返回地址 + 摘要片段） |
| | `mem read <addr>...` | 读全文，可一次多篇 |
| | `mem grep <regex>` | 正则精确搜符号/数字/函数名 |
| | `mem log [N]` | 最近 N 篇 |
| | `mem ls [date]` | 列某天的切片 |
| **记忆写入**（透传服务） | `mem put [<tree>] -- $TEXT` | 写入脏层（正文必须走 `vars` / `args`，不拼进命令） |
| | `mem put <tree> --ref <addr>` | 在树里追加已有记忆的引用 |
| | `mem commit [<p>[@<br>]]` | 显式折叠脏层为 Git 提交 |
| | `mem diff <p>@<br>` | 分支比对 / 提取未审脏流 |

## 记忆写入与模式说明

- **`put` 与 `commit` 分离**：`put` 将内容投递进目录级脏层并立即返回，不代表 Git 历史已经推进；显式 `commit` 或服务端 `[commit]` 后台策略负责折叠成提交；
- **正文走 vars/args**：例如 `mem({ cmd: "put notes -- $TEXT", vars: { TEXT: "正文可含空格、换行与 --" } })`，引用必须独占一个词，避免正文被解析成 DSL；
- **`off`（缺省）**：只读，不自动写入。每轮可做身份注入和语义联想；
- **`auto`**：每轮问答结束时由扩展调用 `put` 自动记录问答对；
- `/mem:sync` 可手动把当前会话完整问答导入记忆库（按水位与内容去重）。

树变更通知基于服务端 SSE `tree` 事件，只有先执行 `mem remind <project>@<branch>[/<path>]` 锚定后，才会收到对应变更事件。服务端当前不提供 mailbox，也不使用 `?profile=`。

---

## 许可证

MIT License. 见 [LICENSE](LICENSE)。
