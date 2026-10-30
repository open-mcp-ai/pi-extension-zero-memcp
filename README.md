# pi-extension-zero-memcp

[zero-memcp](https://github.com/open-mcp-ai/zero-memcp) 记忆服务的 [Pi](https://github.com/badlogic/pi) 智能体扩展。

它在模型生命周期的关键点介入，完成**身份注入、语义联想、逻辑时钟感知与问答对落盘**：

| Pi 钩子 | 触发时机 | 动作 | 容错策略 |
|---|---|---|---|
| `context_with_system` | 构造系统提示词 | 将当前身份的规则块（`inject: every_turn`）拼入开头 | **失败显式报错**（身份缺失不能假装正常） |
| `context` | 每次用户发问前 | 取 2-3 个最相关的记忆地址卡片追加到上下文尾部 | 失败静默（少一次联想不阻断会话） |
| `context` | 每次用户发问前 | 检查服务端 `clock`（逻辑时钟广播）跳变 | 跳变时提示 `zero_mem log` 发现新记忆 |
| `agent_end` | 当轮问答结束 | 将当轮问答对写进当前分支（仅在 `auto` 模式） | 幂等写入，防重复 |
| SSE 监听器 | 后台常驻 | 接收服务端更新广播与未读信箱提醒 | 断线指数退避重连 |

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

创建 `~/.zero-memcp/config.json`：

```json
{
  "endpoint": "http://127.0.0.1:1729",
  "enabled": true,
  "importMode": "off",
  "recallLimit": 3,
  "recallTimeoutMs": 200
}
```

---

## 命令

用户在 TUI 输入框中可使用的 3 个斜杠命令：

- `/mem:role` — **身份**：空参列出凭证内全部可用角色（名称、能力位、挂载表，当前角色带标记）；`/mem:role <name>` 切换并锁定本会话角色（写入会话元数据，重启后自动恢复）；
- `/mem:sync` — **同步到记忆体**：手动把当前会话全部问答导入记忆库（按内容 checksum CAS 幂等，无重复开销）；
- `/mem:mode` — **记忆模式**：空参查看当前模式；`/mem:mode [auto|manual|off]` 切换本会话模式。

## 暴露给模型的工具（只有一个）

遵循 termcp 规范：**声明面恒定为 1，能力增长在命令空间，不在 `tools/list` 堆工具**。

模型在整个会话中只能看到一个工具：

```
mem({ cmd: "recall 数据库选型" })
```

### 命令语法

| 类别 | 动词 | 说明 |
|---|---|---|
| **会话管理**（插件处理） | `mem status` | 当前端点、角色、模式、逻辑时钟、未读信箱 |
| | `mem mode [auto\|manual\|off]` | 模型自主调整本会话记忆模式 |
| | `mem role [<name>]` | 查看/切换本会话角色 |

> **整场会话同步不可由模型触发。** `mem sync` 会直接拒绝并引导模型询问用户——
> 全量写入是重度且难以撤回的操作（可能是几百条、上百万字符），
> 必须由人类敲 `/mem:sync` 决定。模型可以建议，不能代劳。
| **记忆检索**（透传服务） | `mem recall <text>` | BM25 语义搜（返回地址 + 摘要片段） |
| | `mem read <addr>...` | 读全文，可一次多篇 |
| | `mem grep <regex>` | 正则精确搜符号/数字/函数名 |
| | `mem log [N]` | 最近 N 篇 |
| | `mem ls [date]` | 列某天的切片 |
| **记忆写入**（透传服务） | `mem note <kind> -- <text>` | 追加切片（fact/episode/preference/observation） |
| | `mem commit [<p>[@<br>]]` | 显式封箱（引擎绝不自动提交） |
| | `mem diff <p>@<br>` | 分支比对 / 提取未审脏流 |

## 记忆模式说明

- **`off`（缺省）**：只读不写。每轮有身份注入和语义联想，但不会自动写任何记忆；
- **`manual`**：只读，但在用户敲 `/mem:import` 或模型调 `zero_mem_import()` 时批量导入；
- **`auto`**：每轮问答结束时自动抽取并写入对应分支。

---

## 许可证

MIT License. 见 [LICENSE](LICENSE)。
