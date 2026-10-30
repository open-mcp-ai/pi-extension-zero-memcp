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

用户在 TUI 中可直接使用的斜杠命令：

- `/mem:import` — 手动把当前会话全部问答导入记忆库（按内容 checksum CAS 幂等）；
- `/mem:auto on|off` — 仅为当前会话开启或关闭每轮自动记忆（不影响其他会话）；
- `/mem:profile <name>` — 切换记忆身份（分支名，空参 = 默认身份）；
- `/mem:role <zero|chat|dev>` — 切换并锁定本会话角色（空参 = 查看当前角色，写入本会话元数据）。

---

## 暴露给模型的工具

模型可在推理中自主调用的 5 个工具：

| 工具名 | 参数 | 说明 |
|---|---|---|
| `zero_mem` | `{cmd}` | termcp 协议单入口（`recall`/`read`/`grep`/`log`/`ls`/`note`/`status`） |
| `zero_mem_set_mode` | `{mode: "off"\|"manual"\|"auto"}` | 模型自主调整本会话记忆模式 |
| `zero_mem_set_profile` | `{name: string}` | 模型自主切换身份分支 |
| `zero_mem_import` | `{}` | 模型自主导入当前会话 |
| `zero_mem_status` | `{}` | 查询插件状态、开关、身份与逻辑时钟 |

---

## 记忆模式说明

- **`off`（缺省）**：只读不写。每轮有身份注入和语义联想，但不会自动写任何记忆；
- **`manual`**：只读，但在用户敲 `/mem:import` 或模型调 `zero_mem_import()` 时批量导入；
- **`auto`**：每轮问答结束时自动抽取并写入对应分支。

---

## 许可证

MIT License. 见 [LICENSE](LICENSE)。
