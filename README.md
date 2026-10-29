# pi-extension-zero-memcp

[zero-memcp](https://github.com/bl4ck3gg/zero-memcp) 记忆服务的 [Pi](https://github.com/badlogic/pi) 智能体扩展。

它在模型请求的路径上做四件事：

| 钩子 | 动作 | 策略 |
|---|---|---|
| `context_with_system` | 把身份块拼进系统消息开头 | 失败响亮报错（我不是我了） |
| `context` | 每轮取 2-3 个记忆地址卡片追加到尾部 | 失败静默（少一条提示） |
| `context` | 检测服务端 `clock` 跳变（逻辑时钟广播） | 跳变时提示 `zero_mem log` |
| `agent_end` | 将当轮问答写进当前身份的 `daily/` | 仅在 `importMode: auto` 时落盘 |

## 命令

- `/mem:import` — 手动把当前会话全部问答导入记忆库（按内容 checksum 天然 CAS 幂等）
- `/mem:auto on|off` — 仅为当前会话开启或关闭自动记忆（不影响其他会话）
- `/mem:profile <name>` — 切换或新建记忆身份

## 工具

- `zero_mem({cmd})` — termcp 协议单入口（`recall` / `read` / `grep` / `log` / `ls` / `note` / `help`）
- `zero_mem_set_mode({mode})` — 模型自主调整本会话记忆模式（off / manual / auto）
- `zero_mem_set_profile({name})` — 模型自主切换身份
- `zero_mem_import()` — 模型自主导入当前会话
- `zero_mem_status()` — 查看当前会话状态、时钟与开关

## 安装

软链接到 Pi 扩展目录：

```bash
mkdir -p ~/.pi/agent/extensions
ln -s ~/workdir/bl4ck3gg/pi-extension-zero-memcp ~/.pi/agent/extensions/zero-memcp
```
