# 容器内版本与仓库的divergence（2026-11-13）

## 事实

运行时容器 `pi-web-ui-agent`（3060，8788 端口）里的
`/pi-agent/extensions/zero-memcp/index.js` 与仓库 `index.js` **不是同一份代码**：

| | 大小 | 特征 |
|---|---|---|
| 容器内 | 133 KB | 含 `injectExternal` / `externalBacklog` / `maybeFlushExternal`、唤醒套接字、事件一次性令牌 |
| 仓库 | 111 KB | 无上述外部消息直投通道 |

也就是说：**外部消息不走 inbox、改由 qq-mcp webhook 直接打进扩展**这套机制，
只存在于容器里，没有回流到仓库。

## 已经就地修掉的问题（容器 + 仓库两侧）

`pi.sendUserMessage(...)` 在 agent 处于 streaming 时**必须**带 `deliverAs`，
否则宿主直接拒绝，而调用点是 fire-and-forget，异常被吞掉——唤醒静默丢失。

现场日志：
```
[extension] 当前对话 (<runtime> · send_user_message):
Agent is already processing. Specify streamingBehavior ('steer' or 'followUp') to queue the message.
```

已补 `{ deliverAs: 'followUp' }` 的调用点：
1. `settleWake`（记忆变更唤醒）
2. `maybeFlushExternal`（外部消息直投，容器版独有）

## 待办

- [ ] 把容器版 `index.js` 回流到仓库（含 `injectExternal` 那条路径）
- [ ] 仓库 `bin/wake.js`、`package.json`、`tests/wake.test.mjs` 有未提交改动，需确认归属后一并处理
- [ ] 仓库领先 `origin/main` 23 个提交，尚未推送
