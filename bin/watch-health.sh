#!/bin/bash
# zero-memcp 运行观察：把「跑一段时间看看有没有问题」变成可追溯的记录。
#
# 为什么需要独立脚本：观察期不能依赖某个对话窗口活着。这个脚本由 launchd
# 定时拉起，只读地采一份快照追加到日志，事后一次性看趋势。
#
# 采集：hub 健康 / 文档数 / 向量数 / spoke 同步时效 / 仓库体积 / 错误行
# 判定：与上一次快照对比，只在**异常**时打 ANOMALY 标记，便于 grep。

set -uo pipefail

LOG_DIR="${ZERO_MEMCP_WATCH_DIR:-$HOME/.zero-memcp/watch}"
LOG="$LOG_DIR/health.log"
STATE="$LOG_DIR/last.state"
HUB="${ZERO_MEMCP_HUB:-http://10.10.10.12:1729}"
LOCAL="${ZERO_MEMCP_LOCAL:-http://127.0.0.1:1729}"
SECRETS="$HOME/.zero-memcp/data/system/secrets"
mkdir -p "$LOG_DIR"

ts() { date '+%Y-%m-%dT%H:%M:%S'; }

# 调 MCP 的 term 动词。stdout 只留正文，便于 grep。
mcp() {
  local ep="$1" tokfile="$2" cmd="$3"
  local tok=""
  [ -r "$tokfile" ] && tok=$(cat "$tokfile")
  curl -s -m 25 -X POST "$ep/mcp" \
    -H "Authorization: Bearer $tok" -H 'content-type: application/json' \
    -d "$(printf '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"term","arguments":{"cmd":"%s"}}}' "$cmd")" \
    | python3 -c 'import sys,json
try:
    d=json.load(sys.stdin)
    print(d.get("result",{}).get("content",[{}])[0].get("text",""))
except Exception:
    print("PARSE_ERROR")'
}

report() {
  local tag="$1" ep="$2" tokfile="$3"
  local h docs sem
  h=$(curl -s -m 6 "$ep/health" || echo "")
  if [ -z "$h" ]; then
    echo "$(ts) [$tag] FAIL health=unreachable ($ep)"
    return
  fi
  local st
  st=$(mcp "$ep" "$tokfile" "status")
  docs=$(printf '%s' "$st" | sed -n 's/^docs: \([0-9]*\).*/\1/p' | head -1)
  sem=$(printf '%s' "$st" | grep -o 'semantic: [^,]*' | head -1)
  local errs
  errs=$(printf '%s' "$st" | grep -c '\[fetch:\|\[push:\|failed' || true)
  echo "$(ts) [$tag] ok docs=${docs:-?} ${sem:-semantic:?} errors=$errs"
}

{
echo "── 快照 $(ts) ──"
report hub "$HUB" "$SECRETS/tok-hub-3060-admin"
report local "$LOCAL" "$SECRETS/../secrets/tok-hub-3060-admin"

# 本地仓库体积与最新提交：判断历史是否在推进
for r in BlackEggAI/zero BlackEggAI/weft; do
  g="$HOME/.zero-memcp/data/repos/$r.git"
  [ -d "$g" ] || continue
  sz=$(du -sh "$g" 2>/dev/null | cut -f1)
  head=$(git --git-dir="$g" log -1 --format='%h %ci' 2>/dev/null | head -1)
  echo "$(ts) [repo] $r size=$sz head=$head"
done

# hub 侧错误行：只摘最近几条，避免日志膨胀
if [ -r /tmp/zm-spoke.log ]; then
  bad=$(grep -iE 'sync .*(failed|refus|conflict)|semantic: .*failed' /tmp/zm-spoke.log 2>/dev/null | tail -3)
  [ -n "$bad" ] && printf '%s\n' "$bad" | sed "s/^/$(ts) [spoke-log] /"
fi

echo
} >> "$LOG"

# 异常检测：与上一次的 docs 数比较，掉数就是异常（正常只该增长或持平）
cur=$(grep '\[hub\] ok docs=' "$LOG" | tail -1 | sed -n 's/.*docs=\([0-9]*\).*/\1/p')
if [ -n "$cur" ]; then
  if [ -f "$STATE" ]; then
    prev=$(cat "$STATE" 2>/dev/null || echo "")
    if [ -n "$prev" ] && [ "$cur" -lt "$prev" ]; then
      echo "$(ts) ANOMALY hub docs 倒退: $prev -> $cur" >> "$LOG"
    fi
  fi
  echo "$cur" > "$STATE"
fi

# 日志滚动：保留最后 5000 行
if [ -f "$LOG" ] && [ "$(wc -l < "$LOG")" -gt 5000 ]; then
  tail -3000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
fi

exit 0
