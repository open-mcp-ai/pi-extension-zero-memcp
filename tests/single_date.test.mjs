import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'

test('日常写入路径只有单级日期，不产生 double-date', () => {
  const index = fs.readFileSync('index.js', 'utf8')
  // 不再有任何把 day 拼进 scope 的逻辑
  assert.equal(/sc\.recordScope\s*\?\s*`\$\{sc\.recordScope\}\/\$\{day\}`/.test(index), false)
  assert.equal(/\$\{sc\.thoughtScope\}\/\$\{day\}/.test(index), false)
  assert.equal(/daySegment/.test(index), false, 'daySegment 应已彻底删除')
})
