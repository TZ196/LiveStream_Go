-- 只有身份仍与刚读取的一致时才更新，避免并发检查覆盖较新的保护代次。
local old = redis.call('GET', KEYS[1])
if (old or '') ~= ARGV[1] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[2])
return 1
