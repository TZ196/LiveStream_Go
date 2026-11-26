local ready = redis.call('GET', KEYS[2])
if not ready or redis.call('EXISTS', KEYS[3]) == 1 then return {'WARMING'} end
if ready == 'missing' then return {'MISSING'} end
if ready == '1' and redis.call('EXISTS', KEYS[1]) == 0 then return {'WARMING'} end
local now = redis.call('TIME')
local ms = now[1] * 1000 + math.floor(now[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ms - 300000)
local start = 0
if ARGV[1] ~= '' then
  local rank = redis.call('ZREVRANK', KEYS[1], ARGV[1])
  if not rank then return {'READY'} end
  start = rank + 1
end
local rows = redis.call('ZREVRANGE', KEYS[1], start, start + 19)
if redis.call('EXISTS', KEYS[1]) == 0 then redis.call('SET', KEYS[2], '0', 'KEEPTTL') end
table.insert(rows, 1, 'READY')
return rows
