if redis.call('EXISTS', KEYS[2]) == 1 then return redis.error_reply('CACHE_WARMING') end
local now = redis.call('TIME')
local ms = now[1] * 1000 + math.floor(now[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ms - 300000)
local score = math.min(tonumber(ARGV[1]), ms)
if score > ms - 300000 then
  local added = redis.call('ZADD', KEYS[1], 'NX', score, ARGV[2])
  if added == 1 then
    redis.call('PEXPIRE', KEYS[1], 300000)
    if redis.call('EXISTS', KEYS[3]) == 1 then redis.call('PSETEX', KEYS[3], 300000, '1') end
  end
end
return 1
