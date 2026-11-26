if redis.call('EXISTS', KEYS[2]) == 1 then return redis.error_reply('CACHE_WARMING') end
local now = redis.call('TIME')
local ms = now[1] * 1000 + math.floor(now[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ms - 300000)
local maximum = tonumber(ARGV[4])
redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -maximum - 1)
local score = tonumber(ARGV[1])
if score > ms - 300000 then
  local members = redis.call('ZRANGE', KEYS[1], 0, -1)
  for _, member in ipairs(members) do
    local existing = cjson.decode(member)
    if existing.event_id == ARGV[3] and member ~= ARGV[2] then return 2 end
  end
  local added = redis.call('ZADD', KEYS[1], 'NX', score, ARGV[2])
  redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -maximum - 1)
  if added == 1 then
    redis.call('PEXPIRE', KEYS[1], 300000)
    if redis.call('EXISTS', KEYS[3]) == 1 then redis.call('PSETEX', KEYS[3], 300000, '1') end
    return added
  end
end
return 0