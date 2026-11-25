-- 只向前推进弹幕实时组的分区进度；重复处理不使位置回退。
local current=redis.call('HGET',KEYS[1],ARGV[1])
local next=ARGV[2]
if current and not string.match(current,'^%d+$') then return redis.error_reply('INVALID_POSITION') end
if not current or #next>#current or (#next==#current and next>current) then redis.call('HSET',KEYS[1],ARGV[1],next) end
return 1
