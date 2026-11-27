local run = string.match(redis.call('INFO', 'server'), 'run_id:([^%s]+)')
if run ~= ARGV[1] then return 0 end
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local owner = cjson.decode(raw)
if owner.run_id ~= run or not owner.active or owner.instance_id ~= ARGV[2]
    or owner.connection_id ~= ARGV[3] or owner.generation ~= tonumber(ARGV[4]) then return 0 end

-- 断开的 Socket 不再拥有租约。短期保留观看编号，只供同一次观看重新握手。
owner.active = false
redis.call('SET', KEYS[1], cjson.encode(owner), 'PX', ARGV[5])
return 1
