local run = string.match(redis.call('INFO', 'server'), 'run_id:([^%s]+)')
if run ~= ARGV[1] then return -1 end
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local owner = cjson.decode(raw)
if owner.run_id ~= run then return -1 end
if owner.instance_id ~= ARGV[2] or owner.connection_id ~= ARGV[3] or owner.generation ~= tonumber(ARGV[4]) then return 2 end
if not owner.active then return 0 end
return redis.call('PEXPIRE', KEYS[1], ARGV[5])
