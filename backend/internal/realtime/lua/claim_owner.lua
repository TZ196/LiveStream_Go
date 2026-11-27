local run = string.match(redis.call('INFO', 'server'), 'run_id:([^%s]+)')
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local next = cjson.decode(ARGV[1])
if run ~= next.run_id or now >= tonumber(ARGV[4]) then return {'expired'} end

local raw = redis.call('GET', KEYS[1])
local old = raw and cjson.decode(raw)
if old and old.run_id ~= run then old = nil; raw = nil end
if old and old.connection_id == next.connection_id and old.active then
    return {'ok', raw, ''}
end

-- 票据签发后若已有另一次观看成功入房，延迟到达的旧票据不能再取代它。
if ARGV[2] == 'resume' then
    if not old then return {'expired'} end
    if old.watch_id ~= next.watch_id then return {'replaced'} end
    if old.room_id ~= next.room_id or old.session_id ~= next.session_id or old.connection_id ~= ARGV[5] then
        return {'expired'}
    end
elseif (old and old.watch_id or '') ~= ARGV[3] then
    return {'expired'}
end

next.generation = old and old.generation + 1 or 1
local value = cjson.encode(next)
redis.call('SET', KEYS[1], value, 'PX', ARGV[6])
return {'ok', value, raw or ''}
