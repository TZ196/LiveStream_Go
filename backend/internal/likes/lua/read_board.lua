local meta, users, top = unpack(KEYS)
check_instance(ARGV[4])
if redis.call('HGET', meta, 'initialized') ~= '1' or redis.call('HGET', meta, 'room_id') ~= ARGV[1] then error('SESSION_MISMATCH') end
local version, revision = check_board(meta, users, top)
local personal = redis.call('HGET', users, ARGV[2]) or '0'
if not valid_count(personal) then error('INVALID_PERSONAL_COUNT') end
local result = {version, revision, personal}
-- 完整榜和个人数从同一次原子读取获得。调用者已缓存该版本时只读取个人数。
if version ~= ARGV[3] then
  for _, member in ipairs(redis.call('ZRANGE', top, 0, 99)) do
    local user = string.gsub(string.sub(member, 20), '^0+', '')
    local count = redis.call('HGET', users, user)
    if not valid_count(count) or count == '0' or rank_member(count, user) ~= member then error('INVALID_BOARD_MEMBER') end
    table.insert(result, user)
    table.insert(result, count)
  end
end
return result
