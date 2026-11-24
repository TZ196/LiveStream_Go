local meta, seen, partition, users, top = unpack(KEYS)
check_instance(ARGV[7])
local room, fingerprint, ttl, next_offset, clicks, user = unpack(ARGV)
if redis.call('HGET', meta, 'initialized') ~= '1' or redis.call('HGET', meta, 'room_id') ~= room then error('PROJECTION_NOT_READY') end
local board_revision, revision, likes = check_board(meta, users, top)
local personal = redis.call('HGET', users, user) or '0'
if not valid_count(personal) then error('INVALID_PERSONAL_COUNT') end
local prior = redis.call('GET', seen)
local old_offset = redis.call('GET', partition)
if old_offset and not string.match(old_offset, '^%d+$') then error('INVALID_OFFSET') end
if prior and prior ~= fingerprint then error('EVENT_CONFLICT') end
if not prior then
  -- 所有类型与溢出检查先于写入，Redis 脚本遇到错误并不会回滚已执行的命令。
  local next_personal = add_count(personal, clicks)
  likes = add_count(likes, clicks)
  revision = add_count(revision, clicks)
  local member = rank_member(next_personal, user)
  local cutoff = redis.call('ZRANGE', top, 99, 99)[1]
  if not cutoff or member < cutoff then
    if personal ~= '0' then redis.call('ZREM', top, rank_member(personal, user)) end
    redis.call('ZADD', top, 0, member)
    redis.call('ZREMRANGEBYRANK', top, 100, -1)
    board_revision = revision
  end
  personal = next_personal
  redis.call('HSET', users, user, personal)
  redis.call('HSET', meta, 'likes', likes, 'revision', revision, 'board_through', revision, 'board_revision', board_revision, 'board_users', redis.call('HLEN', users))
  redis.call('SET', seen, fingerprint, 'EX', ttl)
end
if not old_offset or #next_offset > #old_offset or (#next_offset == #old_offset and next_offset > old_offset) then
  redis.call('SET', partition, next_offset)
end
return {likes, revision, prior and 'duplicate' or 'applied', personal, board_revision}
