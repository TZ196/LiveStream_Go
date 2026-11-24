-- 总数与个人计数始终以十进制字符串保存，Lua 的浮点数只用来计算单个十进制位。
local function add_count(left, right)
  if not string.match(left, '^%d+$') or not string.match(right, '^%d+$') then error('INVALID_COUNT') end
  local i, j, carry = #left, #right, 0
  local result = ''
  while i > 0 or j > 0 or carry > 0 do
    local digit = carry
    if i > 0 then digit = digit + string.byte(left, i) - 48 end
    if j > 0 then digit = digit + string.byte(right, j) - 48 end
    result = string.char(48 + digit % 10) .. result
    carry = math.floor(digit / 10)
    i, j = i - 1, j - 1
  end
  if #result > 19 or (#result == 19 and result > '9223372036854775807') then error('COUNT_OVERFLOW') end
  return result
end

-- score 均为 0。点击数按位取九的补数，用户编号左补零，两段一起按字典序升序。
local function rank_member(count, user)
  local padded = string.rep('0', 19 - #count) .. count
  local descending = string.gsub(padded, '%d', function(d) return string.char(105 - string.byte(d)) end)
  return descending .. string.rep('0', 20 - #user) .. user
end

local function valid_count(value)
  if not value or (value ~= '0' and not string.match(value, '^[1-9]%d*$')) then return false end
  return #value < 19 or (#value == 19 and value <= '9223372036854775807')
end

local function check_board(meta, users, top)
  local fields = redis.call('HMGET', meta, 'board_users', 'board_revision', 'board_through', 'revision', 'likes')
  local size, version, through, revision, likes = unpack(fields)
  if not valid_count(size) or not valid_count(version) or not valid_count(revision) or not valid_count(likes)
     or through ~= revision or #version > #revision or (#version == #revision and version > revision)
     or redis.call('HLEN', users) ~= tonumber(size) or redis.call('ZCARD', top) ~= math.min(tonumber(size), 100) then
    error('LEADERBOARD_NOT_READY')
  end
  return version, revision, likes
end
