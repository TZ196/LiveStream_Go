if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[2])
if redis.call('EXISTS', KEYS[3]) == 1 then
  redis.call('RENAME', KEYS[3], KEYS[2])
  redis.call('PEXPIRE', KEYS[2], 300000)
  redis.call('PSETEX', KEYS[4], 300000, '1')
else
  redis.call('PSETEX', KEYS[4], 300000, '0')
end
redis.call('DEL', KEYS[1])
return 1
