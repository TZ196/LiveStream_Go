-- 只允许仍持有恢复锁的任务写入短期否定结果，避免旧任务覆盖新恢复结果。
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PSETEX', KEYS[2], 10000, 'missing')
return 1
