-- 在一次原子操作中核对原请求，并登记本用户在本房间的新请求与滑动窗口额度。
local request,rate,ledger,identity=unpack(KEYS)
local hash,token,expires,event_time,generation=unpack(ARGV)
if redis.call('GET',identity)~=generation then return {'RESET','','',''} end
local t=redis.call('TIME')
local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
local cutoff=tonumber(string.match(generation,':(%d+)$'))
for _,item in ipairs({{request,'hash'},{rate,'zset'},{ledger,'zset'}}) do
  local kind=redis.call('TYPE',item[1]).ok
  if kind~='none' and kind~=item[2] then return redis.error_reply('DANMAKU_STATE_TYPE') end
end
if redis.call('EXISTS',request)==1 then
  if redis.call('HGET',request,'hash')~=hash then return {'CONFLICT','','',''} end
  local status=redis.call('HGET',request,'status')
  local event=redis.call('HGET',request,'event') or ''
  if status=='ACCEPTED' or status=='REJECTED' then
    return {status,event,redis.call('HGET',request,'category') or '',redis.call('HGET',request,'version') or ''}
  end
  if status~='PROCESSING' then return {'RESET','','',''} end
  if event=='' and tonumber(event_time)<=cutoff then return {'UNKNOWN','','',''} end
  if tonumber(redis.call('HGET',request,'until') or '0')>now then return {'BUSY','','',''} end
  redis.call('HSET',request,'owner',token,'until',now+15000)
  return {'OWNED',event,'',''}
end
if tonumber(event_time)<=cutoff then return {'UNKNOWN','','',''} end
if now-tonumber(event_time)>86400000 then return {'EXPIRED','','',''} end
redis.call('ZREMRANGEBYSCORE',rate,'-inf',now-10000)
if redis.call('ZCARD',rate)>=10 then return {'LIMITED','','',''} end
redis.call('ZREMRANGEBYSCORE',ledger,'-inf',now)
if redis.call('ZCARD',ledger)>=100000 then return {'FULL','','',''} end
redis.call('ZADD',rate,now,request)
redis.call('PEXPIRE',rate,20000)
redis.call('HSET',request,'hash',hash,'status','PROCESSING','owner',token,'until',now+15000)
redis.call('PEXPIREAT',request,expires)
redis.call('ZADD',ledger,expires,request)
return {'OWNED','','',''}
