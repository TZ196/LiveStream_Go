-- 核对请求指纹及执行权后，冻结事件或更新原请求的审核、接受状态。
local request=KEYS[1]
local hash,token,action,value,version=unpack(ARGV)
if redis.call('HGET',request,'hash')~=hash then return 0 end
local status=redis.call('HGET',request,'status')
if action=='accept' then
  if redis.call('HGET',request,'event')~=value then return 0 end
  redis.call('HSET',request,'status','ACCEPTED','until',0)
  return 1
end
if status~='PROCESSING' or redis.call('HGET',request,'owner')~=token then return 0 end
if action=='freeze' then
  local prior=redis.call('HGET',request,'event')
  if prior and prior~=value then return 0 end
  redis.call('HSET',request,'event',value,'version',version)
elseif action=='reject' then
  if redis.call('HEXISTS',request,'event')==1 then return 0 end
  redis.call('HSET',request,'status','REJECTED','category',value,'version',version,'until',0)
elseif action=='release' then
  redis.call('HSET',request,'until',0)
end
return 1
