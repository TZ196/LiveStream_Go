package danmaku

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RequestStore 的记录是准入依据，不将 PROCESSING 当成 Kafka 接受证明。
type RequestStore struct {
	client     *redis.Client
	mu         sync.RWMutex
	generation string
	cutoff     int64
}
type requestState struct{ Hash, Status, Event, Category, Version string }

const identityKey = "danmaku:requests:identity"

func NewRequestStore(client *redis.Client) *RequestStore { return &RequestStore{client: client} }
func requestKey(user, request string) string             { return "danmaku:request:" + user + ":" + request }

// Check 在 Redis 换代或请求账本丢失时建立保护时间。旧记录缺失不能被当作首次请求。
func (store *RequestStore) Check(ctx context.Context, historical bool) error {
	info, err := store.client.Info(ctx, "server").Result()
	if err != nil {
		return err
	}
	run := ""
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "run_id:") {
			run = strings.TrimSpace(strings.TrimPrefix(line, "run_id:"))
		}
	}
	if run == "" {
		return errors.New("无法识别弹幕 Redis 实例")
	}
	raw, err := store.client.Get(ctx, identityKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !strings.HasPrefix(raw, run+":") {
		clock, err := store.client.Time(ctx).Result()
		if err != nil {
			return err
		}
		cutoff := int64(0)
		if historical || raw != "" || store.generation != "" {
			// 覆盖客户端允许的五分钟未来偏差；保护期内保留草稿，旧请求仍可核对。
			cutoff = clock.Add(5 * time.Minute).UnixMilli()
		}
		next := run + ":" + strconv.FormatInt(cutoff, 10)
		changed, err := store.client.Eval(ctx, `local old=redis.call('GET',KEYS[1]); if (old or '')~=ARGV[1] then return 0 end; redis.call('SET',KEYS[1],ARGV[2]); return 1`, []string{identityKey}, raw, next).Int()
		if err != nil {
			return err
		}
		if changed != 1 {
			return errors.New("弹幕请求账本正在重新核对")
		}
		raw = next
	}
	cutoff, err := strconv.ParseInt(strings.TrimPrefix(raw, run+":"), 10, 64)
	if err != nil || cutoff < 0 {
		return errors.New("弹幕请求账本代次无效")
	}
	store.generation, store.cutoff = raw, cutoff
	return nil
}
func (store *RequestStore) NewAllowed() bool {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.generation != "" && time.Now().UnixMilli() > store.cutoff
}

func (store *RequestStore) Lookup(ctx context.Context, user, request string) (requestState, error) {
	data, err := store.client.HGetAll(ctx, requestKey(user, request)).Result()
	if err != nil {
		return requestState{}, err
	}
	return requestState{Hash: data["hash"], Status: data["status"], Event: data["event"], Category: data["category"], Version: data["version"]}, nil
}

var beginRequest = redis.NewScript(`
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
`)

func (store *RequestStore) Begin(ctx context.Context, event Event, token string) ([]string, error) {
	store.mu.RLock()
	generation := store.generation
	store.mu.RUnlock()
	return beginRequest.Run(ctx, store.client, []string{requestKey(event.UserID, event.RequestID), "danmaku:rate:" + event.UserID, "danmaku:requests:ledger", identityKey},
		event.RequestHash, token, event.Timestamp+int64(25*time.Hour/time.Millisecond), event.Timestamp, generation).StringSlice()
}

var changeRequest = redis.NewScript(`
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
`)

func (store *RequestStore) Change(ctx context.Context, event Event, token, action, value, version string) error {
	changed, err := changeRequest.Run(ctx, store.client, []string{requestKey(event.UserID, event.RequestID)}, event.RequestHash, token, action, value, version).Int()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("弹幕请求执行权或冻结记录已改变")
	}
	return nil
}

func (store *RequestStore) Uncertain(timestamp int64) bool {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.cutoff > 0 && timestamp <= store.cutoff
}

// ValidateRealtime 只核对本业务的 Topic 与处理位置；广播没有需要重算的点赞数。
func (store *RequestStore) ValidateRealtime(ctx context.Context, topicID string, committed map[int32]int64) error {
	identity := "danmaku:realtime:topic"
	if err := store.client.SetNX(ctx, identity, topicID, 0).Err(); err != nil {
		return err
	}
	actual, err := store.client.Get(ctx, identity).Result()
	if err != nil {
		return err
	}
	if actual != topicID {
		return ErrBlocked
	}
	for partition, offset := range committed {
		raw, err := store.client.HGet(ctx, "danmaku:realtime:"+topicID, strconv.Itoa(int(partition))).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return err
		}
		saved, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || saved < 0 || (offset < 0 && saved > 0) {
			return ErrBlocked
		}
	}
	return nil
}

var markRealtime = redis.NewScript(`
local current=redis.call('HGET',KEYS[1],ARGV[1])
local next=ARGV[2]
if current and not string.match(current,'^%d+$') then return redis.error_reply('INVALID_POSITION') end
if not current or #next>#current or (#next==#current and next>current) then redis.call('HSET',KEYS[1],ARGV[1],next) end
return 1
`)

func (store *RequestStore) MarkRealtime(ctx context.Context, record Record) error {
	return markRealtime.Run(ctx, store.client, []string{"danmaku:realtime:" + record.TopicID}, strconv.Itoa(int(record.Partition)), strconv.FormatInt(record.NextOffset, 10)).Err()
}
