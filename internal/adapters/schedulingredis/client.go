package schedulingredis

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/scheduling"
)

type Client struct {
	client *redis.Client
	prefix string
}

func Open(value config.RedisEndpointConfig) *Client {
	return &Client{client: redis.NewClient(&redis.Options{
		Addr: value.Address, Password: value.Password, DB: value.DB,
		DialTimeout: value.OperationTimeout.Duration(), ReadTimeout: value.OperationTimeout.Duration(),
		WriteTimeout: value.OperationTimeout.Duration(), PoolTimeout: value.OperationTimeout.Duration(),
		DialerRetries: 1, DialerRetryTimeout: value.OperationTimeout.Duration(),
		ContextTimeoutEnabled: true, MaxRetries: value.MaxRetries,
	}), prefix: value.KeyPrefix}
}

func (client *Client) Check(ctx context.Context) error {
	if err := client.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Scheduling Redis ping: %w", err)
	}
	return nil
}

func (client *Client) Close() error { return client.client.Close() }

var registerWorkerScript = redis.NewScript(`
local now = redis.call('TIME')
local expires = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000) + tonumber(ARGV[3])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[4])
redis.call('ZADD', KEYS[3], expires, ARGV[1])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]) * 2)
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[3]) * 2)
redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[3]) * 2)
return expires
`)

func (client *Client) RegisterWorker(ctx context.Context, registration scheduling.WorkerRegistration) error {
	if err := registration.Validate(); err != nil {
		return err
	}
	tag := "{workers:" + string(registration.ResourceClass) + "}"
	_, err := registerWorkerScript.Run(ctx, client.client,
		[]string{client.prefix + tag + ":slots", client.prefix + tag + ":capabilities", client.prefix + tag + ":expiry"},
		registration.WorkerID, registration.Slots, registration.TTL.Milliseconds(),
		scheduling.CapabilityFingerprint(registration.ResourceClass)).Result()
	return err
}

var capacityScript = redis.NewScript(`
local now = redis.call('TIME')
local millis = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', millis)
if #expired > 0 then
  redis.call('ZREM', KEYS[3], unpack(expired))
  redis.call('HDEL', KEYS[1], unpack(expired))
  redis.call('HDEL', KEYS[2], unpack(expired))
end
local active = redis.call('ZRANGEBYSCORE', KEYS[3], '(' .. millis, '+inf')
local total = 0
for _, worker in ipairs(active) do
  if redis.call('HGET', KEYS[2], worker) == ARGV[1] then
    total = total + tonumber(redis.call('HGET', KEYS[1], worker) or '0')
  end
end
return total
`)

func (client *Client) HealthyCapacity(ctx context.Context) (scheduling.Capacity, error) {
	result := scheduling.Capacity{Pools: make(map[scheduling.ResourceClass]int, 2)}
	for _, class := range []scheduling.ResourceClass{scheduling.ResourceBuiltin, scheduling.ResourceSandbox} {
		tag := "{workers:" + string(class) + "}"
		value, err := capacityScript.Run(ctx, client.client,
			[]string{client.prefix + tag + ":slots", client.prefix + tag + ":capabilities", client.prefix + tag + ":expiry"},
			scheduling.CapabilityFingerprint(class)).Int()
		if err != nil {
			return scheduling.Capacity{}, err
		}
		result.Pools[class] = value
	}
	return result, nil
}

var _ scheduling.CapacityRegistry = (*Client)(nil)

// ClearDerivedState deletes only keys under this profile-specific Scheduling
// prefix. Admission then remains fail-closed until a full PostgreSQL rebuild.
func (client *Client) ClearDerivedState(ctx context.Context) error {
	var cursor uint64
	for {
		keys, next, err := client.client.Scan(ctx, cursor, client.prefix+"*", 256).Result()
		if err != nil {
			return err
		}
		if len(keys) != 0 {
			if err = client.client.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

var acquireLeaseScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current and string.sub(current, 1, string.len(ARGV[1]) + 1) ~= ARGV[1] .. '|' then
  return {0, current}
end
local now = redis.call('TIME')
local wallFence = tonumber(now[1]) * 1000000 + tonumber(now[2])
local previousFence = tonumber(redis.call('GET', KEYS[2]) or '0')
local fence = math.max(previousFence + 1, wallFence)
redis.call('SET', KEYS[2], string.format('%.0f', fence))
local expires = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000) + tonumber(ARGV[3])
redis.call('PSETEX', KEYS[1], ARGV[3], ARGV[1] .. '|' .. ARGV[2])
return {fence, expires}
`)

func (client *Client) AcquireBalancerLease(ctx context.Context, owner string, duration time.Duration) (scheduling.BalancerLease, error) {
	if owner == "" || duration <= 0 {
		return scheduling.BalancerLease{}, fmt.Errorf("balancer owner and positive lease are required")
	}
	token, err := uuid.NewV7()
	if err != nil {
		return scheduling.BalancerLease{}, err
	}
	value, err := acquireLeaseScript.Run(ctx, client.client,
		[]string{client.prefix + "{balancer}:lease", client.prefix + "{balancer}:fence"},
		owner, token.String(), duration.Milliseconds()).Slice()
	if err != nil {
		return scheduling.BalancerLease{}, err
	}
	fence, err := integer(value[0])
	if err != nil {
		return scheduling.BalancerLease{}, err
	}
	if fence == 0 {
		return scheduling.BalancerLease{}, scheduling.ErrLeaseLost
	}
	expires, err := integer(value[1])
	if err != nil {
		return scheduling.BalancerLease{}, err
	}
	return scheduling.BalancerLease{Owner: owner, Token: token.String(), FencingToken: uint64(fence), ExpiresAt: time.UnixMilli(expires).UTC()}, nil
}

var boundWindowsScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] .. '|' .. ARGV[2] then return {} end
local storedFence = tonumber(redis.call('HGET', KEYS[2], 'fence') or '0')
if storedFence > tonumber(ARGV[3]) then return {} end
local function bound(previous, desired, limit)
  if not previous or tonumber(previous) <= 0 then return tonumber(desired) end
  previous = tonumber(previous)
  desired = tonumber(desired)
	if desired <= previous then return desired end
  local delta = math.max(1, math.ceil(previous * tonumber(limit)))
	return math.min(desired, previous + delta)
end
local global = bound(redis.call('HGET', KEYS[2], 'global'), ARGV[4], ARGV[5])
local result = {global}
local poolTotal = 0
local count = tonumber(ARGV[6])
local index = 7
for item = 1, count do
  local class = ARGV[index]; index = index + 1
  local desired = ARGV[index]; index = index + 1
  local value = bound(redis.call('HGET', KEYS[2], 'pool:' .. class), desired, ARGV[5])
  redis.call('HSET', KEYS[2], 'pool:' .. class, value)
  poolTotal = poolTotal + value
  table.insert(result, class)
  table.insert(result, value)
end
global = math.min(global, poolTotal)
result[1] = global
redis.call('HSET', KEYS[2], 'fence', ARGV[3], 'global', global)
return result
`)

func (client *Client) BoundWindows(ctx context.Context, lease scheduling.BalancerLease, desired scheduling.Windows, limit float64) (scheduling.Windows, error) {
	classes := make([]string, 0, len(desired.Pools))
	for class := range desired.Pools {
		classes = append(classes, string(class))
	}
	sort.Strings(classes)
	args := []any{lease.Owner, lease.Token, lease.FencingToken, desired.Global, limit, len(classes)}
	for _, class := range classes {
		args = append(args, class, desired.Pools[scheduling.ResourceClass(class)])
	}
	values, err := boundWindowsScript.Run(ctx, client.client, []string{client.prefix + "{balancer}:lease", client.prefix + "{balancer}:windows"}, args...).Slice()
	if err != nil {
		return scheduling.Windows{}, err
	}
	if len(values) == 0 {
		return scheduling.Windows{}, scheduling.ErrLeaseLost
	}
	global, err := integer(values[0])
	if err != nil {
		return scheduling.Windows{}, err
	}
	result := scheduling.Windows{Global: int(global), Pools: map[scheduling.ResourceClass]int{}}
	for index := 1; index+1 < len(values); index += 2 {
		value, valueErr := integer(values[index+1])
		if valueErr != nil {
			return scheduling.Windows{}, valueErr
		}
		result.Pools[scheduling.ResourceClass(fmt.Sprint(values[index]))] = int(value)
	}
	return result, nil
}

var pauseScript = redis.NewScript(`
local existing = tonumber(redis.call('HGET', KEYS[1], 'fence') or '0')
local incoming = tonumber(ARGV[3])
local now = redis.call('TIME')
local nowms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if existing > incoming or tonumber(ARGV[4]) <= nowms then return 0 end
redis.call('HSET', KEYS[1], 'owner', ARGV[1], 'token', ARGV[2], 'fence', ARGV[3], 'expires_at', ARGV[4], 'paused', 1)
redis.call('DEL', KEYS[2], KEYS[3])
return 1
`)

func (client *Client) PauseAdmissions(ctx context.Context, lease scheduling.BalancerLease, lane int) error {
	keys := client.laneKeys(lane)
	applied, err := pauseScript.Run(ctx, client.client, []string{keys.meta, keys.credits, keys.grants}, lease.Owner, lease.Token, lease.FencingToken, lease.ExpiresAt.UnixMilli()).Int()
	if err != nil {
		return err
	}
	if applied != 1 {
		return scheduling.ErrLeaseLost
	}
	return nil
}

func (client *Client) ListReservations(ctx context.Context, lane int) ([]scheduling.Reservation, error) {
	keys := client.laneKeys(lane)
	rawValues, err := listReservationsScript.Run(ctx, client.client, []string{keys.reservations, keys.reservationExpiry}).StringSlice()
	if err != nil {
		return nil, err
	}
	result := make([]scheduling.Reservation, 0, len(rawValues))
	for _, raw := range rawValues {
		var reservation scheduling.Reservation
		if err = json.Unmarshal([]byte(raw), &reservation); err != nil {
			return nil, fmt.Errorf("decode scheduling reservation: %w", err)
		}
		result = append(result, reservation)
	}
	return result, nil
}

var listReservationsScript = redis.NewScript(`
local now = redis.call('TIME')
local nowms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', nowms)
for _, attemptID in ipairs(expired) do redis.call('HDEL', KEYS[1], attemptID) end
if #expired > 0 then redis.call('ZREM', KEYS[2], unpack(expired)) end
return redis.call('HVALS', KEYS[1])
`)

var rebuildScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'token') ~= ARGV[2] or
   tonumber(redis.call('HGET', KEYS[1], 'fence') or '0') ~= tonumber(ARGV[3]) or
   redis.call('HGET', KEYS[1], 'paused') ~= '1' then return 0 end
local now = redis.call('TIME')
local nowms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0') <= nowms then return 0 end
redis.call('DEL', KEYS[2], KEYS[3], KEYS[4], KEYS[5])
local expiry = nowms + tonumber(ARGV[4])
local reservationTTL = tonumber(ARGV[5])
local keepCount = tonumber(ARGV[6])
local index = 7
local keep = {}
for keepIndex = 1, keepCount do
  keep[ARGV[index]] = true
  index = index + 1
end
local renewCount = tonumber(ARGV[index]); index = index + 1
local renew = {}
for renewIndex = 1, renewCount do
  renew[ARGV[index]] = true
  index = index + 1
end
local reservationIDs = redis.call('HKEYS', KEYS[6])
for _, attemptID in ipairs(reservationIDs) do
  if keep[attemptID] then
    if renew[attemptID] then
      local raw = redis.call('HGET', KEYS[6], attemptID)
      if raw then
        local reservation = cjson.decode(raw)
        reservation.confirmed = true
        redis.call('HSET', KEYS[6], attemptID, cjson.encode(reservation))
      end
      redis.call('ZADD', KEYS[7], nowms + reservationTTL, attemptID)
    end
  else
    redis.call('HDEL', KEYS[6], attemptID)
    redis.call('ZREM', KEYS[7], attemptID)
  end
end
local projectCount = tonumber(ARGV[index]); index = index + 1
for projectIndex = 1, projectCount do
  local project = ARGV[index]; index = index + 1
  local queue = ARGV[index]; index = index + 1
  redis.call('ZADD', KEYS[3], expiry, project)
  redis.call('HSET', KEYS[2], project, queue)
end
return 1
`)

func (client *Client) RebuildLane(ctx context.Context, lease scheduling.BalancerLease, state scheduling.LaneState, activeTTL, reservationTTL time.Duration) error {
	keys := client.laneKeys(state.Lane)
	groups := make(map[string][]scheduling.Candidate)
	projects := make([]string, 0)
	for _, candidate := range state.Candidates {
		if _, exists := groups[candidate.ProjectID]; !exists {
			projects = append(projects, candidate.ProjectID)
		}
		groups[candidate.ProjectID] = append(groups[candidate.ProjectID], candidate)
	}
	sort.Strings(projects)
	keep := make([]string, 0, len(state.KeepReservations))
	for attemptID := range state.KeepReservations {
		keep = append(keep, attemptID)
	}
	sort.Strings(keep)
	renew := make([]string, 0, len(state.RenewReservations))
	for attemptID := range state.RenewReservations {
		renew = append(renew, attemptID)
	}
	sort.Strings(renew)
	args := []any{lease.Owner, lease.Token, lease.FencingToken, activeTTL.Milliseconds(), reservationTTL.Milliseconds(), len(keep)}
	for _, attemptID := range keep {
		args = append(args, attemptID)
	}
	args = append(args, len(renew))
	for _, attemptID := range renew {
		args = append(args, attemptID)
	}
	args = append(args, len(projects))
	for _, projectID := range projects {
		encoded, err := json.Marshal(groups[projectID])
		if err != nil {
			return err
		}
		args = append(args, projectID, encoded)
	}
	applied, err := rebuildScript.Run(ctx, client.client, []string{keys.meta, keys.ready, keys.active, keys.credits, keys.grants, keys.reservations, keys.reservationExpiry}, args...).Int()
	if err != nil {
		return err
	}
	if applied != 1 {
		return scheduling.ErrLeaseLost
	}
	return nil
}

var grantScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'token') ~= ARGV[2] or
   tonumber(redis.call('HGET', KEYS[1], 'fence') or '0') ~= tonumber(ARGV[3]) or
   redis.call('HGET', KEYS[1], 'paused') ~= '1' then return 0 end
local now = redis.call('TIME')
local nowms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0') <= nowms then return 0 end
local count = tonumber(ARGV[4])
local index = 5
for item = 1, count do
  local project = ARGV[index]; index = index + 1
  redis.call('HINCRBY', KEYS[2], project, 1)
  redis.call('RPUSH', KEYS[3], project)
end
return 1
`)

func (client *Client) Grant(ctx context.Context, lease scheduling.BalancerLease, lane int, values []scheduling.PlannedAdmission) error {
	keys := client.laneKeys(lane)
	args := []any{lease.Owner, lease.Token, lease.FencingToken, len(values)}
	for _, value := range values {
		args = append(args, value.ProjectID)
	}
	applied, err := grantScript.Run(ctx, client.client, []string{keys.meta, keys.credits, keys.grants}, args...).Int()
	if err != nil {
		return err
	}
	if applied != 1 {
		return scheduling.ErrLeaseLost
	}
	return nil
}

var activateScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] or
   redis.call('HGET', KEYS[1], 'token') ~= ARGV[2] or
   tonumber(redis.call('HGET', KEYS[1], 'fence') or '0') ~= tonumber(ARGV[3]) then return 0 end
local now = redis.call('TIME')
local nowms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0') <= nowms then return 0 end
redis.call('HSET', KEYS[1], 'epoch', ARGV[4], 'paused', 0)
return 1
`)

func (client *Client) Activate(ctx context.Context, lease scheduling.BalancerLease, lane int, epoch uint64) error {
	keys := client.laneKeys(lane)
	applied, err := activateScript.Run(ctx, client.client, []string{keys.meta}, lease.Owner, lease.Token, lease.FencingToken, epoch).Int()
	if err != nil {
		return err
	}
	if applied != 1 {
		return scheduling.ErrLeaseLost
	}
	return nil
}

var reserveScript = redis.NewScript(`
local now = redis.call('TIME')
local nowms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if redis.call('HGET', KEYS[1], 'paused') ~= '0' or
   tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0') <= nowms then return {'__PAUSED__'} end
local existing = redis.call('HGET', KEYS[5], ARGV[3])
if existing then
  redis.call('ZADD', KEYS[7], nowms + tonumber(ARGV[5]), ARGV[3])
  return {existing}
end
while true do
  local project = redis.call('LPOP', KEYS[2])
  if not project then return {} end
  local credit = tonumber(redis.call('HGET', KEYS[3], project) or '0')
  local activeUntil = tonumber(redis.call('ZSCORE', KEYS[4], project) or '0')
  if credit > 0 and activeUntil >= nowms then
    local rawQueue = redis.call('HGET', KEYS[6], project)
    if rawQueue then
      local queue = cjson.decode(rawQueue)
      local candidate = table.remove(queue, 1)
      redis.call('HINCRBY', KEYS[3], project, -1)
      local decoded = candidate
      local reservation = cjson.encode({attempt_id=ARGV[3], project_id=decoded.project_id, lane=tonumber(ARGV[1]), resource_class=decoded.resource_class, candidate=decoded, epoch=redis.call('HGET', KEYS[1], 'epoch')})
      redis.call('HSET', KEYS[5], ARGV[3], reservation)
      redis.call('ZADD', KEYS[7], nowms + tonumber(ARGV[5]), ARGV[3])
      if #queue == 0 then
        redis.call('HDEL', KEYS[6], project)
        redis.call('ZREM', KEYS[4], project)
      else
        redis.call('HSET', KEYS[6], project, cjson.encode(queue))
      end
      return {reservation}
    end
  end
end
`)

func (client *Client) ReserveNext(ctx context.Context, lane int, attemptID string, ttl time.Duration) (scheduling.Reservation, bool, error) {
	keys := client.laneKeys(lane)
	values, err := reserveScript.Run(ctx, client.client,
		[]string{keys.meta, keys.grants, keys.credits, keys.active, keys.reservations, keys.ready, keys.reservationExpiry},
		lane, "", attemptID, "", ttl.Milliseconds()).Slice()
	if err != nil {
		return scheduling.Reservation{}, false, err
	}
	if len(values) == 0 {
		return scheduling.Reservation{}, false, nil
	}
	if fmt.Sprint(values[0]) == "__PAUSED__" {
		return scheduling.Reservation{}, false, scheduling.ErrAdmissionPaused
	}
	var reservation scheduling.Reservation
	if err = json.Unmarshal([]byte(fmt.Sprint(values[0])), &reservation); err != nil {
		return scheduling.Reservation{}, false, err
	}
	return reservation, true, nil
}

var abortScript = redis.NewScript(`
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then
  redis.call('ZREM', KEYS[2], ARGV[1])
  return 0
end
local reservation = cjson.decode(raw)
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
if ARGV[2] == '1' and redis.call('HGET', KEYS[3], 'paused') == '0' and
   (redis.call('HGET', KEYS[3], 'epoch') or '0') == reservation.epoch then
  local rawQueue = redis.call('HGET', KEYS[6], reservation.project_id)
  local queue = {}
  if rawQueue then queue = cjson.decode(rawQueue) end
  table.insert(queue, 1, reservation.candidate)
  redis.call('HSET', KEYS[6], reservation.project_id, cjson.encode(queue))
  redis.call('HINCRBY', KEYS[4], reservation.project_id, 1)
  redis.call('LPUSH', KEYS[5], reservation.project_id)
end
return 1
`)

func (client *Client) AbortReservation(ctx context.Context, reservation scheduling.Reservation, restore bool) error {
	keys := client.laneKeys(reservation.Lane)
	flag := 0
	if restore {
		flag = 1
	}
	return abortScript.Run(ctx, client.client,
		[]string{keys.reservations, keys.reservationExpiry, keys.meta, keys.credits, keys.grants, keys.ready},
		reservation.AttemptID, flag).Err()
}

var confirmScript = redis.NewScript(`
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local reservation = cjson.decode(raw)
reservation.confirmed = true
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(reservation))
local now = redis.call('TIME')
local nowms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
redis.call('ZADD', KEYS[2], nowms + tonumber(ARGV[2]), ARGV[1])
return 1
`)

func (client *Client) ConfirmReservation(ctx context.Context, reservation scheduling.Reservation, ttl time.Duration) error {
	keys := client.laneKeys(reservation.Lane)
	_, err := confirmScript.Run(ctx, client.client, []string{keys.reservations, keys.reservationExpiry}, reservation.AttemptID, ttl.Milliseconds()).Int()
	return err
}

type laneKeys struct {
	prefix, tag, meta, ready, active, credits, grants, reservations, reservationExpiry string
}

func (client *Client) laneKeys(lane int) laneKeys {
	tag := "{lane-" + strconv.Itoa(lane) + "}:"
	base := client.prefix + tag
	return laneKeys{prefix: client.prefix, tag: tag, meta: base + "meta", ready: base + "ready", active: base + "active-projects", credits: base + "credits", grants: base + "grants", reservations: base + "reservations", reservationExpiry: base + "reservation-expiry"}
}

func integer(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected Redis integer %T", value)
	}
}

var _ scheduling.CoordinationStore = (*Client)(nil)
