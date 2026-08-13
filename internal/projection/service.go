package projection

import (
	"context"
	"encoding/json"
	"time"

	"github.com/uu999/evalfrog/internal/access"
)

// Cache is deliberately best-effort. A cache failure cannot fail a read: the
// service reloads PostgreSQL authority and opportunistically repopulates it.
type Cache interface {
	GetRunView(context.Context, string) (json.RawMessage, bool)
	PutRunView(context.Context, string, json.RawMessage, time.Duration)
	DeleteRunView(context.Context, string)
}

// Notifier is intentionally lossy. Subscribers must always read the current
// Run view after connecting or reconnecting; a Redis Pub/Sub message is only
// a latency optimization, never a durable Runtime event.
type Notifier interface {
	PublishRunUpdate(context.Context, string)
}

type CachedService struct {
	Service
	cache       Cache
	activeTTL   time.Duration
	terminalTTL time.Duration
}

func NewCachedService(service Service, cache Cache, activeTTL, terminalTTL time.Duration) CachedService {
	return CachedService{Service: service, cache: cache, activeTTL: activeTTL, terminalTTL: terminalTTL}
}

func (service CachedService) GetRun(ctx context.Context, projectID, principalID, runID string) (RunView, error) {
	if service.cache != nil {
		if raw, ok := service.cache.GetRunView(ctx, runID); ok {
			var cached RunView
			if json.Unmarshal(raw, &cached) == nil && cached.ProjectID == projectID && cached.RunID == runID {
				// Authorization remains a non-cacheable, current fact.
				if err := service.access.Require(ctx, projectID, principalID, access.PermissionRunRead); err != nil {
					return RunView{}, err
				}
				return cached, nil
			}
		}
	}
	value, err := service.Service.GetRun(ctx, projectID, principalID, runID)
	if err != nil {
		return RunView{}, err
	}
	if service.cache != nil {
		if raw, marshalErr := json.Marshal(value); marshalErr == nil {
			service.cache.PutRunView(ctx, runID, raw, service.ttl(value))
		}
	}
	return value, nil
}

func (service CachedService) ttl(value RunView) time.Duration {
	if value.State.Terminal() {
		return service.terminalTTL
	}
	return service.activeTTL
}
