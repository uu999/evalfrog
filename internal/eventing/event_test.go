package eventing

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRuntimeEventContractIsStrictVersionedAndLightweight(t *testing.T) {
	event := testEvent()
	payload, err := event.MarshalJSONMessage()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseRuntimeEvent(payload)
	if err != nil || parsed.EventID != event.EventID {
		t.Fatalf("parsed=%+v err=%v", parsed, err)
	}
	for _, payload := range [][]byte{
		[]byte(`{"message_version":2}`),
		[]byte(`{"message_version":1,"unknown":true}`),
		[]byte(`{"message_version":1,"event_id":"event","project_id":"project","run_id":"run","aggregate_type":"workflow_run","aggregate_id":"other","event_type":"run.created","occurred_at":"2026-01-01T00:00:00Z","trace_id":"trace"}`),
	} {
		if _, err := ParseRuntimeEvent(payload); err == nil {
			t.Fatalf("invalid event accepted: %s", payload)
		}
	}
	for _, eventType := range []RuntimeEventType{AttemptCompleted, AttemptLost, RetryDue} {
		attemptEvent := event
		attemptEvent.EventType = eventType
		attemptEvent.AggregateType = NodeAttemptAggregate
		attemptEvent.AggregateID = "attempt"
		if err := attemptEvent.Validate(); err != nil {
			t.Fatalf("valid %s rejected: %v", eventType, err)
		}
	}
	invalid := event
	invalid.EventType = "unknown"
	if err := invalid.Validate(); err == nil {
		t.Fatal("unknown event type accepted")
	}
	invalid = event
	invalid.EventID = ""
	if _, err := invalid.MarshalJSONMessage(); err == nil {
		t.Fatal("invalid event marshaled")
	}
}

func TestRelayIsAtLeastOnceAndReleasesPublishFailure(t *testing.T) {
	repository := &fakeOutbox{claimed: []ClaimedEvent{{Event: testEvent(), ClaimToken: "claim"}}}
	publisher := &fakePublisher{err: errors.New("publish failed")}
	relay, err := NewRelay(repository, publisher, "relay", 10, time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if count, relayErr := relay.RelayOnce(context.Background()); count != 0 || relayErr == nil || repository.released != 1 {
		t.Fatalf("count=%d err=%v released=%d", count, relayErr, repository.released)
	}
	publisher.err = nil
	if count, relayErr := relay.RelayOnce(context.Background()); count != 1 || relayErr != nil || repository.marked != 1 {
		t.Fatalf("count=%d err=%v marked=%d", count, relayErr, repository.marked)
	}
}

func TestRelayValidatesConstructionAndPropagatesRepositoryFailures(t *testing.T) {
	if _, err := NewRelay(nil, &fakePublisher{}, "relay", 1, time.Second, 0); err == nil {
		t.Fatal("relay without repository accepted")
	}
	repository := &fakeOutbox{claimErr: errors.New("claim failed")}
	relay, _ := NewRelay(repository, &fakePublisher{}, "relay", 1, time.Second, 0)
	if _, err := relay.RelayOnce(context.Background()); err == nil {
		t.Fatal("claim failure was hidden")
	}
	repository.claimErr = nil
	repository.claimed = []ClaimedEvent{{Event: testEvent(), ClaimToken: "claim"}}
	repository.markErr = errors.New("mark failed")
	if count, err := relay.RelayOnce(context.Background()); count != 0 || err == nil {
		t.Fatalf("count=%d mark error=%v", count, err)
	}
}

type fakeOutbox struct {
	claimed  []ClaimedEvent
	marked   int
	released int
	claimErr error
	markErr  error
}

func (repository *fakeOutbox) ClaimOutbox(context.Context, string, int, time.Duration) ([]ClaimedEvent, error) {
	return repository.claimed, repository.claimErr
}
func (repository *fakeOutbox) MarkOutboxPublished(context.Context, string, string) error {
	repository.marked++
	return repository.markErr
}
func (repository *fakeOutbox) ReleaseOutboxClaim(context.Context, string, string, time.Duration) error {
	repository.released++
	return nil
}

type fakePublisher struct{ err error }

func (publisher *fakePublisher) PublishRuntimeEvent(context.Context, RuntimeEvent) error {
	return publisher.err
}

func testEvent() RuntimeEvent {
	return RuntimeEvent{
		MessageVersion: 1, EventID: "event", ProjectID: "project", RunID: "run",
		AggregateType: WorkflowRunAggregate, AggregateID: "run", EventType: RunCreated,
		OccurredAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), TraceID: "trace",
	}
}
