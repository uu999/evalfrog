// Package rpc executes registered Service Operations through a narrow
// discovery/invocation port. Protocol details stay outside the IR and DSL.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/resources"
	platformruntime "github.com/uu999/evalfrog/internal/runtime"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
)

type Invoker interface {
	Invoke(context.Context, resources.ServiceOperationRuntime, json.RawMessage, string) (json.RawMessage, error)
}

type Executor struct {
	Resolver resources.RuntimeResolver
	Invoker  Invoker
}

func NewExecutor(resolver resources.RuntimeResolver, invoker Invoker) Executor {
	return Executor{Resolver: resolver, Invoker: invoker}
}
func (executor Executor) Coordinate() dsl.Coordinate {
	return dsl.Coordinate{Type: "task.rpc", Version: 1}
}

func (executor Executor) Execute(ctx context.Context, value runtimecontext.ExecutionContext) platformruntime.AttemptResult {
	if executor.Resolver == nil || executor.Invoker == nil {
		return failed("RPC_SERVICE_DISCOVERY_ERROR", "RPC runtime ports are unavailable")
	}
	serviceID, ok := stringConfig(value.Operation.Config, "service_id")
	if !ok {
		return failed("RPC_SERVICE_NOT_FOUND", "registered service is unavailable")
	}
	operation, ok := stringConfig(value.Operation.Config, "operation")
	if !ok {
		return failed("RPC_OPERATION_NOT_FOUND", "registered operation is unavailable")
	}
	contract, _ := stringConfig(value.Operation.Config, "contract_revision")
	runtime, err := executor.Resolver.ResolveServiceOperation(ctx, resources.RuntimeResolveCommand{
		ProjectID: value.ProjectID, RunID: value.RunID, AttemptID: value.AttemptID,
		AttemptSequence: value.AttemptSequence, LeaseToken: value.LeaseToken,
		FencingToken: value.FencingToken, ServiceID: serviceID, Operation: operation,
		ContractRevision: contract,
	})
	if err != nil {
		return failed("RPC_FORBIDDEN", "registered operation is unavailable")
	}
	request := json.RawMessage(`{}`)
	if raw, exists := value.Inputs["request"]; exists {
		if !json.Valid(raw) {
			return failed("RPC_REQUEST_SCHEMA_MISMATCH", "request is not valid JSON")
		}
		request = raw
	}
	key := value.RunID + ":" + value.ExecutionNodeID
	response, err := executor.Invoker.Invoke(ctx, runtime, request, key)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return failed("RPC_DEADLINE_EXCEEDED", "RPC deadline exceeded")
		}
		return failed("RPC_TRANSPORT_ERROR", "RPC invocation failed")
	}
	if !json.Valid(response) {
		return failed("RPC_RESPONSE_SCHEMA_MISMATCH", "RPC response is not valid JSON")
	}
	return platformruntime.AttemptResult{State: platformruntime.AttemptSucceeded, Outputs: map[string]json.RawMessage{"response": append(json.RawMessage(nil), response...)}}
}

type JSONInvoker struct{}

func (JSONInvoker) Invoke(ctx context.Context, service resources.ServiceOperationRuntime, request json.RawMessage, key string) (json.RawMessage, error) {
	if service.Protocol != "http-json" || service.DiscoveryReference == "" {
		return nil, fmt.Errorf("RPC service discovery is not configured")
	}
	base, err := url.Parse(service.DiscoveryReference)
	if err != nil || base.Scheme == "" || base.Host == "" || base.User != nil {
		return nil, fmt.Errorf("RPC service discovery reference is invalid")
	}
	target := *base
	target.Path = strings.TrimRight(base.Path, "/") + "/" + url.PathEscape(service.Operation)
	target.RawQuery = ""
	body := bytes.NewReader(request)
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), body)
	if err != nil {
		return nil, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Idempotency-Key", key)
	for name, secret := range service.SecretHeaders {
		httpRequest.Header.Set(name, secret)
	}
	response, err := (&http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(httpRequest)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	value, err := io.ReadAll(io.LimitReader(response.Body, 1<<20+1))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("RPC remote status %d", response.StatusCode)
	}
	if len(value) > 1<<20 || !json.Valid(value) {
		return nil, fmt.Errorf("RPC response is invalid")
	}
	return json.RawMessage(value), nil
}

func stringConfig(values map[string]json.RawMessage, key string) (string, bool) {
	var value string
	raw, ok := values[key]
	return value, ok && json.Unmarshal(raw, &value) == nil && value != ""
}
func failed(code, message string) platformruntime.AttemptResult {
	return platformruntime.AttemptResult{State: platformruntime.AttemptFailed, ErrorCode: code, Message: message}
}
