// Package http executes the deliberately small, managed HTTP Node contract.
// The executor never accepts an absolute URL: the Control Plane resolves a
// Connection and supplies its fixed origin at attempt time.
package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/resources"
	platformruntime "github.com/uu999/evalfrog/internal/runtime"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
)

const defaultBodyLimit = 1 << 20

type Executor struct {
	Resolver resources.RuntimeResolver
	Client   *stdhttp.Client
}

func NewExecutor(resolver resources.RuntimeResolver, client *stdhttp.Client) Executor {
	if client == nil {
		client = &stdhttp.Client{Timeout: 30 * time.Second, CheckRedirect: func(*stdhttp.Request, []*stdhttp.Request) error { return stdhttp.ErrUseLastResponse }}
	}
	return Executor{Resolver: resolver, Client: client}
}

func (executor Executor) Coordinate() dsl.Coordinate {
	return dsl.Coordinate{Type: "task.http", Version: 1}
}

func (executor Executor) Execute(ctx context.Context, value runtimecontext.ExecutionContext) platformruntime.AttemptResult {
	if executor.Resolver == nil {
		return failed("HTTP_RESOURCE_RESOLVER_UNAVAILABLE", "managed HTTP connection is unavailable")
	}
	connectionID, ok := stringConfig(value.Operation.Config, "connection_id")
	if !ok {
		return failed("HTTP_CONNECTION_NOT_FOUND", "managed HTTP connection is unavailable")
	}
	method, ok := stringConfig(value.Operation.Config, "method")
	if !ok || !allowedMethod(method) {
		return failed("HTTP_REQUEST_INVALID", "HTTP method is invalid")
	}
	runtime, err := executor.Resolver.ResolveConnection(ctx, resources.RuntimeResolveCommand{
		ProjectID: value.ProjectID, RunID: value.RunID, AttemptID: value.AttemptID,
		AttemptSequence: value.AttemptSequence, LeaseToken: value.LeaseToken,
		FencingToken: value.FencingToken, ConnectionID: connectionID,
	})
	if err != nil {
		return failed("HTTP_CONNECTION_FORBIDDEN", "managed HTTP connection is unavailable")
	}
	if len(runtime.AllowedMethods) > 0 && !runtime.AllowedMethods[method] {
		return failed("HTTP_METHOD_NOT_ALLOWED", "HTTP method is not allowed by the connection")
	}
	relative, ok := inputString(value.Inputs, "relative_path")
	if !ok {
		return failed("HTTP_INVALID_RELATIVE_PATH", "relative_path is required")
	}
	target, err := resolveRelative(runtime.BaseURL, relative)
	if err != nil {
		return failed("HTTP_INVALID_RELATIVE_PATH", "relative_path must stay within the connection origin")
	}
	if len(runtime.AllowedPathPrefixes) > 0 {
		allowed := false
		for _, prefix := range runtime.AllowedPathPrefixes {
			if strings.HasPrefix(parsedPath(target), prefix) {
				allowed = true
				break
			}
		}
		if !allowed {
			return failed("HTTP_INVALID_RELATIVE_PATH", "relative_path is outside the connection policy")
		}
	}
	if query, exists := value.Inputs["query"]; exists {
		target, err = addQuery(target, query)
		if err != nil {
			return failed("HTTP_REQUEST_INVALID", "query must be a JSON object")
		}
	}
	body := json.RawMessage("null")
	if candidate, exists := value.Inputs["body"]; exists {
		if !json.Valid(candidate) {
			return failed("HTTP_REQUEST_INVALID", "body is not valid JSON")
		}
		body = candidate
	}
	limit := runtime.MaxRequestBytes
	if limit <= 0 {
		limit = defaultBodyLimit
	}
	requestBody := bytes.NewReader(body)
	request, err := stdhttp.NewRequestWithContext(ctx, method, target, requestBody)
	if err != nil {
		return failed("HTTP_REQUEST_INVALID", "HTTP request could not be constructed")
	}
	if headers, exists := value.Inputs["headers"]; exists {
		if err := applyHeaders(request, headers); err != nil {
			return failed("HTTP_PROTECTED_HEADER", "request contains a protected header")
		}
	}
	for name, secret := range runtime.SecretHeaders {
		request.Header.Set(name, secret)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", value.RunID+":"+value.ExecutionNodeID)
	if int64(len(body)) > limit {
		return failed("HTTP_REQUEST_TOO_LARGE", "HTTP request body exceeds the connection limit")
	}
	response, err := executor.Client.Do(request)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return failed("HTTP_TIMEOUT", "HTTP request timed out")
		}
		return failed("HTTP_NETWORK_ERROR", "HTTP request failed")
	}
	defer response.Body.Close()
	responseLimit := runtime.MaxResponseBytes
	if responseLimit <= 0 {
		responseLimit = defaultBodyLimit
	}
	bodyBytes, readErr := io.ReadAll(io.LimitReader(response.Body, responseLimit+1))
	if readErr != nil {
		return failed("HTTP_NETWORK_ERROR", "HTTP response could not be read")
	}
	if int64(len(bodyBytes)) > responseLimit || response.ContentLength > responseLimit {
		return failed("HTTP_RESPONSE_TOO_LARGE", "HTTP response exceeds the connection limit")
	}
	payload := json.RawMessage("null")
	if len(bytes.TrimSpace(bodyBytes)) > 0 {
		payload = append(json.RawMessage(nil), bytes.TrimSpace(bodyBytes)...)
		if !json.Valid(payload) {
			if response.StatusCode >= 300 && response.StatusCode < 400 {
				return failed("HTTP_REDIRECT_FORBIDDEN", "HTTP redirects are not allowed")
			}
			return failed("HTTP_RESPONSE_INVALID_JSON", "HTTP response is not valid JSON")
		}
	}
	accepted := acceptedStatuses(value.Operation.Config, response.StatusCode)
	output := map[string]json.RawMessage{
		"status_code": mustJSON(response.StatusCode),
		"headers":     mustJSON(safeHeaders(response.Header)),
		"body":        payload,
	}
	if !accepted {
		return platformruntime.AttemptResult{State: platformruntime.AttemptFailed, ErrorCode: "HTTP_STATUS_ERROR", Message: "HTTP response status was not accepted"}
	}
	return platformruntime.AttemptResult{State: platformruntime.AttemptSucceeded, Outputs: map[string]json.RawMessage{"response": mustJSON(output)}}
}

func resolveRelative(base, relative string) (string, error) {
	if relative == "" || !strings.HasPrefix(relative, "/") || strings.HasPrefix(relative, "//") || strings.ContainsAny(relative, "\\?#") || strings.Contains(relative, "://") {
		return "", errors.New("invalid relative path")
	}
	parsed, err := url.ParseRequestURI(relative)
	if err != nil || parsed.IsAbs() || parsed.Host != "" {
		return "", errors.New("invalid relative path")
	}
	clean := path.Clean(parsed.Path)
	if clean != parsed.Path || strings.Contains(parsed.Path, "..") {
		return "", errors.New("path escape")
	}
	origin, err := url.Parse(base)
	if err != nil || origin.Scheme == "" || origin.Host == "" || origin.User != nil {
		return "", errors.New("invalid connection origin")
	}
	result := *origin
	result.Path = strings.TrimRight(origin.Path, "/") + parsed.Path
	result.RawQuery = parsed.RawQuery
	result.Fragment = ""
	return result.String(), nil
}

func parsedPath(target string) string {
	value, _ := url.Parse(target)
	if value == nil {
		return ""
	}
	return value.Path
}

func addQuery(target string, raw json.RawMessage) (string, error) {
	var values map[string]any
	if err := json.Unmarshal(raw, &values); err != nil {
		return "", err
	}
	parsed, err := url.Parse(target)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	for key, value := range values {
		switch typed := value.(type) {
		case string:
			query.Set(key, typed)
		case bool:
			query.Set(key, strconv.FormatBool(typed))
		case float64:
			query.Set(key, strconv.FormatFloat(typed, 'f', -1, 64))
		default:
			return "", fmt.Errorf("query value %s is not scalar", key)
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func applyHeaders(request *stdhttp.Request, raw json.RawMessage) error {
	var values map[string]string
	if err := json.Unmarshal(raw, &values); err != nil {
		return err
	}
	for name, value := range values {
		lower := strings.ToLower(name)
		if lower == "host" || lower == "content-length" || lower == "connection" || lower == "transfer-encoding" || lower == "upgrade" || strings.HasPrefix(lower, "proxy-") || lower == "idempotency-key" {
			return errors.New("protected header")
		}
		request.Header.Set(name, value)
	}
	return nil
}

func allowedMethod(value string) bool {
	switch value {
	case "GET", "POST", "PUT", "PATCH", "DELETE":
		return true
	}
	return false
}
func stringConfig(values map[string]json.RawMessage, key string) (string, bool) {
	var value string
	raw, ok := values[key]
	return value, ok && json.Unmarshal(raw, &value) == nil && value != ""
}
func inputString(values map[string]json.RawMessage, key string) (string, bool) {
	return stringConfig(values, key)
}
func acceptedStatuses(config map[string]json.RawMessage, status int) bool {
	var values []int
	if json.Unmarshal(config["accepted_statuses"], &values) != nil {
		return status >= 200 && status < 300
	}
	for _, value := range values {
		if value == status {
			return true
		}
	}
	return false
}
func safeHeaders(headers stdhttp.Header) map[string]string {
	result := map[string]string{}
	for key, values := range headers {
		lower := strings.ToLower(key)
		if lower == "authorization" || lower == "cookie" || lower == "set-cookie" || lower == "proxy-authorization" || strings.Contains(lower, "api-key") {
			continue
		}
		if len(values) > 0 {
			result[key] = values[0]
		}
	}
	return result
}
func mustJSON(value any) json.RawMessage { raw, _ := json.Marshal(value); return raw }
func failed(code, message string) platformruntime.AttemptResult {
	return platformruntime.AttemptResult{State: platformruntime.AttemptFailed, ErrorCode: code, Message: message}
}
