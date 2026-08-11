package compiler

import (
	"fmt"
	"sort"

	"github.com/uu999/evalfrog/internal/catalog"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/ir"
)

// Policy is an immutable Project Policy input. Overrides are platform-owned;
// they are never read from author IR.
type Policy struct {
	revision  string
	overrides map[ir.NodeType]dsl.ExecutionPolicy
}

func NewPolicy(revision string, overrides map[ir.NodeType]dsl.ExecutionPolicy) (Policy, error) {
	if revision == "" {
		return Policy{}, fmt.Errorf("policy revision is required")
	}
	result := Policy{revision: revision, overrides: make(map[ir.NodeType]dsl.ExecutionPolicy, len(overrides))}
	for nodeType, policy := range overrides {
		if !ir.ValidNodeType(nodeType) {
			return Policy{}, fmt.Errorf("policy node type %q is invalid", nodeType)
		}
		if err := validatePolicy(policy); err != nil {
			return Policy{}, fmt.Errorf("policy for %s: %w", nodeType, err)
		}
		result.overrides[nodeType] = clonePolicy(policy)
	}
	return result, nil
}

func DefaultPolicyV1() Policy {
	policy, err := NewPolicy("policy-v1", nil)
	if err != nil {
		panic(err)
	}
	return policy
}

func (policy Policy) Revision() string {
	return policy.revision
}

func (policy Policy) resolve(nodeType ir.NodeType, defaults catalog.ExecutionPolicyDefaults, kind catalog.NodeKind) (dsl.ExecutionPolicy, error) {
	if kind == catalog.KindControl {
		return dsl.ExecutionPolicy{}, nil
	}
	resolved := dsl.ExecutionPolicy{
		MaxAttempts: defaults.MaxAttempts, MaxRecoveries: defaults.MaxRecoveries,
		AttemptTimeoutMS:    defaults.AttemptTimeoutMS,
		RetryBackoff:        &dsl.RetryBackoff{Kind: "fixed", DelayMS: defaults.RetryBackoffMS},
		RetryableErrorCodes: append([]string(nil), defaults.RetryableErrorCodes...),
	}
	if override, exists := policy.overrides[nodeType]; exists {
		resolved = clonePolicy(override)
	}
	sort.Strings(resolved.RetryableErrorCodes)
	resolved.RetryableErrorCodes = uniqueStrings(resolved.RetryableErrorCodes)
	if err := validatePolicy(resolved); err != nil {
		return dsl.ExecutionPolicy{}, err
	}
	return resolved, nil
}

func validatePolicy(policy dsl.ExecutionPolicy) error {
	if policy.MaxAttempts == 0 || policy.AttemptTimeoutMS == 0 || policy.RetryBackoff == nil {
		return fmt.Errorf("max_attempts, attempt_timeout_ms, and retry_backoff are required")
	}
	if policy.RetryBackoff.Kind != "fixed" || policy.RetryBackoff.DelayMS == 0 {
		return fmt.Errorf("only a positive fixed retry backoff is supported in DSL v1")
	}
	for _, code := range policy.RetryableErrorCodes {
		if code == "" {
			return fmt.Errorf("retryable error codes cannot be empty")
		}
	}
	return nil
}

func clonePolicy(policy dsl.ExecutionPolicy) dsl.ExecutionPolicy {
	result := policy
	if policy.RetryBackoff != nil {
		copy := *policy.RetryBackoff
		result.RetryBackoff = &copy
	}
	result.RetryableErrorCodes = append([]string(nil), policy.RetryableErrorCodes...)
	return result
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}
