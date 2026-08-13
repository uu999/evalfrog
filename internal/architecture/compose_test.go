package architecture

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestWorkersReceiveNoAuthoritativeStoreCredentials(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(root, "deployments", "compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Services map[string]struct {
			Environment map[string]string `yaml:"environment"`
			Networks    []string          `yaml:"networks"`
			Volumes     []string          `yaml:"volumes"`
			ReadOnly    bool              `yaml:"read_only"`
			CapDrop     []string          `yaml:"cap_drop"`
			SecurityOpt []string          `yaml:"security_opt"`
		} `yaml:"services"`
		Networks map[string]struct {
			Internal bool `yaml:"internal"`
		} `yaml:"networks"`
	}
	if err := yaml.Unmarshal(content, &document); err != nil {
		t.Fatal(err)
	}
	for _, service := range []string{"worker-builtin", "worker-sandbox"} {
		environment := document.Services[service].Environment
		for _, forbidden := range []string{"EVALFROG_POSTGRES_DSN", "EVALFROG_SCHEDULING_REDIS_ADDRESS", "EVALFROG_CACHE_REDIS_ADDRESS"} {
			if _, exists := environment[forbidden]; exists {
				t.Fatalf("%s must not receive %s", service, forbidden)
			}
		}
		if !hasNetwork(document.Services[service].Networks, "worker") || hasNetwork(document.Services[service].Networks, "authority") {
			t.Fatalf("%s must use the Worker-side network without authority-store access: %v", service, document.Services[service].Networks)
		}
		if len(document.Services[service].Volumes) != 0 || !document.Services[service].ReadOnly || !hasString(document.Services[service].CapDrop, "ALL") || !hasString(document.Services[service].SecurityOpt, "no-new-privileges:true") {
			t.Fatalf("%s must have no mounts and must run read-only with dropped capabilities: %+v", service, document.Services[service])
		}
	}
	runtime := document.Services["sandbox-runtime"]
	if !hasNetwork(runtime.Networks, "worker") || hasNetwork(runtime.Networks, "authority") || !hasString(runtime.Volumes, "/var/run/docker.sock:/var/run/docker.sock") ||
		!runtime.ReadOnly || !hasString(runtime.CapDrop, "ALL") || !hasString(runtime.SecurityOpt, "no-new-privileges:true") || runtime.Environment["EVALFROG_SANDBOX_RUNTIME_TOKEN"] == "" {
		t.Fatalf("sandbox runtime must own the sole local Docker socket on the worker network: %+v", runtime)
	}
	for _, service := range []string{"worker-builtin", "worker-sandbox"} {
		if hasString(document.Services[service].Volumes, "/var/run/docker.sock:/var/run/docker.sock") {
			t.Fatalf("%s must not receive the Docker socket", service)
		}
	}
	if !hasNetwork(document.Services["worker-builtin"].Networks, "managed-egress") || hasNetwork(document.Services["worker-sandbox"].Networks, "managed-egress") {
		t.Fatalf("only the builtin pool may have a managed egress path: builtin=%v sandbox=%v", document.Services["worker-builtin"].Networks, document.Services["worker-sandbox"].Networks)
	}
	for _, service := range []string{"postgres", "redis-scheduling", "redis-cache"} {
		if !hasNetwork(document.Services[service].Networks, "authority") || hasNetwork(document.Services[service].Networks, "worker") {
			t.Fatalf("%s must not be attached to the Worker network: %v", service, document.Services[service].Networks)
		}
	}
	for _, name := range []string{"authority", "worker"} {
		if !document.Networks[name].Internal {
			t.Fatalf("%s network must be internal", name)
		}
	}
	for _, service := range []string{"control-plane", "migrate"} {
		if _, exists := document.Services[service].Environment["EVALFROG_POSTGRES_DSN"]; !exists {
			t.Fatalf("%s must receive the PostgreSQL DSN", service)
		}
	}
}

func hasNetwork(values []string, want string) bool { return hasString(values, want) }

func hasString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
