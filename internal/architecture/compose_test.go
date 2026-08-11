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
		} `yaml:"services"`
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
	}
	for _, service := range []string{"control-plane", "migrate"} {
		if _, exists := document.Services[service].Environment["EVALFROG_POSTGRES_DSN"]; !exists {
			t.Fatalf("%s must receive the PostgreSQL DSN", service)
		}
	}
}
