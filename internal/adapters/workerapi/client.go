package workerapi

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	baseURL string
	http    *http.Client
}

func New(baseURL string, timeout time.Duration) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: &http.Client{Timeout: timeout}}
}

func (client *Client) Check(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.baseURL+"/health/ready", nil)
	if err != nil {
		return fmt.Errorf("create Control Plane health request: %w", err)
	}
	response, err := client.http.Do(request)
	if err != nil {
		return fmt.Errorf("Control Plane health request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Control Plane readiness returned %s", response.Status)
	}
	return nil
}
