package httpclient

import (
	"net/http"
	"testing"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/config"
)

func TestFnetClient_SetFnetDataHeaders_SetsJSessionIDCookie(t *testing.T) {
	c := NewFnetClient(&config.Config{HTTPTimeoutMS: 1000})

	req, err := http.NewRequest("GET", "http://example.com", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	c.setFnetDataHeaders(req, "abc123")

	got := req.Header.Get("Cookie")
	if got == "" {
		t.Fatalf("expected Cookie header to be set")
	}
	if got != "JSESSIONID=abc123" {
		t.Fatalf("expected Cookie header to be %q, got %q", "JSESSIONID=abc123", got)
	}
}
