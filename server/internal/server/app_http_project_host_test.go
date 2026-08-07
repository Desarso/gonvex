package server

import (
	"net/http/httptest"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestRegisteredHTTPResolvesProjectFromPublicHost(t *testing.T) {
	server := &Server{config: config.Config{ProjectHosts: map[string]string{
		"cvx-share.example.com": "operations-app",
	}}}

	request := httptest.NewRequest("GET", "https://cvx-share.example.com/share/task", nil)
	if got := server.projectIDForRegisteredHTTP(request); got != "operations-app" {
		t.Fatalf("project = %q, want operations-app", got)
	}

	request.Host = "CVX-SHARE.EXAMPLE.COM:8443"
	if got := server.projectIDForRegisteredHTTP(request); got != "operations-app" {
		t.Fatalf("project with mixed-case host and port = %q, want operations-app", got)
	}
}
