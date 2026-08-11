package server

import (
	"net/http"
	"testing"
)

func TestNewHTTPServerAppliesConnectionSafeguards(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	configured := NewHTTPServer(":9090", handler)

	if configured.Addr != ":9090" || configured.Handler == nil {
		t.Fatalf("server was not configured with requested address and handler: %+v", configured)
	}
	if configured.ReadHeaderTimeout <= 0 {
		t.Fatal("ReadHeaderTimeout must protect against slow header attacks")
	}
	if configured.IdleTimeout <= 0 {
		t.Fatal("IdleTimeout must bound idle keep-alive connections")
	}
	if configured.MaxHeaderBytes <= 0 {
		t.Fatal("MaxHeaderBytes must bound request-header memory")
	}
	if configured.ReadTimeout != 0 || configured.WriteTimeout != 0 {
		t.Fatal("whole-request deadlines would break uploads and long-lived streams")
	}
}
