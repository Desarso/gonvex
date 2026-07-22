package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestRunLoadKeepsPersistentSubscriptionsAndMeasuresWireTraffic(t *testing.T) {
	var sockets atomic.Int64
	var subscriptions atomic.Int64
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/ws" {
			http.NotFound(writer, request)
			return
		}
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		sockets.Add(1)
		defer sockets.Add(-1)
		_ = connection.WriteJSON(map[string]any{"type": "session.ready", "project": "test", "tenant": "loadtest"})
		for {
			var message map[string]any
			if err := connection.ReadJSON(&message); err != nil {
				return
			}
			switch message["type"] {
			case "auth":
				_ = connection.WriteJSON(map[string]any{
					"type": "auth.result",
					"id":   message["id"],
					"result": map[string]any{
						"userId":    "load-user",
						"projectId": "test",
						"tenantId":  "loadtest",
					},
				})
			case "query.subscribe":
				subscriptions.Add(1)
				_ = connection.WriteJSON(map[string]any{
					"type":   "query.result",
					"id":     message["id"],
					"path":   message["path"],
					"reason": "initial",
					"result": []any{},
					"trace": map[string]any{
						"serverSubscriptionStartedAtMs": float64(time.Now().Add(-2*time.Millisecond).UnixNano()) / float64(time.Millisecond),
						"serverSubscriptionSentAtMs":    float64(time.Now().UnixNano()) / float64(time.Millisecond),
						"serverDurationMs":              2,
					},
				})
			}
		}
	}))
	defer server.Close()

	profile, err := loadProfileReader(strings.NewReader(`{
		"version": 1,
		"name": "test",
		"subscriptions": [
			{"path":"users.me","args":{"tenantId":"${tenant}"}},
			{"path":"workspaces.list","args":{"tenantId":"${tenant}"}}
		]
	}`))
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	report, err := runLoad(ctx, runConfig{
		URL:                        server.URL,
		Project:                    "test",
		Tenant:                     "loadtest",
		Connections:                3,
		SubscriptionsPerConnection: 2,
		RampDuration:               10 * time.Millisecond,
		HoldDuration:               50 * time.Millisecond,
		ConnectTimeout:             time.Second,
		InitialTimeout:             time.Second,
		AuthMode:                   authModeSynthetic,
		MaximumDialConcurrency:     3,
		SampleInterval:             10 * time.Millisecond,
		TargetPID:                  os.Getpid(),
		Safety: safetyLimits{
			MinimumHostAvailableBytes: 1,
			MaximumErrorRate:          0.50,
		},
	}, profile)
	if err != nil {
		t.Fatalf("runLoad returned error: %v", err)
	}
	encoded, _ := json.Marshal(report)
	if report.Connections.Established != 3 || report.Subscriptions.Sent != 6 || report.Subscriptions.InitialResults != 6 {
		t.Fatalf("unexpected report: %s", encoded)
	}
	if report.Subscriptions.Errors != 0 || report.Connections.UnexpectedCloses != 0 {
		t.Fatalf("unexpected failures: %s", encoded)
	}
	if report.Wire.BytesRead == 0 || report.Wire.BytesWritten == 0 {
		t.Fatalf("wire traffic was not measured: %s", encoded)
	}
	if report.Latency.InitialResult.Count != 6 || report.Latency.ServerQuery.Count != 6 {
		t.Fatalf("latencies were not recorded: %s", encoded)
	}
	if len(report.Samples) < 2 || report.Samples[len(report.Samples)-1].Target == nil {
		t.Fatalf("resource samples were not captured: %s", encoded)
	}
	if subscriptions.Load() != 6 {
		t.Fatalf("server received %d subscriptions, want 6", subscriptions.Load())
	}
	if sockets.Load() != 0 {
		t.Fatalf("load clients did not close: %d sockets remain", sockets.Load())
	}
}
