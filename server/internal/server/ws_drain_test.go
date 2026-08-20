package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gorilla/websocket"
)

// TestDrainWebSocketsSendsStaggered1012Closes proves a worker drain closes
// every connection with close code 1012 (service restart), lets idle
// connections go first, and holds a connection with an in-flight write until
// that write finishes.
func TestDrainWebSocketsSendsStaggered1012Closes(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverSide := make(chan *websocket.Conn, 4)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverSide <- connection
	}))
	defer httpServer.Close()

	type peerResult struct {
		name     string
		code     int
		closedAt time.Time
	}
	results := make(chan peerResult, 4)
	var peers []*websocket.Conn
	dial := func(name string) *wsConn {
		t.Helper()
		peer, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		peers = append(peers, peer)
		go func() {
			for {
				if _, _, err := peer.ReadMessage(); err != nil {
					code := 0
					if closeErr, ok := err.(*websocket.CloseError); ok {
						code = closeErr.Code
					}
					results <- peerResult{name: name, code: code, closedAt: time.Now()}
					return
				}
			}
		}()
		select {
		case accepted := <-serverSide:
			connection := &wsConn{
				server: server, conn: accepted, id: name, project: "project-a", tenant: "tenant-a",
				subs: map[string]querySubscription{}, replicas: map[string]*replicaSubscription{},
			}
			server.addWSConn(connection)
			return connection
		case <-time.After(2 * time.Second):
			t.Fatal("server never accepted the websocket")
			return nil
		}
	}
	defer func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
	}()

	idleA := dial("idle-a")
	idleB := dial("idle-b")
	busy := dial("busy")
	_ = idleA
	_ = idleB
	busy.writesInFlight.Add(1)
	var release sync.Once
	time.AfterFunc(600*time.Millisecond, func() {
		release.Do(func() { busy.writesInFlight.Add(-1) })
	})

	started := time.Now()
	server.DrainWebSockets(900 * time.Millisecond)
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("drain took %v, want bounded by the window", elapsed)
	}

	received := map[string]peerResult{}
	for range 3 {
		select {
		case result := <-results:
			received[result.name] = result
		case <-time.After(3 * time.Second):
			t.Fatalf("missing close events, got %v", received)
		}
	}
	for name, result := range received {
		if result.code != websocket.CloseServiceRestart {
			t.Fatalf("%s closed with code %d, want 1012", name, result.code)
		}
	}
	if received["busy"].closedAt.Before(received["idle-a"].closedAt) ||
		received["busy"].closedAt.Before(received["idle-b"].closedAt) {
		t.Fatalf("busy connection closed before idle ones: %v", received)
	}
}
