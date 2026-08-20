package gonvextest

import (
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

// HeartbeatResult is the (empty) payload returned by the demo heartbeat job.
type HeartbeatResult struct {
	OK bool `json:"ok"`
}

// RegisterSystem wires a small recurring job so the dashboard health overview
// shows live scheduler activity (running/queued/lag, throughput) out of the
// box. Remove or replace it with real background work for your project.
func RegisterSystem(app *gonvex.App) {
	app.InternalReducer("system.heartbeat", Heartbeat)
	app.Cron("heartbeat", 15*time.Second, "system.heartbeat", nil)
}

// Heartbeat is a no-op internal Reducer run on a schedule by the cron above.
func Heartbeat(ctx *gonvex.ReducerCtx, args struct{}) (HeartbeatResult, error) {
	return HeartbeatResult{OK: true}, nil
}
