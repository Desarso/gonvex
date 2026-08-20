package server

import (
	"context"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/redis/go-redis/v9"
)

type ephemeralTestValue struct {
	State string `json:"state"`
	Seen  int64  `json:"seen"`
}

func TestValkeyEphemeralTTLListDeleteAndNamespaces(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	projectATenantA := newTenantEphemeralAPI(context.Background(), client, "project-a", "tenant-a")
	projectATenantB := newTenantEphemeralAPI(context.Background(), client, "project-a", "tenant-b")
	projectBTenantA := newTenantEphemeralAPI(context.Background(), client, "project-b", "tenant-a")

	want := ephemeralTestValue{State: "active", Seen: 42}
	if err := projectATenantA.Set("presence/leases/user:1", want, time.Second); err != nil {
		t.Fatal(err)
	}
	for name, other := range map[string]gonvex.EphemeralAPI{
		"different tenant":  projectATenantB,
		"different project": projectBTenantA,
	} {
		var got ephemeralTestValue
		found, err := other.Get("presence/leases/user:1", &got)
		if err != nil {
			t.Fatalf("%s Get: %v", name, err)
		}
		if found {
			t.Fatalf("%s read a value outside its automatic namespace", name)
		}
	}

	if err := projectATenantA.Set("other/user:2", want, time.Second); err != nil {
		t.Fatal(err)
	}
	entries, err := projectATenantA.List("presence/leases/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != "presence/leases/user:1" {
		t.Fatalf("prefix List entries = %#v", entries)
	}
	var decoded ephemeralTestValue
	if err := entries[0].Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != want {
		t.Fatalf("decoded value = %#v, want %#v", decoded, want)
	}

	if err := projectATenantA.Delete("presence/leases/user:1"); err != nil {
		t.Fatal(err)
	}
	if entries, err := projectATenantA.List("presence/leases/"); err != nil || len(entries) != 0 {
		t.Fatalf("List after Delete = %#v, %v", entries, err)
	}

	if err := projectATenantA.Set("presence/leases/expiring", want, time.Second); err != nil {
		t.Fatal(err)
	}
	redisServer.FastForward(time.Second)
	var expired ephemeralTestValue
	if found, err := projectATenantA.Get("presence/leases/expiring", &expired); err != nil || found {
		t.Fatalf("Get after TTL = found %v, err %v", found, err)
	}
	if entries, err := projectATenantA.List("presence/leases/"); err != nil || len(entries) != 0 {
		t.Fatalf("List after TTL = %#v, %v", entries, err)
	}

	for _, key := range redisServer.Keys() {
		if key == "presence/leases/user:1" || key == "presence/leases/expiring" {
			t.Fatalf("logical key was not escaped inside Valkey: %q", key)
		}
	}
}

func TestProjectEphemeralIsSharedAcrossTenantsAndIsolatedAcrossProjects(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client, err := newValkeyClient("redis://" + redisServer.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	backend := &valkeyEphemeralBackend{client: client}
	projectAFromTenantA := backend.ForProject(context.Background(), "project-a")
	projectAFromTenantB := backend.ForProject(context.Background(), "project-a")
	projectB := backend.ForProject(context.Background(), "project-b")
	tenantA := backend.ForTenant(context.Background(), "project-a", "tenant-a")

	want := map[string]any{"session": "live"}
	if err := projectAFromTenantA.Set("support/session/1", want, time.Minute); err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	if found, err := projectAFromTenantB.Get("support/session/1", &got); err != nil || !found {
		t.Fatalf("project peer Get found=%v err=%v", found, err)
	}
	if got["session"] != want["session"] {
		t.Fatalf("project peer value = %#v, want %#v", got, want)
	}
	if found, err := projectB.Get("support/session/1", &got); err != nil || found {
		t.Fatalf("other project Get found=%v err=%v", found, err)
	}
	if found, err := tenantA.Get("support/session/1", &got); err != nil || found {
		t.Fatalf("tenant namespace Get found=%v err=%v", found, err)
	}
}

func TestEphemeralValidationRequiresTTLAndSafeFragments(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	api := newTenantEphemeralAPI(context.Background(), client, "project", "tenant")
	if err := api.Set("key", map[string]any{"ok": true}, 0); err == nil {
		t.Fatal("Set accepted a missing TTL")
	}
	if err := api.Set("unsafe\nkey", true, time.Second); err == nil {
		t.Fatal("Set accepted a control character in the key")
	}
	if _, err := api.List("unsafe\x00prefix"); err == nil {
		t.Fatal("List accepted a control character in the prefix")
	}
	if err := api.Set("not-json", func() {}, time.Second); err == nil {
		t.Fatal("Set accepted a non-JSON-serializable value")
	}
}

func TestRuntimeRequiresReachableValkeyAtStartup(t *testing.T) {
	if _, err := NewRequired(config.Config{}); err == nil || !strings.Contains(err.Error(), "VALKEY_URL (or REDIS_URL) is required") {
		t.Fatalf("unset Valkey error = %v", err)
	}
	if _, err := NewRequired(config.Config{ValkeyURL: "redis://127.0.0.1:1/0"}); err == nil || !strings.Contains(err.Error(), "VALKEY_URL (or REDIS_URL)") {
		t.Fatalf("unreachable Valkey error = %v", err)
	}
}
