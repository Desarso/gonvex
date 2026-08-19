//go:build !windows

package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/projectbundle"
)

const supervisorWorkerHelperEnv = "GONVEX_SUPERVISOR_TEST_WORKER"

type workerIdentity struct {
	PID        int    `json:"pid"`
	Generation string `json:"generation"`
}

func TestRecyclesPluginWorkersAcrossBundleGenerations(t *testing.T) {
	if os.Getenv(supervisorWorkerHelperEnv) == "1" {
		runSupervisorWorkerHelper()
		return
	}

	moduleRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	stateDir := t.TempDir()
	generationFile := filepath.Join(stateDir, "generation")
	cacheDir := filepath.Join(stateDir, "plugins")
	if err := os.WriteFile(generationFile, []byte("0"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	supervisor, err := Start(ctx, Config{
		Executable: os.Args[0],
		Args:       []string{"-test.run=TestRecyclesPluginWorkersAcrossBundleGenerations"},
		Env: []string{
			supervisorWorkerHelperEnv + "=1",
			"GONVEX_SUPERVISOR_TEST_GENERATION_FILE=" + generationFile,
			"GONVEX_SUPERVISOR_TEST_CACHE_DIR=" + cacheDir,
			"GONVEX_SUPERVISOR_TEST_MODULE_ROOT=" + moduleRoot,
		},
		StartupTimeout: 2 * time.Minute,
		DrainTimeout:   5 * time.Second,
		HealthInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start supervisor: %v", err)
	}
	defer supervisor.Close()

	gateway := httptest.NewServer(supervisor.Handler())
	defer gateway.Close()

	initial := readWorkerIdentity(t, gateway.URL)
	if initial.Generation != "0" {
		t.Fatalf("initial generation = %q, want 0", initial.Generation)
	}
	failedResponse, err := http.Post(gateway.URL+"/dev/sync?failAfterLoad=true", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("failed sync: %v", err)
	}
	failedResponse.Body.Close()
	if failedResponse.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("failed sync status = %d", failedResponse.StatusCode)
	}
	afterFailure := waitForWorkerIdentity(t, gateway.URL, func(identity workerIdentity) bool {
		return identity.Generation == "0" && identity.PID != initial.PID
	})
	waitForProcessExit(t, initial.PID)

	parentRSSBefore := processRSSBytes(t, os.Getpid())
	previous := afterFailure
	trafficContext, stopTraffic := context.WithCancel(context.Background())
	trafficErrors := make(chan error, 1)
	var traffic sync.WaitGroup
	defer func() {
		stopTraffic()
		traffic.Wait()
	}()
	for probe := 0; probe < 4; probe++ {
		traffic.Add(1)
		go func() {
			defer traffic.Done()
			client := &http.Client{Timeout: 2 * time.Second}
			for trafficContext.Err() == nil {
				request, _ := http.NewRequestWithContext(trafficContext, http.MethodGet, gateway.URL+"/identity", nil)
				response, probeErr := client.Do(request)
				if probeErr == nil {
					var identity workerIdentity
					probeErr = json.NewDecoder(response.Body).Decode(&identity)
					response.Body.Close()
					if response.StatusCode != http.StatusOK {
						probeErr = fmt.Errorf("identity status %d", response.StatusCode)
					}
				}
				if probeErr != nil && trafficContext.Err() == nil {
					select {
					case trafficErrors <- probeErr:
					default:
					}
					return
				}
			}
		}()
	}

	const generations = 6
	for generation := 1; generation <= generations; generation++ {
		response, requestErr := http.Post(gateway.URL+"/dev/sync", "application/json", strings.NewReader(`{}`))
		if requestErr != nil {
			t.Fatalf("sync generation %d: %v", generation, requestErr)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("sync generation %d status = %d", generation, response.StatusCode)
		}

		wantGeneration := strconv.Itoa(generation)
		current := waitForWorkerIdentity(t, gateway.URL, func(identity workerIdentity) bool {
			return identity.Generation == wantGeneration && identity.PID != previous.PID
		})
		waitForProcessExit(t, previous.PID)
		previous = current
	}
	stopTraffic()
	traffic.Wait()
	select {
	case err := <-trafficErrors:
		t.Fatalf("gateway request failed during worker handoff: %v", err)
	default:
	}

	runtime.GC()
	parentRSSAfter := processRSSBytes(t, os.Getpid())
	const maxParentGrowth = 24 << 20
	if growth := parentRSSAfter - parentRSSBefore; growth > maxParentGrowth {
		t.Fatalf("permanent gateway retained worker memory: RSS grew %d bytes", growth)
	}

	compiled, err := filepath.Glob(filepath.Join(cacheDir, "compiled", "*.so"))
	if err != nil {
		t.Fatal(err)
	}
	if len(compiled) > 2 {
		t.Fatalf("compiled plugin cache retained %d generations, want at most 2: %v", len(compiled), compiled)
	}
}

func runSupervisorWorkerHelper() {
	generationFile := os.Getenv("GONVEX_SUPERVISOR_TEST_GENERATION_FILE")
	cacheDir := os.Getenv("GONVEX_SUPERVISOR_TEST_CACHE_DIR")
	moduleRoot := os.Getenv("GONVEX_SUPERVISOR_TEST_MODULE_ROOT")
	loader := projectbundle.NewLoader(cacheDir, moduleRoot)

	generation := strings.TrimSpace(readFileOrExit(generationFile))
	app := loadGenerationOrExit(loader, generation)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /identity", func(response http.ResponseWriter, request *http.Request) {
		result, err := app.ExecuteQuery(&gonvex.QueryCtx{}, "test.generation", json.RawMessage(`{}`))
		if err != nil {
			http.Error(response, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(response).Encode(workerIdentity{PID: os.Getpid(), Generation: fmt.Sprint(result)})
	})
	mux.HandleFunc("POST /dev/sync", func(response http.ResponseWriter, request *http.Request) {
		current, err := strconv.Atoi(strings.TrimSpace(readFileOrExit(generationFile)))
		if err != nil {
			http.Error(response, err.Error(), http.StatusInternalServerError)
			return
		}
		next := strconv.Itoa(current + 1)
		app = loadGenerationOrExit(loader, next)
		// Every generation is a genuinely changed bundle, so this stub always
		// dlopen'd a replacement module: report it exactly like the real
		// /dev/sync handler, which sets the header only for replacement loads
		// (or failed load attempts) and never for unchanged bundles.
		response.Header().Set(recycleWorkerHeader, "1")
		if request.URL.Query().Get("failAfterLoad") == "true" {
			http.Error(response, "simulated persistence failure", http.StatusUnprocessableEntity)
			return
		}
		if err := os.WriteFile(generationFile, []byte(next), 0o600); err != nil {
			http.Error(response, err.Error(), http.StatusInternalServerError)
			return
		}
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte(`{"ok":true}`))
	})

	server := &http.Server{Addr: os.Getenv("GONVEX_ADDR"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	listener, err := InheritedWorkerListener()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	if listener == nil {
		fmt.Fprintln(os.Stderr, "supervisor test worker did not inherit a listener")
		os.Exit(1)
	}
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func loadGenerationOrExit(loader *projectbundle.Loader, generation string) *gonvex.App {
	source := fmt.Sprintf(`package app

import "github.com/gonvex/gonvex/pkg/gonvex"

func Register(app *gonvex.App) {
	app.Query("test.generation", func(_ *gonvex.QueryCtx, _ struct{}) (string, error) {
		return %q, nil
	})
}
`, generation)
	files := map[string]string{"app/register.go": projectbundle.EncodeFile([]byte(source))}
	bundle := manifest.SourceBundle{
		Hash:        projectbundle.HashFiles(files),
		ModulePath:  "gonvexapp/supervisor-stress",
		PackageName: "app",
		Files:       files,
	}
	app, err := loader.Load("supervisor-stress", bundle)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return app
}

func readFileOrExit(path string) string {
	contents, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return string(contents)
}

func readWorkerIdentity(t *testing.T, gatewayURL string) workerIdentity {
	t.Helper()
	identity, err := probeWorkerIdentity(http.DefaultClient, gatewayURL)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func probeWorkerIdentity(client *http.Client, gatewayURL string) (workerIdentity, error) {
	response, err := client.Get(gatewayURL + "/identity")
	if err != nil {
		return workerIdentity{}, err
	}
	defer response.Body.Close()
	var identity workerIdentity
	if err := json.NewDecoder(response.Body).Decode(&identity); err != nil {
		return workerIdentity{}, err
	}
	if response.StatusCode != http.StatusOK {
		return workerIdentity{}, fmt.Errorf("identity status %d", response.StatusCode)
	}
	return identity, nil
}

func waitForWorkerIdentity(t *testing.T, gatewayURL string, ready func(workerIdentity) bool) workerIdentity {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		identity := readWorkerIdentity(t, gatewayURL)
		if ready(identity) {
			return identity
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("replacement worker did not become active")
	return workerIdentity{}
}

func waitForProcessExit(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); err == syscall.ESRCH {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("old worker %d is still alive", pid)
}

func processRSSBytes(t *testing.T, pid int) int64 {
	t.Helper()
	contents, err := os.ReadFile(fmt.Sprintf("/proc/%d/statm", pid))
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(contents))
	if len(fields) < 2 {
		t.Fatalf("invalid statm contents: %q", contents)
	}
	residentPages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return residentPages * int64(os.Getpagesize())
}
