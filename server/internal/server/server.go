package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gonvex/gonvex/server/internal/data"
	"github.com/gonvex/gonvex/server/internal/runtime"
	"github.com/gonvex/gonvex/server/internal/schema"
)

type Server struct {
	config          config.Config
	runtime         *runtime.Runtime
	cache           *rowsCache
	wsMu            sync.RWMutex
	wsConns         map[*wsConn]bool
	tableChangeMu   sync.Mutex
	tableChangeWait map[string]*time.Timer
	tableChanges    map[string]tableChange
}

func New(cfg config.Config) *Server {
	cache, _ := newRowsCache(cfg.ValkeyURL, cfg.RowsCacheTTL)
	server := &Server{
		config:          cfg,
		runtime:         runtime.New(),
		cache:           cache,
		wsConns:         map[*wsConn]bool{},
		tableChangeWait: map[string]*time.Timer{},
		tableChanges:    map[string]tableChange{},
	}
	server.startPostgresNotifications()
	return server
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /dev/manifest", s.handleManifest)
	mux.HandleFunc("GET /dev/data/tables", s.handleDataTables)
	mux.HandleFunc("GET /dev/data/tables/{table}/rows", s.handleDataRows)
	mux.HandleFunc("POST /dev/data/tables/{table}/rows", s.handleInsertDataRow)
	mux.HandleFunc("POST /dev/sync", s.handleDevSync)
	mux.HandleFunc("GET /ws", s.handleWebSocket)
	return withGzip(withJSON(mux))
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":          true,
		"time":        time.Now().UTC().Format(time.RFC3339Nano),
		"postgresSet": s.config.PostgresURL != "",
		"valkeySet":   s.config.ValkeyURL != "",
		"rowsCache":   s.cache.enabled(),
		"s3Set":       s.config.S3Endpoint != "" && s.config.S3Bucket != "",
	})
}

func (s *Server) handleManifest(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.runtime.Manifest())
}

func (s *Server) handleDataTables(w http.ResponseWriter, r *http.Request) {
	tables, err := data.ListTables(r.Context(), s.config.PostgresURL)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tables": tables})
}

func (s *Server) handleDataRows(w http.ResponseWriter, r *http.Request) {
	table := r.PathValue("table")
	if s.cache.enabled() {
		key := s.cache.rowsKey(table, r.URL.Query())
		if payload, ok := s.cache.get(r.Context(), key); ok {
			w.Header().Set("content-type", "application/json")
			w.Header().Set("x-gonvex-cache", "hit")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
			return
		}
		w.Header().Set("x-gonvex-cache", "miss")
	} else {
		w.Header().Set("x-gonvex-cache", "bypass")
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	filters, err := parseRowsFilters(r.URL.Query().Get("filters"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	countMode := r.URL.Query().Get("count")
	result, err := data.ReadRows(r.Context(), s.config.PostgresURL, table, data.RowsOptions{
		Limit:         limit,
		Offset:        offset,
		Search:        r.URL.Query().Get("search"),
		SortColumn:    r.URL.Query().Get("sort"),
		SortDirection: r.URL.Query().Get("direction"),
		Filters:       filters,
		Columns:       parseColumns(r.URL.Query().Get("columns")),
		ExactTotal:    countMode != "false" && countMode != "estimate",
		EstimateTotal: countMode == "estimate",
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	payload, err := json.Marshal(result)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.cache.enabled() {
		s.cache.set(r.Context(), s.cache.rowsKey(table, r.URL.Query()), payload)
	}
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

func parseColumns(raw string) []string {
	if raw == "" {
		return nil
	}
	columns := strings.Split(raw, ",")
	for index, column := range columns {
		columns[index] = strings.TrimSpace(column)
	}
	return columns
}

func parseRowsFilters(raw string) ([]data.RowsFilter, error) {
	if raw == "" {
		return nil, nil
	}
	var filters []data.RowsFilter
	if err := json.Unmarshal([]byte(raw), &filters); err != nil {
		return nil, err
	}
	return filters, nil
}

func (s *Server) handleInsertDataRow(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	result, err := data.InsertRow(r.Context(), s.config.PostgresURL, r.PathValue("table"), payload)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.broadcastTableChange(r.PathValue("table"))
	writeJSON(w, http.StatusCreated, result)
}

func (s *Server) handleDevSync(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if s.config.DevSyncKey != "" && syncKey(r) != s.config.DevSyncKey {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid Gonvex sync key"})
		return
	}

	var next manifest.Manifest
	if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if next.Functions == nil {
		next.Functions = map[string]manifest.FunctionEntry{}
	}
	if next.Project == "" {
		next.Project = r.Header.Get("x-gonvex-project-id")
	}
	if headerProject := r.Header.Get("x-gonvex-project-id"); headerProject != "" && next.Project != "" && headerProject != next.Project {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "manifest project does not match x-gonvex-project-id"})
		return
	}
	if next.Schema.Tables == nil {
		next.Schema = manifest.EmptySchema()
	}

	migrationResult, err := schema.Apply(r.Context(), s.config.PostgresURL, next.Schema)
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}

	s.runtime.SyncManifest(next)
	s.cache.invalidateRows(r.Context(), "")
	s.broadcastTableChange("tasks")
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":              true,
		"project":         next.Project,
		"functionCount":   len(next.Functions),
		"schema":          migrationResult,
		"runtimeReloaded": true,
	})
}

func syncKey(r *http.Request) string {
	if value := r.Header.Get("x-gonvex-key"); value != "" {
		return value
	}
	value := r.Header.Get("authorization")
	if strings.HasPrefix(strings.ToLower(value), "bearer ") {
		return strings.TrimSpace(value[len("Bearer "):])
	}
	return ""
}

func withJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("access-control-allow-origin", "*")
		w.Header().Set("access-control-allow-headers", "content-type, authorization")
		w.Header().Set("access-control-allow-methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
