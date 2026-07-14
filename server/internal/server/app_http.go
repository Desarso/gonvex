package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

const maxRegisteredHTTPBodyBytes = 2 << 20

func (s *Server) handleRegisteredHTTP(w http.ResponseWriter, r *http.Request) {
	project := s.projectIDForRegisteredHTTP(r)
	app := s.appForProject(r.Context(), project)
	if app == nil {
		http.NotFound(w, r)
		return
	}
	function, ok := app.Lookup(r.URL.Path)
	if !ok || function.Kind != gonvex.FunctionKindHTTP {
		http.NotFound(w, r)
		return
	}

	kind := string(gonvex.FunctionKindHTTP)
	s.metrics.recordFunctionStart(kind)
	started := time.Now()
	var opErr error
	defer func() {
		s.metrics.recordFunctionEnd(kind)
		s.metrics.recordFunction(project, r.URL.Path, kind, time.Since(started), opErr)
	}()

	tenant := tenantIDFromRequest(project, tenantID(r))
	caller := callerContext{}
	token := bearerToken(r)
	if s.projectRequiresAuthentication(r.Context(), project) || token != "" {
		user, permissions, authenticatedProject, authenticatedTenant, err := s.authenticateSocket(
			r.Context(), project, tenant, token, tenantID(r),
		)
		if err != nil {
			opErr = err
			w.Header().Set("www-authenticate", "Bearer")
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
			return
		}
		project = authenticatedProject
		tenant = authenticatedTenant
		caller = callerContext{user: user, permissions: permissions}
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxRegisteredHTTPBodyBytes+1))
	if err != nil {
		opErr = err
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if len(body) > maxRegisteredHTTPBodyBytes {
		opErr = fmt.Errorf("request body too large")
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
		return
	}

	httpCtx, err := s.httpContext(r.Context(), project, tenant, caller)
	if err != nil {
		opErr = err
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	headers := mapHeaderValues(r.Header)
	if r.Host != "" {
		headers["host"] = []string{r.Host}
	}

	response, err := app.ExecuteHTTP(httpCtx, r.URL.Path, gonvex.HTTPRequest{
		Method:     r.Method,
		Path:       r.URL.Path,
		RawQuery:   r.URL.RawQuery,
		Query:      mapQueryValues(r.URL.Query()),
		Headers:    headers,
		Body:       body,
		RemoteAddr: r.RemoteAddr,
	})
	if err != nil {
		opErr = err
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeHTTPResponse(w, response)
}

func (s *Server) httpContext(ctx context.Context, projectID string, tenantID string, caller callerContext) (*gonvex.HTTPContext, error) {
	runtimeCtx, err := s.runtimeContext(ctx, projectID, tenantID, caller)
	if err != nil {
		return nil, err
	}
	return &gonvex.HTTPContext{RuntimeContext: runtimeCtx}, nil
}

func (s *Server) projectIDForRegisteredHTTP(r *http.Request) string {
	if project := projectID(r); project != "" {
		return project
	}
	projects := s.runtime.ProjectIDs()
	if len(projects) == 1 {
		return projects[0]
	}
	if len(s.config.ProjectDatabases) == 1 {
		for project := range s.config.ProjectDatabases {
			return project
		}
	}
	return ""
}

func mapHeaderValues(header http.Header) map[string][]string {
	out := make(map[string][]string, len(header))
	for key, values := range header {
		copied := make([]string, len(values))
		copy(copied, values)
		out[strings.ToLower(key)] = copied
	}
	return out
}

func mapQueryValues(values map[string][]string) map[string][]string {
	out := make(map[string][]string, len(values))
	for key, entries := range values {
		copied := make([]string, len(entries))
		copy(copied, entries)
		out[key] = copied
	}
	return out
}

func writeHTTPResponse(w http.ResponseWriter, response gonvex.HTTPResponse) {
	for key, values := range response.Headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	status := response.Status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	if len(response.Body) > 0 {
		_, _ = w.Write(response.Body)
	}
}
