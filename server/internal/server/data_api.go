package server

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/storage"
	"github.com/gonvex/gonvex/server/internal/datafiles"
)

// data_api.go — the per-request gonvex.DataAPI implementation exposed to app
// functions as ctx.Data and to Go sandbox code via the data.* host-call kinds.
// It binds the shared DuckDB artifact manager to the active project/tenant and
// to tenant storage (so missing artifacts re-ingest from the original upload).

const (
	dataIngestTimeout = 5 * time.Minute
	dataQueryTimeout  = 60 * time.Second
)

type tenantDataAPI struct {
	manager *datafiles.Manager
	scope   datafiles.Scope
	storage gonvex.StorageAPI
}

var _ gonvex.DataAPI = (*tenantDataAPI)(nil)

// dataForTenant builds the request-scoped DataAPI. storageAPI may be nil when
// storage is unconfigured; queries against already-ingested artifacts still
// work, only (re-)ingestion needs the original bytes.
func (s *Server) dataForTenant(projectID, tenantID string, storageAPI gonvex.StorageAPI) gonvex.DataAPI {
	if s.dataFiles == nil {
		return gonvex.UnavailableData()
	}
	return &tenantDataAPI{
		manager: s.dataFiles,
		scope:   datafiles.Scope{ProjectID: projectID, TenantID: tenantID},
		storage: storageAPI,
	}
}

// fetchFor re-opens the original upload for (re-)ingestion.
func (d *tenantDataAPI) fetchFor(fileID string) datafiles.FetchFunc {
	tenant, ok := d.storage.(*storage.Tenant)
	if !ok || tenant == nil {
		return nil
	}
	return func(context.Context) (io.ReadCloser, string, error) {
		reader, err := tenant.Open(fileID)
		return reader, "", err
	}
}

func (d *tenantDataAPI) Ingest(ctx context.Context, req gonvex.DataIngestRequest) (gonvex.DataIngestResult, error) {
	fileID := strings.TrimSpace(req.FileID)
	if fileID == "" {
		return gonvex.DataIngestResult{OK: false, Error: "data ingest requires a fileId"}, nil
	}
	fetch := d.fetchFor(fileID)
	if fetch == nil {
		return gonvex.DataIngestResult{OK: false, Error: "storage is not configured on this runtime"}, nil
	}
	ingestCtx, cancel := context.WithTimeout(ctx, dataIngestTimeout)
	defer cancel()
	meta, err := d.manager.Ingest(ingestCtx, d.scope, fileID, req.Filename, fetch)
	if err != nil {
		return gonvex.DataIngestResult{OK: false, Error: err.Error()}, nil
	}
	return gonvex.DataIngestResult{
		OK:       true,
		FileKey:  meta.FileKey,
		Summary:  meta.Summary(),
		Tables:   dataTableInfos(meta.Tables),
		Warnings: meta.Warnings,
	}, nil
}

func (d *tenantDataAPI) Inspect(ctx context.Context, req gonvex.DataInspectRequest) (gonvex.DataInspectResult, error) {
	fileKey := strings.TrimSpace(req.FileKey)
	if fileKey == "" {
		return gonvex.DataInspectResult{OK: false, Error: "data inspect requires a fileKey"}, nil
	}
	queryCtx, cancel := context.WithTimeout(ctx, dataQueryTimeout)
	defer cancel()
	fetch := d.refetch(fileKey)
	meta, err := d.manager.Metadata(queryCtx, d.scope, fileKey, fetch)
	if err != nil {
		return gonvex.DataInspectResult{OK: false, Error: err.Error()}, nil
	}

	operation := strings.ToLower(strings.TrimSpace(req.Operation))
	if operation == "" {
		operation = "overview"
	}
	result := gonvex.DataInspectResult{OK: true, FileKey: fileKey, Summary: meta.Summary(), Warnings: meta.Warnings}
	switch operation {
	case "schema":
		for _, table := range meta.Tables {
			result.Tables = append(result.Tables, dataTableMap(table, nil))
		}
	case "sample":
		tableName := strings.TrimSpace(req.TableName)
		rows, err := d.manager.Sample(queryCtx, d.scope, fileKey, tableName, req.Limit, fetch)
		if err != nil {
			return gonvex.DataInspectResult{OK: false, Error: err.Error()}, nil
		}
		for _, table := range meta.Tables {
			if tableName == "" || strings.EqualFold(table.TableName, tableName) {
				result.Tables = append(result.Tables, dataTableMap(table, rows))
				break
			}
		}
	case "overview":
		for _, table := range meta.Tables {
			rows, err := d.manager.Sample(queryCtx, d.scope, fileKey, table.TableName, 5, fetch)
			if err != nil {
				return gonvex.DataInspectResult{OK: false, Error: err.Error()}, nil
			}
			result.Tables = append(result.Tables, dataTableMap(table, rows))
		}
	default:
		return gonvex.DataInspectResult{OK: false, Error: fmt.Sprintf("unknown inspect operation %q (use overview, schema, or sample)", req.Operation)}, nil
	}
	return result, nil
}

func (d *tenantDataAPI) Query(ctx context.Context, req gonvex.DataQueryRequest) (gonvex.DataQueryResult, error) {
	fileKey := strings.TrimSpace(req.FileKey)
	if fileKey == "" || strings.TrimSpace(req.SQL) == "" {
		return gonvex.DataQueryResult{OK: false, Error: "data query requires fileKey and sql"}, nil
	}
	queryCtx, cancel := context.WithTimeout(ctx, dataQueryTimeout)
	defer cancel()
	columns, rows, truncated, err := d.manager.Query(queryCtx, d.scope, fileKey, req.SQL, req.Limit, d.refetch(fileKey))
	if err != nil {
		return gonvex.DataQueryResult{OK: false, Error: err.Error()}, nil
	}
	result := gonvex.DataQueryResult{OK: true, Columns: columns, Rows: rows, RowCount: len(rows), Truncated: truncated}
	if truncated {
		result.Warnings = append(result.Warnings, fmt.Sprintf("Result truncated to %d rows; aggregate in SQL instead of fetching raw rows.", len(rows)))
	}
	return result, nil
}

func (d *tenantDataAPI) Profile(ctx context.Context, req gonvex.DataProfileRequest) (gonvex.DataProfileResult, error) {
	fileKey := strings.TrimSpace(req.FileKey)
	if fileKey == "" {
		return gonvex.DataProfileResult{OK: false, Error: "data profile requires a fileKey"}, nil
	}
	queryCtx, cancel := context.WithTimeout(ctx, dataQueryTimeout)
	defer cancel()
	tables, profiles, err := d.manager.Profile(queryCtx, d.scope, fileKey, strings.TrimSpace(req.TableName), req.MaxColumns, d.refetch(fileKey))
	if err != nil {
		return gonvex.DataProfileResult{OK: false, Error: err.Error()}, nil
	}
	result := gonvex.DataProfileResult{OK: true, FileKey: fileKey}
	for i, table := range tables {
		profile := gonvex.DataTableProfile{TableName: table.TableName, RowCount: table.RowCount}
		if columns, ok := profiles[i]["columns"].([]map[string]any); ok {
			for _, column := range columns {
				profile.Columns = append(profile.Columns, dataColumnProfile(column))
			}
		}
		result.Tables = append(result.Tables, profile)
	}
	return result, nil
}

// refetch resolves the source upload for a fileKey so a missing artifact can
// be rebuilt. The upload's storage id is embedded in the key.
func (d *tenantDataAPI) refetch(fileKey string) datafiles.FetchFunc {
	fileID, ok := datafiles.FileIDFromKey(fileKey)
	if !ok {
		return nil
	}
	return d.fetchFor(fileID)
}

func dataTableInfos(tables []datafiles.TableMeta) []gonvex.DataTableInfo {
	infos := make([]gonvex.DataTableInfo, 0, len(tables))
	for _, table := range tables {
		infos = append(infos, gonvex.DataTableInfo{
			TableName:   table.TableName,
			RowCount:    table.RowCount,
			Columns:     table.Columns,
			ColumnTypes: table.ColumnTypes,
		})
	}
	return infos
}

func dataTableMap(table datafiles.TableMeta, sampleRows []map[string]any) map[string]any {
	entry := map[string]any{
		"tableName":   table.TableName,
		"rowCount":    table.RowCount,
		"columns":     table.Columns,
		"columnTypes": table.ColumnTypes,
	}
	if sampleRows != nil {
		entry["sampleRows"] = sampleRows
	}
	return entry
}

func dataColumnProfile(raw map[string]any) gonvex.DataColumnProfile {
	profile := gonvex.DataColumnProfile{}
	profile.Name, _ = raw["name"].(string)
	profile.Type, _ = raw["type"].(string)
	if v, ok := raw["nullCount"].(int64); ok {
		profile.NullCount = v
	}
	switch v := raw["distinctCount"].(type) {
	case int64:
		profile.DistinctCount = v
	case float64:
		profile.DistinctCount = int64(v)
	}
	profile.Min = raw["min"]
	profile.Max = raw["max"]
	if mean, ok := raw["mean"].(float64); ok {
		profile.Mean = &mean
	}
	if examples, ok := raw["examples"].([]string); ok {
		profile.Examples = examples
	}
	return profile
}
