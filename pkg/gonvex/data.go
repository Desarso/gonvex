package gonvex

import "context"

// data.go — the uploaded-data-file surface exposed on every runtime context as
// ctx.Data. Uploaded CSV/XLSX/XLS attachments are ingested once into a DuckDB
// artifact owned by the runtime; app functions then inspect, query, and
// profile the artifact by fileKey without ever re-reading the raw upload.
// The concrete implementation lives in the host server (internal/datafiles).

// DataIngestRequest asks the runtime to build (or reuse) a DuckDB artifact for
// a stored upload. FileID is the storage file id the bytes were uploaded to.
type DataIngestRequest struct {
	FileID      string `json:"fileId"`
	Filename    string `json:"filename"`
	ContentType string `json:"contentType,omitempty"`
}

// DataTableInfo summarizes one table (CSV data or workbook sheet) inside an
// ingested artifact.
type DataTableInfo struct {
	TableName string   `json:"tableName"`
	RowCount  int64    `json:"rowCount"`
	Columns   []string `json:"columns"`
	// ColumnTypes aligns with Columns and holds DuckDB type names.
	ColumnTypes []string `json:"columnTypes,omitempty"`
}

type DataIngestResult struct {
	OK       bool            `json:"ok"`
	FileKey  string          `json:"fileKey,omitempty"`
	Summary  string          `json:"summary,omitempty"`
	Tables   []DataTableInfo `json:"tables,omitempty"`
	Warnings []string        `json:"warnings,omitempty"`
	Error    string          `json:"error,omitempty"`
}

type DataInspectRequest struct {
	FileKey   string `json:"fileKey"`
	Operation string `json:"operation,omitempty"` // overview | schema | sample
	TableName string `json:"tableName,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type DataInspectResult struct {
	OK       bool             `json:"ok"`
	FileKey  string           `json:"fileKey,omitempty"`
	Summary  string           `json:"summary,omitempty"`
	Tables   []map[string]any `json:"tables,omitempty"`
	Warnings []string         `json:"warnings,omitempty"`
	Error    string           `json:"error,omitempty"`
}

type DataQueryRequest struct {
	FileKey string `json:"fileKey"`
	SQL     string `json:"sql"`
	Limit   int    `json:"limit,omitempty"`
}

type DataQueryResult struct {
	OK        bool             `json:"ok"`
	Columns   []string         `json:"columns,omitempty"`
	Rows      []map[string]any `json:"rows,omitempty"`
	RowCount  int              `json:"rowCount,omitempty"`
	Truncated bool             `json:"truncated,omitempty"`
	Warnings  []string         `json:"warnings,omitempty"`
	Error     string           `json:"error,omitempty"`
}

type DataProfileRequest struct {
	FileKey    string `json:"fileKey"`
	TableName  string `json:"tableName,omitempty"`
	MaxColumns int    `json:"maxColumns,omitempty"`
}

type DataColumnProfile struct {
	Name          string   `json:"name"`
	Type          string   `json:"type"`
	NullCount     int64    `json:"nullCount"`
	DistinctCount int64    `json:"distinctCount"`
	Min           any      `json:"min,omitempty"`
	Max           any      `json:"max,omitempty"`
	Mean          *float64 `json:"mean,omitempty"`
	Examples      []string `json:"examples,omitempty"`
}

type DataTableProfile struct {
	TableName string              `json:"tableName"`
	RowCount  int64               `json:"rowCount"`
	Columns   []DataColumnProfile `json:"columns"`
}

type DataProfileResult struct {
	OK       bool               `json:"ok"`
	FileKey  string             `json:"fileKey,omitempty"`
	Tables   []DataTableProfile `json:"tables,omitempty"`
	Warnings []string           `json:"warnings,omitempty"`
	Error    string             `json:"error,omitempty"`
}

// DataAPI is the runtime data-file surface. Query is SELECT-only; all methods
// are tenant-scoped by the host.
type DataAPI interface {
	Ingest(ctx context.Context, req DataIngestRequest) (DataIngestResult, error)
	Inspect(ctx context.Context, req DataInspectRequest) (DataInspectResult, error)
	Query(ctx context.Context, req DataQueryRequest) (DataQueryResult, error)
	Profile(ctx context.Context, req DataProfileRequest) (DataProfileResult, error)
}

type dataUnavailable struct{}

const dataUnavailableMessage = "gonvex: data runtime is not configured"

func (dataUnavailable) Ingest(context.Context, DataIngestRequest) (DataIngestResult, error) {
	return DataIngestResult{OK: false, Error: dataUnavailableMessage}, nil
}

func (dataUnavailable) Inspect(context.Context, DataInspectRequest) (DataInspectResult, error) {
	return DataInspectResult{OK: false, Error: dataUnavailableMessage}, nil
}

func (dataUnavailable) Query(context.Context, DataQueryRequest) (DataQueryResult, error) {
	return DataQueryResult{OK: false, Error: dataUnavailableMessage}, nil
}

func (dataUnavailable) Profile(context.Context, DataProfileRequest) (DataProfileResult, error) {
	return DataProfileResult{OK: false, Error: dataUnavailableMessage}, nil
}

func UnavailableData() DataAPI {
	return dataUnavailable{}
}
