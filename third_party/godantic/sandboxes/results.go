package sandboxes

// results.go — the shared result contracts every analysis/import flow returns,
// so hosts and frontends can render results without tool-specific parsing.

// RowError ties a validation error to a specific row of the source data.
type RowError struct {
	RowNumber int    `json:"rowNumber"`
	TableName string `json:"tableName,omitempty"`
	Column    string `json:"column,omitempty"`
	Error     string `json:"error"`
}

// ImportCounts summarizes what an import preview or apply would do.
type ImportCounts struct {
	WillCreate int `json:"willCreate"`
	WillReuse  int `json:"willReuse"`
	WillUpdate int `json:"willUpdate"`
	WillSkip   int `json:"willSkip"`
}

// AnalysisReport is the canonical envelope for analysis and import results.
type AnalysisReport struct {
	OK       bool             `json:"ok"`
	Summary  string           `json:"summary,omitempty"`
	FileKey  string           `json:"fileKey,omitempty"`
	Tables   []map[string]any `json:"tables,omitempty"`
	Mapping  map[string]any   `json:"mapping,omitempty"`
	Counts   *ImportCounts    `json:"counts,omitempty"`
	Warnings []string         `json:"warnings,omitempty"`
	Errors   []RowError       `json:"errors,omitempty"`
}
