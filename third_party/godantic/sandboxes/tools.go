package sandboxes

import models "github.com/Desarso/godantic/models"

func objectSchema(properties map[string]any, required ...string) models.Parameters {
	if properties == nil {
		properties = map[string]any{}
	}
	return models.Parameters{Type: "object", Properties: properties, Required: required}
}

func prop(typ, description string) map[string]any {
	return map[string]any{"type": typ, "description": description}
}

func RunGoTool() models.FunctionDeclaration {
	return models.FunctionDeclaration{
		Name:        "sandbox_run_go",
		Description: "Run backend Go code in a restricted sandbox. Use for data analysis or import preview work that should not run in the browser.",
		Parameters: objectSchema(map[string]any{
			"purpose":   prop("string", "Plain-language explanation of what the Go code will do."),
			"mode":      map[string]any{"type": "string", "enum": []string{string(ModeAnalysis), string(ModePreview), string(ModeApply)}, "description": "Execution mode. apply requires host approval."},
			"code":      prop("string", "Go function body. It must return (any, error) from the generated Run function."),
			"timeoutMs": prop("number", "Optional timeout in milliseconds."),
			"fileKey":   prop("string", "Optional data-file key available to the sandbox."),
		}, "purpose", "code"),
	}
}

func InspectDataTool() models.FunctionDeclaration {
	return models.FunctionDeclaration{
		Name:        "inspect_data_file",
		Description: "Inspect an uploaded CSV/XLS/XLSX data file without loading the whole file into model context.",
		Parameters: objectSchema(map[string]any{
			"fileKey":   prop("string", "Data file key from the uploaded-file summary."),
			"filename":  prop("string", "Uploaded filename when no fileKey is known."),
			"operation": prop("string", "overview | schema | sample | profile"),
			"tableName": prop("string", "Optional table or sheet name."),
			"limit":     prop("number", "Maximum rows or examples to include."),
		}),
	}
}

func QueryDataTool() models.FunctionDeclaration {
	return models.FunctionDeclaration{
		Name:        "query_data_file",
		Description: "Run bounded read-only SQL against an uploaded data file's DuckDB artifact.",
		Parameters: objectSchema(map[string]any{
			"fileKey": prop("string", "Data file key from the uploaded-file summary."),
			"sql":     prop("string", "Read-only SELECT SQL."),
			"limit":   prop("number", "Maximum result rows."),
		}, "fileKey", "sql"),
	}
}

func ProfileDataTool() models.FunctionDeclaration {
	return models.FunctionDeclaration{
		Name:        "profile_data_file",
		Description: "Compute per-column statistics (types, null counts, distinct counts, min/max/mean, examples) for an uploaded data file's DuckDB artifact.",
		Parameters: objectSchema(map[string]any{
			"fileKey":    prop("string", "Data file key from the uploaded-file summary."),
			"tableName":  prop("string", "Optional table or sheet name; defaults to all tables."),
			"maxColumns": prop("number", "Maximum columns to profile."),
		}, "fileKey"),
	}
}

func DefaultTools() []models.FunctionDeclaration {
	return []models.FunctionDeclaration{
		InspectDataTool(),
		QueryDataTool(),
		ProfileDataTool(),
		RunGoTool(),
	}
}
