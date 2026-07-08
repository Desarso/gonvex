package sandboxes

import (
	"encoding/json"
	"testing"
)

func TestDefaultTools(t *testing.T) {
	tools := DefaultTools()
	if len(tools) != 4 {
		t.Fatalf("len(DefaultTools()) = %d, want 4", len(tools))
	}
	names := map[string]bool{}
	for _, tool := range tools {
		names[tool.Name] = true
		if tool.Parameters.Type != "object" {
			t.Fatalf("%s parameters type = %q, want object", tool.Name, tool.Parameters.Type)
		}
	}
	for _, name := range []string{"inspect_data_file", "query_data_file", "profile_data_file", "sandbox_run_go"} {
		if !names[name] {
			t.Fatalf("DefaultTools() missing %s", name)
		}
	}
}

func TestRunPolicyApproval(t *testing.T) {
	cases := []struct {
		mode         Mode
		codeMayWrite bool
		want         ApprovalCategory
		readOnly     bool
	}{
		{ModeAnalysis, false, ApprovalNone, true},
		{ModePreview, false, ApprovalNone, true},
		{"", false, ApprovalNone, true},
		{ModeApply, false, ApprovalHost, false},
		{ModeAnalysis, true, ApprovalHost, false},
		{ModePreview, true, ApprovalHost, false},
	}
	for _, tc := range cases {
		policy := RunPolicy(RunRequest{Language: LanguageGo, Mode: tc.mode}, tc.codeMayWrite)
		if policy.Approval != tc.want {
			t.Fatalf("RunPolicy(mode=%q, mayWrite=%v).Approval = %q, want %q", tc.mode, tc.codeMayWrite, policy.Approval, tc.want)
		}
		if policy.ReadOnly != tc.readOnly {
			t.Fatalf("RunPolicy(mode=%q, mayWrite=%v).ReadOnly = %v, want %v", tc.mode, tc.codeMayWrite, policy.ReadOnly, tc.readOnly)
		}
		if policy.Limits.TimeoutMs <= 0 {
			t.Fatalf("RunPolicy(mode=%q).Limits.TimeoutMs must be positive", tc.mode)
		}
	}
	if p := DataToolPolicy("query_data_file"); p.Approval != ApprovalNone || !p.ReadOnly {
		t.Fatalf("DataToolPolicy must be auto-run read-only, got %+v", p)
	}
}

func TestAnalysisReportRoundTrip(t *testing.T) {
	mean := 4.5
	report := AnalysisReport{
		OK:      true,
		Summary: "Detected 1 sheet and 12 task rows.",
		FileKey: "data_abc",
		Counts:  &ImportCounts{WillCreate: 10, WillSkip: 2},
		Errors:  []RowError{{RowNumber: 1842, TableName: "Tasks", Column: "Workspace", Error: "Workspace is missing"}},
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var decoded AnalysisReport
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Counts == nil || decoded.Counts.WillCreate != 10 || len(decoded.Errors) != 1 || decoded.Errors[0].RowNumber != 1842 {
		t.Fatalf("AnalysisReport round trip mismatch: %+v", decoded)
	}
	profile := ColumnProfile{Name: "priority", Type: "VARCHAR", DistinctCount: 4, Mean: &mean}
	if _, err := json.Marshal(TableProfile{TableName: "tasks", RowCount: 12, Columns: []ColumnProfile{profile}}); err != nil {
		t.Fatal(err)
	}
}
