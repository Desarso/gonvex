package data

import (
	"strings"
	"testing"
)

func TestTaskSearchMatchesPartialPGID(t *testing.T) {
	where, args, err := rowsWhereClause("tasks", []string{"name", "pg_id"}, map[string]bool{"name": true, "pg_id": true}, "73", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(where, `COALESCE("pg_id"::text, '') ILIKE $2`) {
		t.Fatalf("expected partial pg_id search in where clause, got %q", where)
	}
	if len(args) < 2 || args[1] != "%73%" {
		t.Fatalf("expected pg_id search arg to be %%73%%, got %#v", args)
	}
}

func TestTaskPGIDEqualsFilterUsesIntegerPredicate(t *testing.T) {
	where, args, err := rowsWhereClause("tasks", []string{"pg_id"}, map[string]bool{"pg_id": true}, "", []RowsFilter{{Column: "pg_id", Operator: "equals", Value: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if where != ` WHERE "pg_id" = $1` {
		t.Fatalf("expected integer pg_id predicate, got %q", where)
	}
	if len(args) != 1 || args[0] != int64(1) {
		t.Fatalf("expected integer arg 1, got %#v", args)
	}
}

func TestTaskPGIDEqualsFilterRejectsInvalidInteger(t *testing.T) {
	where, args, err := rowsWhereClause("tasks", []string{"pg_id"}, map[string]bool{"pg_id": true}, "", []RowsFilter{{Column: "pg_id", Operator: "equals", Value: "abc"}})
	if err != nil {
		t.Fatal(err)
	}
	if where != " WHERE false" {
		t.Fatalf("expected impossible predicate for invalid integer, got %q", where)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %#v", args)
	}
}

func TestTaskSearchPreservesExactPhrase(t *testing.T) {
	allowed := map[string]bool{"name": true, "title": true, "description": true}
	where, args, err := rowsWhereClause("tasks", []string{"name", "title", "description"}, allowed, "audit dev manifest after", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(where, ` ILIKE '%' || $1 || '%' ESCAPE '\'`) {
		t.Fatalf("expected exact phrase search predicate, got %q", where)
	}
	if len(args) != 1 || args[0] != "audit dev manifest after" {
		t.Fatalf("expected exact phrase arg, got %#v", args)
	}
}

func TestTaskSearchUsesGeneratedSearchTextWhenPresent(t *testing.T) {
	allowed := map[string]bool{"name": true, "title": true, "description": true, "search_text": true}
	where, args, err := rowsWhereClause("tasks", []string{"name", "title", "description", "search_text"}, allowed, "audit dev", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(where, `"search_text" ILIKE '%' || $1 || '%' ESCAPE '\'`) {
		t.Fatalf("expected generated search_text predicate, got %q", where)
	}
	if len(args) != 1 || args[0] != "audit dev" {
		t.Fatalf("expected exact phrase arg, got %#v", args)
	}
}

func TestTaskSearchEscapesLikeWildcards(t *testing.T) {
	allowed := map[string]bool{"name": true, "title": true, "description": true}
	_, args, err := rowsWhereClause("tasks", []string{"name", "title", "description"}, allowed, `audit_% dev`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 1 || args[0] != `audit\_\% dev` {
		t.Fatalf("expected escaped LIKE pattern, got %#v", args)
	}
}

func TestNormalizedLimitBounds(t *testing.T) {
	tests := []struct {
		name  string
		limit int
		want  int
	}{
		{name: "default for zero", limit: 0, want: 100},
		{name: "default for negative", limit: -12, want: 100},
		{name: "keeps positive value", limit: 42, want: 42},
		{name: "caps large values", limit: 5000, want: 1000},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := normalizedLimit(test.limit); got != test.want {
				t.Fatalf("expected %d, got %d", test.want, got)
			}
		})
	}
}

func TestSelectedRowsColumns(t *testing.T) {
	all := []string{"id", "title", "status"}
	allowed := map[string]bool{"id": true, "title": true, "status": true}

	tests := []struct {
		name      string
		requested []string
		want      []string
		wantErr   bool
	}{
		{name: "all columns when none requested", requested: nil, want: all},
		{name: "trims blanks and removes duplicates", requested: []string{" title ", "status", "title", ""}, want: []string{"title", "status"}},
		{name: "falls back to all columns when only blanks requested", requested: []string{"", " "}, want: all},
		{name: "rejects unknown columns", requested: []string{"title", "missing"}, wantErr: true},
		{name: "rejects invalid identifiers", requested: []string{"title;drop"}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := selectedRowsColumns(all, allowed, test.requested)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if strings.Join(got, ",") != strings.Join(test.want, ",") {
				t.Fatalf("expected %#v, got %#v", test.want, got)
			}
		})
	}
}

func TestRowsWhereClauseFilterOperators(t *testing.T) {
	allowed := map[string]bool{"title": true, "status": true, "due_at": true}
	tests := []struct {
		name        string
		filter      RowsFilter
		wantWhere   string
		wantArgs    []any
		wantErrText string
	}{
		{
			name:      "contains default",
			filter:    RowsFilter{Column: "title", Operator: "contains", Value: "bug"},
			wantWhere: ` WHERE COALESCE("title"::text, '') ILIKE $1`,
			wantArgs:  []any{"%bug%"},
		},
		{
			name:      "not contains",
			filter:    RowsFilter{Column: "title", Operator: "notContains", Value: "wip"},
			wantWhere: ` WHERE COALESCE("title"::text, '') NOT ILIKE $1`,
			wantArgs:  []any{"%wip%"},
		},
		{
			name:      "starts with",
			filter:    RowsFilter{Column: "title", Operator: "startsWith", Value: "fix"},
			wantWhere: ` WHERE COALESCE("title"::text, '') ILIKE $1`,
			wantArgs:  []any{"fix%"},
		},
		{
			name:      "ends with",
			filter:    RowsFilter{Column: "title", Operator: "endsWith", Value: "done"},
			wantWhere: ` WHERE COALESCE("title"::text, '') ILIKE $1`,
			wantArgs:  []any{"%done"},
		},
		{
			name:      "empty",
			filter:    RowsFilter{Column: "title", Operator: "empty"},
			wantWhere: ` WHERE ("title" IS NULL OR "title"::text = '')`,
		},
		{
			name:      "not empty",
			filter:    RowsFilter{Column: "title", Operator: "notEmpty"},
			wantWhere: ` WHERE ("title" IS NOT NULL AND "title"::text <> '')`,
		},
		{
			name:      "one of",
			filter:    RowsFilter{Column: "status", Operator: "oneOf", Value: `["open","closed"]`},
			wantWhere: ` WHERE COALESCE("status"::text, '') IN ($1, $2)`,
			wantArgs:  []any{"open", "closed"},
		},
		{
			name:      "range with both bounds",
			filter:    RowsFilter{Column: "due_at", Operator: "inRange", Value: "2026-01-01", ValueTo: "2026-01-31"},
			wantWhere: ` WHERE ("due_at" IS NOT NULL AND "due_at" >= $1 AND "due_at" <= $2)`,
			wantArgs:  []any{"2026-01-01", "2026-01-31"},
		},
		{
			name:      "range with empty bounds is skipped",
			filter:    RowsFilter{Column: "due_at", Operator: "inRange"},
			wantWhere: "",
		},
		{
			name:        "invalid oneOf json",
			filter:      RowsFilter{Column: "status", Operator: "oneOf", Value: `not-json`},
			wantErrText: "invalid oneOf filter value",
		},
		{
			name:        "invalid column",
			filter:      RowsFilter{Column: "bad-column", Operator: "equals", Value: "x"},
			wantErrText: "invalid filter column",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			where, args, err := rowsWhereClause("tickets", []string{"title", "status", "due_at"}, allowed, "", []RowsFilter{test.filter})
			if test.wantErrText != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErrText) {
					t.Fatalf("expected error containing %q, got %v", test.wantErrText, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if where != test.wantWhere {
				t.Fatalf("expected where %q, got %q", test.wantWhere, where)
			}
			if len(args) != len(test.wantArgs) {
				t.Fatalf("expected args %#v, got %#v", test.wantArgs, args)
			}
			for index := range args {
				if args[index] != test.wantArgs[index] {
					t.Fatalf("expected args %#v, got %#v", test.wantArgs, args)
				}
			}
		})
	}
}

func TestRowsWhereClauseCombinesSearchAndFilters(t *testing.T) {
	columns := []string{"title", "status"}
	allowed := map[string]bool{"title": true, "status": true}

	where, args, err := rowsWhereClause("tickets", columns, allowed, "urgent", []RowsFilter{{Column: "status", Operator: "equals", Value: "open"}})
	if err != nil {
		t.Fatal(err)
	}
	if where != ` WHERE (COALESCE("title"::text, '') ILIKE $1 OR COALESCE("status"::text, '') ILIKE $1) AND "status"::text = $2` {
		t.Fatalf("unexpected combined where clause: %q", where)
	}
	if len(args) != 2 || args[0] != "%urgent%" || args[1] != "open" {
		t.Fatalf("unexpected args: %#v", args)
	}
}

func TestRowsOrderBy(t *testing.T) {
	columns := []string{"id", "created_at", "title"}
	allowed := map[string]bool{"id": true, "created_at": true, "title": true}
	tests := []struct {
		name      string
		sort      string
		direction string
		want      string
		wantErr   bool
	}{
		{name: "explicit asc", sort: "title", direction: "asc", want: ` ORDER BY "title" ASC`},
		{name: "explicit desc is case insensitive", sort: "title", direction: "DESC", want: ` ORDER BY "title" DESC`},
		{name: "default prefers created_at descending", want: ` ORDER BY "created_at" DESC`},
		{name: "rejects invalid sort columns", sort: "missing", wantErr: true},
		{name: "rejects invalid identifiers", sort: "title desc", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := rowsOrderBy(columns, allowed, test.sort, test.direction)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != test.want {
				t.Fatalf("expected %q, got %q", test.want, got)
			}
		})
	}
}

func TestTaskPagingQuerySelection(t *testing.T) {
	allowed := map[string]bool{"id": true, "created_at": true}

	if !useDefaultTaskPageQuery("tasks", allowed, RowsOptions{}, "") {
		t.Fatal("expected default task page query")
	}
	if useDefaultTaskPageQuery("tasks", allowed, RowsOptions{SortColumn: "title"}, "") {
		t.Fatal("did not expect default task page query with explicit sort")
	}
	if !useKeysetTaskPageQuery("tasks", allowed, RowsOptions{CursorID: "10", CursorCreatedAt: "2026-01-01T00:00:00Z"}, "") {
		t.Fatal("expected keyset task page query")
	}
	if useKeysetTaskPageQuery("tasks", allowed, RowsOptions{CursorID: "10"}, "") {
		t.Fatal("did not expect keyset query without created_at cursor")
	}
}

func TestIdentifierAndQuotingHelpers(t *testing.T) {
	if !validIdent("tasks_2026") {
		t.Fatal("expected identifier to be valid")
	}
	for _, invalid := range []string{"", "1table", "bad-name", "public.tasks", "tasks;drop"} {
		if validIdent(invalid) {
			t.Fatalf("expected %q to be invalid", invalid)
		}
	}
	if got := quoteIdent(`weird"name`); got != `"weird""name"` {
		t.Fatalf("unexpected quoted identifier: %q", got)
	}
}

func TestBlankValue(t *testing.T) {
	for _, value := range []any{nil, ""} {
		if !blankValue(value) {
			t.Fatalf("expected %#v to be blank", value)
		}
	}
	for _, value := range []any{"0", 0, false} {
		if blankValue(value) {
			t.Fatalf("expected %#v to be nonblank", value)
		}
	}
}

func TestCleanDeleteIDsTrimsAndDeduplicates(t *testing.T) {
	got := cleanDeleteIDs([]string{" one ", "", "two", "one", "two "})
	want := []string{"one", "two"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("expected %v, got %v", want, got)
	}
}
