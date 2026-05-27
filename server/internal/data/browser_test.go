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
