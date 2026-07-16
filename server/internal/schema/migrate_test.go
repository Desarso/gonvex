package schema

import "testing"

func TestRememberExistingColumnPreservesPrimaryKeyAcrossConstraintRows(t *testing.T) {
	for _, rows := range [][]existingColumn{
		{{Type: "id", PrimaryKey: true}, {Type: "id", PrimaryKey: false}},
		{{Type: "id", PrimaryKey: false}, {Type: "id", PrimaryKey: true}},
	} {
		columns := map[string]existingColumn{}
		for _, row := range rows {
			rememberExistingColumn(columns, "id", row)
		}
		if !columns["id"].PrimaryKey {
			t.Fatal("primary-key metadata was lost when another key constraint produced a duplicate row")
		}
	}
}
