package backend

import "github.com/gonvex/gonvex/pkg/gonvex"

func Schema(s *gonvex.Schema) {
	s.TenantTable("messages", func(t *gonvex.Table) {
		t.ID("id")
		t.String("body")
		t.String("author")
		t.Time("created_at")

		t.Index("by_created_at", "created_at")
	})
}
