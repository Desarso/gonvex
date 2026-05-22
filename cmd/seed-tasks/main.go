package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const defaultCount = 3_000

var taskColumns = []string{
	"id",
	"tenant_id",
	"pg_id",
	"name",
	"title",
	"status",
	"description",
	"audience",
	"audience_user_ids",
	"audience_team_ids",
	"requires_acknowledgment",
	"workspace_id",
	"category_id",
	"team_id",
	"template_id",
	"spot_id",
	"status_id",
	"priority_id",
	"sla_id",
	"approval_id",
	"form_id",
	"requires_signature",
	"asset_id",
	"created_by",
	"reported_by_team_id",
	"reported_by_user_id",
	"recurrence_id",
	"workplan_id",
	"workplan_item_id",
	"workplan_instance_id",
	"occurrence_date",
	"expected_duration",
	"flag_color",
	"sla_started_at",
	"sla_response_deadline",
	"sla_resolution_deadline",
	"sla_responded_at",
	"sla_resolved_at",
	"sla_paused_duration",
	"due_date",
	"start_date",
	"completed_at",
	"latitude",
	"longitude",
	"first_viewed_at",
	"deleted_at",
	"row_hash",
	"priority",
	"assignee",
	"project",
	"label",
	"due_at",
	"completed",
	"estimate_minutes",
	"progress",
	"created_at",
	"updated_at",
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	loadDotEnv(".env")

	count := flag.Int("count", defaultCount, "number of task rows to seed")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed used to generate repeatable task data")
	batchSize := flag.Int("batch", 10_000, "rows to copy per batch")
	clear := flag.Bool("clear", false, "delete existing task rows before seeding")
	flag.Parse()

	if *count < 1 {
		return fmt.Errorf("count must be greater than zero")
	}
	if *batchSize < 1 {
		return fmt.Errorf("batch must be greater than zero")
	}

	databaseURL := env("DATABASE_URL", env("POSTGRES_URL", ""))
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL or POSTGRES_URL must be set")
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if err := ensureTasksSchema(ctx, conn); err != nil {
		return err
	}
	if *clear {
		if _, err := conn.Exec(ctx, `TRUNCATE TABLE tasks`); err != nil {
			return err
		}
	}

	rng := rand.New(rand.NewSource(*seed))
	started := time.Now()
	inserted := 0
	for inserted < *count {
		batchCount := min(*batchSize, *count-inserted)
		source := &taskSource{
			start: inserted,
			count: batchCount,
			seed:  *seed,
			rng:   rng,
		}

		copied, err := conn.CopyFrom(ctx, pgx.Identifier{"tasks"}, taskColumns, source)
		if err != nil {
			return err
		}
		inserted += int(copied)
		fmt.Printf("seeded %d/%d tasks\n", inserted, *count)
	}

	fmt.Printf("seeded %d tasks in %s with seed %d\n", inserted, time.Since(started).Round(time.Second), *seed)
	return nil
}

type taskSource struct {
	start int
	count int
	seed  int64
	rng   *rand.Rand
	index int
	row   []any
	err   error
}

func (s *taskSource) Next() bool {
	if s.index >= s.count {
		return false
	}
	s.row = buildTaskRow(s.start+s.index, s.seed, s.rng)
	s.index++
	return true
}

func (s *taskSource) Values() ([]any, error) {
	return s.row, nil
}

func (s *taskSource) Err() error {
	return s.err
}

func buildTaskRow(index int, seed int64, rng *rand.Rand) []any {
	statuses := []struct {
		id     string
		name   string
		action string
	}{
		{"st_new", "New", "OPEN"},
		{"st_progress", "In Progress", "ACTIVE"},
		{"st_waiting", "Waiting", "BLOCKED"},
		{"st_review", "Review", "ACTIVE"},
		{"st_done", "Done", "DONE"},
	}
	prioritiesMeta := []struct {
		id   string
		name string
	}{
		{"pr_low", "Low"},
		{"pr_med", "Medium"},
		{"pr_high", "High"},
		{"pr_urgent", "Urgent"},
	}
	assignees := []string{"Avery", "Blake", "Casey", "Devon", "Elliot", "Finley", "Gray", "Harper", "Jordan", "Morgan", "Parker", "Quinn", "Riley", "Skyler", "Taylor"}
	projects := []string{"Dashboard", "Runtime", "CLI", "Storage", "Realtime", "Docs", "Auth", "Data Browser", "Migrations", "Performance"}
	labels := []string{"bug", "feature", "cleanup", "research", "backend", "frontend", "infra", "testing", "docs", "perf"}
	spots := []string{"Lobby", "Kitchen", "Front Desk", "Pool", "Room 204", "Garden", "Laundry", "Restaurant", "Office", "Storage"}
	flags := []string{"red", "orange", "yellow", "green", "blue", "purple", ""}
	verbs := []string{"Wire", "Audit", "Refactor", "Benchmark", "Design", "Patch", "Validate", "Ship", "Document", "Harden", "Prototype", "Review"}
	objects := []string{"live grid", "schema sync", "runtime route", "query cache", "mutation path", "upload flow", "table browser", "dev manifest", "migration plan", "status panel"}
	contexts := []string{"for local dev", "before release", "under load", "with fallback data", "for dashboard QA", "against Postgres", "with generated bindings", "for realtime patches"}

	statusMeta := statuses[rng.Intn(len(statuses))]
	priorityMeta := prioritiesMeta[rng.Intn(len(prioritiesMeta))]
	status := strings.ToLower(strings.ReplaceAll(statusMeta.name, " ", "_"))
	priority := strings.ToLower(priorityMeta.name)
	assignee := assignees[rng.Intn(len(assignees))]
	project := projects[rng.Intn(len(projects))]
	label := labels[rng.Intn(len(labels))]
	spot := spots[rng.Intn(len(spots))]
	title := fmt.Sprintf("%s %s %s", verbs[rng.Intn(len(verbs))], objects[rng.Intn(len(objects))], contexts[rng.Intn(len(contexts))])
	description := fmt.Sprintf("%s task %d at %s. %s owns the next step; tagged %s. Generated from Whagons-style seed %d.", project, index+1, spot, assignee, label, seed)
	completed := statusMeta.action == "DONE"
	progress := progressForStatus(status, rng)
	estimateMinutes := []int{15, 30, 45, 60, 90, 120, 180, 240, 360, 480}[rng.Intn(10)]
	now := time.Now().UTC()
	createdAt := now.Add(-time.Duration(rng.Intn(180*24)) * time.Hour).Add(-time.Duration(rng.Intn(86_400)) * time.Second)
	dueAt := now.Add(time.Duration(rng.Intn(120*24)-30*24) * time.Hour)
	startAt := dueAt.Add(-time.Duration(estimateMinutes) * time.Minute)
	var completedAt any
	if completed {
		completedAt = createdAt.Add(time.Duration(rng.Intn(14*24)) * time.Hour)
	}
	updatedAt := createdAt.Add(time.Duration(rng.Intn(45*24)) * time.Hour)
	if updatedAt.After(now) {
		updatedAt = now.Add(-time.Duration(rng.Intn(86_400)) * time.Second)
	}
	flagColor := flags[rng.Intn(len(flags))]
	pgID := index + 1

	return []any{
		fmt.Sprintf("task_%x_%08d", uint64(seed), index+1),
		"tenant_demo",
		pgID,
		title,
		title,
		status,
		description,
		"workspace",
		`[]`,
		`[]`,
		rng.Intn(5) == 0,
		fmt.Sprintf("ws_%02d", rng.Intn(4)+1),
		fmt.Sprintf("cat_%02d", rng.Intn(6)+1),
		fmt.Sprintf("team_%02d", rng.Intn(4)+1),
		fmt.Sprintf("tpl_%02d", rng.Intn(8)+1),
		fmt.Sprintf("spot_%02d", rng.Intn(10)+1),
		statusMeta.id,
		priorityMeta.id,
		fmt.Sprintf("sla_%02d", rng.Intn(3)+1),
		fmt.Sprintf("appr_%02d", rng.Intn(3)+1),
		fmt.Sprintf("form_%02d", rng.Intn(4)+1),
		rng.Intn(7) == 0,
		fmt.Sprintf("asset_%02d", rng.Intn(10)+1),
		strings.ToLower(assignee),
		fmt.Sprintf("team_%02d", rng.Intn(4)+1),
		strings.ToLower(assignees[rng.Intn(len(assignees))]),
		nil,
		nil,
		nil,
		nil,
		dueAt.Format("2006-01-02"),
		estimateMinutes,
		flagColor,
		createdAt,
		dueAt.Add(-2 * time.Hour),
		dueAt,
		completedAt,
		completedAt,
		rng.Intn(600),
		dueAt,
		startAt,
		completedAt,
		9.93 + rng.Float64()/10,
		-84.09 - rng.Float64()/10,
		createdAt.Add(time.Duration(rng.Intn(72)) * time.Hour),
		nil,
		fmt.Sprintf("row_%x_%d", uint64(seed), pgID),
		priority,
		assignee,
		project,
		label,
		dueAt,
		completed,
		estimateMinutes,
		progress,
		createdAt,
		updatedAt,
	}
}

func progressForStatus(status string, rng *rand.Rand) int {
	switch status {
	case "todo", "new":
		return rng.Intn(15)
	case "in_progress":
		return 15 + rng.Intn(65)
	case "blocked", "waiting":
		return 5 + rng.Intn(55)
	case "review":
		return 70 + rng.Intn(25)
	case "done", "archived":
		return 100
	default:
		return rng.Intn(101)
	}
}

func ensureTasksSchema(ctx context.Context, conn *pgx.Conn) error {
	statements := []string{
		`CREATE EXTENSION IF NOT EXISTS pg_trgm`,
		`CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL, status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ)`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS tenant_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS pg_id INTEGER`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS name TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS description TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS audience TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS audience_user_ids JSONB`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS audience_team_ids JSONB`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS requires_acknowledgment BOOLEAN`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS workspace_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS category_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS team_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS template_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS spot_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS status_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS priority_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sla_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS approval_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS form_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS requires_signature BOOLEAN`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS asset_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS created_by TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS reported_by_team_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS reported_by_user_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS recurrence_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS workplan_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS workplan_item_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS workplan_instance_id TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS occurrence_date TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS expected_duration INTEGER`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS flag_color TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sla_started_at TIMESTAMPTZ`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sla_response_deadline TIMESTAMPTZ`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sla_resolution_deadline TIMESTAMPTZ`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sla_responded_at TIMESTAMPTZ`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sla_resolved_at TIMESTAMPTZ`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sla_paused_duration INTEGER`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS due_date TIMESTAMPTZ`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS start_date TIMESTAMPTZ`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS first_viewed_at TIMESTAMPTZ`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS row_hash TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS priority TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS assignee TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS project TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS label TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS due_at TIMESTAMPTZ`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS completed BOOLEAN`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS estimate_minutes INTEGER`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS progress INTEGER`,
		`CREATE INDEX IF NOT EXISTS tasks_by_status ON tasks (status)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_tenant_pg_id ON tasks (tenant_id, pg_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_workspace ON tasks (tenant_id, workspace_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_status_id ON tasks (tenant_id, status_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_category ON tasks (tenant_id, category_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_priority_id ON tasks (tenant_id, priority_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_team ON tasks (tenant_id, team_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_spot ON tasks (tenant_id, spot_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_priority ON tasks (priority)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_assignee ON tasks (assignee)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_project ON tasks (project)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_due_at ON tasks (due_at)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_created_at ON tasks (created_at)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_created_at_id ON tasks (created_at, id)`,
		`CREATE INDEX IF NOT EXISTS tasks_name_trgm ON tasks USING gin (name gin_trgm_ops)`,
		`CREATE INDEX IF NOT EXISTS tasks_title_trgm ON tasks USING gin (title gin_trgm_ops)`,
		`CREATE INDEX IF NOT EXISTS tasks_description_trgm ON tasks USING gin (description gin_trgm_ops)`,
		`CREATE INDEX IF NOT EXISTS tasks_search_text_trgm ON tasks USING gin ((COALESCE(name, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(description, '') || ' ' || COALESCE(status, '') || ' ' || COALESCE(priority, '') || ' ' || COALESCE(assignee, '') || ' ' || COALESCE(project, '') || ' ' || COALESCE(label, '') || ' ' || COALESCE(flag_color, '')) gin_trgm_ops)`,
	}
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func loadDotEnv(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || os.Getenv(key) != "" {
			continue
		}
		os.Setenv(strings.TrimSpace(key), strings.TrimSpace(value))
	}
}

func env(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
