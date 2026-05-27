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

// taskColumns matches apps/dashboard/gonvex/schema.go column order.
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
	"status_name",
	"status_color",
	"status_action",
	"status_icon",
	"status_working_animation",
	"status_initial",
	"priority_name",
	"priority_color",
	"category_name",
	"category_icon",
	"category_color",
	"tag_names",
	"tag_colors",
	"attachment_count",
	"view_count",
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
	"spot_name",
	"workspace_name",
	"assignee_names",
	"assignee_ids",
	"assignee_avatar_urls",
	"all_user_names",
	"all_user_avatar_urls",
	"notes_count",
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

type statusMeta struct {
	id, name, slug, color, action, icon, animation string
	initial                                        bool
}

type priorityMeta struct {
	id, name, slug, color string
}

type categoryMeta struct {
	id, name, icon, color string
}

type workspaceMeta struct {
	id, name string
}

type spotMeta struct {
	id, name string
}

type tagMeta struct {
	name, color string
}

type userMeta struct {
	id, name, avatar string
}

var (
	seedStatuses = []statusMeta{
		{"st_new", "New", "new", "#dbeafe", "OPEN", "circle", "", true},
		{"st_progress", "In Progress", "in_progress", "#ccfbf1", "ACTIVE", "play", "pulse", false},
		{"st_waiting", "Waiting", "waiting", "#fef3c7", "BLOCKED", "pause", "", false},
		{"st_review", "Review", "review", "#ede9fe", "ACTIVE", "eye", "", false},
		{"st_done", "Done", "done", "#dcfce7", "DONE", "check", "", false},
		{"st_cancelled", "Cancelled", "cancelled", "#f3f4f6", "DONE", "x", "", false},
		{"st_on_hold", "On Hold", "on_hold", "#fce7f3", "BLOCKED", "clock", "", false},
	}
	seedPriorities = []priorityMeta{
		{"pr_low", "Low", "low", "#dbeafe"},
		{"pr_med", "Medium", "medium", "#fef3c7"},
		{"pr_high", "High", "high", "#fed7aa"},
		{"pr_urgent", "Urgent", "urgent", "#fecaca"},
		{"pr_none", "None", "none", "#f3f4f6"},
	}
	seedCategories = []categoryMeta{
		{"cat_01", "Housekeeping", "broom", "#a7f3d0"},
		{"cat_02", "Maintenance", "wrench", "#fde68a"},
		{"cat_03", "Front Office", "desk", "#bfdbfe"},
		{"cat_04", "F&B", "utensils", "#fecdd3"},
		{"cat_05", "Engineering", "gear", "#ddd6fe"},
		{"cat_06", "Security", "shield", "#fca5a5"},
		{"cat_07", "Concierge", "bell", "#fcd34d"},
		{"cat_08", "Events", "calendar", "#c4b5fd"},
		{"cat_09", "IT", "cpu", "#93c5fd"},
		{"cat_10", "Platform", "grid", "#99f6e4"},
	}
	seedWorkspaces = []workspaceMeta{
		{"ws_01", "Main Tower"},
		{"ws_02", "East Wing"},
		{"ws_03", "Pool & Spa"},
		{"ws_04", "Back of House"},
		{"ws_05", "Conference Center"},
		{"ws_06", "Parking Garage"},
	}
	seedSpots = []spotMeta{
		{"spot_01", "Lobby"},
		{"spot_02", "Kitchen"},
		{"spot_03", "Front Desk"},
		{"spot_04", "Pool"},
		{"spot_05", "Room 204"},
		{"spot_06", "Garden"},
		{"spot_07", "Laundry"},
		{"spot_08", "Restaurant"},
		{"spot_09", "Office"},
		{"spot_10", "Storage"},
		{"spot_11", "Ballroom A"},
		{"spot_12", "Rooftop Bar"},
		{"spot_13", "Loading Dock"},
		{"spot_14", "Suite 1201"},
		{"spot_15", "Fitness Center"},
	}
	seedTags = []tagMeta{
		{"urgent", "#ef4444"},
		{"guest-request", "#f97316"},
		{"vip", "#8b5cf6"},
		{"preventive", "#22c55e"},
		{"inspection", "#3b82f6"},
		{"turnover", "#06b6d4"},
		{"noise", "#eab308"},
		{"billing", "#64748b"},
		{"safety", "#dc2626"},
		{"training", "#0ea5e9"},
		{"backend", "#6366f1"},
		{"frontend", "#ec4899"},
		{"infra", "#14b8a6"},
		{"qa", "#a855f7"},
	}
	seedUsers = []userMeta{
		{"user_avery", "Avery", "https://i.pravatar.cc/64?u=avery"},
		{"user_blake", "Blake", "https://i.pravatar.cc/64?u=blake"},
		{"user_casey", "Casey", "https://i.pravatar.cc/64?u=casey"},
		{"user_devon", "Devon", "https://i.pravatar.cc/64?u=devon"},
		{"user_elliot", "Elliot", "https://i.pravatar.cc/64?u=elliot"},
		{"user_finley", "Finley", "https://i.pravatar.cc/64?u=finley"},
		{"user_gray", "Gray", "https://i.pravatar.cc/64?u=gray"},
		{"user_harper", "Harper", "https://i.pravatar.cc/64?u=harper"},
		{"user_jordan", "Jordan", "https://i.pravatar.cc/64?u=jordan"},
		{"user_morgan", "Morgan", "https://i.pravatar.cc/64?u=morgan"},
		{"user_parker", "Parker", "https://i.pravatar.cc/64?u=parker"},
		{"user_quinn", "Quinn", "https://i.pravatar.cc/64?u=quinn"},
		{"user_riley", "Riley", "https://i.pravatar.cc/64?u=riley"},
		{"user_skyler", "Skyler", "https://i.pravatar.cc/64?u=skyler"},
		{"user_taylor", "Taylor", "https://i.pravatar.cc/64?u=taylor"},
	}
	seedProjects = []string{
		"Dashboard", "Runtime", "CLI", "Storage", "Realtime", "Docs", "Auth",
		"Data Browser", "Migrations", "Performance", "Housekeeping", "Maintenance",
		"Guest Services", "Events", "Engineering",
	}
	seedLabels = []string{
		"bug", "feature", "cleanup", "research", "backend", "frontend", "infra",
		"testing", "docs", "perf", "regression", "hotfix", "spike", "debt",
	}
	seedFlags = []string{"red", "orange", "yellow", "green", "blue", "purple", ""}
	seedVerbs = []string{
		"Wire", "Audit", "Refactor", "Benchmark", "Design", "Patch", "Validate",
		"Ship", "Document", "Harden", "Prototype", "Review", "Escalate", "Close",
		"Inspect", "Restock", "Deep-clean", "Calibrate", "Route", "Schedule",
	}
	seedObjects = []string{
		"live grid", "schema sync", "runtime route", "query cache", "mutation path",
		"upload flow", "table browser", "dev manifest", "migration plan", "status panel",
		"guest room", "HVAC unit", "minibar", "linen cart", "elevator bank",
		"fire panel", "POS terminal", "key card encoder", "pool pump", "laundry chute",
	}
	seedContexts = []string{
		"for local dev", "before release", "under load", "with fallback data",
		"for dashboard QA", "against Postgres", "with generated bindings",
		"for realtime patches", "after guest checkout", "during peak hours",
		"before inspection", "for VIP arrival", "on night shift", "post-storm",
	}
	allUserNames      string
	allUserAvatarURLs string
)

func init() {
	names := make([]string, len(seedUsers))
	avatars := make([]string, len(seedUsers))
	for i, user := range seedUsers {
		names[i] = user.name
		avatars[i] = user.avatar
	}
	allUserNames = strings.Join(names, "|")
	allUserAvatarURLs = strings.Join(avatars, "|")
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
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed for repeatable task data")
	batchSize := flag.Int("batch", 10_000, "rows per COPY batch (use 50000+ for multi-million runs)")
	clear := flag.Bool("clear", false, "truncate tasks before seeding")
	fast := flag.Bool("fast", false, "defer indexes until after load (recommended for 1M+ rows)")
	noTrgm := flag.Bool("no-trgm", false, "skip GIN trigram search indexes")
	analyze := flag.Bool("analyze", false, "run ANALYZE tasks after seeding (recommended for estimate counts)")
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

	if err := ensureTasksColumns(ctx, conn); err != nil {
		return err
	}
	if *fast {
		if err := dropTaskIndexes(ctx, conn); err != nil {
			return err
		}
	} else if err := ensureTasksIndexes(ctx, conn, !*noTrgm); err != nil {
		return err
	}

	if _, err := conn.Exec(ctx, `SET synchronous_commit = off`); err != nil {
		return err
	}

	if *clear {
		fmt.Println("truncating tasks...")
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
		fmt.Printf("seeded %d/%d tasks (%.1f%%)\n", inserted, *count, 100*float64(inserted)/float64(*count))
	}

	if *fast {
		fmt.Println("building indexes...")
		indexStarted := time.Now()
		if err := ensureTasksIndexes(ctx, conn, !*noTrgm); err != nil {
			return err
		}
		fmt.Printf("indexes built in %s\n", time.Since(indexStarted).Round(time.Second))
	}

	if *analyze {
		fmt.Println("analyzing tasks...")
		if _, err := conn.Exec(ctx, `ANALYZE tasks`); err != nil {
			return err
		}
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
	status := seedStatuses[rng.Intn(len(seedStatuses))]
	priority := seedPriorities[rng.Intn(len(seedPriorities))]
	category := seedCategories[rng.Intn(len(seedCategories))]
	workspace := seedWorkspaces[rng.Intn(len(seedWorkspaces))]
	spot := seedSpots[rng.Intn(len(seedSpots))]
	project := seedProjects[rng.Intn(len(seedProjects))]
	label := seedLabels[rng.Intn(len(seedLabels))]
	flagColor := seedFlags[rng.Intn(len(seedFlags))]

	assigneeCount := 1 + rng.Intn(3)
	if rng.Intn(8) == 0 {
		assigneeCount = 0
	}
	picked := pickUsers(rng, assigneeCount)
	primary := picked.primary()

	title := fmt.Sprintf("%s %s %s", seedVerbs[rng.Intn(len(seedVerbs))], seedObjects[rng.Intn(len(seedObjects))], seedContexts[rng.Intn(len(seedContexts))])
	description := fmt.Sprintf(
		"%s · %s · %s. Task %d in %s (%s). %s. Seed %d.",
		category.name,
		spot.name,
		workspace.name,
		index+1,
		project,
		label,
		picked.namesJoined(),
		seed,
	)

	completed := status.action == "DONE"
	progress := progressForStatus(status.slug, rng)
	estimateMinutes := []int{15, 30, 45, 60, 90, 120, 180, 240, 360, 480, 720}[rng.Intn(11)]
	now := time.Now().UTC()
	createdAt := now.Add(-time.Duration(rng.Intn(365*24)) * time.Hour).Add(-time.Duration(rng.Intn(86_400)) * time.Second)
	dueAt := now.Add(time.Duration(rng.Intn(180*24)-60*24) * time.Hour)
	startAt := dueAt.Add(-time.Duration(estimateMinutes) * time.Minute)
	var completedAt any
	if completed {
		completedAt = createdAt.Add(time.Duration(rng.Intn(30*24)) * time.Hour)
	}
	updatedAt := createdAt.Add(time.Duration(rng.Intn(90*24)) * time.Hour)
	if updatedAt.After(now) {
		updatedAt = now.Add(-time.Duration(rng.Intn(86_400)) * time.Second)
	}

	tagNames, tagColors := pickTags(rng)
	attachmentCount := rng.Intn(6)
	if rng.Intn(5) == 0 {
		attachmentCount = rng.Intn(25)
	}
	viewCount := rng.Intn(50)
	notesCount := rng.Intn(12)
	pgID := index + 1

	statusInitial := status.initial
	if rng.Intn(20) != 0 && status.slug != "new" {
		statusInitial = false
	}

	return []any{
		fmt.Sprintf("task_%x_%010d", uint64(seed), index+1),
		"tenant_demo",
		pgID,
		title,
		title,
		status.slug,
		description,
		pickAudience(rng),
		`[]`,
		`[]`,
		rng.Intn(6) == 0,
		workspace.id,
		category.id,
		fmt.Sprintf("team_%02d", rng.Intn(8)+1),
		fmt.Sprintf("tpl_%02d", rng.Intn(12)+1),
		spot.id,
		status.id,
		priority.id,
		status.name,
		status.color,
		status.action,
		status.icon,
		status.animation,
		statusInitial,
		priority.name,
		priority.color,
		category.name,
		category.icon,
		category.color,
		tagNames,
		tagColors,
		attachmentCount,
		viewCount,
		fmt.Sprintf("sla_%02d", rng.Intn(5)+1),
		fmt.Sprintf("appr_%02d", rng.Intn(4)+1),
		fmt.Sprintf("form_%02d", rng.Intn(6)+1),
		rng.Intn(9) == 0,
		fmt.Sprintf("asset_%02d", rng.Intn(20)+1),
		picked.createdBy(),
		fmt.Sprintf("team_%02d", rng.Intn(8)+1),
		seedUsers[rng.Intn(len(seedUsers))].id,
		nil,
		nil,
		nil,
		nil,
		dueAt.Format("2006-01-02"),
		estimateMinutes,
		flagColor,
		spot.name,
		workspace.name,
		picked.namesJoined(),
		picked.idsJoined(),
		picked.avatarsJoined(),
		allUserNames,
		allUserAvatarURLs,
		notesCount,
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
		priority.slug,
		primary.name,
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

type pickedUsers struct {
	users []userMeta
}

func pickUsers(rng *rand.Rand, count int) pickedUsers {
	if count <= 0 {
		return pickedUsers{}
	}
	indices := rng.Perm(len(seedUsers))
	if count > len(indices) {
		count = len(indices)
	}
	out := make([]userMeta, count)
	for i := 0; i < count; i++ {
		out[i] = seedUsers[indices[i]]
	}
	return pickedUsers{users: out}
}

func (p pickedUsers) primary() userMeta {
	if len(p.users) == 0 {
		return seedUsers[0]
	}
	return p.users[0]
}

func (p pickedUsers) namesJoined() string {
	if len(p.users) == 0 {
		return ""
	}
	names := make([]string, len(p.users))
	for i, user := range p.users {
		names[i] = user.name
	}
	return strings.Join(names, ", ")
}

func (p pickedUsers) idsJoined() string {
	if len(p.users) == 0 {
		return ""
	}
	ids := make([]string, len(p.users))
	for i, user := range p.users {
		ids[i] = user.id
	}
	return strings.Join(ids, ",")
}

func (p pickedUsers) avatarsJoined() string {
	if len(p.users) == 0 {
		return ""
	}
	avatars := make([]string, len(p.users))
	for i, user := range p.users {
		avatars[i] = user.avatar
	}
	return strings.Join(avatars, ",")
}

func (p pickedUsers) createdBy() string {
	if len(p.users) == 0 {
		return ""
	}
	return strings.ToLower(p.users[0].name)
}

func pickTags(rng *rand.Rand) (string, string) {
	if rng.Intn(4) == 0 {
		return "", ""
	}
	tagCount := 1 + rng.Intn(3)
	indices := rng.Perm(len(seedTags))
	if tagCount > len(indices) {
		tagCount = len(indices)
	}
	names := make([]string, tagCount)
	colors := make([]string, tagCount)
	for i := 0; i < tagCount; i++ {
		tag := seedTags[indices[i]]
		names[i] = tag.name
		colors[i] = tag.color
	}
	return strings.Join(names, "|"), strings.Join(colors, "|")
}

func pickAudience(rng *rand.Rand) string {
	switch rng.Intn(4) {
	case 0:
		return "workspace"
	case 1:
		return "team"
	case 2:
		return "private"
	default:
		return "public"
	}
}

func progressForStatus(status string, rng *rand.Rand) int {
	switch status {
	case "new":
		return rng.Intn(15)
	case "in_progress":
		return 15 + rng.Intn(65)
	case "waiting", "on_hold":
		return 5 + rng.Intn(55)
	case "review":
		return 70 + rng.Intn(25)
	case "done", "cancelled":
		return 100
	default:
		return rng.Intn(101)
	}
}

func ensureTasksColumns(ctx context.Context, conn *pgx.Conn) error {
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
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS status_name TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS status_color TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS status_action TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS status_icon TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS status_working_animation TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS status_initial BOOLEAN`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS priority_name TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS priority_color TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS category_name TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS category_icon TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS category_color TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS tag_names TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS tag_colors TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS attachment_count INTEGER`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS view_count INTEGER`,
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
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS spot_name TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS workspace_name TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS assignee_names TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS assignee_ids TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS assignee_avatar_urls TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS all_user_names TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS all_user_avatar_urls TEXT`,
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS notes_count INTEGER`,
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
	}
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

var taskIndexNames = []string{
	"tasks_pkey",
	"tasks_by_status",
	"tasks_by_pg_id",
	"tasks_by_name",
	"tasks_by_tenant_pg_id",
	"tasks_by_workspace",
	"tasks_by_status_id",
	"tasks_by_category",
	"tasks_by_priority_id",
	"tasks_status_name_idx",
	"tasks_priority_name_idx",
	"tasks_by_team",
	"tasks_by_spot",
	"tasks_spot_name_idx",
	"tasks_workspace_name_idx",
	"tasks_assignee_names_idx",
	"tasks_flag_color_idx",
	"tasks_by_priority",
	"tasks_by_assignee",
	"tasks_by_project",
	"tasks_due_date_idx",
	"tasks_by_due_at",
	"tasks_by_created_at",
	"tasks_by_created_at_id",
	"tasks_updated_at_idx",
	"tasks_name_trgm",
	"tasks_title_trgm",
	"tasks_description_trgm",
	"tasks_search_text_trgm",
}

func dropTaskIndexes(ctx context.Context, conn *pgx.Conn) error {
	for _, name := range taskIndexNames {
		if name == "tasks_pkey" {
			continue
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf(`DROP INDEX IF EXISTS %s`, name)); err != nil {
			return err
		}
	}
	return nil
}

func ensureTasksIndexes(ctx context.Context, conn *pgx.Conn, withTrgm bool) error {
	statements := []string{
		`CREATE INDEX IF NOT EXISTS tasks_by_status ON tasks (status)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_pg_id ON tasks (pg_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_name ON tasks (name)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_tenant_pg_id ON tasks (tenant_id, pg_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_workspace ON tasks (tenant_id, workspace_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_status_id ON tasks (tenant_id, status_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_category ON tasks (tenant_id, category_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_priority_id ON tasks (tenant_id, priority_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_status_name_idx ON tasks (status_name)`,
		`CREATE INDEX IF NOT EXISTS tasks_priority_name_idx ON tasks (priority_name)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_team ON tasks (tenant_id, team_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_spot ON tasks (tenant_id, spot_id)`,
		`CREATE INDEX IF NOT EXISTS tasks_spot_name_idx ON tasks (spot_name)`,
		`CREATE INDEX IF NOT EXISTS tasks_workspace_name_idx ON tasks (workspace_name)`,
		`CREATE INDEX IF NOT EXISTS tasks_assignee_names_idx ON tasks (assignee_names)`,
		`CREATE INDEX IF NOT EXISTS tasks_flag_color_idx ON tasks (flag_color)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_priority ON tasks (priority)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_assignee ON tasks (assignee)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_project ON tasks (project)`,
		`CREATE INDEX IF NOT EXISTS tasks_due_date_idx ON tasks (due_date)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_due_at ON tasks (due_at)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_created_at ON tasks (created_at)`,
		`CREATE INDEX IF NOT EXISTS tasks_by_created_at_id ON tasks (created_at, id)`,
		`CREATE INDEX IF NOT EXISTS tasks_updated_at_idx ON tasks (updated_at)`,
	}
	if withTrgm {
		statements = append(statements,
			`CREATE INDEX IF NOT EXISTS tasks_name_trgm ON tasks USING gin (name gin_trgm_ops)`,
			`CREATE INDEX IF NOT EXISTS tasks_title_trgm ON tasks USING gin (title gin_trgm_ops)`,
			`CREATE INDEX IF NOT EXISTS tasks_description_trgm ON tasks USING gin (description gin_trgm_ops)`,
			`CREATE INDEX IF NOT EXISTS tasks_search_text_trgm ON tasks USING gin ((COALESCE(name, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(description, '') || ' ' || COALESCE(status, '') || ' ' || COALESCE(priority, '') || ' ' || COALESCE(assignee, '') || ' ' || COALESCE(project, '') || ' ' || COALESCE(label, '') || ' ' || COALESCE(flag_color, '')) gin_trgm_ops)`,
		)
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
