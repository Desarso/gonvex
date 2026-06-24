package gonvextest

import "github.com/gonvex/gonvex/pkg/gonvex"

type Task struct {
	ID              string `json:"id"`
	PGID            int    `json:"pg_id,omitempty"`
	Name            string `json:"name,omitempty"`
	Title           string `json:"title"`
	Status          string `json:"status"`
	StatusName      string `json:"status_name,omitempty"`
	StatusColor     string `json:"status_color,omitempty"`
	StatusAction    string `json:"status_action,omitempty"`
	StatusIcon      string `json:"status_icon,omitempty"`
	StatusAnimation string `json:"status_working_animation,omitempty"`
	StatusInitial   bool   `json:"status_initial,omitempty"`
	Description     string `json:"description,omitempty"`
	Priority        string `json:"priority,omitempty"`
	PriorityName    string `json:"priority_name,omitempty"`
	PriorityColor   string `json:"priority_color,omitempty"`
	CategoryName    string `json:"category_name,omitempty"`
	CategoryIcon    string `json:"category_icon,omitempty"`
	CategoryColor   string `json:"category_color,omitempty"`
	TagNames        string `json:"tag_names,omitempty"`
	TagColors       string `json:"tag_colors,omitempty"`
	AttachmentCount int    `json:"attachment_count,omitempty"`
	ViewCount       int    `json:"view_count,omitempty"`
	Assignee        string `json:"assignee,omitempty"`
	AssigneeNames   string `json:"assignee_names,omitempty"`
	AssigneeIDs     string `json:"assignee_ids,omitempty"`
	AssigneeAvatars string `json:"assignee_avatar_urls,omitempty"`
	AllUserNames    string `json:"all_user_names,omitempty"`
	AllUserAvatars  string `json:"all_user_avatar_urls,omitempty"`
	NotesCount      int    `json:"notes_count,omitempty"`
	Project         string `json:"project,omitempty"`
	Label           string `json:"label,omitempty"`
	DueDate         string `json:"due_date,omitempty"`
	DueAt           string `json:"due_at,omitempty"`
	StartDate       string `json:"start_date,omitempty"`
	SpotID          string `json:"spot_id,omitempty"`
	SpotName        string `json:"spot_name,omitempty"`
	WorkspaceName   string `json:"workspace_name,omitempty"`
	FlagColor       string `json:"flag_color,omitempty"`
	Completed       bool   `json:"completed,omitempty"`
	EstimateMinutes int    `json:"estimate_minutes,omitempty"`
	Progress        int    `json:"progress,omitempty"`
}

type TasksGridArgs struct {
	Offset    int      `json:"offset"`
	Limit     int      `json:"limit"`
	Columns   []string `json:"columns"`
	Search    string   `json:"search,omitempty"`
	Sort      string   `json:"sort,omitempty"`
	Direction string   `json:"direction,omitempty"`
	Count     string   `json:"count,omitempty"`
	Filters   []struct {
		ID       string `json:"id,omitempty"`
		Column   string `json:"column"`
		Operator string `json:"operator"`
		Value    string `json:"value"`
		ValueTo  string `json:"valueTo,omitempty"`
	} `json:"filters,omitempty"`
}

type TasksGridResult struct {
	Table   string           `json:"table"`
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Total   int64            `json:"total"`
	Offset  int              `json:"offset"`
	Limit   int              `json:"limit"`
}

type CreateTaskArgs struct {
	Title string `json:"title"`
}

type RandomizeStatusPriorityArgs struct {
	Count int `json:"count"`
}

type RandomizeStatusPriorityResult struct {
	Updated    int64 `json:"updated"`
	Requested  int   `json:"requested"`
	DurationMS int64 `json:"durationMs"`
}

func Register(app *gonvex.App) {
	app.Query("tasks.list", ListTasks)
	app.Mutation("tasks.create", CreateTask)
	app.Mutation("tasks.randomizeStatusPriority", RandomizeStatusPriority)
	app.LiveGrid("tasks.grid", TasksGrid)
	RegisterFiles(app)
	RegisterSystem(app)
}

func ListTasks(ctx *gonvex.QueryCtx, args struct{}) ([]Task, error) {
	return []Task{}, nil
}

func CreateTask(ctx *gonvex.MutationCtx, args CreateTaskArgs) (Task, error) {
	return Task{ID: "task_dev", Title: args.Title, Status: "todo"}, nil
}

func RandomizeStatusPriority(ctx *gonvex.MutationCtx, args RandomizeStatusPriorityArgs) (RandomizeStatusPriorityResult, error) {
	return RandomizeStatusPriorityResult{}, nil
}

func TasksGrid(ctx *gonvex.QueryCtx, args TasksGridArgs) (TasksGridResult, error) {
	return TasksGridResult{}, nil
}
