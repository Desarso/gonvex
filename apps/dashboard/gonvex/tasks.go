package gonvextest

import "github.com/gonvex/gonvex/pkg/gonvex"

type Task struct {
	ID              string `json:"id"`
	PGID            int    `json:"pg_id,omitempty"`
	Name            string `json:"name,omitempty"`
	Title           string `json:"title"`
	Status          string `json:"status"`
	Description     string `json:"description,omitempty"`
	Priority        string `json:"priority,omitempty"`
	Assignee        string `json:"assignee,omitempty"`
	Project         string `json:"project,omitempty"`
	Label           string `json:"label,omitempty"`
	DueDate         string `json:"due_date,omitempty"`
	DueAt           string `json:"due_at,omitempty"`
	StartDate       string `json:"start_date,omitempty"`
	SpotID          string `json:"spot_id,omitempty"`
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
