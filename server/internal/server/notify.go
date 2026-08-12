package server

type tableNotifyPayload struct {
	Table          string   `json:"table"`
	Operation      string   `json:"operation,omitempty"`
	MutationID     string   `json:"mutationId,omitempty"`
	Broad          bool     `json:"broad"`
	IDs            []string `json:"ids"`
	TaskIDs        []string `json:"taskIds,omitempty"`
	UserIDs        []string `json:"userIds,omitempty"`
	WorkspaceIDs   []string `json:"workspaceIds,omitempty"`
	ChangedColumns []string `json:"changedColumns,omitempty"`
	Count          int      `json:"count"`
}
