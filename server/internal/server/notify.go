package server

type tableNotifyPayload struct {
	Table          string   `json:"table"`
	Operation      string   `json:"operation,omitempty"`
	Broad          bool     `json:"broad"`
	IDs            []string `json:"ids"`
	ChangedColumns []string `json:"changedColumns,omitempty"`
	Count          int      `json:"count"`
}
