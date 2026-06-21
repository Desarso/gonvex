package manifest

type SourceBundle struct {
	Hash        string            `json:"hash"`
	ModulePath  string            `json:"modulePath"`
	PackageName string            `json:"packageName"`
	GoVersion   string            `json:"goVersion,omitempty"`
	Files       map[string]string `json:"files"`
}
