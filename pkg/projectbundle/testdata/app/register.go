package app

import "github.com/gonvex/gonvex/pkg/gonvex"

func Register(app *gonvex.App) {
	app.Query("sample.echo", SampleEcho)
}

type sampleArgs struct {
	Name string `json:"name"`
}

func SampleEcho(_ *gonvex.QueryCtx, args sampleArgs) (map[string]any, error) {
	return map[string]any{"name": args.Name}, nil
}
