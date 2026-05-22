package gonvextest

import "github.com/gonvex/gonvex/pkg/gonvex"

type CreateUploadURLArgs struct {
	ContentType string `json:"contentType"`
}

type CreateUploadURLResult struct {
	URL string `json:"url"`
}

func RegisterFiles(app *gonvex.App) {
	app.Mutation("files.createUploadUrl", CreateUploadURL)
}

func CreateUploadURL(ctx *gonvex.MutationCtx, args CreateUploadURLArgs) (CreateUploadURLResult, error) {
	return CreateUploadURLResult{URL: "http://localhost:9000/gonvex-dev/dev-object"}, nil
}
