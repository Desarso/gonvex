package backend

import (
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

type ListMessagesArgs struct{}

type SendMessageArgs struct {
	Body string `json:"body"`
}

type Message struct {
	ID        string    `json:"id"`
	Body      string    `json:"body"`
	Author    string    `json:"author"`
	CreatedAt time.Time `json:"created_at"`
}

func Register(app *gonvex.App) {
	app.Query("messages.list", ListMessages)
	app.Mutation("messages.send", SendMessage)
}

func ListMessages(ctx *gonvex.QueryCtx, args ListMessagesArgs) ([]Message, error) {
	return []Message{}, nil
}

func SendMessage(ctx *gonvex.MutationCtx, args SendMessageArgs) (Message, error) {
	return Message{
		ID:        "pending",
		Body:      args.Body,
		Author:    "demo-user",
		CreatedAt: time.Now().UTC(),
	}, nil
}
