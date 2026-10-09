# Tether

Tether is a reactive backend framework for Go. Write your queries and mutations as plain Go functions, and Tether keeps every connected client in sync with your database in real time. You don't write WebSocket code, cache invalidation logic, or pub/sub plumbing.

**[Read the docs →](https://docs.tetherdb.dev)**

```sh
go get github.com/recodeorg/tether
```

## Why Tether?

### Reactive by default

In Tether, every query is live. When a client subscribes to a query, Tether records which rows and collections the query read. When a mutation changes any of that data, Tether reruns the affected queries and pushes the new results to every subscribed client. You don't opt in and you don't wire up events; it's how the framework works.

Identical subscriptions are batched automatically. If a thousand clients are watching the same chat room, the query runs once and the result is fanned out to all of them.

### Secure by design

Tether assumes your code will eventually make a mistake, so its defaults are safe:

- **Guards** are reusable, reactive authorization checks. You write "can this user see this room?" once and call it from any query. Guards are cached per user and re-evaluated automatically when the data behind them changes, so revoking access takes effect immediately on every open subscription. Clients who pass the same guard with the same result still share one batched query execution, so you get per-user authorization without per-user query cost.
- **Queries and guards are read-only.** Any attempt to write to the database from a query or guard is rejected at the connection level.
- **Internal functions stay internal.** Pass `Internal()` when registering a query or mutation and clients cannot call it. An internal mutation can still be run from your server code, such as a scheduled task.
- **Hardened defaults** include WebSocket message and subscription limits, origin checks, redaction of auth tokens from logs and metrics, and path-traversal and content-sniffing protection for file storage.

### Scales horizontally with no extra code

Swap SQLite for PostgreSQL and you can run as many Tether instances as you like behind a load balancer. Instances coordinate through Postgres, so a mutation on one server updates clients connected to any other server, and scheduled tasks run exactly once across the cluster. There's no Redis, message broker, or extra configuration.

### Batteries included

- **Authentication**: bring your own tokens (JWTs, session tokens, and so on) by implementing a single `VerifyToken` method, or use the included Clerk and OpenID Connect adapters.
- **HTTP actions**: handle webhooks and other HTTP endpoints with the same auth, guards, storage, and scheduler as a mutation.
- **File storage**: signed upload and download URLs, server-side uploads, optional public file URLs, and server-side commit hooks, with included local disk and S3-compatible adapters.
- **Scheduling**: run mutations after a delay or on a cron schedule. Tasks persist across restarts.
- **Transactions**: writes inside a database transaction only notify clients once the transaction commits. Rolled-back writes never reach clients.
- **Profiling**: built-in performance profiling for queries, mutations, guards, and database calls.
- **Client libraries**: first-class TypeScript and React clients.

## Philosophy

Tether is built around a few principles:

- **Radical simplicity.** The public API should stay small and obvious. If a feature needs a lot of explanation, it probably doesn't belong in Tether.
- **Effortless infrastructure changes.** Switching from SQLite to PostgreSQL, or from local disk to S3, should be a one-line change.
- **No vendor lock-in.** Tether always runs as a single, self-contained Go binary on any server you control.
- **Pluggable, not hard-coded.** Features that depend on outside infrastructure, like storage, come in through adapters instead of being baked into the core.

See [CONTRIBUTING.md](CONTRIBUTING.md) for more on how these principles guide the project.

## A quick look

A chat room where only members can read messages, and new messages appear for everyone instantly:

```go
type Message struct {
	ID     uint `gorm:"primaryKey"`
	Body   string
	RoomID string `tether:"track"`
}

type RoomMember struct {
	UserID string `gorm:"primaryKey"`
	RoomID string `gorm:"primaryKey"`
}

func main() {
	db, err := gorm.Open(sqlite.Open("app.db"), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	engine, err := tether.NewEngine(db)
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()
	engine.CreateTable(&Message{})
	engine.CreateTable(&RoomMember{})

	// A reusable authorization check. Its result is cached per user and
	// re-evaluated automatically when room membership changes.
	engine.RegisterGuard("isMember", func(ctx *tether.GuardCtx) (any, error) {
		userID, err := ctx.Auth.GetIdentity()
		if err != nil {
			return false, nil
		}
		ctx.TrackCollection("room_members", "user_id", userID)

		var member RoomMember
		err = ctx.DB.Where("user_id = ? AND room_id = ?", userID, ctx.Params["room"]).First(&member).Error
		return err == nil, nil
	})

	// A live query. Subscribers get a fresh result whenever the messages
	// in this room change.
	engine.RegisterQuery("getMessages", func(ctx *tether.QueryCtx) (any, error) {
		room := ctx.Params["room"].(string)
		allowed, err := ctx.Auth.ExecuteGuard("isMember", map[string]interface{}{"room": room})
		if err != nil || allowed != true {
			return nil, errors.New("forbidden")
		}

		ctx.TrackCollection("messages", "room_id", room)
		var messages []Message
		ctx.DB.Where("room_id = ?", room).Find(&messages)
		return messages, nil
	})

	// Mutations don't need to notify anyone. Tether sees the write and
	// updates every affected query on its own.
	engine.RegisterMutation("sendMessage", func(ctx *tether.MutationCtx) (any, error) {
		message := Message{
			Body:   ctx.Params["body"].(string),
			RoomID: ctx.Params["room"].(string),
		}
		if err := ctx.DB.Create(&message).Error; err != nil {
			return nil, errors.New(err.Error())
		}
		return message, nil
	})

	http.HandleFunc("/tether", engine.Handle)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

On the frontend, `useQuery` stays in sync with the backend automatically:

```tsx
import { useMutation, useQuery } from "@tetherdb/react"

export const Room = () => {
	const { data: messages, error } = useQuery("getMessages", { room: "general" })
	const { mutate } = useMutation("sendMessage")

	return (
		<div>
			{messages?.map((msg) => <p key={msg.id}>{msg.body}</p>)}
			<button onClick={() => mutate({ room: "general", body: "Hello!" })}>Say hello</button>
		</div>
	)
}
```

To scale this app horizontally, replace `sqlite.Open("app.db")` with `postgres.Open(dsn)` and run as many copies as you need.

The [documentation](https://docs.tetherdb.dev) covers authentication, file storage, scheduling, transactions, profiling, deployment, and the full client API.

## Requirements

- Go 1.26 or newer
- SQLite or PostgreSQL. PostgreSQL is required for horizontal scaling.

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request, and report security issues privately through GitHub's vulnerability reporting.

## License

Licensed under the [Apache License 2.0](LICENSE).
