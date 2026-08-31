# Tether

Tether is an experimental reactive backend framework for Go. It automatically syncs your database queries with your frontend in real-time. No need to write custom WebSocket logic or manage complex state. Just write your queries, and Tether keeps your UI up to date.

> [!WARNING]
> Tether is in early alpha. Its API may change without notice, and it has not been tested in production. Use it for experimentation, not production workloads.

## Features

- **Real-time UI**: Queries automatically update over WebSockets when data changes.
- **Smart Tracking**: Automatically tracks dependencies for records loaded through GORM.
- **Client Libraries**: First-class support for TypeScript and React.
- **Authentication**: Built-in support for optional token-based auth (JWTs, session tokens, etc).
- **Database Support**: Currently supports SQLite (PostgreSQL planned).

## Getting started

Install the Go package:

```sh
go get github.com/recodeorg/tether
```

### 1. Define your models

Define a GORM model. Mark fields used to group records with `tether:"track"`:

```go
type Message struct {
	ID     uint   `gorm:"primaryKey"`
	Body   string
	RoomID string `tether:"track"`
}
```

### 2. Set up the backend

Create an engine, register your queries and mutations, and expose the WebSocket handler:

```go
db, err := gorm.Open(sqlite.Open("app.db"), &gorm.Config{})
if err != nil {
	log.Fatal(err)
}

engine := tether.NewEngine(db, "sqlite")
engine.CreateTable("messages", &Message{})

// Queries automatically re-run and push updates when their dependencies change
engine.RegisterQuery("getMessages", func(ctx *tether.QueryCtx) interface{} {
	roomID := ctx.Params["room"].(string)
	ctx.TrackCollection("messages", "room_id", roomID)

	var messages []Message
	if err := ctx.DB.Where("room_id = ?", roomID).Find(&messages).Error; err != nil {
		return map[string]interface{}{"error": err.Error()}
	}
	return messages
}, []string{"messages"})

// Mutations automatically trigger updates for any affected queries
engine.RegisterMutation("createMessage", func(ctx *tether.MutationCtx) interface{} {
	message := Message{
		Body:   ctx.Params["body"].(string),
		RoomID: ctx.Params["room"].(string),
	}
	if err := ctx.DB.Create(&message).Error; err != nil {
		return map[string]interface{}{"error": err.Error()}
	}
	return message
})

http.HandleFunc("/tether", engine.Handle)
log.Fatal(http.ListenAndServe(":8080", nil))
```

### 3. Connect your frontend

You can use the companion React library to automatically bind queries to your UI:

```tsx
import { useState } from "react"
import { useMutation, useQuery } from "@tetherdb/react"

export const App = () => {
	// useQuery automatically reflects the backend state in real-time!
    const data = useQuery("getMessages", { room: "1" })
	
    const [message, setMessage] = useState("")
    const [localError, setLocalError] = useState<string | null>(null)
    const { mutate, isPending } = useMutation("createMessage")

    const handleSendMessage = async () => {
        const result = await mutate({ room: "1", message })
        if (result.error) {
            setLocalError(result.error)
        }
        setMessage("")
    }

    return (
        <div>
            {data?.map((msg) => (
                <div key={msg.id}>{msg.sender?.name}: {msg.message}</div>
            ))}
            <input type="text" value={message} onChange={(e) => setMessage(e.target.value)} />
            <button onClick={handleSendMessage}>Send Message</button>
            {isPending && <div>Sending...</div>}
            {localError && <div>Error: {localError}</div>}
        </div>
    )
}
```

Or use the vanilla TypeScript client:

```ts
import { TetherClient } from "@tetherdb/client";

const client = new TetherClient();
client.connect("ws://localhost:8080/tether");

// The callback runs again whenever a message in the `general` room is created, updated, or deleted
client.subscribe("getMessages", { room: "general" }, (messages) => {
	console.log("Messages updated:", messages);
});

await client.sendMutation("createMessage", {
	room: "general",
	body: "Hello, Tether!",
});
```

## How it works

Clients subscribe to named queries with a set of parameters. While a query runs, Tether records the rows and collections it depends on. When a mutation changes one of those dependencies, Tether reruns the affected query and sends the new result to each subscribed client.

### Authentication

Authentication is optional. Implement the `tether.Auth` interface and pass it to `engine.SetAuth`. Tether associates the returned user ID and expiration time with the client's WebSocket connection.

## Current status

Tether currently targets SQLite. PostgreSQL support, broader client support, API stabilization, and production hardening are still in progress.

## License

Licensed under the [Apache License 2.0](LICENSE).
