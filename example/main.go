package main

import (
	"fmt"
	"time"

	"net/http"

	"github.com/glebarez/sqlite"
	"github.com/google/uuid"
	"github.com/wisplite/tether"
	"gorm.io/gorm"
)

type User struct {
	ID       string `gorm:"primaryKey"`
	Name     string
	Password string // hash in production, this is just an example
}

type Token struct {
	ID        string `gorm:"primaryKey"`
	Token     string
	UserID    string
	ExpiresAt time.Time
}

type Messages struct {
	ID        string `gorm:"primaryKey"`
	Message   string
	SenderID  string
	RoomID    string `tether:"track"`
	CreatedAt time.Time
}

type auth struct{}

func (a *auth) VerifyToken(db *gorm.DB, token string) (string, time.Time, error) {
	var tokenDB Token
	db.Where("token = ?", token).First(&tokenDB)
	if tokenDB.UserID == "" {
		return "", time.Time{}, fmt.Errorf("invalid token")
	}
	return tokenDB.UserID, tokenDB.ExpiresAt, nil
}

func main() {
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	engine := tether.NewEngine(db, "sqlite")
	engine.SetCheckOrigin(func(r *http.Request) bool {
		return true
	})

	engine.SetAuth(
		&auth{},
	)

	engine.CreateTable("users", &User{})
	engine.CreateTable("messages", &Messages{})
	engine.CreateTable("tokens", &Token{})

	engine.RegisterQuery("getMessages", func(ctx *tether.QueryCtx) interface{} {
		ctx.TrackCollection("messages", "room_id", ctx.Params["room"].(string))
		var messages []Messages
		ctx.DB.Where("room_id = ?", ctx.Params["room"].(string)).Find(&messages)
		var extendedMessages []map[string]interface{}
		for _, message := range messages {
			var sender User
			ctx.DB.Where("id = ?", message.SenderID).First(&sender)
			extendedMessages = append(extendedMessages, map[string]interface{}{
				"id":      message.ID,
				"message": message.Message,
				"sender": map[string]interface{}{
					"id":   sender.ID,
					"name": sender.Name,
				},
			})
		}
		return extendedMessages
	}, []string{"messages"})

	engine.RegisterMutation("createMessage", func(ctx *tether.MutationCtx) interface{} {
		userID, err := ctx.AuthCtx.GetIdentity()
		if err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		if userID == "" {
			return map[string]interface{}{"error": "unauthenticated"}
		}
		msg := &Messages{ID: uuid.NewString(), Message: ctx.Params["message"].(string), SenderID: userID, RoomID: ctx.Params["room"].(string)}
		if err := ctx.DB.Create(msg).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return msg
	})

	engine.RegisterMutation("createAccount", func(ctx *tether.MutationCtx) interface{} {
		user := &User{ID: uuid.NewString(), Name: ctx.Params["name"].(string), Password: ctx.Params["password"].(string)}
		token := &Token{ID: uuid.NewString(), Token: uuid.NewString(), UserID: user.ID, ExpiresAt: time.Now().Add(1 * time.Hour)}
		if err := ctx.DB.Create(token).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		if err := ctx.DB.Create(user).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return map[string]interface{}{"token": token.Token}
	})

	engine.RegisterMutation("login", func(ctx *tether.MutationCtx) interface{} {
		var user User
		ctx.DB.Where("name = ?", ctx.Params["name"].(string)).First(&user)
		if user.ID == "" {
			return map[string]interface{}{"error": "user not found"}
		}
		if user.Password != ctx.Params["password"].(string) {
			return map[string]interface{}{"error": "invalid password"}
		}

		token := &Token{ID: uuid.NewString(), Token: uuid.NewString(), UserID: user.ID, ExpiresAt: time.Now().Add(1 * time.Hour)}
		if err := ctx.DB.Create(token).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return map[string]interface{}{"token": token.Token}
	})

	http.HandleFunc("/tether", engine.Handle)
	http.ListenAndServe(":8080", nil)
}
