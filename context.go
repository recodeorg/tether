package tether

import (
	"fmt"
	"time"

	"gorm.io/gorm"
)

type AuthCtx struct {
	GetIdentity func() (string, error)
}

type QueryCtx struct {
	DB           *gorm.DB
	Auth         *AuthCtx
	Params       map[string]interface{}
	Dependencies []string
}

func (c *QueryCtx) TrackCollection(tableName string, columnName string, value interface{}) {
	// Adds a collection to the tracked tags. E.g. "messages_channel_id:5" for tagging rows in the messages table with the channel id "5".
	tag := tableName + "_" + columnName + ":" + fmt.Sprint(value)
	c.Dependencies = append(c.Dependencies, tag)
}

func (c *QueryCtx) TrackTable(tableName string) {
	c.Dependencies = append(c.Dependencies, "table_"+tableName+":mutated")
}

type MutationCtx struct {
	DB      *gorm.DB
	AuthCtx *AuthCtx
	Params  map[string]interface{}
}

type Auth interface {
	VerifyToken(DB *gorm.DB, token string) (userID string, expiresAt time.Time, error error)
}

type MutationOptions struct {
	Internal bool
}

type QueryOptions struct {
	Internal bool
}
