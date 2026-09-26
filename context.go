package tether

import (
	"fmt"
	"time"

	"github.com/recodeorg/tether/storage"
	"github.com/recodeorg/tether/utilities"
	"gorm.io/gorm"
)

type AuthCtx struct {
	GetIdentity  func() (string, error)
	ExecuteGuard func(guardName string, params map[string]interface{}) (interface{}, error)
}

type SchedulerCtx struct {
	RunAfter func(duration time.Duration, functionName string, params map[string]interface{}) (string, error)
	Cancel   func(taskID string) bool
}

type StorageCtx struct {
	GetUploadURL   func(opts storage.UploadOptions) (storage.UploadInfo, error)
	GetDownloadURL func(fileID string) (string, error)
	DeleteFile     func(fileID string) error
}

type QueryCtx struct {
	DB           *gorm.DB
	Auth         *AuthCtx
	Scheduler    *SchedulerCtx
	Params       map[string]interface{}
	Dependencies []string
	Storage      *StorageCtx
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
	DB        *gorm.DB
	Auth      *AuthCtx
	Scheduler *SchedulerCtx
	Params    map[string]interface{}
	Profiler  *utilities.Profiler
	Storage   *StorageCtx
}

type GuardCtx struct {
	DB           *gorm.DB
	Auth         *AuthCtx
	Params       map[string]interface{}
	Profiler     *utilities.Profiler
	Dependencies []string
}

func (c *GuardCtx) TrackCollection(tableName string, columnName string, value interface{}) {
	c.Dependencies = append(c.Dependencies, tableName+"_"+columnName+":"+fmt.Sprint(value))
}

func (c *GuardCtx) TrackTable(tableName string) {
	c.Dependencies = append(c.Dependencies, "table_"+tableName+":mutated")
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
