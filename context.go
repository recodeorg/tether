package tether

import (
	"fmt"
	"time"

	"github.com/recodeorg/tether/storage"
	"github.com/recodeorg/tether/utilities"
	"gorm.io/gorm"
)

// AuthCtx gives queries, guards and mutations access to the caller's identity
// and to guards.
type AuthCtx struct {
	// GetIdentity returns the caller's user ID, or "" for an anonymous
	// client. In a query or guard, calling it makes the result depend on the
	// caller's identity: it re-runs when the client's identity changes, and
	// is shared only with clients of the same identity. In a mutation run by
	// the scheduler or a cron there is no caller, and GetIdentity panics.
	GetIdentity func() (string, error)
	// ExecuteGuard runs the guard registered as guardName with params and
	// returns its result. In a query the result is cached and passed through
	// JSON, so numbers come back as float64 and structs as
	// map[string]interface{}; compare simple values such as bools. It returns
	// an error if the guard is not registered, when called from a guard, and
	// in a mutation run by the scheduler or a cron.
	ExecuteGuard func(guardName string, params map[string]interface{}) (interface{}, error)
}

// SchedulerCtx schedules mutations to run later. Scheduled tasks are stored
// in the database, survive restarts, and run once across all engines sharing
// the database.
type SchedulerCtx struct {
	// RunAfter runs the mutation functionName with params once duration has
	// passed, and returns the task's ID. The mutation may be registered with
	// [Internal]. It runs without a caller, so its ctx.Auth.GetIdentity
	// panics, and params are stored as JSON, so it receives numbers as
	// float64. The task is saved outside any transaction ctx.DB is in and
	// stays scheduled even if that transaction rolls back. RunAfter returns
	// [ErrEngineClosed] once the engine is closed.
	RunAfter func(duration time.Duration, functionName string, params map[string]interface{}) (string, error)
	// Cancel removes the task or cron with the given ID so it does not run
	// again. It returns false if the task could not be deleted or has
	// already started running on this engine. A run already in progress on
	// another engine is not interrupted.
	Cancel func(taskID string) bool
}

// StorageCtx manages files stored through the adapter passed to
// [Engine.SetStorage]. Every function returns an error if storage is not
// configured. File URLs are paths served by [Engine.StorageHandler].
type StorageCtx struct {
	// GetUploadURL creates a file record and returns its ID with a single-use
	// upload URL. The client uploads the file by sending its raw bytes, with
	// a Content-Type header, in a PUT request to that URL. Store the FileID in
	// your own tables to refer to the file later. The upload is limited to
	// 20 MB and must start within 15 minutes, unless changed with
	// [storage.WithMaxBytes] or [storage.WithExpiresIn]. Options passed to the
	// storage adapter's constructor are applied first; opts are applied after
	// them, in order. A zero size or lifetime selects the built-in default.
	// Not available in queries.
	GetUploadURL func(opts ...storage.UploadOption) (storage.UploadInfo, error)
	// GetDownloadURL returns a URL that serves the file to anyone who has it
	// for the next 15 minutes. It does not check that the file exists or that
	// the caller may read it, so authorize the caller first.
	GetDownloadURL func(fileID string) (string, error)
	// DeleteFile deletes the file's contents and record, and invalidates its
	// download URLs. Not available in queries.
	DeleteFile func(fileID string) error
}

// QueryCtx is passed to query functions registered with
// [Engine.RegisterQuery].
type QueryCtx struct {
	// DB is a read-only handle to the engine's database. Rows loaded through
	// it are tracked by primary key automatically.
	DB   *gorm.DB
	Auth *AuthCtx
	// Params holds the parameters sent by the client, decoded from JSON.
	// They are untrusted input, and numbers arrive as float64.
	Params map[string]interface{}
	// Dependencies holds the tags this execution depends on so far. Add to
	// it with TrackCollection and TrackTable rather than directly.
	Dependencies []string
	Storage      *StorageCtx
}

// TrackCollection makes the query re-run whenever a row in tableName whose
// columnName equals value is created, updated or deleted, including rows that
// move into or out of the collection. tableName and columnName are database
// names, such as "messages" and "room_id", and the column's model field must
// be tagged `tether:"track"`. value is compared by its fmt.Sprint form.
func (c *QueryCtx) TrackCollection(tableName string, columnName string, value interface{}) {
	// Adds a collection to the tracked tags. E.g. "messages_channel_id:5" for tagging rows in the messages table with the channel id "5".
	tag := tableName + "_" + columnName + ":" + fmt.Sprint(value)
	c.Dependencies = append(c.Dependencies, tag)
}

// TrackTable makes the query re-run whenever any row in tableName is created,
// updated or deleted. Prefer TrackCollection when the query reads only part
// of the table.
func (c *QueryCtx) TrackTable(tableName string) {
	c.Dependencies = append(c.Dependencies, "table_"+tableName+":mutated")
}

// MutationCtx is passed to mutation functions registered with
// [Engine.RegisterMutation].
type MutationCtx struct {
	// DB is a read-write handle to the engine's database. Writes made through
	// it re-run the queries that depend on the changed rows.
	DB        *gorm.DB
	Auth      *AuthCtx
	Scheduler *SchedulerCtx
	// Params holds the parameters sent by the client, decoded from JSON.
	// They are untrusted input, and numbers arrive as float64.
	Params map[string]interface{}
	// Profiler is the engine's profiler, the same as Engine.Profiler.
	Profiler *utilities.Profiler
	Storage  *StorageCtx
}

// GuardCtx is passed to guard functions registered with
// [Engine.RegisterGuard].
type GuardCtx struct {
	// DB is a read-only handle to the engine's database. Rows loaded through
	// it are tracked by primary key automatically.
	DB   *gorm.DB
	Auth *AuthCtx
	// Params holds the parameters passed to ExecuteGuard.
	Params map[string]interface{}
	// Dependencies holds the tags this execution depends on so far. Add to
	// it with TrackCollection and TrackTable rather than directly.
	Dependencies []string
}

// TrackCollection is like [QueryCtx.TrackCollection], but makes the guard
// re-run.
func (c *GuardCtx) TrackCollection(tableName string, columnName string, value interface{}) {
	c.Dependencies = append(c.Dependencies, tableName+"_"+columnName+":"+fmt.Sprint(value))
}

// TrackTable is like [QueryCtx.TrackTable], but makes the guard re-run.
func (c *GuardCtx) TrackTable(tableName string) {
	c.Dependencies = append(c.Dependencies, "table_"+tableName+":mutated")
}

// Auth verifies the tokens clients send to identify themselves. Set it with
// [Engine.SetAuth].
type Auth interface {
	// VerifyToken checks token and returns the user ID it belongs to and
	// when that identity expires. At expiresAt the client is told its
	// identity expired and its subscriptions re-run as anonymous. A zero
	// expiresAt means the identity does not expire.
	// Returning a different user ID than the connection had re-runs its
	// subscriptions as the new user. A non-nil error rejects the token and
	// leaves the connection's identity unchanged. DB is the engine's
	// database.
	VerifyToken(DB *gorm.DB, token string) (userID string, expiresAt time.Time, error error)
}

// Option configures a query or mutation registered with [Engine.RegisterQuery]
// or [Engine.RegisterMutation].
type Option func(*optionConfig)

// optionConfig is filled in by [Option] values.
type optionConfig struct {
	internal bool
}

// GuardOption configures a guard registered with [Engine.RegisterGuard].
// None are defined yet. The parameter is reserved so options can be added
// without changing RegisterGuard's signature.
type GuardOption func(*guardConfig)

// guardConfig is filled in by [GuardOption] values.
type guardConfig struct{}

// Internal hides a query or mutation from clients. They receive the same
// error as for an unknown name. An internal mutation can still be run by
// the scheduler and by crons.
func Internal() Option {
	return func(cfg *optionConfig) {
		cfg.internal = true
	}
}
