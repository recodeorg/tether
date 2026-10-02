package tether

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/recodeorg/tether/storage"
	"gorm.io/gorm"
)

// AuthCtx gives queries, guards and mutations access to the caller's identity
// and to guards.
type AuthCtx struct {
	// GetIdentity returns the caller's user ID, or "" for an anonymous
	// client. In a query or guard, calling it makes the result depend on the
	// caller's identity: it re-runs when the client's identity changes, and
	// is shared only with clients of the same identity. When there is no
	// caller — a mutation run by the scheduler or a cron, or a query or
	// mutation run with [Engine.ExecuteQuery] or [Engine.ExecuteMutation] —
	// GetIdentity returns [ErrNoCaller].
	GetIdentity func() (string, error)
	// ExecuteGuard runs the guard registered as guardName with params and
	// returns its value. In a query the value is cached and passed through
	// JSON, so numbers come back as float64 and structs as
	// map[string]interface{}; compare simple values such as bools. It returns
	// an error if the guard is not registered, if the guard returns an error,
	// when called from a guard, and when there is no caller (a mutation run
	// by the scheduler or a cron, or a query or mutation run with
	// [Engine.ExecuteQuery] or [Engine.ExecuteMutation]).
	ExecuteGuard func(guardName string, params map[string]interface{}) (interface{}, error)
}

// SchedulerCtx schedules mutations to run later. Scheduled tasks are stored
// in the database, survive restarts, and run once across all engines sharing
// the database.
type SchedulerCtx struct {
	// RunAfter runs the mutation functionName with params once duration has
	// passed, and returns the task's ID. The mutation may be registered with
	// [Internal]. It runs without a caller, so its ctx.Auth.GetIdentity
	// returns [ErrNoCaller], and params are stored as JSON, so it receives
	// numbers as float64. The task is saved outside any transaction ctx.DB is in and
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
	// [storage.WithMaxBytes] or [storage.WithExpiresIn]. Options passed to
	// [Engine.SetStorage] are applied first; opts are applied after them, in
	// order. A zero size or lifetime selects the built-in default. The upload
	// URL uses the base path passed to SetStorage, or /storage when that path
	// is empty.
	// Not available in queries.
	GetUploadURL func(opts ...storage.UploadOption) (storage.UploadInfo, error)
	// GetDownloadURL returns a URL that serves the file to anyone who has it
	// for the next 15 minutes. It does not check that the file exists or that
	// the caller may read it, so authorize the caller first. The URL uses the
	// base path passed to [Engine.SetStorage], or /storage when that path is
	// empty.
	GetDownloadURL func(fileID string) (string, error)
	// DeleteFile deletes the file's contents and record, and invalidates its
	// download URLs. Not available in queries.
	DeleteFile func(fileID string) error

	// PutFile creates a file record, uploads the data, and returns its ID.
	// Store the FileID in your own tables to refer to the file later.
	// The upload is unlimited in size and lifetime, and [storage.WithMaxByes]
	// and [storage.WithExpiresIn] are ignored.
	// Not available in queries.
	PutFile func(contentType string, data io.Reader, opts ...storage.UploadOption) (string, error)
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
	// dependencies holds the tags this execution depends on so far. Add to
	// it with TrackCollection and TrackTable rather than directly.
	dependencies []string
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
	c.dependencies = append(c.dependencies, tag)
}

// TrackTable makes the query re-run whenever any row in tableName is created,
// updated or deleted. Prefer TrackCollection when the query reads only part
// of the table.
func (c *QueryCtx) TrackTable(tableName string) {
	c.dependencies = append(c.dependencies, "table_"+tableName+":mutated")
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
	// Profiler is the engine's profiler, the same value [Engine.Profiler] returns.
	Profiler *Profiler
	Storage  *StorageCtx
	// ExecuteMutation runs the mutation name with params and returns its result.
	//
	// It is safe to call from code that is not itself inside a mutation, including
	// a goroutine that holds the engine. Writes made through the mutation's ctx.DB
	// re-run subscribed queries the same way a client mutation does.
	//
	// name may be registered with [Internal]. There is no caller:
	// ctx.Auth.GetIdentity returns [ErrNoCaller] and ctx.Auth.ExecuteGuard returns
	// an error. A non-nil error from the mutation, including a returned value that
	// implements error, is returned to the caller. params are passed through
	// unchanged.
	ExecuteMutation func(mutationName string, params map[string]interface{}) (any, error)
	// ExecuteQuery runs the query name once with params and returns its result.
	//
	// The query is not subscribed and its result is not sent to clients. It is
	// safe to call from code that is not inside a mutation, including a goroutine
	// that holds the engine. name may be registered with [Internal]. There is no
	// caller: ctx.Auth.GetIdentity returns [ErrNoCaller] and ctx.Auth.ExecuteGuard
	// returns an error. ctx.DB is read-only. A non-nil error from the query,
	// including a returned value that implements error, is returned to the caller.
	// params are passed through unchanged.
	ExecuteQuery func(queryName string, params map[string]interface{}) (any, error)
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
	// dependencies holds the tags this execution depends on so far. Add to
	// it with TrackCollection and TrackTable rather than directly.
	dependencies []string
}

// TrackCollection is like [QueryCtx.TrackCollection], but makes the guard
// re-run.
func (c *GuardCtx) TrackCollection(tableName string, columnName string, value interface{}) {
	c.dependencies = append(c.dependencies, tableName+"_"+columnName+":"+fmt.Sprint(value))
}

// TrackTable is like [QueryCtx.TrackTable], but makes the guard re-run.
func (c *GuardCtx) TrackTable(tableName string) {
	c.dependencies = append(c.dependencies, "table_"+tableName+":mutated")
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
	// leaves the connection's identity unchanged. ctx is cancelled when the
	// engine shuts down. DB is the engine's database.
	VerifyToken(ctx context.Context, DB *gorm.DB, token string) (userID string, expiresAt time.Time, err error)
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
// error as for an unknown name. [Engine.ExecuteQuery] and
// [Engine.ExecuteMutation] can still run it, and an internal mutation can
// still be run by the scheduler and by crons.
func Internal() Option {
	return func(cfg *optionConfig) {
		cfg.internal = true
	}
}
