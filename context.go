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
	// is shared only with clients of the same identity. An HTTP handler
	// wrapped with [Engine.HTTPAction] has a caller: the user ID from the
	// request's bearer token, or "" when that request has no Authorization
	// header. When there is no caller — a mutation run by the scheduler or a
	// cron, or a query or mutation run with [Engine.ExecuteQuery] or
	// [Engine.ExecuteMutation], including one started from an HTTP action —
	// GetIdentity returns [ErrNoCaller].
	GetIdentity func() (string, error)
	// ExecuteGuard runs the guard registered as guardName with params and
	// returns its value. In a query the value is cached and passed through
	// JSON, so numbers come back as float64 and structs as
	// map[string]interface{}; compare simple values such as bools. An HTTP
	// action can call it for the request's identity. It returns an error if
	// the guard is not registered, if the guard returns an error, when called
	// from a guard, and when there is no caller (a mutation run by the
	// scheduler or a cron, or a query or mutation run with
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
	// GetDownloadURL returns a URL that serves the file to anyone who has it.
	// It does not check that the file exists or that the caller may read it,
	// so authorize the caller first. The URL stays valid for 15 minutes
	// unless [storage.WithDownloadExpiresIn] says otherwise. Options passed
	// to [Engine.SetStorage] are applied first; opts are applied after them.
	// [storage.UseCachedURLs] returns an existing URL for fileID when that
	// URL has more than five minutes left, instead of issuing a new one.
	// While a cached URL is returned, a different WithDownloadExpiresIn on
	// that call is ignored.
	//
	// A subscribed query that calls GetDownloadURL depends on the URL's
	// token. Storage cleanup deletes the token once it expires and re-runs
	// the query, which can return a fresh URL. The URL uses the base path
	// passed to [Engine.SetStorage], or /storage when that path is empty.
	GetDownloadURL func(fileID string, opts ...storage.UploadOption) (string, error)
	// DeleteFile deletes the file's contents and record, and invalidates its
	// download URLs. Not available in queries.
	DeleteFile func(fileID string) error

	// PutFile stores data and returns the file's ID. The record is active
	// immediately. Store that ID in your own tables to refer to the file later.
	//
	// contentType is saved as the file's MIME type and sent when the file is
	// served. data is read to completion. The upload has no size or lifetime
	// limit: [storage.WithMaxBytes], [storage.WithExpiresIn],
	// [storage.WithDownloadExpiresIn], and [storage.UseCachedURLs] are
	// ignored. [storage.Public] is honored, including when it was passed to
	// [Engine.SetStorage], and a public file can be fetched at
	// {basePath}/public/{fileID} with no download token.
	//
	// Returns an error if storage is not configured. Not available in queries.
	PutFile func(contentType string, data io.Reader, opts ...storage.UploadOption) (string, error)

	// RunAfterUpload runs mutationName once the client finishes uploading
	// fileID. fileID comes from [StorageCtx.GetUploadURL]. The mutation runs
	// in the background after the file is stored and marked active, so the
	// upload response does not wait for it and does not report its error. A
	// failed mutation is logged and is not retried. The hook is removed when
	// the upload succeeds, and also when an abandoned upload is cleaned up.
	//
	// params are stored as JSON, so the mutation receives numbers as float64.
	// A nil params map is accepted. These keys are set when the
	// mutation runs, replacing any value params already had:
	//
	//	storage.fileID     the file's ID
	//	storage.fileSize   the upload's Content-Length, or -1 when it was omitted
	//	storage.mimeType   the upload's Content-Type, or application/octet-stream
	//	storage.public     whether the file is public
	//	storage.expiresAt  when the upload URL would have expired (time.Time)
	//	storage.createdAt  when the upload URL was created (time.Time)
	//
	// Headers on the upload request named X-Tether-Meta-* are copied in as
	// metadata.<name>, with <name> lowercased. X-Tether-Meta-Room: general
	// arrives as metadata.room = "general".
	//
	// One file has one hook. A second call for the same fileID returns an
	// error. The mutation may be registered with [Internal]. It runs without
	// a caller, the same way as [Engine.ExecuteMutation].
	//
	// Returns an error if the hook cannot be saved, including when
	// [Engine.SetStorage] has not been called. Not available in queries.
	RunAfterUpload func(fileID string, mutationName string, params map[string]interface{}) error
}

// ActionCtx is passed to an HTTP handler wrapped with [Engine.HTTPAction].
// The handler reads the request and writes the response itself.
type ActionCtx struct {
	// Auth identifies the caller from the request's Authorization header.
	// See [Engine.HTTPAction].
	Auth *AuthCtx
	// Scheduler schedules mutations the same way [MutationCtx.Scheduler] does.
	Scheduler *SchedulerCtx
	// Storage manages files the same way [MutationCtx.Storage] does, including
	// [StorageCtx.RunAfterUpload].
	Storage *StorageCtx
	// Profiler is the engine's profiler, the same value [Engine.Profiler] returns.
	Profiler *Profiler
	// ExecuteMutation runs a mutation and returns its result. It is
	// [Engine.ExecuteMutation]: the nested run has no caller, even when this
	// request does, and name may be registered with [Internal].
	ExecuteMutation func(name string, params map[string]any) (any, error)
	// ExecuteQuery runs a query once and returns its result. It is
	// [Engine.ExecuteQuery]: the query is not subscribed, its result is not
	// sent to clients, its ctx.DB is read-only, and there is no caller. name
	// may be registered with [Internal].
	ExecuteQuery func(name string, params map[string]any) (any, error)
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
	// ExecuteMutation runs another mutation and returns its result. It is
	// [Engine.ExecuteMutation]: the nested run has no caller, even when this
	// mutation does, writes through its ctx.DB re-run subscribed queries, and
	// name may be registered with [Internal]. params are passed through
	// unchanged.
	ExecuteMutation func(mutationName string, params map[string]interface{}) (any, error)
	// ExecuteQuery runs a query once and returns its result. It is
	// [Engine.ExecuteQuery]: the query is not subscribed, its result is not
	// sent to clients, its ctx.DB is read-only, and there is no caller. name
	// may be registered with [Internal]. params are passed through unchanged.
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

// ActionOption configures an HTTP handler returned by [Engine.HTTPAction].
// None are defined yet. The parameter is reserved so options can be added
// without changing HTTPAction's signature.
type ActionOption func(*actionConfig)

// actionConfig is filled in by [ActionOption] values.
type actionConfig struct{}

// Internal hides a query or mutation from clients. They receive the same
// error as for an unknown name. [Engine.ExecuteQuery],
// [Engine.ExecuteMutation], [MutationCtx.ExecuteQuery], and
// [MutationCtx.ExecuteMutation] can still run it, and an internal mutation
// can still be run by the scheduler and by crons.
func Internal() Option {
	return func(cfg *optionConfig) {
		cfg.internal = true
	}
}
