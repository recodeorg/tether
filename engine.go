// Package tether is a reactive backend framework. Queries and mutations are
// plain Go functions registered on an [Engine]; clients subscribe to queries
// over a WebSocket, and Tether re-runs a query and pushes the new result
// whenever a mutation writes data that the query depends on.
//
// A typical server creates an engine, registers its tables, guards, queries
// and mutations, and mounts the engine's HTTP handlers:
//
//	engine, err := tether.NewEngine(db)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer engine.Close()
//	engine.CreateTable(&Message{})
//	engine.RegisterQuery("getMessages", getMessages)
//	engine.RegisterMutation("sendMessage", sendMessage)
//	http.HandleFunc("/tether", engine.Handle)
//
// Queries depend automatically on the primary keys of the rows they load.
// Use [QueryCtx.TrackCollection] or [QueryCtx.TrackTable] to also pick up
// rows that are inserted later. Writes made through [MutationCtx.DB] are
// detected automatically, so mutations never notify clients themselves.
//
// With SQLite, Tether runs as a single instance. With PostgreSQL, any number
// of engines can share one database: writes on one instance update
// subscribers on every instance, and each scheduled task runs exactly once
// across the cluster.
package tether

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/recodeorg/tether/internal/reactivity"
	"github.com/recodeorg/tether/storage"
	"github.com/robfig/cron/v3"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// TetherTask is the database model for scheduled tasks and crons, stored in
// the tether_tasks table. The engine creates and manages these rows; the type
// is exported so applications can inspect pending work.
type TetherTask struct {
	ID           string `gorm:"primaryKey"`
	Name         string `gorm:"uniqueIndex:idx_tether_cron_name,where:is_cron = true"` // unique among crons; one-shot tasks leave this empty
	FunctionName string `gorm:"not null"`
	ParamsJSON   string
	ExecuteAt    time.Time `gorm:"index"`
	ClaimedBy    *string   `gorm:"index"`
	LockedUntil  *time.Time

	// Cron specific (Can be empty for runAfter tasks)
	CronString   *string // e.g., "0 16 1 * *"
	LastExecuted *time.Time
	IsCron       bool
}

// TetherStorage is the database model for a stored file, created by
// [StorageCtx.GetUploadURL] or [StorageCtx.PutFile] in the table set up by
// [Engine.SetStorage].
type TetherStorage struct {
	ID        string `gorm:"primaryKey"`  // file ID
	Token     string `gorm:"uniqueIndex"` // token used to upload the file
	Status    string `gorm:"index"`       // status of the upload (pending, uploading, active)
	FileSize  int64
	MaxBytes  int64     // maximum size allowed at upload time
	MimeType  string    // MIME type of the file
	Adapter   string    // adapter used to store the file (local, s3, etc.), helpful for migrations
	Public    bool      // whether the file is public (can be accessed without a token)
	ExpiresAt time.Time `gorm:"index"` // time when the upload token expires
	CreatedAt time.Time // time when the upload token was created
}

// TetherDownloadToken is the database model for a download link issued by
// [StorageCtx.GetDownloadURL].
type TetherDownloadToken struct {
	Token     string    `gorm:"primaryKey"`
	FileID    string    `gorm:"index"`
	ExpiresAt time.Time `gorm:"index"` // time when the download token expires
}

// Engine is a Tether server. Create one with [NewEngine], register tables,
// guards, queries and mutations, then mount [Engine.Handle] (and
// [Engine.StorageHandler] if file storage is used) on an HTTP server.
//
// The Register*, Set*, CreateTable and SetStorage methods are not safe for
// concurrent use and must be called before the engine starts serving
// requests.
type Engine struct {
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	closeMu         sync.Mutex
	closed          bool
	db              *gorm.DB
	dbType          string // sqlite or postgres
	mutations       map[string]mutation
	queries         map[string]query
	queryClock      atomic.Int64
	tracker         *reactivity.Tracker
	auth            Auth
	guards          map[string]guard
	timerMutex      sync.RWMutex
	taskToTimer     map[string]*time.Timer
	websocketHelper *reactivity.WebsocketHelper
	// profiler records timings for queries, mutations, guards, auth and
	// database calls. See [Engine.Profiler].
	profiler *Profiler
	// ephemeralID identifies this engine instance. See [Engine.EphemeralID].
	ephemeralID string
	storage     storage.StorageAdapter
	// storageDefaults are the options passed to [Engine.SetStorage].
	// [StorageCtx.GetUploadURL], [StorageCtx.PutFile], and
	// [StorageCtx.GetDownloadURL] apply them before their own options.
	storageDefaults []storage.UploadOption
	// storageBasePath is the URL prefix for upload and download routes.
	// Empty until [Engine.SetStorage] runs; that call stores "/storage" when
	// the caller passes an empty path.
	storageBasePath string
	readOnlyPool    *readOnlyPool
	// protocolVersion is the WebSocket protocol this engine speaks. It is
	// written into every JSON frame sent to a client so a later protocol
	// change can be recognized without breaking older clients.
	protocolVersion int
}

// mutation is a mutation as stored by [Engine.RegisterMutation].
type mutation struct {
	Func     func(ctx *MutationCtx) (any, error)
	Internal bool
}

// query is a query as stored by [Engine.RegisterQuery].
type query struct {
	Func     func(ctx *QueryCtx) (any, error)
	Internal bool
}

// guard is a guard as stored by [Engine.RegisterGuard].
type guard struct {
	Func func(ctx *GuardCtx) (any, error)
}

// scheduleLoopInterval is how often each engine polls the database for
// scheduled tasks that are due.
const scheduleLoopInterval = 10 * time.Second

// scheduleLookahead is how far past the current time a poll claims tasks.
// A claimed task runs on a local timer at its scheduled time, so tasks are
// not delayed by the polling interval.
const scheduleLookahead = 15 * time.Second

type defaultAuth struct{}

type contextKey string

const tetherCtxKey contextKey = "tether_query_ctx"

type traceContextKey string

// Context keys set on the context of the *gorm.DB given to queries, guards
// and mutations. Both values are strings: ContextKeyExecutionID holds an ID
// unique to one execution, and ContextKeyActionName holds the name of the
// query, guard or mutation. GORM callbacks and loggers can read them with
// db.Statement.Context.Value(tether.ContextKeyExecutionID).
const (
	ContextKeyExecutionID traceContextKey = "exec_id"
	ContextKeyActionName  traceContextKey = "action_name"
)

func (defaultAuth) VerifyToken(_ context.Context, _ *gorm.DB, _ string) (string, time.Time, error) {
	return "", time.Time{}, nil
}

func getIdentity(deps *[]string, authID string) (string, error) {
	if authID != "" {
		*deps = append(*deps, "*user_identity:"+authID)
	} else {
		*deps = append(*deps, "*user_identity:anonymous")
	}
	return authID, nil
}

// normalizeHandlerResult separates a handler's value from its failure.
// A non-nil error is the failure. A value that implements error is also a
// failure: encoding/json encodes error values as an empty object, and queries
// and mutations send that text as a websocket error frame instead.
func normalizeHandlerResult(result any, err error) (any, error) {
	if err != nil {
		return nil, err
	}
	if resultErr, ok := result.(error); ok && resultErr != nil {
		return nil, resultErr
	}
	return result, nil
}

// Profiler returns the engine's profiler. Recording is stopped until
// [Profiler.Start] or [Profiler.StartWithCallback] runs. Mutations see this
// same profiler as [MutationCtx.Profiler].
func (e *Engine) Profiler() *Profiler {
	return e.profiler
}

func executeGuard(e *Engine, guardCtx *GuardCtx, guardName string) (interface{}, error) {
	guard, ok := e.guards[guardName]
	if !ok {
		return nil, fmt.Errorf("guard not found")
	}
	return normalizeHandlerResult(guard.Func(guardCtx))
}

func dependenciesFromContext(v interface{}) *[]string {
	switch ctx := v.(type) {
	case *QueryCtx:
		return &ctx.dependencies
	case *GuardCtx:
		return &ctx.dependencies
	default:
		return nil
	}
}

func decodeGuardFingerprint(guardName, paramsHash, fingerprint string) (interface{}, error) {
	raw := strings.TrimPrefix(fingerprint, "*guard_"+guardName+"_"+paramsHash+":")
	var result interface{}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, err
	}
	return result, nil
}

// canonicalGuardResult round-trips a guard value through JSON so every caller
// sees the same Go types: objects as maps, arrays as slices, and scalars such
// as bools left as scalars.
func canonicalGuardResult(result any) (any, []byte, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, nil, err
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, nil, err
	}
	return decoded, raw, nil
}

func (e *Engine) bindGuardAuth(guardCtx *GuardCtx, authID string) {
	authCtx := &AuthCtx{
		GetIdentity: func() (string, error) {
			return getIdentity(&guardCtx.dependencies, authID)
		},
	}
	authCtx.ExecuteGuard = func(name string, params map[string]interface{}) (interface{}, error) {
		return nil, fmt.Errorf("Guards cannot execute other guards")
	}
	guardCtx.Auth = authCtx
}

func (e *Engine) prepareGuardCtx(params map[string]interface{}, authID, execID, actionName string) *GuardCtx {
	guardCtx := &GuardCtx{
		Params:       params,
		dependencies: []string{},
	}
	e.bindGuardAuth(guardCtx, authID)
	gormCtx := context.WithValue(context.Background(), tetherCtxKey, guardCtx)
	gormCtx = context.WithValue(gormCtx, ContextKeyExecutionID, execID)
	gormCtx = context.WithValue(gormCtx, ContextKeyActionName, actionName)
	guardCtx.DB = e.readOnlyDB(gormCtx)
	return guardCtx
}

// readOnlyDB returns the handle given to queries and guards. Both the
// statement and config pools are replaced so that ctx.DB.ConnPool and
// PrepareStmt sessions also go through readOnlyPool.
func (e *Engine) readOnlyDB(ctx context.Context) *gorm.DB {
	db := e.db.WithContext(ctx)
	db.Statement.ConnPool = e.readOnlyPool
	db.Config.ConnPool = e.readOnlyPool
	return db
}

// reevaluateGuard runs a guard subscription after one of its data tags changed.
// Identity and DB dependencies stay on the guard; the attached query only
// stores the *guard_ fingerprint used for batching. Returns the attached
// query subscription when that fingerprint actually changed. If the guard
// fails, the query's cached guard results are dropped so it cannot keep using
// a grant that could not be revalidated.
func (e *Engine) reevaluateGuard(subscription *reactivity.Subscription, execID string) (rerun *reactivity.Subscription) {
	// Stamp before the guard reads anything so an older reevaluation cannot
	// overwrite dependencies or the cached result published by a newer one.
	ts := e.nextQueryTimestamp()
	// Runs on the caller's goroutine, before InvalidateTags starts workers.
	// A panicking guard would otherwise take down that caller: the postgres
	// listener, a scheduler timer, or a profiler flush.
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Recovered from panic", "error", r)
			e.tracker.DropGuards(subscription.SubID, ts)
			rerun = nil
		}
	}()
	if len(subscription.LinkedSubIDs) == 0 {
		return nil
	}
	attachedID := subscription.LinkedSubIDs[0]
	attached, ok := e.tracker.GetSubscription(attachedID)
	if !ok || attached.Client == nil {
		slog.Error("Tracker: Attached subscription not found", "subID", attachedID)
		return nil
	}
	auth, ok := e.tracker.GetAuth(attached.Client.ID)
	if !ok {
		return nil
	}
	guardName := subscription.Query
	guardCtx := e.prepareGuardCtx(subscription.Params, auth.UserID, execID, guardName)
	guardResult, err := executeGuard(e, guardCtx, guardName)
	if err != nil {
		slog.Error("Failed to execute guard", "error", err)
		e.tracker.DropGuards(subscription.SubID, ts)
		return nil
	}
	if !e.tracker.UpdateTagsAtEpoch(subscription.SubID, guardCtx.dependencies, auth.AuthEpoch, ts) {
		return nil
	}
	guardResultJSON, err := json.Marshal(guardResult)
	if err != nil {
		slog.Error("Failed to marshal guard result", "error", err)
		e.tracker.DropGuards(subscription.SubID, ts)
		return nil
	}
	paramsJSON, err := json.Marshal(subscription.Params)
	if err != nil {
		slog.Error("Failed to marshal params", "error", err)
		e.tracker.DropGuards(subscription.SubID, ts)
		return nil
	}
	paramsHash := xxhash.Sum64(paramsJSON)
	if e.tracker.UpdateGuardFingerprint(attachedID, guardName, strconv.FormatUint(paramsHash, 10), string(guardResultJSON), auth.AuthEpoch, ts) {
		return attached
	}
	return nil
}

func (e *Engine) scheduleTask(timestamp time.Time, functionName string, params map[string]interface{}) (string, error) {
	// Counted in wg so Close releases the claim on a row inserted here even if
	// the timer below is refused.
	if !e.beginWork() {
		return "", ErrEngineClosed
	}
	defer e.wg.Done()
	paramsJSON, err := json.Marshal(params)
	taskID := uuid.New().String()
	if err != nil {
		slog.Error("Failed to marshal params", "error", err)
		return "", err
	}

	if time.Until(timestamp) < scheduleLoopInterval+scheduleLookahead+(5*time.Second) {
		// if the task is due within the schedule loop interval, we need to run it immediately
		// the task is added to the database in case of a crash or restart, so that it is not lost
		// it is claimed at insertion so another instance doesn't claim it before it is executed
		lockedUntil := time.Now().Add(5 * time.Minute)
		task := TetherTask{
			ID:           taskID,
			FunctionName: functionName,
			ParamsJSON:   string(paramsJSON),
			ExecuteAt:    timestamp,
			ClaimedBy:    &e.ephemeralID,
			LockedUntil:  &lockedUntil,
			IsCron:       false,
		}
		err := e.db.Create(&task).Error
		if err != nil {
			slog.Error("Failed to create scheduled task", "error", err)
			return "", err
		}

		// Hold the mutex across AfterFunc so a zero-delay callback cannot
		// remove the entry before it is stored.
		e.timerMutex.Lock()
		defer e.timerMutex.Unlock()
		if e.isClosed() {
			return taskID, nil
		}
		e.taskToTimer[taskID] = e.afterFunc(time.Until(timestamp), func() {
			e.timerMutex.Lock()
			delete(e.taskToTimer, taskID)
			e.timerMutex.Unlock()
			var check TetherTask
			if err := e.db.Where("id = ?", taskID).First(&check).Error; err != nil {
				slog.Error("Task removed before execution", "taskID", taskID, "error", err)
				return
			}
			defer func() {
				if r := recover(); r != nil {
					slog.Error("Failed to execute scheduled task", "taskID", taskID, "error", r)
				}
				e.db.Delete(&TetherTask{}, "id = ?", taskID)
			}()
			_, err := e.executeMutationInternal(functionName, params)
			if err != nil {
				slog.Error("Failed to execute mutation internally", "error", err)
			}
		})
	} else {
		// if it is not due within the schedule loop interval, we simply wait for the loop to catch it
		// it is created without a claimant so that whatever instance is alive at the time of execution can claim it
		task := TetherTask{
			ID:           taskID,
			FunctionName: functionName,
			ParamsJSON:   string(paramsJSON),
			ExecuteAt:    timestamp,
			IsCron:       false,
		}
		err = e.db.Create(&task).Error
		if err != nil {
			slog.Error("Failed to create scheduled task", "error", err)
			return "", err
		}
	}

	return taskID, nil
}

// RegisterCron runs the mutation functionName with params on the schedule
// cronString, and returns the cron's task ID.
//
// cronString is a standard five-field expression (minute, hour, day of month,
// month, day of week), such as "0 16 1 * *", evaluated in the server's local
// time zone. Each occurrence runs once across all engines sharing the
// database. An occurrence missed while no engine was running runs once when
// an engine next polls.
//
// cronName identifies the cron. Registering an existing name updates its
// schedule, mutation and params in place and returns the same ID, so it is
// safe to call RegisterCron on every startup. To remove a cron, stop
// registering it and pass its ID to [SchedulerCtx.Cancel].
//
// The mutation may be registered with [Internal]. It runs without a caller, so
// ctx.Auth.GetIdentity returns [ErrNoCaller] and ctx.Auth.ExecuteGuard returns an error.
// params are stored as JSON, so the mutation receives numbers as float64.
// functionName is not checked here; an unknown name is logged when it runs.
func (e *Engine) RegisterCron(cronName string, cronString string, functionName string, params map[string]interface{}) (string, error) {
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		slog.Error("Failed to marshal params", "error", err)
		return "", err
	}
	now := time.Now()
	nextTime, err := calculateNextCronTime(cronString, now)
	if err != nil {
		slog.Error("Failed to calculate next cron time", "error", err)
		return "", err
	}
	if nextTime.IsZero() {
		err := fmt.Errorf("impossible cron schedule with no future occurrences: %s", cronString)
		slog.Error("Failed to register cron", "error", err, "name", cronName)
		return "", err
	}
	task := TetherTask{
		ID:           uuid.New().String(),
		Name:         cronName,
		FunctionName: functionName,
		ParamsJSON:   string(paramsJSON),
		ExecuteAt:    nextTime,
		IsCron:       true,
		CronString:   &cronString,
	}
	// Conflict target matches idx_tether_cron_name. ID, claim, and last_executed stay on the existing row.
	// execute_at is decided in the upsert so two instances cannot both move a row that one of them
	// already holds, or that is still owed. A live lease matches the scheduler's claim predicate.
	// Overdue means now is past execute_at and this occurrence has not run yet.
	err = e.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "name"}},
		TargetWhere: clause.Where{Exprs: []clause.Expression{
			clause.Expr{SQL: "is_cron = true"},
		}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"function_name": clause.Column{Table: "excluded", Name: "function_name"},
			"params_json":   clause.Column{Table: "excluded", Name: "params_json"},
			"cron_string":   clause.Column{Table: "excluded", Name: "cron_string"},
			"is_cron":       clause.Column{Table: "excluded", Name: "is_cron"},
			"execute_at": clause.Expr{
				SQL:  `CASE WHEN (tether_tasks.claimed_by IS NOT NULL AND tether_tasks.locked_until IS NOT NULL AND tether_tasks.locked_until > ?) OR (tether_tasks.execute_at < ? AND (tether_tasks.last_executed IS NULL OR tether_tasks.last_executed < tether_tasks.execute_at)) THEN tether_tasks.execute_at ELSE excluded.execute_at END`,
				Vars: []interface{}{now, now},
			},
		}),
	}).Create(&task).Error
	if err != nil {
		slog.Error("Failed to register cron", "error", err, "name", cronName)
		return "", err
	}
	// Create keeps the UUID from the insert attempt. On conflict the stored row still has its original id.
	var id string
	if err := e.db.Model(&TetherTask{}).Where("name = ? AND is_cron = ?", cronName, true).Pluck("id", &id).Error; err != nil {
		slog.Error("Failed to load cron id", "error", err, "name", cronName)
		return "", err
	}
	return id, nil
}

func calculateNextCronTime(cronString string, fromTime time.Time) (time.Time, error) {
	cron := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := cron.Parse(cronString)
	if err != nil {
		slog.Error("Failed to parse cron string", "error", err)
		return time.Time{}, err
	}
	return schedule.Next(fromTime), nil
}

func getPostgresDSN(db *gorm.DB) (string, error) {
	if dia, ok := db.Dialector.(*postgres.Dialector); ok {
		return dia.Config.DSN, nil
	}
	return "", fmt.Errorf("dialector is not *postgres.Dialector")
}

type engineConfig struct{}

// EngineOption configures an engine created by [NewEngine]. None are defined
// yet. The parameter is reserved so options can be added without changing
// NewEngine's signature.
type EngineOption func(*engineConfig) error

// NewEngine creates an engine backed by db and starts its background work.
// Call [Engine.Close] when the engine is no longer needed.
//
// db must use the SQLite (github.com/glebarez/sqlite) or PostgreSQL
// (gorm.io/driver/postgres) dialector. NewEngine returns an error for any
// other dialector, if it cannot create the tether_tasks table, and, on
// PostgreSQL, if it cannot read the connection's DSN. On those failures any
// background work already started is shut down before the error is returned.
// It registers callbacks on db that detect writes and enforce read-only
// access for queries and guards, so db should not be shared with another
// engine. It also starts the task scheduler. On PostgreSQL it listens for
// changes made by other engines.
func NewEngine(db *gorm.DB, options ...EngineOption) (*Engine, error) {
	tracker := reactivity.NewTracker()
	dbType := db.Dialector.Name()
	if dbType != "sqlite" && dbType != "postgres" {
		return nil, fmt.Errorf("invalid database type")
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		ctx:             ctx,
		cancel:          cancel,
		wg:              sync.WaitGroup{},
		db:              db,
		dbType:          dbType,
		mutations:       make(map[string]mutation),
		queries:         make(map[string]query),
		tracker:         tracker,
		auth:            defaultAuth{},
		guards:          make(map[string]guard),
		taskToTimer:     make(map[string]*time.Timer),
		websocketHelper: &reactivity.WebsocketHelper{},
		ephemeralID:     uuid.New().String(),
		protocolVersion: 1,
	}
	e.tracker.SetProtocolVersion(e.protocolVersion)
	trackTransactions(db)
	e.readOnlyPool = &readOnlyPool{pool: db.ConnPool, postgres: dbType == "postgres"}
	// scheduler initialization
	err := e.CreateTable([]TetherTask{}) // Create the internal table for the scheduled tasks
	if err != nil {
		slog.Error("Failed to create table", "error", err)
		e.Close()
		return nil, err
	}
	e.startScheduler()

	if e.dbType == "postgres" {
		dsn, err := getPostgresDSN(db)
		if err != nil {
			slog.Error("Failed to get PostgreSQL DSN", "error", err)
			e.Close()
			return nil, err
		}
		e.startPostgresListener(dsn)
	}

	// profiler initialization
	e.profiler = newProfiler(func(mutationName string) {
		if !e.beginWork() {
			return
		}
		defer e.wg.Done()
		_, err := e.executeMutationInternal(mutationName, map[string]interface{}{})
		if err != nil {
			slog.Error("Failed to execute mutation internally", "error", err)
		}
	})
	// Rejects GORM's write builders before they build SQL or open a
	// transaction; readOnlyPool would refuse the resulting statement anyway.
	enforceReadOnly := func(db *gorm.DB) {
		if isReadOnlyConn(db.Statement.ConnPool) {
			db.AddError(errReadOnly)
		}
	}
	beforeProfiler := func(db *gorm.DB) {
		// Profile the start time of the database operation
		db.InstanceSet("tether:profiler_start", time.Now())
	}
	invalidate := func(tx *gorm.DB) {
		// GORM runs every later callback even after AddError, so a write rejected
		// by the read-only guard still reaches here. Publishing its tags would
		// re-run the query that attempted the write, and that query would try
		// the write again.
		if tx.Error != nil {
			return
		}
		tags := extractMutationTags(tx)
		execID, _ := tx.Statement.Context.Value(ContextKeyExecutionID).(string)
		actionName, _ := tx.Statement.Context.Value(ContextKeyActionName).(string)
		if e.Profiler().IsActive() {
			if startTime, ok := tx.InstanceGet("tether:profiler_start"); ok {

				e.Profiler().Add(Metric{
					ID:       execID,
					Name:     "gorm:" + tx.Statement.Name(),
					Type:     MetricTypeDatabase,
					Time:     startTime.(time.Time),
					Duration: time.Since(startTime.(time.Time)),
					Tags:     tags,
				})
			}
		}
		// Subscriptions re-run on other pooled connections, so writes inside a
		// transaction are only published once it commits. A transaction begun
		// from db.Connection's pinned *sql.Conn is a raw *sql.Tx; its hook
		// publishes on that commit. NOTIFY still runs now so it shares the
		// transaction and PostgreSQL delivers it only if the commit succeeds.
		if pending := trackedTxOf(tx.Statement.ConnPool); pending != nil {
			pending.queue(e, tags, execID, actionName)
			return
		}
		if sqlTx := sqlTxOf(tx.Statement.ConnPool); sqlTx != nil {
			if hook := sqlTxHookOf(sqlTx); hook != nil {
				hook.queue(e, tags, execID, actionName)
				e.notifyRemote(tx.Statement.Context, sqlTx, tags)
				return
			}
		}
		e.notifyRemote(tx.Statement.Context, tx.Statement.ConnPool, tags)
		e.invalidateTags(tags, execID, actionName)
	}
	// Updates only expose the post-write Dest, and Delete(&Model{}, id) leaves
	// collection fields zero. Snapshot matching rows before either write so a
	// collection move or a delete still invalidates the values that disappear.
	snapshotOld := func(tx *gorm.DB) {
		snapshotOldTrackedTags(tx)
	}
	db.Callback().Create().Before("*").Register("tether:readonly_unwrap", unwrapPreparedReadOnly)
	db.Callback().Query().Before("*").Register("tether:readonly_unwrap", unwrapPreparedReadOnly)
	db.Callback().Update().Before("*").Register("tether:readonly_unwrap", unwrapPreparedReadOnly)
	db.Callback().Delete().Before("*").Register("tether:readonly_unwrap", unwrapPreparedReadOnly)
	db.Callback().Row().Before("*").Register("tether:readonly_unwrap", unwrapPreparedReadOnly)
	db.Callback().Raw().Before("*").Register("tether:readonly_unwrap", unwrapPreparedReadOnly)
	db.Callback().Create().Before("gorm:create").Register("tether:readonly_guard", enforceReadOnly)
	db.Callback().Update().Before("gorm:update").Register("tether:readonly_guard", enforceReadOnly)
	db.Callback().Delete().Before("gorm:delete").Register("tether:readonly_guard", enforceReadOnly)

	db.Callback().Create().Before("gorm:create").Register("tether:before_create_profiler", beforeProfiler)
	db.Callback().Update().Before("gorm:update").Register("tether:before_update_profiler", beforeProfiler)
	db.Callback().Delete().Before("gorm:delete").Register("tether:before_delete_profiler", beforeProfiler)
	db.Callback().Query().Before("gorm:query").Register("tether:before_query_profiler", beforeProfiler)

	// GORM callbacks for invalidation
	db.Callback().Create().After("gorm:create").Register("tether:after_create", invalidate)
	db.Callback().Update().Before("gorm:update").Register("tether:before_update", snapshotOld)
	db.Callback().Update().After("gorm:update").Register("tether:after_update", invalidate)
	db.Callback().Delete().Before("gorm:delete").Register("tether:before_delete", snapshotOld)
	db.Callback().Delete().After("gorm:delete").Register("tether:after_delete", invalidate)

	db.Callback().Query().After("gorm:query").Register("tether:after_query_profiler", func(tx *gorm.DB) {
		if e.Profiler().IsActive() {
			if startTime, ok := tx.InstanceGet("tether:profiler_start"); ok {
				execID, _ := tx.Statement.Context.Value(ContextKeyExecutionID).(string)
				e.Profiler().Add(Metric{
					ID:       execID,
					Name:     "gorm:query",
					Type:     MetricTypeDatabase,
					Time:     startTime.(time.Time),
					Duration: time.Since(startTime.(time.Time)),
					Tags:     []string{},
				})
			}
		}
	})

	// Automatically track the dependencies for the query
	db.Callback().Query().After("gorm:query").Register("tether:auto_track", func(tx *gorm.DB) {
		deps := dependenciesFromContext(tx.Statement.Context.Value(tetherCtxKey))
		if deps == nil || tx.Statement.Dest == nil || tx.Statement.Schema == nil {
			return
		}

		tableName := tx.Statement.Table
		val := reflect.Indirect(reflect.ValueOf(tx.Statement.Dest))

		var items []reflect.Value
		if val.Kind() == reflect.Slice {
			for i := 0; i < val.Len(); i++ {
				items = append(items, reflect.Indirect(val.Index(i)))
			}
		} else if val.Kind() == reflect.Struct {
			items = append(items, val)
		}

		for _, item := range items {
			for _, field := range tx.Statement.Schema.PrimaryFields {
				if pkVal, isZero := field.ValueOf(tx.Statement.Context, item); !isZero {
					*deps = append(*deps, fmt.Sprintf("%s:%v", tableName, pkVal))
				}
			}
		}
	})
	return e, nil
}

// EphemeralID returns this engine instance's ID. A new one is generated by
// every [NewEngine] call. It marks the scheduled tasks this instance has
// claimed and lets it ignore its own PostgreSQL notifications.
func (e *Engine) EphemeralID() string {
	return e.ephemeralID
}

// ErrEngineClosed is returned by [SchedulerCtx.RunAfter] once [Engine.Close]
// has been called, and by [Engine.SetStorage] if it is called after close.
var ErrEngineClosed = errors.New("tether: engine is closed")

// ErrNoCaller is returned by [AuthCtx.GetIdentity] when a query or mutation
// runs without a caller: scheduled tasks, crons, and calls to
// [Engine.ExecuteQuery] or [Engine.ExecuteMutation].
var ErrNoCaller = errors.New("tether: no caller")

// Close shuts down all engine-owned background work. It is safe to call more
// than once.
//
// It cancels the scheduler, Postgres listener and storage cleanup loops, stops
// the profiler, and stops every pending scheduled-task and auth-expiry timer.
// Timer and profiler-flush callbacks that are already running are waited for;
// any that fire after Close has begun do nothing, and scheduling new tasks
// returns ErrEngineClosed.
//
// Persisted tasks are never deleted by Close. Once in-flight callbacks have
// finished, the claims this instance holds on tasks it did not run are
// released so that another instance, or the next engine started on the same
// database, runs them at their scheduled time (or immediately, if overdue).
// If that release fails, the claims lapse when their lease expires.
func (e *Engine) Close() {
	e.closeMu.Lock()
	first := !e.closed
	e.closed = true
	e.closeMu.Unlock()

	e.cancel()
	if e.profiler != nil {
		e.Profiler().Stop()
	}
	e.timerMutex.Lock()
	for id, timer := range e.taskToTimer {
		timer.Stop()
		delete(e.taskToTimer, id)
	}
	e.timerMutex.Unlock()
	e.tracker.StopAuthExpiryTimers()
	e.wg.Wait()

	if first {
		err := e.db.Model(&TetherTask{}).Where("claimed_by = ?", e.ephemeralID).Updates(map[string]interface{}{
			"claimed_by":   nil,
			"locked_until": nil,
		}).Error
		if err != nil {
			slog.Error("Failed to release scheduled task claims", "error", err)
		}
	}
}

// beginWork registers engine-owned work with wg so Close waits for it. It
// returns false once Close has begun; the caller must then skip the work.
// Otherwise the caller must call e.wg.Done when finished.
func (e *Engine) beginWork() bool {
	e.closeMu.Lock()
	defer e.closeMu.Unlock()
	if e.closed {
		return false
	}
	e.wg.Add(1)
	return true
}

func (e *Engine) isClosed() bool {
	e.closeMu.Lock()
	defer e.closeMu.Unlock()
	return e.closed
}

// afterFunc is time.AfterFunc for engine-owned callbacks: Close waits for a
// run in progress, and a run that starts after Close has begun is skipped.
func (e *Engine) afterFunc(d time.Duration, f func()) *time.Timer {
	return time.AfterFunc(d, func() {
		if !e.beginWork() {
			return
		}
		defer e.wg.Done()
		f()
	})
}

// contextSleep waits for d or until ctx is cancelled. It reports whether the
// full delay elapsed.
func contextSleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func closePgConn(conn *pgx.Conn) {
	// The engine context is already cancelled during shutdown, and passing it
	// here would abort the close.
	if err := conn.Close(context.Background()); err != nil {
		slog.Debug("Failed to close PostgreSQL listener", "error", err)
	}
}

func (e *Engine) startPostgresListener(dsn string) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			if e.ctx.Err() != nil {
				return
			}
			conn, err := pgx.Connect(e.ctx, dsn)
			if err != nil {
				if e.ctx.Err() != nil {
					return
				}
				slog.Error("Failed to connect to PostgreSQL", "error", err)
				if !contextSleep(e.ctx, 2*time.Second) {
					return
				}
				continue
			}
			_, err = conn.Exec(e.ctx, "LISTEN tether_sync")
			if err != nil {
				closePgConn(conn)
				if e.ctx.Err() != nil {
					return
				}
				slog.Error("Failed to listen to PostgreSQL", "error", err)
				if !contextSleep(e.ctx, 2*time.Second) {
					return
				}
				continue
			}

			slog.Info("Connected to Postgres pub/sub channel")

			for {
				notification, err := conn.WaitForNotification(e.ctx)
				if err != nil {
					closePgConn(conn)
					if e.ctx.Err() != nil {
						return
					}
					slog.Error("Postgres notification error, reconnecting", "error", err)
					break
				}

				var msg notifyMessage
				if err := json.Unmarshal([]byte(notification.Payload), &msg); err != nil || msg.Sender == "" {
					slog.Error("Invalid payload format", "bytes", len(notification.Payload), "error", err)
					continue
				}

				if msg.Sender != e.ephemeralID && len(msg.Tags) > 0 {
					uniqueID := uuid.New().String()
					e.invalidateTags(msg.Tags, msg.Sender+"|"+uniqueID, "remote_update") // TODO: forward execID and actionName from the sender for better profiling
				}
			}
		}
	}()
}

func (e *Engine) startScheduler() {
	slog.Debug("Starting scheduler")
	ticker := time.NewTicker(scheduleLoopInterval)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-e.ctx.Done():
				slog.Debug("Scheduler stopped")
				return
			case <-ticker.C:
				e.pollScheduledTasks()
			}
		}
	}()
}

// defaultStorageBasePath is the URL prefix used when [Engine.SetStorage] is
// called with an empty base path.
const defaultStorageBasePath = "/storage"

// SetStorage enables file storage through the given adapter, such as
// local.New or s3.New. basePath is the URL prefix for the upload, download,
// and public routes. An empty basePath uses "/storage", so those routes are
// PUT {basePath}/upload/{token}, GET {basePath}/file/{token}, and
// GET {basePath}/public/{fileID}. Mount [Engine.StorageHandler] at that
// prefix, without http.StripPrefix, so clients can reach the URLs. The path
// must start with "/" and must not contain "..". A trailing slash is removed.
//
// defaults apply to every later GetUploadURL, PutFile, and GetDownloadURL.
// Options passed to those calls apply after them, in order. Nil options are
// ignored. A zero size or lifetime selects the built-in default of 20 MB or
// 15 minutes. [storage.Public] among the defaults marks every upload public.
// [storage.WithDownloadExpiresIn] and [storage.UseCachedURLs] apply when a
// download URL is issued. PutFile honors Public and ignores size, lifetime,
// and download-URL defaults.
//
// It creates the storage tables and starts an hourly cleanup of expired
// download links and abandoned uploads. Call it at most once. An invalid
// basePath is rejected before any of that work starts. If creating the
// tables fails, SetStorage returns that error and leaves the engine running,
// with storage still disabled. If the engine is already closed, it returns
// [ErrEngineClosed].
func (e *Engine) SetStorage(adapter storage.StorageAdapter, basePath string, defaults ...storage.UploadOption) error {
	basePath, err := normalizeStorageBasePath(basePath)
	if err != nil {
		return err
	}
	if err := e.CreateTable([]TetherStorage{}); err != nil {
		slog.Error("Failed to create table", "error", err)
		return err
	}
	if err := e.CreateTable([]TetherDownloadToken{}); err != nil {
		slog.Error("Failed to create table", "error", err)
		return err
	}
	if !e.beginWork() {
		return ErrEngineClosed
	}
	e.storage = adapter
	e.storageDefaults = append([]storage.UploadOption(nil), defaults...)
	e.storageBasePath = basePath
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-e.ctx.Done():
				return
			case now := <-ticker.C:
				e.cleanStorage(now)
			}
		}
	}()
	return nil
}

// normalizeStorageBasePath returns the URL prefix for storage routes.
// An empty basePath selects [defaultStorageBasePath]. The result has no
// trailing slash, except "/" itself, which keeps routes at the server root.
func normalizeStorageBasePath(basePath string) (string, error) {
	if basePath == "" {
		return defaultStorageBasePath, nil
	}
	if strings.ContainsAny(basePath, "?# \t\r\n\\") || !strings.HasPrefix(basePath, "/") {
		return "", fmt.Errorf("storage base path must be an absolute URL path starting with /")
	}
	if strings.Contains(basePath, "..") {
		return "", fmt.Errorf("storage base path must not contain ..")
	}
	cleaned := path.Clean(basePath)
	if cleaned == "." || !strings.HasPrefix(cleaned, "/") {
		return "", fmt.Errorf("storage base path must be an absolute URL path starting with /")
	}
	return cleaned, nil
}

// storageKindPrefix is the path prefix for one storage route, including the
// trailing slash before the token or file ID. kind is "upload", "file", or
// "public".
func storageKindPrefix(base, kind string) string {
	if base == "/" {
		return "/" + kind + "/"
	}
	return base + "/" + kind + "/"
}

// cleanStorage removes expired download tokens and upload rows that expired
// before they became active. Those rows stay until ExpiresAt, so a longer
// ExpiresIn remains usable. Object bytes are deleted before that metadata.
// A missing object is fine; any other delete error keeps the row for retry.
func (e *Engine) cleanStorage(now time.Time) {
	var expiredTokens []TetherDownloadToken
	if err := e.db.Where("expires_at < ?", now).Find(&expiredTokens).Error; err != nil {
		slog.Error("Failed to find expired download tokens", "error", err)
		return
	}
	tokenIDs := make([]string, 0, len(expiredTokens))
	for _, token := range expiredTokens {
		tokenIDs = append(tokenIDs, fmt.Sprintf("~storage_token:%s", token.Token))
	}
	if err := e.db.Delete(&expiredTokens).Error; err != nil {
		slog.Error("Failed to delete expired download tokens", "error", err)
		return
	}

	e.invalidateTags(tokenIDs, e.ephemeralID, "storage_token_expired")

	var abandonedFiles []TetherStorage
	e.db.Where("status IN ? AND expires_at < ?", []string{"pending", "uploading"}, now).Find(&abandonedFiles)

	var ids []string
	for _, file := range abandonedFiles {
		err := e.storage.Delete(e.ctx, file.ID)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Error("Failed to delete abandoned upload", "fileID", file.ID, "error", err)
			continue
		}
		ids = append(ids, file.ID)
	}
	if len(ids) == 0 {
		return
	}
	e.db.Where("id IN ?", ids).Delete(&TetherStorage{})
}

func (e *Engine) pollScheduledTasks() {
	lookAhead := time.Now().Add(scheduleLookahead)
	now := time.Now()

	var tasks []TetherTask
	q := e.db.Where("execute_at <= ? AND (claimed_by IS NULL or locked_until IS NULL or locked_until <= ?)", lookAhead, now)

	if e.dbType == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
	}
	err := q.Find(&tasks).Error
	if err != nil {
		slog.Error("Failed to poll scheduled tasks", "error", err)
		return
	}

	slog.Debug("Polled scheduled tasks", "tasks", len(tasks))

	for _, task := range tasks {
		lease := now.Add(5 * time.Minute)
		res := e.db.Model(&TetherTask{}).Where("id = ? AND (claimed_by IS NULL or locked_until IS NULL or locked_until <= ?)", task.ID, now).Updates(map[string]interface{}{
			"claimed_by":   e.ephemeralID,
			"locked_until": lease,
		})
		if res.Error != nil {
			slog.Error("Failed to update scheduled task", "error", res.Error)
			continue
		}
		if res.RowsAffected == 0 {
			continue // task was claimed by another instance
		}

		t := task
		delay := time.Until(t.ExecuteAt)
		if delay < 0 {
			delay = 0 // overdue, run immediately
		}

		var params map[string]interface{}
		_ = json.Unmarshal([]byte(t.ParamsJSON), &params)

		// Hold the mutex across AfterFunc so a zero-delay callback cannot
		// remove the entry before it is stored.
		e.timerMutex.Lock()
		if e.isClosed() {
			e.timerMutex.Unlock()
			return
		}
		e.taskToTimer[t.ID] = e.afterFunc(delay, func() {
			e.timerMutex.Lock()
			delete(e.taskToTimer, t.ID)
			e.timerMutex.Unlock()
			var check TetherTask
			if err := e.db.Where("id = ?", t.ID).First(&check).Error; err != nil {
				slog.Error("Task removed before execution", "taskID", t.ID, "error", err)
				return
			}
			defer func() {
				if err := recover(); err != nil {
					slog.Error("Failed to execute scheduled task", "taskID", t.ID, "error", err)
				}
				if t.IsCron && t.CronString != nil && *t.CronString != "" {
					nextTime, err := calculateNextCronTime(*t.CronString, time.Now())
					if err != nil {
						slog.Error("Failed to calculate next cron time", "error", err)
						return
					}
					err = e.db.Model(&TetherTask{}).Where("id = ?", t.ID).Updates(map[string]interface{}{
						"execute_at":    nextTime,
						"claimed_by":    nil,
						"locked_until":  nil,
						"last_executed": time.Now(),
					}).Error
					if err != nil {
						slog.Error("Failed to update scheduled task", "error", err)
						return
					}
				} else {
					e.db.Delete(&TetherTask{}, "id = ?", t.ID)
				}
			}()
			_, err := e.executeMutationInternal(t.FunctionName, params)
			if err != nil {
				slog.Error("Failed to execute scheduled task", "taskID", t.ID, "error", err)
			}
		})
		e.timerMutex.Unlock()
	}
}

func (e *Engine) cancelTask(taskID string) bool {
	err := e.db.Delete(&TetherTask{}, "id = ?", taskID).Error
	if err != nil {
		slog.Error("Failed to cancel task", "taskID", taskID, "error", err)
		return false
	}
	e.timerMutex.Lock()
	defer e.timerMutex.Unlock()
	if timer, ok := e.taskToTimer[taskID]; ok {
		stopped := timer.Stop()
		delete(e.taskToTimer, taskID)
		if !stopped {
			return false
		}
	}
	return true
}

// Helper function to extract the tags for a mutation
func extractMutationTags(tx *gorm.DB) []string {
	var tags []string
	// If GORM didn't parse a schema (e.g., raw SQL), we can't extract tags this way
	if tx.Statement.Schema == nil || tx.Statement.Dest == nil {
		return tags
	}

	tableName := tx.Statement.Table
	val := reflect.Indirect(reflect.ValueOf(tx.Statement.Dest))

	// Convert everything to a slice of reflect.Values so we can iterate uniformly
	var items []reflect.Value
	if val.Kind() == reflect.Slice {
		for i := 0; i < val.Len(); i++ {
			items = append(items, reflect.Indirect(val.Index(i)))
		}
	} else if val.Kind() == reflect.Struct {
		items = append(items, val)
	}

	seen := make(map[string]struct{})
	add := func(tag string) {
		if _, ok := seen[tag]; ok {
			return
		}
		seen[tag] = struct{}{}
		tags = append(tags, tag)
	}

	// Emit table mutated tag
	add(fmt.Sprintf("table_%s:mutated", tableName))

	for _, item := range items {
		appendRecordTags(tx, item, add)
	}

	// Updates(map) stores assignments in Dest, not the row. Pull PK / collection
	// values from the map, the Model (e.g. Model(&msg).Updates(map)), and WHERE.
	if val.Kind() == reflect.Map {
		appendMapTags(tx, val, add)
	}
	if tx.Statement.Model != nil && tx.Statement.Model != tx.Statement.Dest {
		if modelVal := reflect.Indirect(reflect.ValueOf(tx.Statement.Model)); modelVal.Kind() == reflect.Struct {
			appendRecordTags(tx, modelVal, add)
		}
	}
	appendWhereTags(tx, add)

	if old, ok := tx.InstanceGet(oldTrackedTagsKey); ok {
		if oldTags, ok := old.([]string); ok {
			for _, tag := range oldTags {
				add(tag)
			}
		}
	}

	return tags
}

const oldTrackedTagsKey = "tether:old_tracked_tags"

func hasTrackedFields(tx *gorm.DB) bool {
	if tx.Statement == nil || tx.Statement.Schema == nil {
		return false
	}
	for _, field := range tx.Statement.Schema.Fields {
		if field.Tag.Get("tether") == "track" {
			return true
		}
	}
	return false
}

// snapshotOldTrackedTags loads the rows about to be updated or deleted and
// stashes their primary-key and tracked-field tags on the statement. GORM does
// not expose a before-image, so this extra SELECT (same transaction, hooks
// skipped) is what lets a predicate that names neither the primary key nor a
// tracked collection still invalidate the affected rows, a collection move
// invalidate both sides, and a delete of an unloaded struct invalidate the
// deleted row's collections.
func snapshotOldTrackedTags(tx *gorm.DB) {
	if tx.Error != nil || tx.DryRun || tx.Statement == nil || tx.Statement.Schema == nil {
		return
	}
	// Nothing on the row can become a tag, so the SELECT cannot help.
	if len(tx.Statement.Schema.PrimaryFields) == 0 && !hasTrackedFields(tx) {
		return
	}

	dest := tx.Statement.Schema.MakeSlice().Interface()
	q := tx.Session(&gorm.Session{
		NewDB:       true,
		SkipHooks:   true,
		Initialized: true,
		Context:     context.Background(),
	}).Model(reflect.New(tx.Statement.Schema.ModelType).Interface())
	if tx.Statement.Table != "" {
		q = q.Table(tx.Statement.Table)
	}
	q.Statement.Unscoped = tx.Statement.Unscoped

	if !restrictToUpdatingRows(tx, q) {
		return
	}
	if err := q.Find(dest).Error; err != nil {
		slog.Debug("tether: failed to snapshot rows before update", "error", err)
		return
	}

	seen := make(map[string]struct{})
	var tags []string
	add := func(tag string) {
		if _, ok := seen[tag]; ok {
			return
		}
		seen[tag] = struct{}{}
		tags = append(tags, tag)
	}
	slice := reflect.Indirect(reflect.ValueOf(dest))
	for i := 0; i < slice.Len(); i++ {
		appendRecordTags(tx, reflect.Indirect(slice.Index(i)), add)
	}
	if len(tags) > 0 {
		tx.InstanceSet(oldTrackedTagsKey, tags)
	}
}

// restrictToUpdatingRows copies the write's WHERE and/or primary keys onto q.
// Save() and Delete(model) do not attach the primary key to WHERE until GORM's
// own callback, so we also read primary keys from Dest and Model. Returns
// false if we cannot identify rows without scanning the whole table.
func restrictToUpdatingRows(updateTx, queryTx *gorm.DB) bool {
	identified := false
	if where, ok := updateTx.Statement.Clauses["WHERE"]; ok && where.Expression != nil {
		queryTx.Statement.AddClause(clause.Where{Exprs: []clause.Expression{where.Expression}})
		identified = true
	}
	if addPrimaryKeyIdentity(updateTx, queryTx) {
		identified = true
	}
	return identified
}

func addPrimaryKeyIdentity(updateTx, queryTx *gorm.DB) bool {
	if updateTx.Statement.Schema == nil {
		return false
	}
	pks := updateTx.Statement.Schema.PrimaryFields
	if len(pks) == 0 {
		return false
	}

	var items []reflect.Value
	collectStructItems := func(v interface{}) {
		if v == nil {
			return
		}
		val := reflect.Indirect(reflect.ValueOf(v))
		switch val.Kind() {
		case reflect.Struct:
			items = append(items, val)
		case reflect.Slice:
			for i := 0; i < val.Len(); i++ {
				items = append(items, reflect.Indirect(val.Index(i)))
			}
		}
	}
	collectStructItems(updateTx.Statement.Dest)
	if updateTx.Statement.Model != nil && updateTx.Statement.Model != updateTx.Statement.Dest {
		collectStructItems(updateTx.Statement.Model)
	}

	var groups []clause.Expression
	for _, item := range items {
		if item.Kind() != reflect.Struct {
			continue
		}
		var eqs []clause.Expression
		complete := true
		for _, field := range pks {
			pkVal, isZero := field.ValueOf(updateTx.Statement.Context, item)
			if isZero {
				complete = false
				break
			}
			eqs = append(eqs, clause.Eq{Column: field.DBName, Value: pkVal})
		}
		if !complete || len(eqs) == 0 {
			continue
		}
		if len(eqs) == 1 {
			groups = append(groups, eqs[0])
		} else {
			groups = append(groups, clause.And(eqs...))
		}
	}
	if len(groups) == 0 {
		return false
	}
	if len(groups) == 1 {
		queryTx.Statement.AddClause(clause.Where{Exprs: groups})
	} else {
		queryTx.Statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.Or(groups...)}})
	}
	return true
}

func appendRecordTags(tx *gorm.DB, item reflect.Value, add func(string)) {
	tableName := tx.Statement.Table
	for _, field := range tx.Statement.Schema.PrimaryFields {
		if pkVal, isZero := field.ValueOf(tx.Statement.Context, item); !isZero {
			add(fmt.Sprintf("%s:%v", tableName, pkVal))
		}
	}
	for _, field := range tx.Statement.Schema.Fields {
		if field.Tag.Get("tether") == "track" {
			if trackVal, isZero := field.ValueOf(tx.Statement.Context, item); !isZero {
				// We use field.DBName to ensure it matches what the developer
				// writes in ctx.TrackCollection!
				add(fmt.Sprintf("%s_%s:%v", tableName, field.DBName, trackVal))
			}
		}
	}
}

func appendMapTags(tx *gorm.DB, val reflect.Value, add func(string)) {
	m, ok := mapStringInterface(val)
	if !ok {
		return
	}
	tableName := tx.Statement.Table
	for _, field := range tx.Statement.Schema.PrimaryFields {
		if v, exists := lookupMap(m, field.Name, field.DBName); exists && !isZeroValue(v) {
			add(fmt.Sprintf("%s:%v", tableName, v))
		}
	}
	for _, field := range tx.Statement.Schema.Fields {
		if field.Tag.Get("tether") != "track" {
			continue
		}
		if v, exists := lookupMap(m, field.Name, field.DBName); exists && !isZeroValue(v) {
			add(fmt.Sprintf("%s_%s:%v", tableName, field.DBName, v))
		}
	}
}

func appendWhereTags(tx *gorm.DB, add func(string)) {
	where, ok := tx.Statement.Clauses["WHERE"]
	if !ok || where.Expression == nil {
		return
	}
	walkWhereExpr(tx, where.Expression, add)
}

func walkWhereExpr(tx *gorm.DB, expr clause.Expression, add func(string)) {
	switch e := expr.(type) {
	case clause.Where:
		for _, x := range e.Exprs {
			walkWhereExpr(tx, x, add)
		}
	case clause.AndConditions:
		for _, x := range e.Exprs {
			walkWhereExpr(tx, x, add)
		}
	case clause.Eq:
		appendColumnValueTag(tx, e.Column, e.Value, add)
	case clause.IN:
		for _, v := range flattenIN(e.Values) {
			appendColumnValueTag(tx, e.Column, v, add)
		}
	case clause.Expr:
		appendSQLExprTags(tx, e, add)
	}
}

func appendColumnValueTag(tx *gorm.DB, column, value interface{}, add func(string)) {
	if isZeroValue(value) {
		return
	}
	tableName := tx.Statement.Table
	name := clauseColumnName(column)
	if name == clause.PrimaryKey {
		add(fmt.Sprintf("%s:%v", tableName, value))
		return
	}
	for _, field := range tx.Statement.Schema.PrimaryFields {
		if columnMatchesField(name, field.Name, field.DBName) {
			add(fmt.Sprintf("%s:%v", tableName, value))
			return
		}
	}
	for _, field := range tx.Statement.Schema.Fields {
		if field.Tag.Get("tether") != "track" {
			continue
		}
		if columnMatchesField(name, field.Name, field.DBName) {
			add(fmt.Sprintf("%s_%s:%v", tableName, field.DBName, value))
			return
		}
	}
}

func appendSQLExprTags(tx *gorm.DB, expr clause.Expr, add func(string)) {
	if len(expr.Vars) == 0 {
		return
	}
	sql := normalizeSQL(expr.SQL)
	for _, field := range tx.Statement.Schema.PrimaryFields {
		if sqlIsEquality(sql, field.Name, field.DBName) {
			appendColumnValueTag(tx, field.DBName, expr.Vars[0], add)
			return
		}
		if sqlIsIN(sql, field.Name, field.DBName) {
			for _, v := range flattenIN(expr.Vars) {
				appendColumnValueTag(tx, field.DBName, v, add)
			}
			return
		}
	}
	for _, field := range tx.Statement.Schema.Fields {
		if field.Tag.Get("tether") != "track" {
			continue
		}
		if sqlIsEquality(sql, field.Name, field.DBName) {
			appendColumnValueTag(tx, field.DBName, expr.Vars[0], add)
			return
		}
	}
}

func mapStringInterface(val reflect.Value) (map[string]interface{}, bool) {
	if val.Kind() != reflect.Map || val.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	if m, ok := val.Interface().(map[string]interface{}); ok {
		return m, true
	}
	m := make(map[string]interface{}, val.Len())
	iter := val.MapRange()
	for iter.Next() {
		m[iter.Key().String()] = iter.Value().Interface()
	}
	return m, true
}

func lookupMap(m map[string]interface{}, names ...string) (interface{}, bool) {
	for _, name := range names {
		if v, ok := m[name]; ok {
			return v, true
		}
	}
	return nil, false
}

func isZeroValue(v interface{}) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return !rv.IsValid() || rv.IsZero()
}

func clauseColumnName(col interface{}) string {
	switch c := col.(type) {
	case string:
		return c
	case clause.Column:
		return c.Name
	default:
		return fmt.Sprint(c)
	}
}

func columnMatchesField(col, fieldName, dbName string) bool {
	col = stripColumn(col)
	return strings.EqualFold(col, dbName) || strings.EqualFold(col, fieldName)
}

func stripColumn(col string) string {
	col = strings.Trim(col, "`\"[]")
	if i := strings.LastIndex(col, "."); i >= 0 {
		col = strings.Trim(col[i+1:], "`\"[]")
	}
	return col
}

func normalizeSQL(sql string) string {
	sql = strings.ToLower(sql)
	for _, q := range []string{"`", `"`, "[", "]"} {
		sql = strings.ReplaceAll(sql, q, "")
	}
	return strings.Join(strings.Fields(sql), " ")
}

func sqlIsEquality(sql, fieldName, dbName string) bool {
	for _, col := range sqlColumnNames(fieldName, dbName) {
		if sql == col+" = ?" || sql == col+"=?" ||
			strings.HasSuffix(sql, "."+col+" = ?") || strings.HasSuffix(sql, "."+col+"=?") {
			return true
		}
	}
	return false
}

func sqlIsIN(sql, fieldName, dbName string) bool {
	for _, col := range sqlColumnNames(fieldName, dbName) {
		if sql == col+" in ?" || sql == col+" in (?)" ||
			strings.HasSuffix(sql, "."+col+" in ?") || strings.HasSuffix(sql, "."+col+" in (?)") {
			return true
		}
	}
	return false
}

func sqlColumnNames(fieldName, dbName string) []string {
	names := []string{strings.ToLower(dbName), strings.ToLower(fieldName)}
	var out []string
	seen := make(map[string]struct{})
	for _, name := range names {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func flattenIN(values []interface{}) []interface{} {
	var out []interface{}
	for _, v := range values {
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice && rv.Type() != reflect.TypeOf([]byte(nil)) {
			for i := 0; i < rv.Len(); i++ {
				out = append(out, rv.Index(i).Interface())
			}
			continue
		}
		out = append(out, v)
	}
	return out
}

// SetAuth sets how clients authenticate. When a client sends a token, the
// engine passes it to auth.VerifyToken and binds the returned user ID to that
// connection. Without SetAuth, every token is accepted and the client stays
// anonymous.
func (e *Engine) SetAuth(auth Auth) {
	e.auth = auth
}

// RegisterMutation registers fn as the mutation name. It panics if name is
// already registered.
//
// fn returns the value sent to the calling client and an error. The value is
// encoded as JSON. A non-nil error is sent as an error frame containing
// err.Error() instead; that text is sent verbatim, so it should not reveal
// internal details. A value that implements error is treated the same way.
// An internal mutation has no client, so its error is returned to whoever
// ran it: [Engine.ExecuteMutation], the scheduler, or a cron.
//
// Writes made through ctx.DB with GORM's Create, Save, Update(s) and Delete
// re-run every subscribed query that depends on the changed rows. Writes
// inside a transaction do so only once it commits. Raw SQL run with Exec is
// not detected.
//
// Options are applied in order. [Internal] hides the mutation from clients,
// who receive the same error as for an unknown name, but it can still be run
// with [Engine.ExecuteMutation], [SchedulerCtx.RunAfter], or [Engine.RegisterCron].
func (e *Engine) RegisterMutation(name string, fn func(ctx *MutationCtx) (any, error), opts ...Option) {
	cfg := applyOptions(opts)
	// check if the mutation is already registered
	if _, ok := e.mutations[name]; ok {
		panic(fmt.Sprintf("mutation %s already registered", name))
	}
	e.mutations[name] = mutation{Func: fn, Internal: cfg.internal} // stores the mutation in the list of valid mutations
	slog.Debug("Registered mutation", "name", name)
}

// RegisterQuery registers fn as the live query name. It panics if name is
// already registered.
//
// When a client subscribes, fn runs and its return value is encoded as JSON
// and sent to the client. That first run does not block the connection, so
// other subscriptions on the same connection are evaluated at the same time.
// fn runs again, and the new result is pushed, whenever something it depends
// on changes. A query depends on:
//
//   - the primary keys of every row it loads through ctx.DB (automatic);
//   - collections and tables named with [QueryCtx.TrackCollection] and
//     [QueryCtx.TrackTable];
//   - the caller's identity, if it calls ctx.Auth.GetIdentity;
//   - the result of every guard it runs with ctx.Auth.ExecuteGuard.
//
// Primary-key tracking cannot see rows inserted after the query ran, so a
// query that lists rows should also track their collection or table.
//
// Subscriptions with the same query, params and identity-related state share
// one execution of fn, so its result must depend only on ctx. ctx.DB is
// read-only: any write fails. fn returns the value sent to the client and an
// error. The value is encoded as JSON. A non-nil error is sent as an error
// frame containing err.Error(). A value that implements error is treated the
// same way.
//
// Options are applied in order. Clients cannot subscribe to a query
// registered with [Internal] and receive the same error as for an unknown name.
// [Engine.ExecuteQuery] can still run it.
func (e *Engine) RegisterQuery(name string, fn func(ctx *QueryCtx) (any, error), opts ...Option) {
	cfg := applyOptions(opts)
	// check if the query is already registered
	if _, ok := e.queries[name]; ok {
		panic(fmt.Sprintf("query %s already registered", name))
	}
	e.queries[name] = query{Func: fn, Internal: cfg.internal} // stores the query in the list of valid queries
	slog.Debug("Registered query", "name", name)
}

func applyOptions(opts []Option) optionConfig {
	var cfg optionConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// RegisterGuard registers fn as the guard name. It panics if name is already
// registered. Queries and mutations run guards with
// ctx.Auth.ExecuteGuard; guards cannot run other guards.
//
// A guard is a reusable authorization check. fn returns the check's value and
// an error. The value must be encodable as JSON. A non-nil error fails the
// guard: [AuthCtx.ExecuteGuard] returns it, and a later re-run drops the
// cached result so a query cannot keep a grant that could not be rechecked.
// A value that implements error fails the guard the same way.
//
// Inside a query the value is cached per user and params, and the guard is
// re-run when its own dependencies change (tracked the same way as a query's,
// through ctx.DB, TrackCollection, TrackTable and GetIdentity). If the value
// changes, the queries that used it re-run. Subscribers whose guards returned
// the same value can still share one query execution.
//
// ctx.DB is read-only: any write fails.
//
// opts accepts [GuardOption] values. None are defined yet; the parameter is
// reserved so options can be added without changing this signature.
func (e *Engine) RegisterGuard(name string, fn func(ctx *GuardCtx) (any, error), opts ...GuardOption) {
	var cfg guardConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	// check if the guard is already registered
	if _, ok := e.guards[name]; ok {
		panic(fmt.Sprintf("guard %s already registered", name))
	}
	e.guards[name] = guard{Func: fn}
	slog.Debug("Registered guard", "name", name)
}

// CreateTable creates or migrates the table for schema, a pointer to a GORM
// model, using GORM's AutoMigrate, and returns any migration error.
//
// Primary keys are always tracked. Tag a field with `tether:"track"` to make
// it a collection that queries can depend on with [QueryCtx.TrackCollection]:
//
//	type Message struct {
//		ID     uint `gorm:"primaryKey"`
//		RoomID string `tether:"track"`
//	}
func (e *Engine) CreateTable(schema interface{}) error {
	err := e.db.AutoMigrate(schema)
	if err != nil {
		return err
	}
	return nil
}

// Handle upgrades the request to a WebSocket connection speaking the Tether
// client protocol. Mount it at the path the client libraries connect to.
// Every JSON object frame sent on that connection includes protocol_version.
//
// By default only same-origin browser connections are accepted; use
// [Engine.SetAllowedOrigins] or [Engine.SetCheckOrigin] to allow others.
// A client frame larger than 8 KB closes the connection.
func (e *Engine) Handle(w http.ResponseWriter, r *http.Request) {
	reactivity.Handle(w, r, e.onReceiveMessage, e.tracker, e.websocketHelper) // wraps the raw websocket connection with the engine handler
}

// StorageHandler serves upload, download, and public file URLs. By default
// those are PUT /storage/upload/{token}, which stores the request body as
// the file, GET /storage/file/{token}, which serves a file to anyone holding
// a download link from [StorageCtx.GetDownloadURL], and
// GET /storage/public/{fileID}, which serves a file marked public with
// [storage.Public]. A file that is missing, private, or not yet active is
// not found. [Engine.SetStorage] can replace the "/storage" prefix. The
// handler matches the full request path, so mount it at that prefix without
// http.StripPrefix:
//
//	http.HandleFunc("/storage/", engine.StorageHandler)
//
// Served files include X-Content-Type-Options: nosniff. Types that are not
// safe to display in the browser, including SVG, are sent as attachments.
// Browser requests are subject to the same origin policy as [Engine.Handle],
// and allowed origins receive the CORS headers they need. Responses vary on
// Origin even when the request omits it, so a cached media load of the same
// URL cannot satisfy a later cross-origin fetch. Responds with 501
// Not Implemented if [Engine.SetStorage] has not been called.
func (e *Engine) StorageHandler(w http.ResponseWriter, r *http.Request) {
	if e.storage == nil {
		http.Error(w, "Storage not configured", http.StatusNotImplemented)
		return
	}
	if !e.allowStorageOrigin(w, r) {
		return
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, storageKindPrefix(e.storageBasePath, "upload")) {
		token := filepath.Base(r.URL.Path)

		var record TetherStorage
		err := e.db.Where("token = ? AND status = 'pending' AND expires_at > ?", token, time.Now()).First(&record).Error
		if err != nil {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		result := e.db.Model(&TetherStorage{}).Where("id = ? AND status = 'pending'", record.ID).Update("status", "uploading")
		if result.Error != nil {
			slog.Error("Failed to update status", "error", err)
			http.Error(w, "Failed to update status", http.StatusInternalServerError)
			return
		}
		if result.RowsAffected == 0 {
			http.Error(w, "Upload already in progress", http.StatusConflict)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, record.MaxBytes)
		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		err = e.storage.UploadStream(r.Context(), record.ID, contentType, r)
		if err != nil {
			slog.Error("Failed to upload file", "error", err)
			http.Error(w, "Failed to upload file", http.StatusInternalServerError)
			e.db.Model(&TetherStorage{}).Where("id = ?", record.ID).Update("status", "pending")
			return
		}
		result = e.db.Model(&record).Updates(map[string]interface{}{
			"status":    "active",
			"mime_type": contentType,
			"file_size": r.ContentLength,
			"adapter":   e.storage.Name(), // log the adapter used to store the file
		})
		if result.Error != nil {
			slog.Error("Failed to update upload status", "error", result.Error)
			http.Error(w, "Failed to update upload status", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, storageKindPrefix(e.storageBasePath, "file")) {
		token := filepath.Base(r.URL.Path)

		var dlToken TetherDownloadToken
		err := e.db.Where("token = ? AND expires_at > ?", token, time.Now()).First(&dlToken).Error
		if err != nil {
			http.Error(w, "Invalid or expired download link", http.StatusUnauthorized)
			return
		}

		var record TetherStorage
		err = e.db.Where("id = ? AND status = 'active'", dlToken.FileID).First(&record).Error
		if err != nil {
			slog.Error("Failed to serve file", "error", err)
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		setSafeFileHeaders(w, record)
		ctx := context.WithValue(r.Context(), "expires_at", dlToken.ExpiresAt)
		err = e.storage.ServeFile(dlToken.FileID, w, r.WithContext(ctx))
		if err != nil {
			http.Error(w, "Failed to serve file", http.StatusInternalServerError)
		}
		return
	}

	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, storageKindPrefix(e.storageBasePath, "public")) {
		fileID := filepath.Base(r.URL.Path)
		var record TetherStorage
		err := e.db.Where("id = ? AND status = 'active' AND public = true", fileID).First(&record).Error
		if err != nil {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		setSafeFileHeaders(w, record)
		err = e.storage.ServeFile(record.ID, w, r)
		if err != nil {
			http.Error(w, "Failed to serve file", http.StatusInternalServerError)
		}
		return
	}
}

func setSafeFileHeaders(w http.ResponseWriter, record TetherStorage) {
	if record.MimeType != "" {
		w.Header().Set("Content-Type", record.MimeType)
		w.Header().Set("X-Content-Type-Options", "nosniff")

		if !isSafeInlineMime(record.MimeType) {
			w.Header().Set("Content-Disposition", "attachment; filename=\""+record.ID+"\"")
		} else {
			w.Header().Set("Content-Disposition", "inline; filename=\""+record.ID+"\"")
		}
	}
}

// Checks if the mime type is safe to inline in the browser.
func isSafeInlineMime(mimeType string) bool {
	safePrefixes := []string{"image/", "video/", "audio/", "text/plain"}
	mediaType, _, err := mime.ParseMediaType(mimeType)
	if err != nil {
		return false
	}
	for _, p := range safePrefixes {
		if strings.HasPrefix(mediaType, p) {
			// Block SVG because it can contain embedded JavaScript
			return !strings.Contains(mediaType, "svg")
		}
	}
	return mediaType == "application/pdf"
}

func (e *Engine) getUploadURL(opts ...storage.UploadOption) (storage.UploadInfo, error) {
	if e.storage == nil {
		return storage.UploadInfo{}, fmt.Errorf("storage not configured")
	}

	fileID := uuid.NewString()
	token := uuid.NewString()
	maxBytes, expiresIn, _, public, _ := storage.EffectiveUploadLimits(e.storageDefaults, opts)

	err := e.db.Create(&TetherStorage{
		ID:        fileID,
		Token:     token,
		Status:    "pending",
		MaxBytes:  maxBytes,
		ExpiresAt: time.Now().Add(expiresIn),
		Public:    public,
	}).Error
	if err != nil {
		return storage.UploadInfo{}, err
	}
	return storage.UploadInfo{
		FileID:    fileID,
		UploadURL: storageKindPrefix(e.storageBasePath, "upload") + token,
	}, nil
}

func (e *Engine) getDownloadURL(fileID string, opts ...storage.UploadOption) (string, error) {
	if e.storage == nil {
		return "", fmt.Errorf("storage not configured")
	}

	_, _, downloadExpiresIn, _, cache := storage.EffectiveUploadLimits(e.storageDefaults, opts)

	if cache {
		var record TetherDownloadToken
		err := e.db.Where("file_id = ?", fileID).First(&record).Error
		if err == nil {
			if record.ExpiresAt.After(time.Now().Add(5 * time.Minute)) {
				return storageKindPrefix(e.storageBasePath, "file") + record.Token, nil
			}
		}
	}

	token := uuid.NewString()

	err := e.db.Create(&TetherDownloadToken{
		Token:     token,
		FileID:    fileID,
		ExpiresAt: time.Now().Add(downloadExpiresIn),
	}).Error
	if err != nil {
		return "", err
	}
	return storageKindPrefix(e.storageBasePath, "file") + token, nil
}

func (e *Engine) deleteFile(fileID string) error {
	if e.storage == nil {
		return fmt.Errorf("storage not configured")
	}
	var record TetherStorage
	if err := e.db.Where("id = ?", fileID).First(&record).Error; err != nil {
		return err
	}
	if err := e.storage.Delete(e.ctx, record.ID); err != nil {
		return err
	}
	e.db.Where("file_id = ?", fileID).Delete(&TetherDownloadToken{})
	return e.db.Where("id = ?", record.ID).Delete(&TetherStorage{}).Error
}

// allowStorageOrigin applies the WebSocket origin policy to browser calls against
// storage routes. Requests with no Origin header are allowed so non-browser
// clients can still use an upload or download token. An allowed browser origin
// receives the CORS headers a cross-origin upload or download needs.
//
// Vary: Origin is set on every response, including when Origin is absent.
// img, video, and audio load a download URL without sending Origin. The local
// adapter answers with Last-Modified and no Cache-Control, so the browser may
// cache that response. Without Vary, the cache key ignores Origin and a later
// cross-origin fetch of the same URL can reuse the media response, which has
// no Access-Control-Allow-Origin.
func (e *Engine) allowStorageOrigin(w http.ResponseWriter, r *http.Request) bool {
	header := w.Header()
	header.Add("Vary", "Origin")

	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	if !originHeaderOK(origin) || !e.originPermitted(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return false
	}
	header.Set("Access-Control-Allow-Origin", origin)
	header.Set("Access-Control-Allow-Methods", "GET, PUT, OPTIONS")
	if requested := r.Header.Get("Access-Control-Request-Headers"); requested != "" {
		header.Set("Access-Control-Allow-Headers", requested)
	} else {
		header.Set("Access-Control-Allow-Headers", "Content-Type")
	}
	header.Set("Access-Control-Max-Age", "600")
	return true
}

func (e *Engine) originPermitted(r *http.Request) bool {
	if e.websocketHelper != nil && e.websocketHelper.CheckOrigin != nil {
		return e.websocketHelper.CheckOrigin(r)
	}
	return sameOrigin(r)
}

func originHeaderOK(origin string) bool {
	if strings.ContainsAny(origin, "\r\n") {
		return false
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" {
		return false
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return false
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	return parsed.Path == "" || parsed.Path == "/"
}

func sameOrigin(r *http.Request) bool {
	parsed, err := url.Parse(r.Header.Get("Origin"))
	if err != nil {
		return false
	}
	return strings.EqualFold(parsed.Host, r.Host)
}

// SetCheckOrigin sets the function that accepts or rejects browser origins.
// It applies to WebSocket upgrades and to storage routes served by the engine.
func (e *Engine) SetCheckOrigin(checkOrigin func(r *http.Request) bool) {
	e.websocketHelper.CheckOrigin = checkOrigin
}

// SetAllowedOrigins allows the listed origins for WebSocket upgrades and storage
// routes. Browsers calling those storage routes from an allowed origin receive
// the CORS headers the request needs.
func (e *Engine) SetAllowedOrigins(allowedOrigins []string) {
	e.websocketHelper.CheckOrigin = func(r *http.Request) bool {
		return slices.Contains(allowedOrigins, r.Header.Get("Origin"))
	}
}

func (e *Engine) invalidateTag(tag string) {
	e.invalidateTags([]string{tag}, "legacy", "legacy_invalidate_tag")
}

// invalidateTags re-runs each distinct subscription that listens to any of the
// given tags. Subscriptions are snapshotted before any query executes so a
// re-run that picks up additional tags (e.g. auto-tracked primary keys) cannot
// cause the same mutation to fire the query a second time. Matching
// subscriptions (same query, params, and auth fingerprint) share one execution
// and are fanned out with each client's own query_key. If that execution
// records an auth tag the batch was not partitioned on, the result is delivered
// only to the representative and every other subscription is executed under its
// own identity.
func (e *Engine) invalidateTags(tags []string, execID string, actionName string) {
	start := time.Now()
	seen := make(map[string]struct{})
	var unique []*reactivity.Subscription
	for _, tag := range tags {
		for _, subscription := range e.tracker.GetSubscriptionsToTag(tag) {
			if subscription == nil {
				continue
			}
			if _, ok := seen[subscription.SubID]; ok {
				continue
			}
			if subscription.Client == nil {
				// Guard subscriptions are per-user and cannot be batched. Re-run
				// the guard first so the attached query's *guard_ fingerprint is
				// current before we dedupe and execute queries.
				seen[subscription.SubID] = struct{}{}
				if attached := e.reevaluateGuard(subscription, execID); attached != nil {
					if _, ok := seen[attached.SubID]; !ok {
						seen[attached.SubID] = struct{}{}
						unique = append(unique, attached)
					}
				}
				continue
			}
			seen[subscription.SubID] = struct{}{}
			unique = append(unique, subscription)
		}
	}
	// Each client's auth is read before its fingerprint so an identity change
	// between the two leaves a stale epoch that rejects this run's writes.
	auths := make(map[string]reactivity.AuthCtx, len(unique))
	batchedExecutions := make(map[string][]*reactivity.Subscription)
	for _, subscription := range unique {
		auth, ok := e.tracker.GetAuth(subscription.Client.ID)
		if !ok {
			continue
		}
		auths[subscription.SubID] = auth
		authFingerprint := e.tracker.GetAuthFingerprint(subscription)
		dedupKey := fmt.Sprintf("%s|%s|%s", subscription.Query, subscription.ParamsHash, authFingerprint)
		batchedExecutions[dedupKey] = append(batchedExecutions[dedupKey], subscription)
	}

	sem := make(chan struct{}, 20)
	var wg sync.WaitGroup

	for _, subscriptions := range batchedExecutions {
		wg.Add(1)
		sem <- struct{}{}
		go func(subscriptions []*reactivity.Subscription) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					slog.Error("Recovered from panic", "error", r)
				}
			}()
			representative := subscriptions[0]
			repAuth := auths[representative.SubID]
			result, deps, ts, err := e.runQuery(representative.Query, representative.Params, representative, repAuth)
			if err != nil {
				slog.Error("Failed to execute query", "error", err)
				return
			}
			// Batch membership was chosen from the previous execution's auth tags.
			// A query can start calling GetIdentity (or a guard) only on this run.
			// Sharing that result would leak the representative's identity-specific
			// data and copy their new auth tag onto every other client.
			if len(subscriptions) > 1 && queryDepsIntroduceAuthTag(e.tracker, representative.SubID, deps) {
				e.sendQueryResult(representative, result, deps, ts, repAuth.AuthEpoch)
				for _, subscription := range subscriptions[1:] {
					auth := auths[subscription.SubID]
					result, deps, ts, err := e.runQuery(subscription.Query, subscription.Params, subscription, auth)
					if err != nil {
						slog.Error("Failed to execute query", "error", err)
						continue
					}
					e.sendQueryResult(subscription, result, deps, ts, auth.AuthEpoch)
				}
				return
			}
			e.Profiler().Add(Metric{
				ID:       execID,
				Name:     "batch_execution:" + representative.Query,
				Type:     MetricTypeRouting,
				Time:     start,
				Duration: time.Since(start),
				Value:    len(subscriptions),
			})
			// Apply the same dependency set to every subscription in the batch so
			// auto-tracked tags (e.g. new primary keys) stay in sync even though
			// the query function ran only once.
			for _, subscription := range subscriptions {
				e.sendQueryResult(subscription, result, deps, ts, auths[subscription.SubID].AuthEpoch)
			}
		}(subscriptions)
	}
	wg.Wait()
	e.Profiler().Add(Metric{
		ID:       execID,
		Name:     "invalidate_tags:" + actionName,
		Type:     MetricTypeRouting,
		Time:     start,
		Duration: time.Since(start),
		Tags:     tags,
	})
}

// marshalQueryMessage encodes a query result frame. A returned error becomes an
// error frame, since encoding/json turns error values into an empty object. It
// keeps the timestamp so clients can order it against other results.
func marshalQueryMessage(query string, result interface{}, queryKey string, timestamp int64) ([]byte, error) {
	if err, ok := result.(error); ok {
		return json.Marshal(map[string]interface{}{
			"type":      "error",
			"location":  query,
			"error":     err.Error(),
			"query_key": queryKey,
			"timestamp": timestamp,
		})
	}
	return json.Marshal(map[string]interface{}{
		"type":      "query",
		"location":  query,
		"data":      result,
		"query_key": queryKey,
		"timestamp": timestamp,
	})
}

// nextQueryTimestamp is a strictly increasing Unix-millisecond timestamp taken
// immediately before a query runs. Milliseconds fit in a JSON number exactly,
// and calls in the same millisecond still receive distinct ordered values.
func (e *Engine) nextQueryTimestamp() int64 {
	now := time.Now().UnixMilli()
	for {
		prev := e.queryClock.Load()
		next := now
		if next <= prev {
			next = prev + 1
		}
		if e.queryClock.CompareAndSwap(prev, next) {
			return next
		}
	}
}

// queryDepsIntroduceAuthTag reports whether deps contain a permanent auth tag
// (prefix "*") that subID was not already tracking. Those tags partition
// batches, so a newly recorded one means this result is not safe to share.
func queryDepsIntroduceAuthTag(tracker *reactivity.Tracker, subID string, deps []string) bool {
	for _, dep := range deps {
		if !strings.HasPrefix(dep, "*") {
			continue
		}
		if !tracker.SubscriptionHasTag(subID, dep) {
			return true
		}
	}
	return false
}

// sendQueryResult records deps on one subscription and pushes result to its
// client, unless that client's identity changed since authEpoch was read.
// Dependency and guard tags from an execution older than ts are left as the
// newer execution recorded them; the frame is still delivered so the client
// can drop it.
func (e *Engine) sendQueryResult(subscription *reactivity.Subscription, result interface{}, deps []string, ts int64, authEpoch int) {
	responseJSON, err := marshalQueryMessage(subscription.Query, result, subscription.QueryKey, ts)
	if err != nil {
		slog.Error("Failed to encode query result", "error", err)
		return
	}
	if !e.tracker.CommitQueryResult(subscription.SubID, deps, responseJSON, authEpoch, ts) {
		slog.Debug("Dropping query result from a previous identity", "subID", subscription.SubID)
	}
}

func (e *Engine) denyCapability() error {
	return fmt.Errorf("tether: this capability is not available in this context")
}

// runQuery executes the query function as auth's identity and returns the
// result plus the dependency tags it collected. It does not push to clients or
// update subscription tags; callers decide how to fan those out and must commit
// them against auth.AuthEpoch and the returned execution timestamp.
func (e *Engine) runQuery(query string, params map[string]interface{}, subscription *reactivity.Subscription, auth reactivity.AuthCtx) (interface{}, []string, int64, error) {
	if _, exists := e.queries[query]; !exists {
		return nil, nil, 0, fmt.Errorf("query not found")
	}
	if e.queries[query].Internal {
		return nil, nil, 0, fmt.Errorf("query not found") // return non-descriptive error to prevent enumeration
	}
	if _, err := json.Marshal(params); err != nil {
		return nil, nil, 0, err
	}
	authID := auth.UserID
	slog.Debug("Executing query", "query", query)

	// Assigned before the query function runs. ExecuteGuard closes over it and
	// only runs after the assignment below.
	var ts int64

	// Create the query context first so GetIdentity can append dependency tags.
	queryCtx := &QueryCtx{
		DB:           e.db,
		Params:       params,
		dependencies: []string{},
	}
	queryCtx.Storage = &StorageCtx{
		GetUploadURL: func(opts ...storage.UploadOption) (storage.UploadInfo, error) {
			return storage.UploadInfo{}, e.denyCapability()
		},
		GetDownloadURL: func(fileID string, opts ...storage.UploadOption) (string, error) {
			downloadURL, err := e.getDownloadURL(fileID, opts...)
			if err != nil {
				return "", err
			}
			queryCtx.dependencies = append(queryCtx.dependencies, fmt.Sprintf("~storage_token:%s", path.Base(downloadURL))) // store the token in the dependencies so the query can be re-run if the token expires
			return downloadURL, nil
		},
		DeleteFile: func(fileID string) error { return e.denyCapability() },
		PutFile: func(contentType string, data io.Reader, opts ...storage.UploadOption) (string, error) {
			return "", e.denyCapability()
		},
	}
	queryCtx.Auth = &AuthCtx{
		GetIdentity: func() (string, error) {
			return getIdentity(&queryCtx.dependencies, authID)
		},
		ExecuteGuard: func(guardName string, params map[string]interface{}) (interface{}, error) {
			paramsJSON, err := json.Marshal(params)
			paramsHash := strconv.FormatUint(xxhash.Sum64(paramsJSON), 10)
			guardFingerprint := e.tracker.GetGuardFingerprint(subscription, guardName, paramsHash)
			if guardFingerprint != "" {
				return decodeGuardFingerprint(guardName, paramsHash, guardFingerprint)
			}
			// First run: execute the guard, attach it as its own subscription,
			// and store only the result fingerprint on the query so matching
			// clients can share one batched execution.
			guardID := uuid.NewString()
			guardCtx := e.prepareGuardCtx(params, authID, guardID, guardName)
			result, err := executeGuard(e, guardCtx, guardName)
			if err != nil {
				return nil, err
			}
			e.tracker.AttachGuardToSubscription(subscription.SubID, guardID, guardName, params, guardCtx.dependencies, auth.AuthEpoch, ts)
			decoded, resultJSON, err := canonicalGuardResult(result)
			if err != nil {
				return nil, err
			}
			queryCtx.dependencies = append(queryCtx.dependencies, fmt.Sprintf("*guard_%s_%s:%s", guardName, paramsHash, resultJSON))
			return decoded, nil
		},
	}
	execID := uuid.NewString()

	// Create the GORM context
	gormCtx := context.WithValue(context.Background(), tetherCtxKey, queryCtx)
	gormCtx = context.WithValue(gormCtx, ContextKeyExecutionID, execID)
	gormCtx = context.WithValue(gormCtx, ContextKeyActionName, query)
	queryCtx.DB = e.readOnlyDB(gormCtx)

	// Stamp the execution before the query reads anything so the client can drop
	// this message if a later invalidation arrives first, and so an older
	// execution cannot overwrite dependency or guard state.
	ts = e.nextQueryTimestamp()
	start := time.Now()
	// Carry a handler error in the result so it becomes an error frame.
	// Returning it from runQuery would replace that text with a generic
	// execution failure.
	result, err := normalizeHandlerResult(e.queries[query].Func(queryCtx))
	if err != nil {
		result = err
	}
	e.Profiler().Add(Metric{
		ID:       execID,
		Name:     "query:" + query,
		Type:     MetricTypeQuery,
		Time:     start,
		Duration: time.Since(start),
		Tags:     []string{},
	})
	return result, queryCtx.dependencies, ts, nil
}

// executeQuery runs a query for one subscription and pushes the result. A
// result computed under an identity that has since changed is dropped without
// error: the auth change re-runs the subscription under the new identity.
func (e *Engine) executeQuery(query string, params map[string]interface{}, subscription *reactivity.Subscription) ([]byte, error) {
	auth, ok := e.tracker.GetAuth(subscription.Client.ID)
	if !ok {
		return nil, fmt.Errorf("client not found")
	}
	result, deps, ts, err := e.runQuery(query, params, subscription, auth)
	if err != nil {
		return nil, err
	}

	responseJSON, err := marshalQueryMessage(query, result, subscription.QueryKey, ts)
	if err != nil {
		return nil, err
	}
	if !e.tracker.CommitQueryResult(subscription.SubID, deps, responseJSON, auth.AuthEpoch, ts) {
		slog.Debug("Dropping query result from a previous identity", "subID", subscription.SubID)
		return nil, nil
	}
	slog.Debug("Updated dependencies on subscription", "subID", subscription.SubID, "dependencies", len(deps))
	return responseJSON, nil
}

// noCallerAuth is the auth context for a query or mutation that has no
// client: scheduled tasks, crons, [Engine.ExecuteQuery],
// [Engine.ExecuteMutation], [MutationCtx.ExecuteQuery], and
// [MutationCtx.ExecuteMutation].
func noCallerAuth() *AuthCtx {
	return &AuthCtx{
		GetIdentity: func() (string, error) {
			return "", ErrNoCaller
		},
		ExecuteGuard: func(string, map[string]interface{}) (interface{}, error) {
			return nil, fmt.Errorf("guards cannot be executed internally")
		},
	}
}

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
func (e *Engine) ExecuteMutation(name string, params map[string]interface{}) (any, error) {
	return e.executeMutationInternal(name, params)
}

// ExecuteQuery runs the query name once with params and returns its result.
//
// The query is not subscribed and its result is not sent to clients. It is
// safe to call from code that is not inside a mutation, including a goroutine
// that holds the engine. name may be registered with [Internal]. There is no
// caller: ctx.Auth.GetIdentity returns [ErrNoCaller] and ctx.Auth.ExecuteGuard
// returns an error. ctx.DB is read-only. A non-nil error from the query,
// including a returned value that implements error, is returned to the caller.
// params are passed through unchanged.
func (e *Engine) ExecuteQuery(name string, params map[string]interface{}) (any, error) {
	if _, exists := e.queries[name]; !exists {
		return nil, fmt.Errorf("query not found")
	}
	queryCtx := &QueryCtx{
		Params:       params,
		dependencies: []string{},
		Auth:         noCallerAuth(),
		Storage: &StorageCtx{
			GetUploadURL: func(opts ...storage.UploadOption) (storage.UploadInfo, error) {
				return storage.UploadInfo{}, e.denyCapability()
			},
			GetDownloadURL: e.getDownloadURL,
			DeleteFile:     func(fileID string) error { return e.denyCapability() },
			PutFile: func(contentType string, data io.Reader, opts ...storage.UploadOption) (string, error) {
				return "", e.denyCapability()
			},
		},
	}
	execID := uuid.NewString()
	gormCtx := context.WithValue(context.Background(), tetherCtxKey, queryCtx)
	gormCtx = context.WithValue(gormCtx, ContextKeyExecutionID, execID)
	gormCtx = context.WithValue(gormCtx, ContextKeyActionName, name)
	queryCtx.DB = e.readOnlyDB(gormCtx)

	start := time.Now()
	result, err := normalizeHandlerResult(e.queries[name].Func(queryCtx))
	e.Profiler().Add(Metric{
		ID:       execID,
		Name:     "query:" + name,
		Type:     MetricTypeQuery,
		Time:     start,
		Duration: time.Since(start),
		Tags:     []string{},
	})
	slog.Debug("Executed query internally", "query", name)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) putFile(contentType string, data io.Reader, opts ...storage.UploadOption) (string, error) {
	if e.storage == nil {
		return "", fmt.Errorf("storage not configured")
	}

	fileID := uuid.NewString()
	_, _, _, public, _ := storage.EffectiveUploadLimits(e.storageDefaults, opts)

	record := &TetherStorage{
		ID: fileID,
		// Token is unused once the file is active. The column is unique, so
		// an empty token would reject every file after the first.
		Token:     fileID,
		Status:    "active",
		Public:    public,
		MimeType:  contentType,
		Adapter:   e.storage.Name(),
		CreatedAt: time.Now(),
	}

	req, err := http.NewRequestWithContext(e.ctx, http.MethodPut, "http://internal.tether/upload", data)
	if err != nil {
		return "", fmt.Errorf("failed to create synthetic request: %w", err)
	}

	if req.ContentLength == 0 && data != nil {
		if seeker, ok := data.(io.Seeker); ok {
			if currentPos, err := seeker.Seek(0, io.SeekCurrent); err == nil {
				if size, err := seeker.Seek(0, io.SeekEnd); err == nil {
					seeker.Seek(currentPos, io.SeekStart)
					req.ContentLength = size - currentPos
				}
			}
		}
	}

	req.Header.Set("Content-Type", contentType)
	if req.ContentLength > 0 {
		req.Header.Set("Content-Length", strconv.FormatInt(req.ContentLength, 10))
	}

	err = e.storage.UploadStream(e.ctx, fileID, contentType, req)
	if err != nil {
		return "", fmt.Errorf("failed to upload file: %w", err)
	}

	err = e.db.Create(record).Error
	if err != nil {
		return "", fmt.Errorf("failed to create file record: %w", err)
	}

	return fileID, nil
}

func (e *Engine) executeMutationInternal(mutation string, params map[string]interface{}) (interface{}, error) {
	if _, exists := e.mutations[mutation]; !exists {
		return nil, fmt.Errorf("mutation not found")
	}
	execID := uuid.NewString()
	traceCtx := context.WithValue(context.Background(), ContextKeyExecutionID, execID)
	traceCtx = context.WithValue(traceCtx, ContextKeyActionName, mutation)
	authCtx := noCallerAuth()
	mutationCtx := &MutationCtx{DB: e.db.WithContext(traceCtx), Storage: &StorageCtx{
		GetUploadURL:   e.getUploadURL,
		GetDownloadURL: e.getDownloadURL,
		DeleteFile:     e.deleteFile,
		PutFile:        e.putFile,
	}, Auth: authCtx, Params: params, Profiler: e.Profiler(), Scheduler: &SchedulerCtx{
		RunAfter: func(duration time.Duration, functionName string, params map[string]interface{}) (string, error) {
			return e.scheduleTask(time.Now().Add(duration), functionName, params)
		},
		Cancel: func(taskID string) bool {
			return e.cancelTask(taskID)
		},
	},
		ExecuteMutation: func(mutationName string, params map[string]interface{}) (any, error) {
			return e.ExecuteMutation(mutationName, params)
		},
		ExecuteQuery: func(queryName string, params map[string]interface{}) (any, error) {
			return e.ExecuteQuery(queryName, params)
		},
	}
	start := time.Now()
	result, err := normalizeHandlerResult(e.mutations[mutation].Func(mutationCtx))
	e.Profiler().Add(Metric{
		ID:       execID,
		Name:     "mutation:" + mutation,
		Type:     MetricTypeMutation,
		Time:     start,
		Duration: time.Since(start),
		Tags:     []string{},
	})
	slog.Debug("Executed mutation internally", "mutation", mutation)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) executeMutation(mutation string, params map[string]interface{}, clientID string, mutationID string) (interface{}, error) {
	if _, exists := e.mutations[mutation]; !exists {
		return nil, fmt.Errorf("mutation not found")
	}
	if e.mutations[mutation].Internal {
		return nil, fmt.Errorf("mutation not found") // return non-descriptive error to prevent enumeration
	}
	auth, ok := e.tracker.GetAuth(clientID)
	if !ok {
		return nil, fmt.Errorf("client not found")
	}
	authID := auth.UserID

	execID := uuid.NewString()
	traceCtx := context.WithValue(context.Background(), ContextKeyExecutionID, execID)
	traceCtx = context.WithValue(traceCtx, ContextKeyActionName, mutation)
	scopedDB := e.db.WithContext(traceCtx)

	authCtx := &AuthCtx{
		GetIdentity: func() (string, error) { return authID, nil },
	}

	authCtx.ExecuteGuard = func(guardName string, params map[string]interface{}) (interface{}, error) {
		// Execute the guard and return the result
		// Mutations are single-fire, so no caching is needed
		guardAuth := &AuthCtx{
			GetIdentity: func() (string, error) { return authID, nil },
			ExecuteGuard: func(guardName string, params map[string]interface{}) (interface{}, error) {
				return nil, fmt.Errorf("guards cannot execute other guards")
			},
		}
		guardCtx := &GuardCtx{
			DB:     e.readOnlyDB(traceCtx),
			Auth:   guardAuth,
			Params: params,
		}
		result, err := executeGuard(e, guardCtx, guardName)
		if err != nil {
			return nil, err
		}
		decoded, _, err := canonicalGuardResult(result)
		if err != nil {
			return nil, err
		}
		return decoded, nil
	}

	mutationCtx := &MutationCtx{DB: scopedDB, Profiler: e.Profiler(), Storage: &StorageCtx{
		GetUploadURL:   e.getUploadURL,
		GetDownloadURL: e.getDownloadURL,
		DeleteFile:     e.deleteFile,
		PutFile:        e.putFile,
	}, Auth: authCtx, Params: params, Scheduler: &SchedulerCtx{
		RunAfter: func(duration time.Duration, functionName string, params map[string]interface{}) (string, error) {
			return e.scheduleTask(time.Now().Add(duration), functionName, params)
		},
		Cancel: func(taskID string) bool {
			return e.cancelTask(taskID)
		}},
		ExecuteMutation: func(mutationName string, params map[string]interface{}) (any, error) {
			return e.ExecuteMutation(mutationName, params)
		},
		ExecuteQuery: func(queryName string, params map[string]interface{}) (any, error) {
			return e.ExecuteQuery(queryName, params)
		},
	}
	start := time.Now()
	result, err := normalizeHandlerResult(e.mutations[mutation].Func(mutationCtx))
	e.Profiler().Add(Metric{
		ID:       execID,
		Name:     "mutation:" + mutation,
		Type:     MetricTypeMutation,
		Time:     start,
		Duration: time.Since(start),
		Tags:     []string{},
	})
	slog.Debug("Executed mutation", "mutation", mutation)
	// encoding/json turns error values into an empty object, so a returned
	// error is sent as an error frame instead of mutation data.
	response := map[string]interface{}{"type": "mutation", "location": mutation, "data": result, "mutation_id": mutationID}
	if err != nil {
		response = map[string]interface{}{"type": "error", "location": mutation, "error": err.Error(), "mutation": mutation, "mutation_id": mutationID}
		result = err
	}
	responseJSON, err := json.Marshal(response)
	if err != nil {
		slog.Error("Failed to encode mutation result", "mutation", mutation, "error", err)
		return nil, err
	}
	e.tracker.SendMessage(clientID, responseJSON)
	return result, nil
}

// rerunSubscriptions re-runs and pushes fresh results for subscriptions whose
// authorization state was reset by an identity change.
func (e *Engine) rerunSubscriptions(subscriptions []*reactivity.Subscription) {
	for _, subscription := range subscriptions {
		if _, err := e.executeQuery(subscription.Query, subscription.Params, subscription); err != nil {
			slog.Error("Failed to re-run query after auth change", "query", subscription.Query, "error", err)
		}
	}
}

// runInitialSubscription evaluates a subscription that was just registered and
// sends the result. It returns immediately. The evaluation runs alongside
// other initial subscriptions on the same connection. Close waits for it.
// A result for a subscription removed before it is published, including by
// unsubscribe, is not sent.
func (e *Engine) runInitialSubscription(clientID, query string, params map[string]interface{}, subscription *reactivity.Subscription) {
	if !e.beginWork() {
		return
	}
	go func() {
		defer e.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				slog.Error("Recovered from panic", "error", r)
			}
		}()
		queryKey := subscription.QueryKey
		_, err := e.executeQuery(query, params, subscription)
		if err != nil {
			slog.Error("Failed to execute query", "query", query, "error", err)
			if _, ok := e.tracker.GetSubscription(subscription.SubID); !ok {
				return
			}
			responseJSON, err := json.Marshal(map[string]interface{}{"type": "error", "error": "Failed to execute query", "query_key": queryKey, "params": params})
			if err != nil {
				slog.Error("Failed to encode error message", "error", err)
				return
			}
			e.tracker.SendMessage(clientID, responseJSON)
		}
	}()
}

func (e *Engine) onReceiveMessage(clientID string, msg map[string]interface{}) error {
	// Frames carry application params (passwords, refresh tokens, ...), so only
	// the frame type is ever logged.
	msgType, ok := msg["type"].(string)
	if !ok {
		slog.Error("Could not get message type", "from", clientID)
		return nil
	}
	protocolVersion, ok := msg["protocol_version"].(float64)
	if !ok {
		slog.Error("Could not get protocol version, assuming latest", "from", clientID)
	}
	slog.Debug("Received message", "from", clientID, "type", msgType, "protocol_version", protocolVersion)
	switch msgType {
	case "subscribe":
		query, ok := msg["location"].(string)
		if !ok {
			slog.Error("Invalid message", "from", clientID, "type", msgType)
			return nil
		}
		params, ok := msg["params"].(map[string]interface{})
		if !ok {
			slog.Error("Invalid message", "from", clientID, "type", msgType)
			return nil
		}
		queryKey, ok := msg["query_key"].(string)
		if !ok {
			slog.Error("Invalid message", "from", clientID, "type", msgType)
			return nil
		}
		subscription := e.tracker.SubscribeToQuery(clientID, query, queryKey, params)
		if subscription == nil {
			slog.Error("Failed to subscribe to query", "query", query)
			e.tracker.SendMessage(clientID, []byte(`{"type": "error", "error": "Failed to subscribe to query"}`))
			return nil
		}
		// Registration stays on this goroutine so a following unsubscribe or
		// auth frame observes the subscription. Only the evaluation is
		// concurrent: a slow query must not delay other subscriptions on this
		// connection.
		e.runInitialSubscription(clientID, query, params, subscription)
	case "unsubscribe":
		query, ok := msg["location"].(string)
		if !ok {
			slog.Error("Invalid message", "from", clientID, "type", msgType)
			e.tracker.SendMessage(clientID, []byte(`{"type": "error", "error": "Invalid message"}`))
			return nil
		}
		params, ok := msg["params"].(map[string]interface{})
		if !ok {
			slog.Error("Invalid message", "from", clientID, "type", msgType)
			e.tracker.SendMessage(clientID, []byte(`{"type": "error", "error": "Invalid message"}`))
			return nil
		}
		e.tracker.UnsubscribeFromQuery(clientID, query, params)
	case "mutation":
		mutation, ok := msg["location"].(string)
		if !ok {
			slog.Error("Invalid message", "from", clientID, "type", msgType)
			return nil
		}
		params, ok := msg["params"].(map[string]interface{})
		if !ok {
			slog.Error("Invalid message", "from", clientID, "type", msgType)
			return nil
		}
		mutationID, ok := msg["mutation_id"].(string)
		if !ok {
			slog.Error("Invalid message", "from", clientID, "type", msgType)
			return nil
		}
		_, err := e.executeMutation(mutation, params, clientID, mutationID)
		if err != nil {
			slog.Error("Failed to execute mutation", "mutation", mutation, "error", err)
			responseJSON, err := json.Marshal(map[string]interface{}{"type": "error", "error": "Failed to execute mutation", "mutation": mutation, "mutation_id": mutationID, "params": params})
			if err != nil {
				slog.Error("Failed to encode error message", "error", err)
				return fmt.Errorf("failed to encode error message: %w", err)
			}
			e.tracker.SendMessage(clientID, responseJSON)
			return fmt.Errorf("failed to execute mutation: %w", err)
		}
	case "auth":
		token, ok := msg["token"].(string)
		if !ok {
			slog.Error("Invalid message", "from", clientID, "type", msgType)
			e.tracker.SendMessage(clientID, []byte(`{"type": "error", "error": "Invalid message"}`))
			return nil
		}
		start := time.Now()
		execID := uuid.NewString()
		userID, expiresAt, err := e.auth.VerifyToken(e.ctx, e.db, token)
		e.Profiler().Add(Metric{
			ID:       execID,
			Name:     string(MetricTypeAuthentication),
			Type:     MetricTypeAuthentication,
			Time:     start,
			Duration: time.Since(start),
			Tags:     []string{},
		})
		if err != nil {
			slog.Error("Failed to get user ID", "error", err)
			e.tracker.SendMessage(clientID, []byte(`{"type": "error", "error": "Failed to get user ID"}`))
			return fmt.Errorf("failed to get user ID: %w", err)
		}
		e.rerunSubscriptions(e.tracker.SetAuth(clientID, userID, expiresAt))
		if !expiresAt.IsZero() { // 0 is treated as no expiration
			e.tracker.ArmAuthExpiry(clientID, func() *time.Timer {
				if e.isClosed() {
					return nil
				}
				return e.afterFunc(time.Until(expiresAt), func() {
					defer func() {
						if r := recover(); r != nil {
							slog.Error("Recovered from panic", "error", r)
						}
					}()
					e.rerunSubscriptions(e.tracker.ExpireAuth(clientID, expiresAt))
				})
			})
		}
		message := map[string]interface{}{"type": "auth", "success": true, "data": map[string]interface{}{"user_id": userID}}
		messageJSON, err := json.Marshal(message)
		if err != nil {
			slog.Error("Failed to encode auth message", "error", err)
			e.tracker.SendMessage(clientID, []byte(`{"type": "error", "error": "Failed to encode auth message"}`))
			return fmt.Errorf("failed to encode auth message: %w", err)
		}
		e.tracker.SendMessage(clientID, messageJSON)
		return nil
	}
	return nil
}
