package tether

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/recodeorg/tether/reactivity"
	"github.com/recodeorg/tether/storage"
	"github.com/recodeorg/tether/utilities"
	"github.com/robfig/cron/v3"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

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

type TetherStorage struct {
	ID        string `gorm:"primaryKey"`  // file ID
	Token     string `gorm:"uniqueIndex"` // token used to upload the file
	Status    string `gorm:"index"`       // status of the upload (pending, completed, failed)
	FileSize  int64
	MaxBytes  int64     // maximum size allowed at upload time
	MimeType  string    // MIME type of the file
	ExpiresAt time.Time // time when the upload token expires
	CreatedAt time.Time // time when the upload token was created
}

type TetherDownloadToken struct {
	Token     string    `gorm:"primaryKey"`
	FileID    string    `gorm:"index"`
	ExpiresAt time.Time // time when the download token expires
}

type Engine struct {
	db              *gorm.DB
	dbType          string // sqlite or postgres
	mutations       map[string]Mutation
	queries         map[string]Query
	dependencies    map[string][]string
	hashMu          sync.RWMutex
	queryHashes     map[string]uint64
	tracker         *reactivity.Tracker
	auth            Auth
	guards          map[string]Guard
	timerMutex      sync.RWMutex
	taskToTimer     map[string]*time.Timer
	websocketHelper *reactivity.WebsocketHelper
	Profiler        *utilities.Profiler
	EphemeralID     string
	storage         storage.StorageAdapter
}

type Mutation struct {
	Func     func(ctx *MutationCtx) interface{}
	Internal bool
}

type Query struct {
	Func     func(ctx *QueryCtx) interface{}
	Internal bool
}

type Guard struct {
	Func func(ctx *GuardCtx) interface{}
}

const SCHEDULE_LOOP_INTERVAL = 10 * time.Second

const SCHEDULE_LOOKAHEAD = 15 * time.Second

type defaultAuth struct{}

type contextKey string

const tetherCtxKey contextKey = "tether_query_ctx"

type traceContextKey string

const (
	ContextKeyExecutionID traceContextKey = "exec_id"
	ContextKeyActionName  traceContextKey = "action_name"
)

func (defaultAuth) VerifyToken(_ *gorm.DB, _ string) (string, time.Time, error) {
	return "", time.Time{}, nil
}

func getIdentity(deps *[]string, authID string) (string, error) {
	if authID != "" {
		*deps = append(*deps, "*user_identity:"+authID)
	}
	return authID, nil
}

func executeGuard(e *Engine, guardCtx *GuardCtx, guardName string) (interface{}, error) {
	guard, ok := e.guards[guardName]
	if !ok {
		return nil, fmt.Errorf("guard not found")
	}
	return guard.Func(guardCtx), nil
}

func dependenciesFromContext(v interface{}) *[]string {
	switch ctx := v.(type) {
	case *QueryCtx:
		return &ctx.Dependencies
	case *GuardCtx:
		return &ctx.Dependencies
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

func (e *Engine) bindGuardAuth(guardCtx *GuardCtx, authID string) {
	authCtx := &AuthCtx{
		GetIdentity: func() (string, error) {
			return getIdentity(&guardCtx.Dependencies, authID)
		},
	}
	authCtx.ExecuteGuard = func(name string, params map[string]interface{}) (interface{}, error) {
		nested := &GuardCtx{
			DB:       e.db,
			Auth:     authCtx,
			Params:   params,
			Profiler: e.Profiler,
		}
		return executeGuard(e, nested, name)
	}
	guardCtx.Auth = authCtx
}

func (e *Engine) prepareGuardCtx(params map[string]interface{}, authID, execID, actionName string) *GuardCtx {
	guardCtx := &GuardCtx{
		Params:       params,
		Profiler:     e.Profiler,
		Dependencies: []string{},
	}
	e.bindGuardAuth(guardCtx, authID)
	gormCtx := context.WithValue(context.Background(), tetherCtxKey, guardCtx)
	gormCtx = context.WithValue(gormCtx, ContextKeyExecutionID, execID)
	gormCtx = context.WithValue(gormCtx, ContextKeyActionName, actionName)
	guardCtx.DB = e.db.WithContext(gormCtx)
	return guardCtx
}

// reevaluateGuard runs a guard subscription after one of its data tags changed.
// Identity and DB dependencies stay on the guard; the attached query only
// stores the *guard_ fingerprint used for batching. Returns the attached
// query subscription when that fingerprint actually changed.
func (e *Engine) reevaluateGuard(subscription *reactivity.Subscription, execID string) *reactivity.Subscription {
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
		return nil
	}
	e.tracker.UpdateTags(subscription.SubID, guardCtx.Dependencies)
	guardResultJSON, err := json.Marshal(guardResult)
	if err != nil {
		slog.Error("Failed to marshal guard result", "error", err)
		return nil
	}
	paramsJSON, err := json.Marshal(subscription.Params)
	if err != nil {
		slog.Error("Failed to marshal params", "error", err)
		return nil
	}
	paramsHash := xxhash.Sum64(paramsJSON)
	if e.tracker.UpdateGuardFingerprint(attachedID, guardName, strconv.FormatUint(paramsHash, 10), string(guardResultJSON)) {
		return attached
	}
	return nil
}

func (e *Engine) scheduleTask(timestamp time.Time, functionName string, params map[string]interface{}) (string, error) {
	paramsJSON, err := json.Marshal(params)
	taskID := uuid.New().String()
	if err != nil {
		slog.Error("Failed to marshal params", "error", err)
		return "", err
	}

	if time.Until(timestamp) < SCHEDULE_LOOP_INTERVAL+SCHEDULE_LOOKAHEAD+(5*time.Second) {
		// if the task is due within the schedule loop interval, we need to run it immediately
		// the task is added to the database in case of a crash or restart, so that it is not lost
		// it is claimed at insertion so another instance doesn't claim it before it is executed
		lockedUntil := time.Now().Add(5 * time.Minute)
		task := TetherTask{
			ID:           taskID,
			FunctionName: functionName,
			ParamsJSON:   string(paramsJSON),
			ExecuteAt:    timestamp,
			ClaimedBy:    &e.EphemeralID,
			LockedUntil:  &lockedUntil,
			IsCron:       false,
		}
		err := e.db.Create(&task).Error
		if err != nil {
			slog.Error("Failed to create scheduled task", "error", err)
			return "", err
		}

		timer := time.AfterFunc(time.Until(timestamp), func() {
			defer e.db.Delete(&TetherTask{}, "id = ?", taskID)
			_, err := e.ExecuteMutationInternal(functionName, params)
			if err != nil {
				slog.Error("Failed to execute mutation internally", "error", err)
			}
		})
		e.timerMutex.Lock()
		e.taskToTimer[taskID] = timer
		e.timerMutex.Unlock()
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
		e.db.Create(&task)
	}

	return taskID, nil
}

func (e *Engine) RegisterCron(cronName string, cronString string, functionName string, params map[string]interface{}) (string, error) {
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		slog.Error("Failed to marshal params", "error", err)
		return "", err
	}
	now := time.Now()
	task := TetherTask{
		ID:           uuid.New().String(),
		Name:         cronName,
		FunctionName: functionName,
		ParamsJSON:   string(paramsJSON),
		ExecuteAt:    calculateNextCronTime(cronString, now),
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

func calculateNextCronTime(cronString string, fromTime time.Time) time.Time {
	cron := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := cron.Parse(cronString)
	if err != nil {
		slog.Error("Failed to parse cron string", "error", err)
		return time.Time{}
	}
	return schedule.Next(fromTime)
}

func getPostgresDSN(db *gorm.DB) (string, error) {
	if dia, ok := db.Dialector.(*postgres.Dialector); ok {
		return dia.Config.DSN, nil
	}
	return "", fmt.Errorf("dialector is not *postgres.Dialector")
}

func NewEngine(db *gorm.DB) *Engine {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	tracker := reactivity.NewTracker()
	dbType := db.Dialector.Name()
	if dbType != "sqlite" && dbType != "postgres" {
		panic("Invalid database type")
	}
	e := &Engine{
		db:              db,
		dbType:          dbType,
		mutations:       make(map[string]Mutation),
		queries:         make(map[string]Query),
		dependencies:    make(map[string][]string),
		queryHashes:     make(map[string]uint64),
		tracker:         tracker,
		auth:            defaultAuth{},
		guards:          make(map[string]Guard),
		taskToTimer:     make(map[string]*time.Timer),
		websocketHelper: &reactivity.WebsocketHelper{},
		EphemeralID:     uuid.New().String(),
	}
	// scheduler initialization
	e.CreateTable("_tether_tasks", []TetherTask{}) // Create the internal table for the scheduled tasks
	e.startScheduler(context.Background())

	if e.dbType == "postgres" {
		dsn, err := getPostgresDSN(db)
		if err != nil {
			slog.Error("Failed to get PostgreSQL DSN", "error", err)
			return nil
		}
		e.startPostgresListener(context.Background(), dsn)
	}

	// profiler initialization
	e.Profiler = utilities.NewProfiler(func(mutationName string) {
		_, err := e.ExecuteMutationInternal(mutationName, map[string]interface{}{})
		if err != nil {
			slog.Error("Failed to execute mutation internally", "error", err)
		}
	})
	beforeProfiler := func(db *gorm.DB) {
		// Profile the start time of the database operation
		db.InstanceSet("tether:profiler_start", time.Now())
	}
	invalidate := func(tx *gorm.DB) {
		tags := extractMutationTags(tx)
		execID, _ := tx.Statement.Context.Value(ContextKeyExecutionID).(string)
		actionName, _ := tx.Statement.Context.Value(ContextKeyActionName).(string)
		if e.Profiler.IsActive() {
			if startTime, ok := tx.InstanceGet("tether:profiler_start"); ok {

				e.Profiler.Add(utilities.Metric{
					ID:       execID,
					Name:     "gorm:" + tx.Statement.Name(),
					Type:     utilities.MetricTypeDatabase,
					Time:     startTime.(time.Time),
					Duration: time.Since(startTime.(time.Time)),
					Tags:     tags,
				})
			}
		}
		if e.dbType == "postgres" {
			joinedTags := strings.Join(tags, ",")
			payload := fmt.Sprintf("%s|%s", e.EphemeralID, joinedTags)

			if len(payload) < 8000 {
				e.db.Exec("SELECT pg_notify('tether_sync', ?)", payload)
			} else {
				// TODO: send the payload in chunks
				slog.Error("Payload too large to send via PostgreSQL notification", "payload", payload)
			}
		}
		e.InvalidateTags(tags, execID, actionName)
	}
	// GORM callbacks only see the post-update Dest, so a Save that moves a
	// tracked collection field would miss the old collection. Snapshot the
	// matching rows before the UPDATE SQL runs.
	snapshotOld := func(tx *gorm.DB) {
		snapshotOldTrackedTags(tx)
	}
	db.Callback().Create().Before("gorm:create").Register("tether:before_create_profiler", beforeProfiler)
	db.Callback().Update().Before("gorm:update").Register("tether:before_update_profiler", beforeProfiler)
	db.Callback().Delete().Before("gorm:delete").Register("tether:before_delete_profiler", beforeProfiler)
	db.Callback().Query().Before("gorm:query").Register("tether:before_query_profiler", beforeProfiler)

	// GORM callbacks for invalidation
	db.Callback().Create().After("gorm:create").Register("tether:after_create", invalidate)
	db.Callback().Update().Before("gorm:update").Register("tether:before_update", snapshotOld)
	db.Callback().Update().After("gorm:update").Register("tether:after_update", invalidate)
	db.Callback().Delete().After("gorm:delete").Register("tether:after_delete", invalidate)

	db.Callback().Query().After("gorm:query").Register("tether:after_query_profiler", func(tx *gorm.DB) {
		if e.Profiler.IsActive() {
			if startTime, ok := tx.InstanceGet("tether:profiler_start"); ok {
				execID, _ := tx.Statement.Context.Value(ContextKeyExecutionID).(string)
				e.Profiler.Add(utilities.Metric{
					ID:       execID,
					Name:     "gorm:query",
					Type:     utilities.MetricTypeDatabase,
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
	return e
}

func (e *Engine) startPostgresListener(ctx context.Context, dsn string) {
	go func() {
		for {
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				slog.Error("Failed to connect to PostgreSQL", "error", err)
				time.Sleep(2 * time.Second)
				continue
			}
			_, err = conn.Exec(ctx, "LISTEN tether_sync")
			if err != nil {
				slog.Error("Failed to listen to PostgreSQL", "error", err)
				conn.Close(ctx)
				time.Sleep(2 * time.Second)
				continue
			}

			slog.Info("Connected to Postgres pub/sub channel")

			for {
				notification, err := conn.WaitForNotification(ctx)
				if err != nil {
					slog.Error("Postgres notification error, reconnecting", "error", err)
					conn.Close(ctx)
					break
				}

				parts := strings.SplitN(notification.Payload, "|", 2)
				if len(parts) != 2 {
					slog.Error("Invalid payload format", "payload", notification.Payload)
					continue
				}
				senderID, tags := parts[0], parts[1]

				if senderID != e.EphemeralID {
					remoteTags := strings.Split(tags, ",")
					uniqueID := uuid.New().String()
					e.InvalidateTags(remoteTags, senderID+"|"+uniqueID, "remote_update") // TODO: forward execID and actionName from the sender for better profiling
				}
			}
		}
	}()
}

func (e *Engine) startScheduler(ctx context.Context) {
	slog.Debug("Starting scheduler")
	ticker := time.NewTicker(SCHEDULE_LOOP_INTERVAL)
	go func() {
		for {
			select {
			case <-ctx.Done():
				slog.Debug("Scheduler stopped")
				return
			case <-ticker.C:
				e.pollScheduledTasks()
			}
		}
	}()
}

func (e *Engine) UseStorage(storage storage.StorageAdapter) {
	e.storage = storage
	e.CreateTable("_tether_storage", []TetherStorage{})
	e.CreateTable("_tether_download_tokens", []TetherDownloadToken{})
}

func (e *Engine) pollScheduledTasks() {
	lookAhead := time.Now().Add(SCHEDULE_LOOKAHEAD)
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
			"claimed_by":   e.EphemeralID,
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

		timer := time.AfterFunc(delay, func() {
			defer func() {
				if err := recover(); err != nil {
					slog.Error("Failed to execute scheduled task", "taskID", t.ID, "error", err)
				}
				if t.IsCron && t.CronString != nil && *t.CronString != "" {
					nextTime := calculateNextCronTime(*t.CronString, time.Now())
					e.db.Model(&TetherTask{}).Where("id = ?", t.ID).Updates(map[string]interface{}{
						"execute_at":    nextTime,
						"claimed_by":    nil,
						"locked_until":  nil,
						"last_executed": time.Now(),
					})
				} else {
					e.db.Delete(&TetherTask{}, "id = ?", t.ID)
				}
			}()
			_, err := e.ExecuteMutationInternal(t.FunctionName, params)
			if err != nil {
				slog.Error("Failed to execute scheduled task", "taskID", t.ID, "error", err)
			}
		})
		e.timerMutex.Lock()
		e.taskToTimer[t.ID] = timer
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

// snapshotOldTrackedTags loads the rows about to be updated and stashes their
// tracked-field tags on the statement. GORM does not expose a before-image in
// update callbacks, so this extra SELECT (same transaction, hooks skipped) is
// what lets a collection move invalidate both the old and new collections.
func snapshotOldTrackedTags(tx *gorm.DB) {
	if tx.Error != nil || tx.DryRun || tx.Statement == nil || tx.Statement.Schema == nil {
		return
	}
	if !hasTrackedFields(tx) {
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

// restrictToUpdatingRows copies the update's WHERE and/or primary keys onto q.
// Save() does not attach the PK to WHERE until gorm:update itself, so we also
// read PKs from Dest and Model. Returns false if we cannot identify rows
// without scanning the whole table.
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

func (e *Engine) SetAuth(auth Auth) {
	e.auth = auth
}

func (e *Engine) RegisterMutation(name string, mutation func(ctx *MutationCtx) interface{}, opts ...MutationOptions) {
	options := MutationOptions{
		Internal: false,
	}
	if len(opts) > 0 {
		options = opts[0]
	}
	e.mutations[name] = Mutation{Func: mutation, Internal: options.Internal} // stores the mutation in the list of valid mutations
	slog.Debug("Registered mutation", "name", name)
}

func (e *Engine) RegisterQuery(name string, query func(ctx *QueryCtx) interface{}, opts ...QueryOptions) {
	options := QueryOptions{
		Internal: false,
	}
	if len(opts) > 0 {
		options = opts[0]
	}
	e.queries[name] = Query{Func: query, Internal: options.Internal} // stores the query in the list of valid queries
	slog.Debug("Registered query", "name", name)
}

func (e *Engine) RegisterGuard(name string, guard func(ctx *GuardCtx) interface{}) {
	e.guards[name] = Guard{Func: guard}
	slog.Debug("Registered guard", "name", name)
}

func (e *Engine) CreateTable(schema interface{}) {
	e.db.AutoMigrate(schema)
}

func (e *Engine) Handle(w http.ResponseWriter, r *http.Request) {
	reactivity.Handle(w, r, e, e.tracker, e.websocketHelper) // wraps the raw websocket connection with the engine handler
}

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
	if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/storage/upload/") {
		token := filepath.Base(r.URL.Path)

		var record TetherStorage
		err := e.db.Where("token = ? AND status = 'pending' AND expires_at > ?", token, time.Now()).First(&record).Error
		if err != nil {
			http.Error(w, "Not found", http.StatusNotFound)
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
			return
		}
		e.db.Model(&record).Updates(map[string]interface{}{
			"status":    "active",
			"mime_type": contentType,
			"file_size": r.ContentLength,
		})
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/storage/file/") {
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
		if record.MimeType != "" {
			w.Header().Set("Content-Type", record.MimeType)

			if !isSafeInlineMime(record.MimeType) {
				w.Header().Set("Content-Disposition", "attachment; filename=\""+record.ID+"\"")
			} else {
				w.Header().Set("Content-Disposition", "inline; filename=\""+record.ID+"\"")
			}
		}

		err = e.storage.ServeFile(dlToken.FileID, w, r)
		if err != nil {
			http.Error(w, "Failed to serve file", http.StatusInternalServerError)
		}
		return
	}
}

// Checks if the mime type is safe to inline in the browser.
func isSafeInlineMime(mime string) bool {
	safePrefixes := []string{"image/", "video/", "audio/", "text/plain"}
	for _, p := range safePrefixes {
		if strings.HasPrefix(mime, p) {
			// Block SVG because it can contain embedded JavaScript
			return mime != "image/svg+xml"
		}
	}
	return mime == "application/pdf"
}

func (e *Engine) getUploadURL(opts storage.UploadOptions) (storage.UploadInfo, error) {
	if e.storage == nil {
		return storage.UploadInfo{}, fmt.Errorf("storage not configured")
	}

	fileID := uuid.NewString()
	token := uuid.NewString()
	maxBytes := opts.MaxBytes
	expiresIn := opts.ExpiresIn
	if maxBytes == 0 {
		maxBytes = 1024 * 1024 * 20 // 20MB
	}
	if expiresIn == 0 {
		expiresIn = time.Minute * 15 // 15 minutes
	}

	err := e.db.Create(&TetherStorage{
		ID:        fileID,
		Token:     token,
		Status:    "pending",
		MaxBytes:  maxBytes,
		ExpiresAt: time.Now().Add(expiresIn),
	}).Error
	if err != nil {
		return storage.UploadInfo{}, err
	}
	return storage.UploadInfo{
		FileID:    fileID,
		UploadURL: "/storage/upload/" + token,
	}, nil
}

func (e *Engine) getDownloadURL(fileID string) (string, error) {
	if e.storage == nil {
		return "", fmt.Errorf("storage not configured")
	}

	token := uuid.NewString()
	expiresIn := time.Minute * 15 // 15 minutes

	err := e.db.Create(&TetherDownloadToken{
		Token:     token,
		FileID:    fileID,
		ExpiresAt: time.Now().Add(expiresIn),
	}).Error
	if err != nil {
		return "", err
	}
	return "/storage/file/" + token, nil
}

func (e *Engine) deleteFile(fileID string) error {
	if e.storage == nil {
		return fmt.Errorf("storage not configured")
	}
	err := e.db.Where("id = ?", fileID).Delete(&TetherStorage{}).Error
	if err != nil {
		return err
	}
	return e.storage.Delete(fileID)
}

// allowStorageOrigin applies the WebSocket origin policy to browser calls against
// storage routes. Requests with no Origin header are allowed so non-browser
// clients can still use an upload or download token. An allowed browser origin
// receives the CORS headers a cross-origin upload or download needs.
func (e *Engine) allowStorageOrigin(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	if !originHeaderOK(origin) || !e.originPermitted(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return false
	}
	header := w.Header()
	header.Set("Access-Control-Allow-Origin", origin)
	header.Add("Vary", "Origin")
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

func (e *Engine) OnConnect(clientID string) error {
	slog.Debug("Connected to websocket", "client", clientID)
	// TODO: implement the logic to handle the connection
	return nil
}

func (e *Engine) OnDisconnect(clientID string) error {
	slog.Debug("Disconnected from websocket", "client", clientID)
	// TODO: implement the logic to handle the disconnection
	return nil
}

func (e *Engine) InvalidateTag(tag string) {
	e.InvalidateTags([]string{tag}, "legacy", "legacy_invalidate_tag")
}

// InvalidateTags re-runs each distinct subscription that listens to any of the
// given tags. Subscriptions are snapshotted before any query executes so a
// re-run that picks up additional tags (e.g. auto-tracked primary keys) cannot
// cause the same mutation to fire the query a second time. Matching
// subscriptions (same query, params, and auth fingerprint) share one execution
// and are fanned out with each client's own query_key.
func (e *Engine) InvalidateTags(tags []string, execID string, actionName string) {
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
	batchedExecutions := make(map[string][]*reactivity.Subscription)
	for _, subscription := range unique {
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
			representative := subscriptions[0]
			result, deps, cacheKey, err := e.runQuery(representative.Query, representative.Params, representative)
			if err != nil {
				slog.Error("Failed to execute query", "error", err)
				return
			}
			e.Profiler.Add(utilities.Metric{
				ID:       execID,
				Name:     "batch_execution:" + representative.Query,
				Type:     utilities.MetricTypeRouting,
				Time:     start,
				Duration: time.Since(start),
				Value:    len(subscriptions),
			})
			// Apply the same dependency set to every subscription in the batch so
			// auto-tracked tags (e.g. new primary keys) stay in sync even though
			// the query function ran only once.
			for _, subscription := range subscriptions {
				e.tracker.UpdateTags(subscription.SubID, deps)
				responseJSON, err := marshalQueryMessage(representative.Query, result, subscription.QueryKey)
				if err != nil {
					slog.Error("Failed to encode query result", "error", err)
					continue
				}
				e.tracker.SendMessage(subscription.Client.ID, responseJSON)
			}
			if dataJSON, err := json.Marshal(result); err == nil {
				e.hashMu.Lock()
				e.queryHashes[cacheKey] = xxhash.Sum64(dataJSON)
				e.hashMu.Unlock()
			}
		}(subscriptions)
	}
	wg.Wait()
	e.Profiler.Add(utilities.Metric{
		ID:       execID,
		Name:     "invalidate_tags:" + actionName,
		Type:     utilities.MetricTypeRouting,
		Time:     start,
		Duration: time.Since(start),
		Tags:     tags,
	})
}

func marshalQueryMessage(query string, result interface{}, queryKey string) ([]byte, error) {
	return json.Marshal(map[string]interface{}{"type": "query", "location": query, "data": result, "query_key": queryKey})
}

// runQuery executes the query function and returns the result plus the
// dependency tags it collected. It does not push to clients or update
// subscription tags; callers decide how to fan those out.
func (e *Engine) runQuery(query string, params map[string]interface{}, subscription *reactivity.Subscription) (interface{}, []string, string, error) {
	if _, exists := e.queries[query]; !exists {
		return nil, nil, "", fmt.Errorf("query not found")
	}
	if e.queries[query].Internal {
		return nil, nil, "", fmt.Errorf("query not found") // return non-descriptive error to prevent enumeration
	}
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, nil, "", err
	}
	auth, ok := e.tracker.GetAuth(subscription.Client.ID)
	if !ok {
		return nil, nil, "", fmt.Errorf("client not found")
	}
	authID := auth.UserID
	cacheKey := query + "?" + string(paramsJSON) + "?" + authID
	slog.Debug("Executing query", "query", query, "params", params)

	// Create the query context first so GetIdentity can append dependency tags.
	queryCtx := &QueryCtx{
		DB:           e.db,
		Params:       params,
		Dependencies: []string{},
	}
	queryCtx.Storage = &StorageCtx{
		GetUploadURL:   e.getUploadURL,
		GetDownloadURL: e.getDownloadURL,
		DeleteFile:     e.deleteFile,
	}
	queryCtx.Scheduler = &SchedulerCtx{
		RunAfter: func(duration time.Duration, functionName string, params map[string]interface{}) (string, error) {
			return e.scheduleTask(time.Now().Add(duration), functionName, params)
		},
		Cancel: func(taskID string) bool {
			return e.cancelTask(taskID)
		},
	}
	queryCtx.Auth = &AuthCtx{
		GetIdentity: func() (string, error) {
			return getIdentity(&queryCtx.Dependencies, authID)
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
			e.tracker.AttachGuardToSubscription(subscription.SubID, guardID, guardName, params)
			e.tracker.UpdateTags(guardID, guardCtx.Dependencies)
			resultJSON, err := json.Marshal(result)
			if err != nil {
				return nil, err
			}
			queryCtx.Dependencies = append(queryCtx.Dependencies, fmt.Sprintf("*guard_%s_%s:%s", guardName, paramsHash, resultJSON))
			return result, nil
		},
	}
	execID := uuid.NewString()

	// Create the GORM context
	gormCtx := context.WithValue(context.Background(), tetherCtxKey, queryCtx)
	gormCtx = context.WithValue(gormCtx, ContextKeyExecutionID, execID)
	gormCtx = context.WithValue(gormCtx, ContextKeyActionName, query)
	queryCtx.DB = e.db.WithContext(gormCtx)

	start := time.Now()
	result := e.queries[query].Func(queryCtx)
	e.Profiler.Add(utilities.Metric{
		ID:       execID,
		Name:     "query:" + query,
		Type:     utilities.MetricTypeQuery,
		Time:     start,
		Duration: time.Since(start),
		Tags:     []string{},
	})
	return result, queryCtx.Dependencies, cacheKey, nil
}

func (e *Engine) ExecuteQuery(query string, params map[string]interface{}, subscription *reactivity.Subscription, forceSend bool) ([]byte, error) {
	result, deps, cacheKey, err := e.runQuery(query, params, subscription)
	if err != nil {
		return nil, err
	}

	e.tracker.UpdateTags(subscription.SubID, deps)
	slog.Debug("Updated dependencies on subscription", "subID", subscription.SubID, "dependencies", deps)

	e.hashMu.Lock()
	lastHash := e.queryHashes[cacheKey]
	e.hashMu.Unlock()

	responseJSON, err := marshalQueryMessage(query, result, subscription.QueryKey)
	if err != nil {
		return nil, err
	}
	dataJSON, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	queryHash := xxhash.Sum64(dataJSON)
	if lastHash == queryHash && !forceSend { // we want to force send on first subscription, regardless of if the query hasn't changed
		return responseJSON, nil
	}

	e.hashMu.Lock()
	e.queryHashes[cacheKey] = queryHash
	e.hashMu.Unlock()

	e.tracker.SendMessage(subscription.Client.ID, responseJSON)
	return responseJSON, nil
}

func (e *Engine) ExecuteMutationInternal(mutation string, params map[string]interface{}) (interface{}, error) {
	if _, exists := e.mutations[mutation]; !exists {
		return nil, fmt.Errorf("mutation not found")
	}
	authCtx := &AuthCtx{
		GetIdentity: func() (string, error) { panic("tether: mutations with auth cannot be executed internally") },
		ExecuteGuard: func(guardName string, params map[string]interface{}) (interface{}, error) {
			return nil, fmt.Errorf("guards cannot be executed internally")
		},
	}
	mutationCtx := &MutationCtx{DB: e.db, Storage: &StorageCtx{
		GetUploadURL:   e.getUploadURL,
		GetDownloadURL: e.getDownloadURL,
		DeleteFile:     e.deleteFile,
	}, Auth: authCtx, Params: params, Profiler: e.Profiler, Scheduler: &SchedulerCtx{
		RunAfter: func(duration time.Duration, functionName string, params map[string]interface{}) (string, error) {
			return e.scheduleTask(time.Now().Add(duration), functionName, params)
		},
		Cancel: func(taskID string) bool {
			return e.cancelTask(taskID)
		},
	}}
	execID := uuid.NewString()
	start := time.Now()
	result := e.mutations[mutation].Func(mutationCtx)
	e.Profiler.Add(utilities.Metric{
		ID:       execID,
		Name:     "mutation:" + mutation,
		Type:     utilities.MetricTypeMutation,
		Time:     start,
		Duration: time.Since(start),
		Tags:     []string{},
	})
	slog.Debug("Executing mutation internally", "mutation", mutation, "params", params, "result", result)
	return result, nil
}

func (e *Engine) ExecuteMutation(mutation string, params map[string]interface{}, clientID string, mutationID string) (interface{}, error) {
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
		guardCtx := &GuardCtx{
			DB:       e.db,
			Auth:     authCtx,
			Params:   params,
			Profiler: e.Profiler,
		}
		result, err := executeGuard(e, guardCtx, guardName)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	mutationCtx := &MutationCtx{DB: scopedDB, Storage: &StorageCtx{
		GetUploadURL:   e.getUploadURL,
		GetDownloadURL: e.getDownloadURL,
		DeleteFile:     e.deleteFile,
	}, Auth: authCtx, Params: params, Scheduler: &SchedulerCtx{
		RunAfter: func(duration time.Duration, functionName string, params map[string]interface{}) (string, error) {
			return e.scheduleTask(time.Now().Add(duration), functionName, params)
		},
		Cancel: func(taskID string) bool {
			return e.cancelTask(taskID)
		},
	}}
	start := time.Now()
	result := e.mutations[mutation].Func(mutationCtx)
	e.Profiler.Add(utilities.Metric{
		ID:       execID,
		Name:     "mutation:" + mutation,
		Type:     utilities.MetricTypeMutation,
		Time:     start,
		Duration: time.Since(start),
		Tags:     []string{},
	})
	slog.Debug("Executing mutation", "mutation", mutation, "params", params, "result", result)
	responseJSON, err := json.Marshal(map[string]interface{}{"type": "mutation", "location": mutation, "data": result, "mutation_id": mutationID})
	if err != nil {
		slog.Error("Failed to encode mutation result", "mutation", mutation, "error", err)
		return nil, err
	}
	e.tracker.SendMessage(clientID, responseJSON)
	return result, nil
}

// rerunSubscriptions force-pushes fresh results for subscriptions whose
// authorization state was reset by an identity change.
func (e *Engine) rerunSubscriptions(subscriptions []*reactivity.Subscription) {
	for _, subscription := range subscriptions {
		if _, err := e.ExecuteQuery(subscription.Query, subscription.Params, subscription, true); err != nil {
			slog.Error("Failed to re-run query after auth change", "query", subscription.Query, "error", err)
		}
	}
}

func (e *Engine) OnReceiveMessage(clientID string, msg map[string]interface{}) error {
	slog.Debug("Received message", "from", clientID, "message", msg)
	switch msg["type"] {
	case "subscribe":
		query, ok := msg["location"].(string)
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		params, ok := msg["params"].(map[string]interface{})
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		queryKey, ok := msg["query_key"].(string)
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		subscription := e.tracker.SubscribeToQuery(clientID, query, queryKey, params)
		if subscription == nil {
			slog.Error("Failed to subscribe to query", "query", query, "params", params)
			return nil
		}
		e.ExecuteQuery(query, params, subscription, true)
	case "unsubscribe":
		query, ok := msg["location"].(string)
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		params, ok := msg["params"].(map[string]interface{})
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		e.tracker.UnsubscribeFromQuery(clientID, query, params)
	case "mutation":
		mutation, ok := msg["location"].(string)
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		params, ok := msg["params"].(map[string]interface{})
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		mutationID, ok := msg["mutation_id"].(string)
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		e.ExecuteMutation(mutation, params, clientID, mutationID)
	case "auth":
		token, ok := msg["token"].(string)
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		start := time.Now()
		execID := uuid.NewString()
		userID, expiresAt, err := e.auth.VerifyToken(e.db, token)
		e.Profiler.Add(utilities.Metric{
			ID:       execID,
			Name:     "authentication:" + token,
			Type:     utilities.MetricTypeAuthentication,
			Time:     start,
			Duration: time.Since(start),
			Tags:     []string{},
		})
		if err != nil {
			slog.Error("Failed to get user ID", "error", err)
			e.tracker.SendMessage(clientID, []byte(`{"type": "error", "error": "Failed to get user ID"}`))
			return err
		}
		e.rerunSubscriptions(e.tracker.SetAuth(clientID, userID, expiresAt))
		time.AfterFunc(time.Until(expiresAt), func() {
			e.rerunSubscriptions(e.tracker.ExpireAuth(clientID, expiresAt))
		})
		message := map[string]interface{}{"type": "auth", "success": true, "data": map[string]interface{}{"user_id": userID}}
		messageJSON, err := json.Marshal(message)
		if err != nil {
			slog.Error("Failed to encode auth message", "error", err)
			e.tracker.SendMessage(clientID, []byte(`{"type": "error", "error": "Failed to encode auth message"}`))
			return err
		}
		e.tracker.SendMessage(clientID, messageJSON)
		return nil
	}
	return nil
}
