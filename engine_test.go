package tether

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cespare/xxhash"
	"github.com/glebarez/sqlite"
	"github.com/gorilla/websocket"
	"github.com/recodeorg/tether/internal/reactivity"
	"github.com/recodeorg/tether/storage"
	"github.com/recodeorg/tether/storage/local"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// postgresTestDSN is the server used when tests are run with -postgres.
const postgresTestDSN = "host=localhost user=postgres password=secret dbname=mydb port=5432 sslmode=disable"

// usePostgres swaps the engine test suite from in-memory SQLite to Postgres.
var usePostgres = flag.Bool("postgres", false, "run the engine test suite against Postgres instead of SQLite")

var postgresSchemaSeq atomic.Uint64

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

type testMessage struct {
	ID     uint   `gorm:"primaryKey"`
	Body   string `gorm:"not null"`
	RoomID string `tether:"track"`
}

func (testMessage) TableName() string { return "messages" }

type testNote struct {
	NoteID uint `gorm:"primaryKey"`
	Body   string
}

func (testNote) TableName() string { return "notes" }

type stubAuth struct {
	userID    string
	expiresAt time.Time
	err       error
	tokens    []string
	sawDB     bool
}

func (a *stubAuth) VerifyToken(ctx context.Context, db *gorm.DB, token string) (string, time.Time, error) {
	a.tokens = append(a.tokens, token)
	a.sawDB = db != nil
	if a.err != nil {
		return "", time.Time{}, a.err
	}
	return a.userID, a.expiresAt, nil
}

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	if *usePostgres {
		return newPostgresTestDB(t)
	}
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

// newPostgresTestDB opens the shared Postgres server and isolates this test in
// its own schema. search_path is a startup parameter, so every pooled connection
// sees the schema.
func newPostgresTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	schema := fmt.Sprintf("tether_%d_%d", os.Getpid(), postgresSchemaSeq.Add(1))
	db, err := gorm.Open(postgres.Open(postgresTestDSN+" search_path="+schema), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	if err := db.Exec("CREATE SCHEMA " + schema).Error; err != nil {
		_ = sqlDB.Close()
		t.Fatalf("create schema %s: %v", schema, err)
	}
	// database/sql's default pool is unbounded. A fresh Postgres often allows
	// about 100 clients, and the concurrent websocket tests will open one
	// connection per in-flight query unless this is capped.
	sqlDB.SetMaxOpenConns(20)
	sqlDB.SetMaxIdleConns(4)
	t.Cleanup(func() {
		_ = sqlDB.Close()
		dropPostgresSchema(schema)
	})
	return db
}

func dropPostgresSchema(schema string) {
	db, err := gorm.Open(postgres.Open(postgresTestDSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return
	}
	sqlDB, err := db.DB()
	if err != nil {
		return
	}
	defer sqlDB.Close()
	_ = db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE").Error
}

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	return newTestEngineWithType(t)
}

func newTestEngineWithType(t *testing.T) *Engine {
	t.Helper()
	e, err := NewEngine(newTestDB(t))
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}
	t.Cleanup(e.Close)
	e.CreateTable(&testMessage{})
	return e
}

func TestRegisterCronUpsertsByName(t *testing.T) {
	e := newTestEngine(t)

	id, err := e.RegisterCron("cleanup", "0 0 1 1 *", "cleanupOld", map[string]interface{}{"days": 7})
	if err != nil {
		t.Fatalf("first register: %v", err)
	}
	executed := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	if err := e.db.Model(&TetherTask{}).Where("id = ?", id).Update("last_executed", executed).Error; err != nil {
		t.Fatalf("set last_executed: %v", err)
	}
	var before TetherTask
	if err := e.db.First(&before, "id = ?", id).Error; err != nil {
		t.Fatalf("load cron: %v", err)
	}

	again, err := e.RegisterCron("cleanup", "15 4 * * *", "cleanupNew", map[string]interface{}{"days": 30})
	if err != nil {
		t.Fatalf("second register: %v", err)
	}
	if again != id {
		t.Fatalf("upsert returned %s, want existing id %s", again, id)
	}

	var crons []TetherTask
	if err := e.db.Where("is_cron = ?", true).Find(&crons).Error; err != nil {
		t.Fatalf("find crons: %v", err)
	}
	if len(crons) != 1 {
		t.Fatalf("cron rows = %d, want 1", len(crons))
	}
	got := crons[0]
	if got.ID != id || got.FunctionName != "cleanupNew" || got.ParamsJSON != `{"days":30}` {
		t.Fatalf("updated cron = %+v", got)
	}
	if got.CronString == nil || *got.CronString != "15 4 * * *" {
		t.Fatalf("cron string = %v", got.CronString)
	}
	if got.LastExecuted == nil || !got.LastExecuted.Equal(executed) {
		t.Fatalf("last_executed = %v, want %v", got.LastExecuted, executed)
	}
	if got.ExecuteAt.Equal(before.ExecuteAt) {
		t.Fatalf("idle cron execute_at stayed %v, want a recalculated time", got.ExecuteAt)
	}

	if _, err := e.RegisterCron("other", "0 0 1 1 *", "otherFn", nil); err != nil {
		t.Fatalf("register other: %v", err)
	}
	var count int64
	if err := e.db.Model(&TetherTask{}).Where("is_cron = ?", true).Count(&count).Error; err != nil {
		t.Fatalf("count crons: %v", err)
	}
	if count != 2 {
		t.Fatalf("cron rows = %d, want 2", count)
	}

	if _, err := e.scheduleTask(time.Now().Add(time.Hour), "later", nil); err != nil {
		t.Fatalf("schedule task: %v", err)
	}
	if _, err := e.scheduleTask(time.Now().Add(2*time.Hour), "later", nil); err != nil {
		t.Fatalf("schedule second task: %v", err)
	}
}

func TestRegisterCronKeepsExecuteAtWhenClaimedOrOverdue(t *testing.T) {
	e := newTestEngine(t)

	id, err := e.RegisterCron("cleanup", "0 0 1 1 *", "cleanupOld", map[string]interface{}{"days": 7})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	claimedBy := "other-instance"
	lockedUntil := time.Now().Add(5 * time.Minute).Truncate(time.Second)
	claimedAt := time.Now().Add(2 * time.Hour).Truncate(time.Second)
	if err := e.db.Model(&TetherTask{}).Where("id = ?", id).Updates(map[string]interface{}{
		"claimed_by":   claimedBy,
		"locked_until": lockedUntil,
		"execute_at":   claimedAt,
	}).Error; err != nil {
		t.Fatalf("claim cron: %v", err)
	}

	if _, err := e.RegisterCron("cleanup", "15 4 * * *", "cleanupNew", map[string]interface{}{"days": 30}); err != nil {
		t.Fatalf("register while claimed: %v", err)
	}
	got := loadCron(t, e, id)
	if !sameSecond(got.ExecuteAt, claimedAt) {
		t.Fatalf("claimed execute_at = %v, want %v", got.ExecuteAt, claimedAt)
	}
	if got.ClaimedBy == nil || *got.ClaimedBy != claimedBy {
		t.Fatalf("claimed_by = %v", got.ClaimedBy)
	}
	if got.LockedUntil == nil || !got.LockedUntil.Equal(lockedUntil) {
		t.Fatalf("locked_until = %v, want %v", got.LockedUntil, lockedUntil)
	}
	if got.CronString == nil || *got.CronString != "15 4 * * *" || got.FunctionName != "cleanupNew" {
		t.Fatalf("claimed cron definition = %+v", got)
	}

	overdueAt := time.Now().Add(-time.Minute).Truncate(time.Second)
	lastExecuted := overdueAt.Add(-time.Hour)
	if err := e.db.Model(&TetherTask{}).Where("id = ?", id).Updates(map[string]interface{}{
		"claimed_by":    nil,
		"locked_until":  nil,
		"execute_at":    overdueAt,
		"last_executed": lastExecuted,
	}).Error; err != nil {
		t.Fatalf("mark overdue: %v", err)
	}
	if _, err := e.RegisterCron("cleanup", "30 5 * * *", "cleanupOverdue", map[string]interface{}{"days": 1}); err != nil {
		t.Fatalf("register while overdue: %v", err)
	}
	got = loadCron(t, e, id)
	if !sameSecond(got.ExecuteAt, overdueAt) {
		t.Fatalf("overdue execute_at = %v, want %v", got.ExecuteAt, overdueAt)
	}
	if got.CronString == nil || *got.CronString != "30 5 * * *" || got.FunctionName != "cleanupOverdue" {
		t.Fatalf("overdue cron definition = %+v", got)
	}
	if got.LastExecuted == nil || !got.LastExecuted.Equal(lastExecuted) {
		t.Fatalf("last_executed = %v, want %v", got.LastExecuted, lastExecuted)
	}

	// A never-run slot that is already due is still owed.
	if err := e.db.Model(&TetherTask{}).Where("id = ?", id).Updates(map[string]interface{}{
		"execute_at":    overdueAt,
		"last_executed": nil,
	}).Error; err != nil {
		t.Fatalf("clear last_executed: %v", err)
	}
	if _, err := e.RegisterCron("cleanup", "45 6 * * *", "cleanupNeverRun", nil); err != nil {
		t.Fatalf("register never-run overdue: %v", err)
	}
	got = loadCron(t, e, id)
	if !sameSecond(got.ExecuteAt, overdueAt) {
		t.Fatalf("never-run execute_at = %v, want %v", got.ExecuteAt, overdueAt)
	}

	// An expired lease is not a current claim, so a future slot can move.
	expired := time.Now().Add(-time.Minute).Truncate(time.Second)
	future := time.Now().Add(3 * time.Hour).Truncate(time.Second)
	if err := e.db.Model(&TetherTask{}).Where("id = ?", id).Updates(map[string]interface{}{
		"claimed_by":   claimedBy,
		"locked_until": expired,
		"execute_at":   future,
	}).Error; err != nil {
		t.Fatalf("expire claim: %v", err)
	}
	if _, err := e.RegisterCron("cleanup", "0 7 * * *", "cleanupExpired", nil); err != nil {
		t.Fatalf("register after expired claim: %v", err)
	}
	got = loadCron(t, e, id)
	if sameSecond(got.ExecuteAt, future) {
		t.Fatalf("expired claim kept execute_at %v", got.ExecuteAt)
	}
	if got.ClaimedBy == nil || *got.ClaimedBy != claimedBy {
		t.Fatalf("expired claim cleared claimed_by: %v", got.ClaimedBy)
	}
}

// Completed one-shot tasks must drop their taskToTimer entries. The map is the
// only thing keeping each fired time.Timer reachable, so a leftover entry leaks
// the timer and the callback that closed over the task.
func TestCompletedOneShotTasksReleaseTaskTimers(t *testing.T) {
	// Timers fire on other goroutines. A shared file keeps every connection on
	// the same database; :memory: would give each connection an empty schema.
	e := newConcurrentTestEngine(t)
	var ran atomic.Int32
	e.RegisterMutation("scheduledTick", func(ctx *MutationCtx) (any, error) {
		ran.Add(1)
		return nil, nil
	})

	const n = 25
	var want int32

	// Due immediately: scheduleTask arms the timer itself.
	for i := 0; i < n; i++ {
		if _, err := e.scheduleTask(time.Now(), "scheduledTick", nil); err != nil {
			t.Fatalf("schedule immediate task: %v", err)
		}
		want++
	}

	// Due later, then pulled overdue so pollScheduledTasks arms a zero-delay timer.
	overdueIDs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id, err := e.scheduleTask(time.Now().Add(time.Hour), "scheduledTick", nil)
		if err != nil {
			t.Fatalf("schedule overdue task: %v", err)
		}
		overdueIDs = append(overdueIDs, id)
		want++
	}
	if err := e.db.Model(&TetherTask{}).Where("id IN ?", overdueIDs).Update("execute_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatalf("backdate overdue tasks: %v", err)
	}

	// Still ahead of now, but inside the poll lookahead, so the timer waits out a real delay.
	delayedID, err := e.scheduleTask(time.Now().Add(time.Hour), "scheduledTick", nil)
	if err != nil {
		t.Fatalf("schedule delayed task: %v", err)
	}
	want++
	if err := e.db.Model(&TetherTask{}).Where("id = ?", delayedID).Update("execute_at", time.Now().Add(150*time.Millisecond)).Error; err != nil {
		t.Fatalf("set delayed execute_at: %v", err)
	}

	e.pollScheduledTasks()

	deadline := time.Now().Add(3 * time.Second)
	for {
		e.timerMutex.RLock()
		left := len(e.taskToTimer)
		e.timerMutex.RUnlock()
		if ran.Load() >= want && left == 0 {
			return
		}
		if time.Now().After(deadline) {
			e.timerMutex.RLock()
			ids := make([]string, 0, len(e.taskToTimer))
			for id := range e.taskToTimer {
				ids = append(ids, id)
			}
			e.timerMutex.RUnlock()
			t.Fatalf("one-shot timers still tracked after tasks finished: ran %d/%d, remaining %v", ran.Load(), want, ids)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A mutation due inside the schedule lookahead is armed with time.AfterFunc
// on this process. The callback has to recover: an unrecovered panic there
// crashes the process, and the task row plus timer entry must still be released.
func TestNearTermScheduledTaskPanicIsRecovered(t *testing.T) {
	if os.Getenv("TETHER_TEST_CHILD") == "1" {
		e := newConcurrentTestEngine(t)
		e.RegisterMutation("boom", func(ctx *MutationCtx) (any, error) {
			panic("scheduled boom")
		})
		id, err := e.scheduleTask(time.Now(), "boom", nil)
		if err != nil {
			t.Fatalf("schedule panicking task: %v", err)
		}
		if !waitUntil(t, 2*time.Second, func() bool {
			e.timerMutex.RLock()
			_, tracked := e.taskToTimer[id]
			e.timerMutex.RUnlock()
			if tracked {
				return false
			}
			var n int64
			e.db.Model(&TetherTask{}).Where("id = ?", id).Count(&n)
			return n == 0
		}) {
			t.Fatal("panicking scheduled task was not cleaned up")
		}

		var ran atomic.Bool
		e.RegisterMutation("ok", func(ctx *MutationCtx) (any, error) {
			ran.Store(true)
			return nil, nil
		})
		if _, err := e.scheduleTask(time.Now(), "ok", nil); err != nil {
			t.Fatalf("schedule follow-up task: %v", err)
		}
		if !waitUntil(t, 2*time.Second, func() bool { return ran.Load() }) {
			t.Fatal("later scheduled task did not run after the panic")
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestNearTermScheduledTaskPanicIsRecovered$")
	cmd.Env = append(os.Environ(), "TETHER_TEST_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("near-term scheduled task panic crashed the process: %v\n%s", err, out)
	}
}

// A failed insert must not hand back the id scheduleTask generated locally.
// Tasks inside the lookahead are inserted and armed here; later tasks wait for
// the schedule loop. Both paths have to fail when the database is closed.
func TestScheduleTaskClosedDatabaseReturnsNoID(t *testing.T) {
	e := newTestEngine(t)
	sqlDB, err := e.db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	for _, when := range []time.Time{
		time.Now(),
		time.Now().Add(time.Hour),
	} {
		id, err := e.scheduleTask(when, "later", nil)
		if err == nil {
			t.Fatalf("schedule at %s with closed db returned nil error", when.Format(time.RFC3339))
		}
		if id != "" {
			t.Fatalf("schedule at %s with closed db returned task id %q", when.Format(time.RFC3339), id)
		}
	}
}

func loadCron(t *testing.T, e *Engine, id string) TetherTask {
	t.Helper()
	var got TetherTask
	if err := e.db.First(&got, "id = ?", id).Error; err != nil {
		t.Fatalf("load cron: %v", err)
	}
	return got
}

func sameSecond(a, b time.Time) bool {
	return a.Truncate(time.Second).Equal(b.Truncate(time.Second))
}

func trackClient(t *testing.T, e *Engine) *reactivity.Client {
	t.Helper()
	client := reactivity.NewClient(nil)
	e.tracker.Track(client)
	return client
}

func subscribe(t *testing.T, e *Engine, client *reactivity.Client, query, queryKey string, params map[string]interface{}) *reactivity.Subscription {
	t.Helper()
	if params == nil {
		params = map[string]interface{}{}
	}
	sub := e.tracker.SubscribeToQuery(client.ID, query, queryKey, params)
	if sub == nil {
		t.Fatal("SubscribeToQuery returned nil")
	}
	if _, err := e.executeQuery(query, params, sub); err != nil {
		t.Fatalf("ExecuteQuery(%q): %v", query, err)
	}
	return sub
}

func drain(c *reactivity.Client) []map[string]interface{} {
	var out []map[string]interface{}
	for {
		select {
		case raw := <-c.Send:
			var msg map[string]interface{}
			if err := json.Unmarshal(raw, &msg); err != nil {
				out = append(out, map[string]interface{}{"_raw": string(raw), "_error": err.Error()})
				continue
			}
			out = append(out, msg)
		default:
			return out
		}
	}
}

func queryMessages(t *testing.T, msgs []map[string]interface{}) []map[string]interface{} {
	t.Helper()
	var out []map[string]interface{}
	for _, msg := range msgs {
		if msg["type"] == "query" {
			out = append(out, msg)
		}
	}
	return out
}

func hasSubscription(subs []*reactivity.Subscription, subID string) bool {
	for _, sub := range subs {
		if sub != nil && sub.SubID == subID {
			return true
		}
	}
	return false
}

func waitUntil(t *testing.T, d time.Duration, pred func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func mustNoPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s panicked: %v", name, r)
		}
	}()
	fn()
}

func captureMutationTags(t *testing.T, db *gorm.DB) *[]string {
	t.Helper()
	var tags []string
	hook := func(tx *gorm.DB) {
		tags = extractMutationTags(tx)
	}
	prefix := fmt.Sprintf("test_capture_%s", t.Name())
	if err := db.Callback().Create().After("tether:after_create").Register(prefix+"_c", hook); err != nil {
		t.Fatalf("register create capture: %v", err)
	}
	if err := db.Callback().Update().After("tether:after_update").Register(prefix+"_u", hook); err != nil {
		t.Fatalf("register update capture: %v", err)
	}
	if err := db.Callback().Delete().After("tether:after_delete").Register(prefix+"_d", hook); err != nil {
		t.Fatalf("register delete capture: %v", err)
	}
	t.Cleanup(func() {
		db.Callback().Create().Remove(prefix + "_c")
		db.Callback().Update().Remove(prefix + "_u")
		db.Callback().Delete().Remove(prefix + "_d")
	})
	return &tags
}

func TestNewEngineAcceptsSQLiteAndPostgres(t *testing.T) {
	for _, dbType := range []string{"sqlite", "postgres"} {
		t.Run(dbType, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("NewEngine(%q) panicked: %v", dbType, r)
				}
			}()
			e, err := NewEngine(newTestDB(t))
			if err != nil {
				t.Fatalf("create engine: %v", err)
			}
			if e != nil {
				t.Cleanup(e.Close)
			}
		})
	}
}

func TestCloseStopsBackgroundLoops(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	e.SetStorage(store, "")

	done := make(chan struct{})
	go func() {
		e.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close blocked")
	}
	if err := e.ctx.Err(); err == nil {
		t.Fatal("expected engine context to be cancelled")
	}
	e.Close()
}

// Close must stop timers armed by scheduleTask and by the schedule loop, keep
// their rows, and release this instance's claims so another instance or the
// next start runs them.
func TestCloseStopsScheduledTaskTimers(t *testing.T) {
	e := newConcurrentTestEngine(t)
	var ran atomic.Int32
	e.RegisterMutation("tick", func(ctx *MutationCtx) (any, error) {
		ran.Add(1)
		return nil, nil
	})

	// Long enough that neither timer fires before Close on a remote database.
	const delay = time.Second
	due := time.Now().Add(delay)
	direct, err := e.scheduleTask(due, "tick", nil)
	if err != nil {
		t.Fatalf("schedule direct task: %v", err)
	}
	polled, err := e.scheduleTask(time.Now().Add(time.Hour), "tick", nil)
	if err != nil {
		t.Fatalf("schedule polled task: %v", err)
	}
	if err := e.db.Model(&TetherTask{}).Where("id = ?", polled).Update("execute_at", due).Error; err != nil {
		t.Fatalf("move polled task into lookahead: %v", err)
	}
	e.pollScheduledTasks()
	e.timerMutex.RLock()
	armed := len(e.taskToTimer)
	e.timerMutex.RUnlock()
	if armed != 2 {
		t.Fatalf("armed timers = %d, want 2", armed)
	}

	e.Close()
	time.Sleep(time.Until(due) + 300*time.Millisecond)
	if n := ran.Load(); n != 0 {
		t.Fatalf("%d scheduled mutations ran after Close", n)
	}
	e.timerMutex.RLock()
	left := len(e.taskToTimer)
	e.timerMutex.RUnlock()
	if left != 0 {
		t.Fatalf("taskToTimer still holds %d timers after Close", left)
	}

	var tasks []TetherTask
	if err := e.db.Where("id IN ?", []string{direct, polled}).Find(&tasks).Error; err != nil {
		t.Fatalf("load tasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("Close removed persisted tasks: have %d, want 2", len(tasks))
	}
	for _, task := range tasks {
		if task.ClaimedBy != nil || task.LockedUntil != nil {
			t.Errorf("task %s still claimed after Close: claimed_by=%v locked_until=%v", task.ID, task.ClaimedBy, task.LockedUntil)
		}
	}

	if id, err := e.scheduleTask(time.Now(), "tick", nil); !errors.Is(err, ErrEngineClosed) || id != "" {
		t.Fatalf("scheduleTask after Close = (%q, %v), want ErrEngineClosed", id, err)
	}
}

// A scheduled mutation already running when Close is called must finish
// before Close returns, and its row is then removed as usual.
func TestCloseWaitsForRunningScheduledTask(t *testing.T) {
	e := newConcurrentTestEngine(t)
	started := make(chan struct{})
	release := make(chan struct{})
	e.RegisterMutation("slow", func(ctx *MutationCtx) (any, error) {
		close(started)
		<-release
		return nil, nil
	})
	id, err := e.scheduleTask(time.Now(), "slow", nil)
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduled mutation did not start")
	}

	done := make(chan struct{})
	go func() {
		e.Close()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("Close returned while a scheduled mutation was still running")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the scheduled mutation finished")
	}

	var n int64
	e.db.Model(&TetherTask{}).Where("id = ?", id).Count(&n)
	if n != 0 {
		t.Fatal("completed task row was not removed")
	}
}

func TestCloseStopsAuthExpiryTimers(t *testing.T) {
	e := newConcurrentTestEngine(t)
	client := trackClient(t, e)
	expiresAt := time.Now().Add(500 * time.Millisecond)
	e.SetAuth(&stubAuth{userID: "temp", expiresAt: expiresAt})
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
		t.Fatalf("auth: %v", err)
	}

	e.Close()
	time.Sleep(time.Until(expiresAt) + 200*time.Millisecond)
	auth, ok := e.tracker.GetAuth(client.ID)
	if !ok {
		t.Fatal("client no longer tracked")
	}
	if auth.UserID != "temp" {
		t.Fatalf("auth expiry ran after Close: user = %q", auth.UserID)
	}
	if auth.ExpiryTimer == nil {
		return
	}
	if auth.ExpiryTimer.Stop() {
		t.Fatal("auth expiry timer was still pending after Close")
	}
}

// Re-authenticating and disconnecting must each stop the previous expiry
// timer instead of leaving it to fire.
func TestAuthExpiryTimerReplacedAndStoppedOnUntrack(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.SetAuth(&stubAuth{userID: "temp", expiresAt: time.Now().Add(time.Hour)})
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
		t.Fatalf("auth: %v", err)
	}
	first, _ := e.tracker.GetAuth(client.ID)
	if first.ExpiryTimer == nil {
		t.Fatal("auth did not arm an expiry timer")
	}
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
		t.Fatalf("re-auth: %v", err)
	}
	second, _ := e.tracker.GetAuth(client.ID)
	if first.ExpiryTimer.Stop() {
		t.Fatal("re-auth left the previous expiry timer pending")
	}
	e.tracker.Untrack(client)
	if second.ExpiryTimer.Stop() {
		t.Fatal("Untrack left the expiry timer pending")
	}
}

func TestCloseStopsProfilerFlush(t *testing.T) {
	e := newConcurrentTestEngine(t)
	var flushes atomic.Int32
	e.RegisterMutation("flush", func(ctx *MutationCtx) (any, error) {
		flushes.Add(1)
		return nil, nil
	})
	if err := e.Profiler().StartWithCallback(10*time.Millisecond, "flush"); err != nil {
		t.Fatalf("start profiler: %v", err)
	}
	if !waitUntil(t, 2*time.Second, func() bool { return flushes.Load() > 0 }) {
		t.Fatal("profiler never flushed")
	}

	e.Close()
	if e.Profiler().IsActive() {
		t.Fatal("profiler still active after Close")
	}
	after := flushes.Load()
	time.Sleep(100 * time.Millisecond)
	if n := flushes.Load(); n != after {
		t.Fatalf("profiler flushed %d more times after Close", n-after)
	}
}
func TestTrackCollectionAndTrackTableTagFormat(t *testing.T) {
	ctx := &QueryCtx{}
	ctx.TrackCollection("messages", "room_id", "lobby")
	ctx.TrackCollection("messages", "room_id", 5)
	ctx.TrackTable("messages")

	want := []string{"messages_room_id:lobby", "messages_room_id:5", "table_messages:mutated"}
	if !slices.Equal(ctx.dependencies, want) {
		t.Errorf("dependencies = %v, want %v", ctx.dependencies, want)
	}

	guard := &GuardCtx{}
	guard.TrackCollection("room_members", "user_id", "user-7")
	guard.TrackTable("room_members")
	wantGuard := []string{"room_members_user_id:user-7", "table_room_members:mutated"}
	if !slices.Equal(guard.dependencies, wantGuard) {
		t.Errorf("GuardCtx.dependencies = %v, want %v", guard.dependencies, wantGuard)
	}
}

func TestExtractMutationTagsFromCreate(t *testing.T) {
	e := newTestEngine(t)
	tags := captureMutationTags(t, e.db)
	msg := testMessage{Body: "hello", RoomID: "lobby"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}

	wantPK := fmt.Sprintf("messages:%v", msg.ID)
	wantCol := "messages_room_id:lobby"
	if !slices.Contains(*tags, wantPK) {
		t.Errorf("tags = %v, missing primary key tag %q", *tags, wantPK)
	}
	if !slices.Contains(*tags, wantCol) {
		t.Errorf("tags = %v, missing collection tag %q", *tags, wantCol)
	}
}

func TestExtractMutationTagsFromBatchCreate(t *testing.T) {
	e := newTestEngine(t)
	tags := captureMutationTags(t, e.db)
	msgs := []testMessage{
		{Body: "a", RoomID: "r1"},
		{Body: "b", RoomID: "r2"},
	}
	if err := e.db.Create(&msgs).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}

	for _, msg := range msgs {
		wantPK := fmt.Sprintf("messages:%v", msg.ID)
		wantCol := "messages_room_id:" + msg.RoomID
		if !slices.Contains(*tags, wantPK) {
			t.Errorf("tags = %v, missing %q", *tags, wantPK)
		}
		if !slices.Contains(*tags, wantCol) {
			t.Errorf("tags = %v, missing %q", *tags, wantCol)
		}
	}
}

func TestExtractMutationTagsSkipsZeroValues(t *testing.T) {
	e := newTestEngine(t)
	tags := captureMutationTags(t, e.db)
	msg := testMessage{Body: "no-room", RoomID: ""}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}

	if slices.Contains(*tags, "messages_room_id:") {
		t.Errorf("tags = %v, unexpectedly included zero-value collection tag", *tags)
	}
	if !slices.Contains(*tags, fmt.Sprintf("messages:%v", msg.ID)) {
		t.Errorf("tags = %v, missing primary key tag", *tags)
	}
}

func TestExtractMutationTagsFromMapUpdate(t *testing.T) {
	e := newTestEngine(t)
	tags := captureMutationTags(t, e.db)
	msg := testMessage{Body: "old", RoomID: "lobby"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := e.db.Model(&testMessage{}).Where("id = ?", msg.ID).Updates(map[string]interface{}{"body": "patched"}).Error; err != nil {
		t.Fatalf("Updates: %v", err)
	}
	wantPK := fmt.Sprintf("messages:%v", msg.ID)
	if !slices.Contains(*tags, wantPK) {
		t.Errorf("tags = %v, missing primary key tag %q", *tags, wantPK)
	}
}

func TestExtractMutationTagsFromSaveIncludesOldAndNewCollection(t *testing.T) {
	e := newTestEngine(t)
	tags := captureMutationTags(t, e.db)
	msg := testMessage{Body: "moving", RoomID: "old"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}
	msg.RoomID = "new"
	if err := e.db.Save(&msg).Error; err != nil {
		t.Fatalf("Save: %v", err)
	}

	if !slices.Contains(*tags, "messages_room_id:old") {
		t.Errorf("tags = %v, missing old collection tag", *tags)
	}
	if !slices.Contains(*tags, "messages_room_id:new") {
		t.Errorf("tags = %v, missing new collection tag", *tags)
	}
	if !slices.Contains(*tags, fmt.Sprintf("messages:%v", msg.ID)) {
		t.Errorf("tags = %v, missing primary key tag", *tags)
	}
}

func TestExtractMutationTagsWithoutSchema(t *testing.T) {
	e := newTestEngine(t)
	if err := e.db.Exec("SELECT 1").Error; err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if tags := extractMutationTags(e.db); len(tags) != 0 {
		t.Errorf("extractMutationTags(raw SQL) = %v, want empty", tags)
	}
}

func TestCreateInvalidatesTrackCollection(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var runs atomic.Int64
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		roomID := ctx.Params["room"].(string)
		ctx.TrackCollection("messages", "room_id", roomID)
		var msgs []testMessage
		if err := ctx.DB.Where("room_id = ?", roomID).Find(&msgs).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return msgs, nil
	})

	subscribe(t, e, client, "getMessages", "lobby", map[string]interface{}{"room": "lobby"})
	if got := runs.Load(); got != 1 {
		t.Fatalf("query runs after subscribe = %d, want 1", got)
	}
	drain(client)

	e.RegisterMutation("createMessage", func(ctx *MutationCtx) (any, error) {
		msg := testMessage{Body: ctx.Params["body"].(string), RoomID: ctx.Params["room"].(string)}
		if err := ctx.DB.Create(&msg).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return msg, nil
	})
	if _, err := e.executeMutation("createMessage", map[string]interface{}{"body": "hi", "room": "lobby"}, client.ID, "m1"); err != nil {
		t.Fatalf("ExecuteMutation: %v", err)
	}

	if got := runs.Load(); got != 2 {
		t.Errorf("query runs after Create in tracked collection = %d, want 2", got)
	}
	if got := queryMessages(t, drain(client)); len(got) != 1 {
		t.Errorf("query pushes after Create = %d, want 1", len(got))
	}
}

func TestCreateDoesNotInvalidateOtherCollections(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var lobbyRuns, otherRuns atomic.Int64
	e.RegisterQuery("getLobby", func(ctx *QueryCtx) (any, error) {
		lobbyRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&msgs)
		return msgs, nil
	})
	e.RegisterQuery("getOther", func(ctx *QueryCtx) (any, error) {
		otherRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "other")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "other").Find(&msgs)
		return msgs, nil
	})

	subscribe(t, e, client, "getLobby", "lobby", nil)
	subscribe(t, e, client, "getOther", "other", nil)
	if lobbyRuns.Load() != 1 || otherRuns.Load() != 1 {
		t.Fatalf("subscribe runs lobby=%d other=%d, want 1/1", lobbyRuns.Load(), otherRuns.Load())
	}

	if err := e.db.Create(&testMessage{Body: "hi", RoomID: "lobby"}).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := lobbyRuns.Load(); got != 2 {
		t.Errorf("lobby query runs = %d, want 2", got)
	}
	if got := otherRuns.Load(); got != 1 {
		t.Errorf("unrelated collection query runs = %d, want 1", got)
	}
}

func TestUpdateInvalidatesPrimaryKeyAndCollectionTags(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	msg := testMessage{Body: "old", RoomID: "lobby"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("seed Create: %v", err)
	}

	var pkRuns, colRuns, otherRuns atomic.Int64
	e.RegisterQuery("byID", func(ctx *QueryCtx) (any, error) {
		pkRuns.Add(1)
		var got testMessage
		ctx.DB.First(&got, msg.ID)
		return got, nil
	})
	e.RegisterQuery("byRoom", func(ctx *QueryCtx) (any, error) {
		colRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&msgs)
		return msgs, nil
	})
	e.RegisterQuery("otherRoom", func(ctx *QueryCtx) (any, error) {
		otherRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "other")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "other").Find(&msgs)
		return msgs, nil
	})

	subscribe(t, e, client, "byID", "id", nil)
	subscribe(t, e, client, "byRoom", "room", nil)
	subscribe(t, e, client, "otherRoom", "other", nil)

	msg.Body = "new"
	if err := e.db.Save(&msg).Error; err != nil {
		t.Fatalf("Save: %v", err)
	}

	if got := pkRuns.Load(); got != 2 {
		t.Errorf("primary-key query runs after Update = %d, want 2", got)
	}
	// The room query is subscribed to both the collection tag and the
	// auto-tracked row id, but a single Save must re-run it only once.
	if got := colRuns.Load(); got != 2 {
		t.Errorf("collection query runs after Update = %d, want 2", got)
	}
	if got := otherRuns.Load(); got != 1 {
		t.Errorf("unrelated collection query runs after Update = %d, want 1", got)
	}
}

func TestDeleteInvalidatesTrackedTags(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	msg := testMessage{Body: "bye", RoomID: "lobby"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("seed Create: %v", err)
	}

	var runs atomic.Int64
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&msgs)
		return msgs, nil
	})
	subscribe(t, e, client, "getMessages", "lobby", nil)
	if runs.Load() != 1 {
		t.Fatalf("runs after subscribe = %d, want 1", runs.Load())
	}

	if err := e.db.Delete(&msg).Error; err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Delete emits both the PK tag and the collection tag; this query listens
	// to both, but a single Delete must re-run it only once.
	if got := runs.Load(); got != 2 {
		t.Errorf("query runs after Delete = %d, want 2", got)
	}
}

// Count does not load rows, so auto-tracked primary keys cannot mask a delete
// that omitted the row's collection tag.
func TestDeleteByIDInvalidatesCollectionCount(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	msg := testMessage{Body: "bye", RoomID: "r"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("seed Create: %v", err)
	}

	var roomRuns, otherRuns atomic.Int64
	e.RegisterQuery("countRoom", func(ctx *QueryCtx) (any, error) {
		roomRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "r")
		var n int64
		if err := ctx.DB.Model(&testMessage{}).Where("room_id = ?", "r").Count(&n).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return n, nil
	})
	e.RegisterQuery("countOther", func(ctx *QueryCtx) (any, error) {
		otherRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "other")
		var n int64
		if err := ctx.DB.Model(&testMessage{}).Where("room_id = ?", "other").Count(&n).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return n, nil
	})
	subscribe(t, e, client, "countRoom", "room", nil)
	subscribe(t, e, client, "countOther", "other", nil)
	drain(client)
	if roomRuns.Load() != 1 || otherRuns.Load() != 1 {
		t.Fatalf("subscribe runs room=%d other=%d, want 1/1", roomRuns.Load(), otherRuns.Load())
	}

	if err := e.db.Delete(&testMessage{}, msg.ID).Error; err != nil {
		t.Fatalf("Delete: %v", err)
	}
	var left int64
	if err := e.db.Model(&testMessage{}).Where("id = ?", msg.ID).Count(&left).Error; err != nil {
		t.Fatalf("count remaining: %v", err)
	}
	if left != 0 {
		t.Fatalf("row still present after Delete, count = %d", left)
	}
	if got := roomRuns.Load(); got != 2 {
		t.Errorf("collection count runs after Delete by id = %d, want 2", got)
	}
	if got := otherRuns.Load(); got != 1 {
		t.Errorf("unrelated collection count runs after Delete by id = %d, want 1", got)
	}
	data, n := lastQueryData(t, client)
	if n != 1 || data != float64(0) {
		t.Errorf("count push after Delete by id = %v (%d messages), want one push of 0", data, n)
	}
}

func TestBatchDeleteByIDsInvalidatesCollectionCount(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	msgs := []testMessage{
		{Body: "a", RoomID: "r"},
		{Body: "b", RoomID: "r"},
		{Body: "c", RoomID: "other"},
	}
	if err := e.db.Create(&msgs).Error; err != nil {
		t.Fatalf("seed Create: %v", err)
	}

	var runs atomic.Int64
	e.RegisterQuery("countRoom", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "r")
		var n int64
		if err := ctx.DB.Model(&testMessage{}).Where("room_id = ?", "r").Count(&n).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return n, nil
	})
	subscribe(t, e, client, "countRoom", "room", nil)
	drain(client)

	if err := e.db.Delete(&testMessage{}, []uint{msgs[0].ID, msgs[1].ID}).Error; err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := runs.Load(); got != 2 {
		t.Errorf("collection count runs after batch Delete = %d, want 2", got)
	}
	data, n := lastQueryData(t, client)
	if n != 1 || data != float64(0) {
		t.Errorf("count push after batch Delete = %v (%d messages), want one push of 0", data, n)
	}
}

func TestPredicateDeleteInvalidatesCollectionCounts(t *testing.T) {
	// Both subscriptions re-run concurrently, so they can land on different
	// pooled connections; :memory: would give each connection an empty schema.
	e := newConcurrentTestEngine(t)
	client := trackClient(t, e)

	msgs := []testMessage{
		{Body: "drop", RoomID: "r"},
		{Body: "drop", RoomID: "other"},
		{Body: "keep", RoomID: "r"},
	}
	if err := e.db.Create(&msgs).Error; err != nil {
		t.Fatalf("seed Create: %v", err)
	}

	var roomRuns, otherRuns atomic.Int64
	registerCount := func(name, room string, runs *atomic.Int64) {
		e.RegisterQuery(name, func(ctx *QueryCtx) (any, error) {
			runs.Add(1)
			ctx.TrackCollection("messages", "room_id", room)
			var n int64
			if err := ctx.DB.Model(&testMessage{}).Where("room_id = ?", room).Count(&n).Error; err != nil {
				return map[string]interface{}{"error": err.Error()}, nil
			}
			return n, nil
		})
	}
	registerCount("countRoom", "r", &roomRuns)
	registerCount("countOther", "other", &otherRuns)
	subscribe(t, e, client, "countRoom", "room", nil)
	subscribe(t, e, client, "countOther", "other", nil)
	drain(client)

	// The predicate is not the tracked column, so collection tags have to come
	// from the rows that match, not from the WHERE clause.
	if err := e.db.Where("body = ?", "drop").Delete(&testMessage{}).Error; err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := roomRuns.Load(); got != 2 {
		t.Errorf("room count runs after predicate Delete = %d, want 2", got)
	}
	if got := otherRuns.Load(); got != 2 {
		t.Errorf("other count runs after predicate Delete = %d, want 2", got)
	}
	got := map[string]interface{}{}
	for _, msg := range queryMessages(t, drain(client)) {
		key, _ := msg["query_key"].(string)
		got[key] = msg["data"]
	}
	if got["room"] != float64(1) || got["other"] != float64(0) {
		t.Errorf("count pushes after predicate Delete = %v, want room 1 and other 0", got)
	}
}

func TestMovingRecordInvalidatesOldAndNewCollections(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	msg := testMessage{Body: "moving", RoomID: "old"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("seed Create: %v", err)
	}

	var oldRuns, newRuns atomic.Int64
	e.RegisterQuery("oldRoom", func(ctx *QueryCtx) (any, error) {
		oldRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "old")
		return "old", nil
	})
	e.RegisterQuery("newRoom", func(ctx *QueryCtx) (any, error) {
		newRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "new")
		return "new", nil
	})
	subscribe(t, e, client, "oldRoom", "old", nil)
	subscribe(t, e, client, "newRoom", "new", nil)

	msg.RoomID = "new"
	if err := e.db.Save(&msg).Error; err != nil {
		t.Fatalf("Save: %v", err)
	}

	if got := newRuns.Load(); got != 2 {
		t.Errorf("destination collection runs = %d, want 2", got)
	}
	if got := oldRuns.Load(); got != 2 {
		t.Errorf("source collection runs = %d, want 2 (old collection should also be invalidated)", got)
	}
}

func TestMapUpdatesMovingRecordInvalidatesOldAndNewCollections(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	msg := testMessage{Body: "moving", RoomID: "old"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("seed Create: %v", err)
	}

	var oldRuns, newRuns atomic.Int64
	e.RegisterQuery("oldRoom", func(ctx *QueryCtx) (any, error) {
		oldRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "old")
		return "old", nil
	})
	e.RegisterQuery("newRoom", func(ctx *QueryCtx) (any, error) {
		newRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "new")
		return "new", nil
	})
	subscribe(t, e, client, "oldRoom", "old", nil)
	subscribe(t, e, client, "newRoom", "new", nil)

	if err := e.db.Model(&testMessage{}).Where("id = ?", msg.ID).Updates(map[string]interface{}{"room_id": "new"}).Error; err != nil {
		t.Fatalf("Updates: %v", err)
	}

	if got := newRuns.Load(); got != 2 {
		t.Errorf("destination collection runs = %d, want 2", got)
	}
	if got := oldRuns.Load(); got != 2 {
		t.Errorf("source collection runs = %d, want 2 (old collection should also be invalidated)", got)
	}
}

func TestMapUpdatesInvalidateLoadedRecord(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	msg := testMessage{Body: "old", RoomID: "lobby"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("seed Create: %v", err)
	}

	var runs atomic.Int64
	e.RegisterQuery("byID", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		var got testMessage
		ctx.DB.First(&got, msg.ID)
		return got, nil
	})
	subscribe(t, e, client, "byID", "id", nil)

	if err := e.db.Model(&testMessage{}).Where("id = ?", msg.ID).Updates(map[string]interface{}{"body": "patched"}).Error; err != nil {
		t.Fatalf("Updates: %v", err)
	}
	if got := runs.Load(); got != 2 {
		t.Errorf("query runs after map Updates = %d, want 2", got)
	}
}

// A predicate that names neither the primary key nor a tracked collection
// still has to invalidate queries that auto-tracked the affected row.
func TestPredicateUpdateInvalidatesAutoTrackedPrimaryKey(t *testing.T) {
	e := newTestEngine(t)
	if err := e.db.AutoMigrate(&testNote{}); err != nil {
		t.Fatalf("AutoMigrate notes: %v", err)
	}
	client := trackClient(t, e)

	note := testNote{Body: "old"}
	other := testNote{Body: "keep"}
	if err := e.db.Create(&note).Error; err != nil {
		t.Fatalf("seed updated note: %v", err)
	}
	if err := e.db.Create(&other).Error; err != nil {
		t.Fatalf("seed untouched note: %v", err)
	}

	var updatedRuns, otherRuns atomic.Int64
	e.RegisterQuery("updatedNote", func(ctx *QueryCtx) (any, error) {
		updatedRuns.Add(1)
		var got testNote
		if err := ctx.DB.First(&got, note.NoteID).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return got, nil
	})
	e.RegisterQuery("otherNote", func(ctx *QueryCtx) (any, error) {
		otherRuns.Add(1)
		var got testNote
		if err := ctx.DB.First(&got, other.NoteID).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return got, nil
	})
	subscribe(t, e, client, "updatedNote", "updated", nil)
	subscribe(t, e, client, "otherNote", "other", nil)
	if updatedRuns.Load() != 1 || otherRuns.Load() != 1 {
		t.Fatalf("subscribe runs updated=%d other=%d, want 1/1", updatedRuns.Load(), otherRuns.Load())
	}
	drain(client)

	if err := e.db.Model(&testNote{}).Where("body = ?", "old").Update("body", "new").Error; err != nil {
		t.Fatalf("Update: %v", err)
	}
	var stored testNote
	if err := e.db.First(&stored, note.NoteID).Error; err != nil {
		t.Fatalf("reload: %v", err)
	}
	if stored.Body != "new" {
		t.Fatalf("stored body = %q, want new", stored.Body)
	}
	if got := updatedRuns.Load(); got != 2 {
		t.Errorf("primary-key query runs after predicate Update = %d, want 2", got)
	}
	if got := otherRuns.Load(); got != 1 {
		t.Errorf("unrelated primary-key query runs after predicate Update = %d, want 1", got)
	}
	data, n := lastQueryData(t, client)
	row, _ := data.(map[string]interface{})
	if n != 1 || row["Body"] != "new" {
		t.Errorf("query push after predicate Update = %v (%d messages), want one push with Body new", data, n)
	}
}

func TestAutoTrackRecordsLoadedIDs(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	msg := testMessage{Body: "tracked", RoomID: "lobby"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("seed Create: %v", err)
	}

	e.RegisterQuery("getOne", func(ctx *QueryCtx) (any, error) {
		var got testMessage
		ctx.DB.First(&got, msg.ID)
		return got, nil
	})
	sub := subscribe(t, e, client, "getOne", "one", nil)

	tag := fmt.Sprintf("messages:%v", msg.ID)
	if !hasSubscription(e.tracker.GetSubscriptionsToTag(tag), sub.SubID) {
		t.Errorf("subscription not mapped to auto-tracked tag %q", tag)
	}
}

func TestAutoTrackSliceLoadsEveryID(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	msgs := []testMessage{
		{Body: "a", RoomID: "lobby"},
		{Body: "b", RoomID: "lobby"},
	}
	if err := e.db.Create(&msgs).Error; err != nil {
		t.Fatalf("seed Create: %v", err)
	}

	e.RegisterQuery("getAll", func(ctx *QueryCtx) (any, error) {
		var got []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&got)
		return got, nil
	})
	sub := subscribe(t, e, client, "getAll", "all", nil)

	for _, msg := range msgs {
		tag := fmt.Sprintf("messages:%v", msg.ID)
		if !hasSubscription(e.tracker.GetSubscriptionsToTag(tag), sub.SubID) {
			t.Errorf("subscription not mapped to auto-tracked tag %q", tag)
		}
	}
}

func TestAutoTrackRequiresExportedIDField(t *testing.T) {
	e := newTestEngine(t)
	if err := e.db.AutoMigrate(&testNote{}); err != nil {
		t.Fatalf("AutoMigrate notes: %v", err)
	}
	client := trackClient(t, e)

	note := testNote{Body: "n"}
	if err := e.db.Create(&note).Error; err != nil {
		t.Fatalf("Create note: %v", err)
	}

	e.RegisterQuery("getNote", func(ctx *QueryCtx) (any, error) {
		var got testNote
		ctx.DB.First(&got, note.NoteID)
		return got, nil
	})
	sub := subscribe(t, e, client, "getNote", "note", nil)

	tag := fmt.Sprintf("notes:%v", note.NoteID)
	if !hasSubscription(e.tracker.GetSubscriptionsToTag(tag), sub.SubID) {
		t.Errorf("auto-track did not record primary key tag %q for a model whose PK is not named ID", tag)
	}
}

func TestQueryWriteDoesNotMutateOrRetrigger(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var runs atomic.Int64
	var writeErr atomic.Value
	e.RegisterQuery("returnCount", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		var msgs []testMessage
		if err := ctx.DB.Table("messages").Select("*").Find(&msgs).Error; err != nil {
			t.Errorf("find: %v", err)
		}
		ctx.TrackTable("messages")
		err := ctx.DB.Table("messages").Create(&testMessage{Body: "from-query", RoomID: "lobby"}).Error
		if err != nil {
			writeErr.Store(err.Error())
		} else {
			writeErr.Store("")
		}
		return len(msgs), nil
	})
	subscribe(t, e, client, "returnCount", "k", nil)
	if got := runs.Load(); got != 1 {
		t.Fatalf("subscribe runs = %d, want 1", got)
	}
	if got, _ := writeErr.Load().(string); got == "" {
		t.Fatal("query Create returned nil error")
	}

	var before int64
	if err := e.db.Table("messages").Count(&before).Error; err != nil {
		t.Fatal(err)
	}
	if before != 0 {
		t.Fatalf("rows after subscribe = %d, want 0", before)
	}

	if err := e.db.Create(&testMessage{Body: "real", RoomID: "lobby"}).Error; err != nil {
		t.Fatal(err)
	}
	if got := runs.Load(); got != 2 {
		t.Fatalf("runs after external create = %d, want 2", got)
	}
	var rows []testMessage
	if err := e.db.Order("id").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Body != "real" {
		t.Fatalf("rows = %+v, want one real message", rows)
	}
}

// cachedUpdate is prepared by a mutation before the query runs, so a query's
// PrepareStmt session would find it in GORM's shared statement cache.
const cachedUpdate = "UPDATE messages SET body = 'changed' WHERE body = ?"

// readOnlyWriteAttempts covers every route out of ctx.DB that can reach the
// database, not just GORM's Raw callback.
var readOnlyWriteAttempts = []struct {
	name string
	run  func(db *gorm.DB) error
}{
	{"exec after block comment", func(db *gorm.DB) error {
		return db.Exec("/* application comment */ UPDATE messages SET body = 'changed'").Error
	}},
	{"exec after line comment", func(db *gorm.DB) error {
		return db.Exec("-- note\nUPDATE messages SET body = 'changed'").Error
	}},
	{"exec lowercase with leading whitespace", func(db *gorm.DB) error {
		return db.Exec("\n\t update messages set body = 'changed'").Error
	}},
	{"raw returning scan", func(db *gorm.DB) error {
		var ids []uint
		return db.Raw("UPDATE messages SET body = 'changed' RETURNING id").Scan(&ids).Error
	}},
	{"raw returning find", func(db *gorm.DB) error {
		var ids []uint
		return db.Raw("UPDATE messages SET body = 'changed' RETURNING id").Find(&ids).Error
	}},
	{"raw returning row", func(db *gorm.DB) error {
		var id uint
		return db.Raw("UPDATE messages SET body = 'changed' RETURNING id").Row().Scan(&id)
	}},
	{"raw returning rows", func(db *gorm.DB) error {
		rows, err := db.Raw("UPDATE messages SET body = 'changed' RETURNING id").Rows()
		if err == nil {
			rows.Close()
		}
		return err
	}},
	{"second statement", func(db *gorm.DB) error {
		return db.Exec("SELECT 1; UPDATE messages SET body = 'changed'").Error
	}},
	{"cte main statement", func(db *gorm.DB) error {
		return db.Exec("WITH t AS (SELECT 1) UPDATE messages SET body = 'changed'").Error
	}},
	{"data-modifying cte", func(db *gorm.DB) error {
		var ids []uint
		return db.Raw("WITH u AS (UPDATE messages SET body = 'changed' RETURNING id) SELECT id FROM u").Scan(&ids).Error
	}},
	{"insert", func(db *gorm.DB) error {
		return db.Exec("INSERT INTO messages (body, room_id) VALUES ('changed', 'lobby')").Error
	}},
	{"select into", func(db *gorm.DB) error {
		return db.Exec("SELECT * INTO query_copy FROM messages").Error
	}},
	{"create table", func(db *gorm.DB) error {
		return db.Exec("CREATE TABLE query_ddl (id integer)").Error
	}},
	{"alter table", func(db *gorm.DB) error {
		return db.Exec("ALTER TABLE messages ADD COLUMN extra text").Error
	}},
	{"migrator", func(db *gorm.DB) error {
		return db.Migrator().CreateTable(&testNote{})
	}},
	{"transaction", func(db *gorm.DB) error {
		return db.Transaction(func(tx *gorm.DB) error {
			return tx.Exec("UPDATE messages SET body = 'changed'").Error
		})
	}},
	{"transaction commit statement", func(db *gorm.DB) error {
		return db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec("COMMIT").Error; err != nil {
				return err
			}
			return tx.Exec("UPDATE messages SET body = 'changed'").Error
		})
	}},
	{"replaced context", func(db *gorm.DB) error {
		return db.WithContext(context.Background()).Exec("UPDATE messages SET body = 'changed'").Error
	}},
	{"config conn pool", func(db *gorm.DB) error {
		_, err := db.ConnPool.ExecContext(context.Background(), "UPDATE messages SET body = 'changed'")
		return err
	}},
	{"statement conn pool", func(db *gorm.DB) error {
		_, err := db.Statement.ConnPool.ExecContext(context.Background(), "UPDATE messages SET body = 'changed'")
		return err
	}},
	{"sql.DB handle", func(db *gorm.DB) error {
		sqlDB, err := db.DB()
		if err != nil {
			return err
		}
		_, err = sqlDB.Exec("UPDATE messages SET body = 'changed'")
		return err
	}},
	{"connection", func(db *gorm.DB) error {
		return db.Connection(func(tx *gorm.DB) error {
			return tx.Exec("UPDATE messages SET body = 'changed'").Error
		})
	}},
	{"cached prepared statement", func(db *gorm.DB) error {
		return db.Session(&gorm.Session{PrepareStmt: true}).Exec(cachedUpdate, "original").Error
	}},
	{"update builder", func(db *gorm.DB) error {
		return db.Model(&testMessage{}).Where("body = ?", "original").Update("body", "changed").Error
	}},
	{"delete builder", func(db *gorm.DB) error {
		return db.Where("body = ?", "original").Delete(&testMessage{}).Error
	}},
	{"drop table", func(db *gorm.DB) error {
		return db.Exec("DROP TABLE messages").Error
	}},
}

func runReadOnlyWriteAttempts(db *gorm.DB) map[string]error {
	errs := make(map[string]error, len(readOnlyWriteAttempts))
	for _, attempt := range readOnlyWriteAttempts {
		errs[attempt.name] = attempt.run(db)
	}
	return errs
}

func checkReadOnlyWriteAttempts(t *testing.T, e *Engine, errs map[string]error) {
	t.Helper()
	if len(errs) != len(readOnlyWriteAttempts) {
		t.Fatalf("recorded %d write attempts, want %d", len(errs), len(readOnlyWriteAttempts))
	}
	for _, attempt := range readOnlyWriteAttempts {
		err := errs[attempt.name]
		switch {
		case err == nil:
			t.Errorf("%s: write succeeded", attempt.name)
		case attempt.name == "sql.DB handle" || attempt.name == "connection":
			// Refused before any SQL is sent: ctx.DB exposes no *sql.DB.
		case !errors.Is(err, errReadOnly):
			t.Errorf("%s: err = %v, want the read-only error", attempt.name, err)
		}
	}

	var rows []testMessage
	if err := e.db.Order("id").Find(&rows).Error; err != nil {
		t.Fatalf("read messages: %v", err)
	}
	if len(rows) != 1 || rows[0].Body != "original" {
		t.Errorf("messages = %+v, want the single original row", rows)
	}
	for _, table := range []string{"query_ddl", "query_copy", "notes"} {
		if e.db.Migrator().HasTable(table) {
			t.Errorf("table %s was created", table)
		}
	}
	if e.db.Migrator().HasColumn(&testMessage{}, "extra") {
		t.Error("messages.extra column was added")
	}
}

func seedReadOnlyWriteAttempts(t *testing.T, e *Engine) {
	t.Helper()
	if err := e.db.Create(&testMessage{Body: "original", RoomID: "lobby"}).Error; err != nil {
		t.Fatal(err)
	}
	prepared := e.db.Session(&gorm.Session{PrepareStmt: true})
	if err := prepared.Exec(cachedUpdate, "no-such-body").Error; err != nil {
		t.Fatalf("prime prepared statement: %v", err)
	}
}

func TestQueryRejectsEveryWritePath(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	seedReadOnlyWriteAttempts(t, e)

	var errs map[string]error
	e.RegisterQuery("writer", func(ctx *QueryCtx) (any, error) {
		errs = runReadOnlyWriteAttempts(ctx.DB)
		return nil, nil
	})
	subscribe(t, e, client, "writer", "k", nil)
	checkReadOnlyWriteAttempts(t, e, errs)
}

func TestGuardRejectsEveryWritePath(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	seedReadOnlyWriteAttempts(t, e)

	var errs map[string]error
	e.RegisterGuard("writer", func(ctx *GuardCtx) (any, error) {
		errs = runReadOnlyWriteAttempts(ctx.DB)
		return true, nil
	})
	e.RegisterQuery("guarded", func(ctx *QueryCtx) (any, error) {
		if _, err := ctx.Auth.ExecuteGuard("writer", map[string]interface{}{}); err != nil {
			t.Errorf("ExecuteGuard: %v", err)
		}
		return nil, nil
	})
	subscribe(t, e, client, "guarded", "k", nil)
	checkReadOnlyWriteAttempts(t, e, errs)
}

func TestMutationGuardRejectsWrites(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	seedReadOnlyWriteAttempts(t, e)

	var errs map[string]error
	e.RegisterGuard("writer", func(ctx *GuardCtx) (any, error) {
		errs = runReadOnlyWriteAttempts(ctx.DB)
		return true, nil
	})
	e.RegisterMutation("guarded", func(ctx *MutationCtx) (any, error) {
		if _, err := ctx.Auth.ExecuteGuard("writer", map[string]interface{}{}); err != nil {
			t.Errorf("ExecuteGuard: %v", err)
		}
		return nil, nil
	})
	if _, err := e.executeMutation("guarded", map[string]interface{}{}, client.ID, "m1"); err != nil {
		t.Fatalf("executeMutation: %v", err)
	}
	checkReadOnlyWriteAttempts(t, e, errs)
}

func TestQueryReadPathsStillWork(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	if err := e.db.Create(&testMessage{Body: "UPDATE; not a statement", RoomID: "lobby"}).Error; err != nil {
		t.Fatal(err)
	}

	reads := []struct {
		name string
		run  func(db *gorm.DB) (int64, error)
	}{
		{"commented select", func(db *gorm.DB) (n int64, err error) {
			err = db.Raw("/* dashboard */ SELECT count(*) FROM messages -- trailing").Scan(&n).Error
			return
		}},
		{"string literal naming a write", func(db *gorm.DB) (n int64, err error) {
			err = db.Raw("SELECT count(*) FROM messages WHERE body = 'UPDATE; not a statement'").Scan(&n).Error
			return
		}},
		{"cte select", func(db *gorm.DB) (n int64, err error) {
			err = db.Raw("WITH m AS (SELECT id FROM messages) SELECT count(*) FROM m").Scan(&n).Error
			return
		}},
		{"row", func(db *gorm.DB) (n int64, err error) {
			err = db.Raw("SELECT count(*) FROM messages WHERE room_id = ?", "lobby").Row().Scan(&n)
			return
		}},
		{"builder count", func(db *gorm.DB) (n int64, err error) {
			err = db.Model(&testMessage{}).Where("room_id = ?", "lobby").Count(&n).Error
			return
		}},
		{"transaction", func(db *gorm.DB) (n int64, err error) {
			err = db.Transaction(func(tx *gorm.DB) error {
				return tx.Transaction(func(inner *gorm.DB) error {
					return inner.Model(&testMessage{}).Count(&n).Error
				})
			})
			return
		}},
		{"prepared session", func(db *gorm.DB) (n int64, err error) {
			err = db.Session(&gorm.Session{PrepareStmt: true}).Model(&testMessage{}).Count(&n).Error
			return
		}},
	}

	got := map[string]int64{}
	errs := map[string]error{}
	e.RegisterQuery("reader", func(ctx *QueryCtx) (any, error) {
		for _, read := range reads {
			got[read.name], errs[read.name] = read.run(ctx.DB)
		}
		return nil, nil
	})
	subscribe(t, e, client, "reader", "k", nil)
	for _, read := range reads {
		if errs[read.name] != nil || got[read.name] != 1 {
			t.Errorf("%s: count = %d, err = %v; want 1, nil", read.name, got[read.name], errs[read.name])
		}
	}
}

func TestTrackTableIsInvalidatedByMutations(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var runs atomic.Int64
	e.RegisterQuery("allMessages", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		ctx.TrackTable("messages")
		var msgs []testMessage
		ctx.DB.Find(&msgs)
		return msgs, nil
	})
	subscribe(t, e, client, "allMessages", "all", nil)

	if err := e.db.Create(&testMessage{Body: "x", RoomID: "r"}).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := runs.Load(); got != 2 {
		t.Errorf("TrackTable query runs after Create = %d, want 2", got)
	}
}

func TestInvalidateTagRerunsEverySubscriber(t *testing.T) {
	e := newTestEngine(t)
	a := trackClient(t, e)
	b := trackClient(t, e)

	var runs atomic.Int64
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		return []testMessage{}, nil
	})
	subscribe(t, e, a, "getMessages", "a", map[string]interface{}{"who": "a"})
	subscribe(t, e, b, "getMessages", "b", map[string]interface{}{"who": "b"})
	drain(a)
	drain(b)

	e.invalidateTag("messages_room_id:lobby")
	if got := runs.Load(); got != 4 {
		t.Errorf("query runs after InvalidateTag = %d, want 4", got)
	}
	if got := queryMessages(t, drain(a)); len(got) != 1 {
		t.Errorf("client a query pushes = %d, want 1", len(got))
	}
	if got := queryMessages(t, drain(b)); len(got) != 1 {
		t.Errorf("client b query pushes = %d, want 1", len(got))
	}
}

func TestInvalidateTagWithNoSubscribersDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	mustNoPanic(t, "InvalidateTag", func() {
		e.invalidateTag("messages:999")
	})
}

func TestMutationOnOneClientPushesQueryToSubscribersOnly(t *testing.T) {
	e := newTestEngine(t)
	subscriber := trackClient(t, e)
	mutator := trackClient(t, e)

	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&msgs)
		return len(msgs), nil
	})
	e.RegisterMutation("createMessage", func(ctx *MutationCtx) (any, error) {
		msg := testMessage{Body: "hi", RoomID: "lobby"}
		ctx.DB.Create(&msg)
		return msg.ID, nil
	})

	subscribe(t, e, subscriber, "getMessages", "lobby", nil)
	drain(subscriber)
	drain(mutator)

	if _, err := e.executeMutation("createMessage", map[string]interface{}{}, mutator.ID, "mut-1"); err != nil {
		t.Fatalf("ExecuteMutation: %v", err)
	}

	subMsgs := drain(subscriber)
	mutMsgs := drain(mutator)

	if got := queryMessages(t, subMsgs); len(got) != 1 {
		t.Errorf("subscriber query pushes = %d, want 1", len(got))
	}
	for _, msg := range subMsgs {
		if msg["type"] == "mutation" {
			t.Errorf("subscriber received mutation payload: %v", msg)
		}
	}

	var mutationHits int
	for _, msg := range mutMsgs {
		if msg["type"] == "mutation" {
			mutationHits++
			if msg["mutation_id"] != "mut-1" {
				t.Errorf("mutation_id = %v, want mut-1", msg["mutation_id"])
			}
			if msg["location"] != "createMessage" {
				t.Errorf("mutation location = %v, want createMessage", msg["location"])
			}
		}
		if msg["type"] == "query" {
			t.Errorf("mutator received query payload without subscribing: %v", msg)
		}
	}
	if mutationHits != 1 {
		t.Errorf("mutator mutation pushes = %d, want 1", mutationHits)
	}
}

func TestQueryFailureDoesNotPanicAndStillTracksCollections(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var runs atomic.Int64
	e.RegisterQuery("brokenFind", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		err := ctx.DB.Where("not_a_column = 1").Find(&msgs).Error
		if err == nil {
			t.Error("expected GORM error from invalid column")
		}
		return map[string]interface{}{"error": err.Error()}, nil
	})

	mustNoPanic(t, "failing query subscribe", func() {
		subscribe(t, e, client, "brokenFind", "broken", nil)
	})
	if runs.Load() != 1 {
		t.Fatalf("runs after subscribe = %d, want 1", runs.Load())
	}
	drain(client)

	mustNoPanic(t, "Create after failing query", func() {
		if err := e.db.Create(&testMessage{Body: "hi", RoomID: "lobby"}).Error; err != nil {
			t.Errorf("Create: %v", err)
		}
	})
	if got := runs.Load(); got != 2 {
		t.Errorf("failing query was not re-run after collection Create; runs = %d, want 2", got)
	}
}

func TestMutationFailureDoesNotPanicOrInvalidate(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var runs atomic.Int64
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Find(&msgs)
		return msgs, nil
	})
	subscribe(t, e, client, "getMessages", "lobby", nil)
	drain(client)

	e.RegisterMutation("badCreate", func(ctx *MutationCtx) (any, error) {
		err := ctx.DB.Exec("INSERT INTO messages (not_a_column) VALUES (1)").Error
		if err == nil {
			return "unexpected success", nil
		}
		return map[string]interface{}{"error": err.Error()}, nil
	})

	var result interface{}
	var err error
	mustNoPanic(t, "failing mutation", func() {
		result, err = e.executeMutation("badCreate", map[string]interface{}{}, client.ID, "m-bad")
	})
	if err != nil {
		t.Errorf("ExecuteMutation returned error %v; failing mutations should encode the handler result", err)
	}
	data, _ := result.(map[string]interface{})
	if data["error"] == nil {
		t.Errorf("mutation result = %#v, want an error map", result)
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("query runs after failed mutation = %d, want 1", got)
	}
}

func TestMutationReturningErrorSendsErrorFrame(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.RegisterMutation("denied", func(ctx *MutationCtx) (any, error) {
		return nil, errors.New("not logged in")
	})

	if _, err := e.executeMutation("denied", map[string]interface{}{}, client.ID, "m-denied"); err != nil {
		t.Fatalf("executeMutation: %v", err)
	}
	msgs := drain(client)
	if len(msgs) != 1 {
		t.Fatalf("messages = %#v, want one error frame", msgs)
	}
	msg := msgs[0]
	if msg["type"] != "error" || msg["error"] != "not logged in" || msg["mutation_id"] != "m-denied" || msg["mutation"] != "denied" {
		t.Errorf("message = %#v, want error frame for m-denied", msg)
	}
}

func TestInternalOptionHidesFunctionsFromClients(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterQuery("hidden", func(ctx *QueryCtx) (any, error) { return "secret", nil }, Internal())
	e.RegisterMutation("hidden", func(ctx *MutationCtx) (any, error) { return "secret", nil }, Internal())
	e.RegisterGuard("plain", func(ctx *GuardCtx) (any, error) { return true, nil })

	client := trackClient(t, e)
	sub := e.tracker.SubscribeToQuery(client.ID, "hidden", "k", map[string]interface{}{})
	if sub == nil {
		t.Fatal("SubscribeToQuery returned nil")
	}
	if _, err := e.executeQuery("hidden", map[string]interface{}{}, sub); err == nil || err.Error() != "query not found" {
		t.Errorf("internal query error = %v, want query not found", err)
	}
	if _, err := e.executeMutation("hidden", map[string]interface{}{}, client.ID, "m"); err == nil || err.Error() != "mutation not found" {
		t.Errorf("internal mutation error = %v, want mutation not found", err)
	}
	got, err := e.executeMutationInternal("hidden", map[string]interface{}{})
	if err != nil || got != "secret" {
		t.Errorf("executeMutationInternal() = %#v, %v; want secret, nil", got, err)
	}
}

func TestInternalMutationReturningErrorReturnsIt(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterMutation("fails", func(ctx *MutationCtx) (any, error) {
		return nil, errors.New("boom")
	})

	result, err := e.executeMutationInternal("fails", map[string]interface{}{})
	if err == nil || err.Error() != "boom" {
		t.Errorf("executeMutationInternal error = %v, want boom", err)
	}
	if result != nil {
		t.Errorf("executeMutationInternal result = %#v, want nil", result)
	}
}

func TestScheduledMutationGetIdentityReturnsErrNoCaller(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterMutation("who", func(ctx *MutationCtx) (any, error) {
		_, err := ctx.Auth.GetIdentity()
		return nil, err
	}, Internal())

	_, err := e.executeMutationInternal("who", nil)
	if !errors.Is(err, ErrNoCaller) {
		t.Fatalf("GetIdentity error = %v, want ErrNoCaller", err)
	}
}

func TestExecuteMutationReturnsResult(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterMutation("hidden", func(ctx *MutationCtx) (any, error) {
		return ctx.Params["n"], nil
	}, Internal())

	got, err := e.ExecuteMutation("hidden", map[string]interface{}{"n": 3})
	if err != nil || got != 3 {
		t.Errorf("ExecuteMutation() = %#v, %v; want 3, nil", got, err)
	}
}

func TestExecuteMutationReturnsHandlerError(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterMutation("fails", func(ctx *MutationCtx) (any, error) {
		return nil, errors.New("boom")
	})

	result, err := e.ExecuteMutation("fails", nil)
	if err == nil || err.Error() != "boom" {
		t.Errorf("ExecuteMutation error = %v, want boom", err)
	}
	if result != nil {
		t.Errorf("ExecuteMutation result = %#v, want nil", result)
	}
}

func TestExecuteMutationAuthHasNoCaller(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterGuard("plain", func(ctx *GuardCtx) (any, error) { return true, nil })
	e.RegisterMutation("who", func(ctx *MutationCtx) (any, error) {
		if _, err := ctx.Auth.GetIdentity(); !errors.Is(err, ErrNoCaller) {
			return nil, fmt.Errorf("GetIdentity error = %v, want ErrNoCaller", err)
		}
		_, err := ctx.Auth.ExecuteGuard("plain", nil)
		return nil, err
	})

	_, err := e.ExecuteMutation("who", nil)
	if err == nil || err.Error() != "guards cannot be executed internally" {
		t.Fatalf("ExecuteGuard error = %v, want guards cannot be executed internally", err)
	}
}

func TestExecuteMutationUnknownName(t *testing.T) {
	e := newTestEngine(t)
	_, err := e.ExecuteMutation("missing", nil)
	if err == nil || err.Error() != "mutation not found" {
		t.Errorf("ExecuteMutation error = %v, want mutation not found", err)
	}
}

func TestExecuteMutationInvalidatesSubscribers(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var runs atomic.Int64
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&msgs)
		return msgs, nil
	})
	subscribe(t, e, client, "getMessages", "lobby", nil)
	drain(client)

	e.RegisterMutation("createMessage", func(ctx *MutationCtx) (any, error) {
		return nil, ctx.DB.Create(&testMessage{Body: "hi", RoomID: "lobby"}).Error
	}, Internal())
	if _, err := e.ExecuteMutation("createMessage", nil); err != nil {
		t.Fatalf("ExecuteMutation: %v", err)
	}
	if got := runs.Load(); got != 2 {
		t.Errorf("query runs after ExecuteMutation = %d, want 2", got)
	}
	if got := queryMessages(t, drain(client)); len(got) != 1 {
		t.Errorf("query pushes after ExecuteMutation = %d, want 1", len(got))
	}
}

func TestExecuteMutationTracesDatabaseWrites(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Profiler().Start(); err != nil {
		t.Fatalf("start profiler: %v", err)
	}
	t.Cleanup(e.Profiler().Stop)

	var execID, actionName string
	e.RegisterMutation("createMessage", func(ctx *MutationCtx) (any, error) {
		execID, _ = ctx.DB.Statement.Context.Value(ContextKeyExecutionID).(string)
		actionName, _ = ctx.DB.Statement.Context.Value(ContextKeyActionName).(string)
		return nil, ctx.DB.Create(&testMessage{Body: "hi", RoomID: "lobby"}).Error
	}, Internal())

	if _, err := e.ExecuteMutation("createMessage", nil); err != nil {
		t.Fatalf("ExecuteMutation: %v", err)
	}
	if execID == "" {
		t.Fatal("mutation DB context has no execution ID")
	}
	if actionName != "createMessage" {
		t.Fatalf("action name = %q, want createMessage", actionName)
	}

	var sawMutation, sawDB, sawInvalidate bool
	for _, m := range e.Profiler().DumpMetricsAndFlush() {
		switch {
		case m.Type == MetricTypeMutation && m.Name == "mutation:createMessage":
			sawMutation = true
			if m.ID != execID {
				t.Errorf("mutation metric ID = %q, want %q", m.ID, execID)
			}
		case m.Type == MetricTypeDatabase && m.ID == execID:
			sawDB = true
			if m.Name == "" || !strings.HasPrefix(m.Name, "gorm:") {
				t.Errorf("database metric name = %q, want gorm:<model>", m.Name)
			}
		case m.Type == MetricTypeRouting && m.Name == "invalidate_tags:createMessage":
			sawInvalidate = true
			if m.ID != execID {
				t.Errorf("invalidate metric ID = %q, want %q", m.ID, execID)
			}
		}
	}
	if !sawMutation {
		t.Error("profiler did not record the mutation")
	}
	if !sawDB {
		t.Error("profiler did not attribute the write to the mutation execution")
	}
	if !sawInvalidate {
		t.Error("profiler did not attribute invalidation to the mutation")
	}
}

func TestExecuteQueryReturnsResult(t *testing.T) {
	e := newTestEngine(t)
	if err := e.db.Create(&testMessage{Body: "hello", RoomID: "lobby"}).Error; err != nil {
		t.Fatal(err)
	}
	e.RegisterQuery("hidden", func(ctx *QueryCtx) (any, error) {
		if ctx.Params["n"] != 3 {
			return nil, fmt.Errorf("params[n] = %#v, want 3", ctx.Params["n"])
		}
		var msg testMessage
		if err := ctx.DB.Where("room_id = ?", "lobby").First(&msg).Error; err != nil {
			return nil, err
		}
		return msg.Body, nil
	}, Internal())

	got, err := e.ExecuteQuery("hidden", map[string]interface{}{"n": 3})
	if err != nil || got != "hello" {
		t.Errorf("ExecuteQuery() = %#v, %v; want hello, nil", got, err)
	}
}

func TestExecuteQueryReturnsHandlerError(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterQuery("fails", func(ctx *QueryCtx) (any, error) {
		return nil, errors.New("boom")
	})

	result, err := e.ExecuteQuery("fails", nil)
	if err == nil || err.Error() != "boom" {
		t.Errorf("ExecuteQuery error = %v, want boom", err)
	}
	if result != nil {
		t.Errorf("ExecuteQuery result = %#v, want nil", result)
	}
}

func TestExecuteQueryAuthHasNoCaller(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterGuard("plain", func(ctx *GuardCtx) (any, error) { return true, nil })
	e.RegisterQuery("who", func(ctx *QueryCtx) (any, error) {
		if _, err := ctx.Auth.GetIdentity(); !errors.Is(err, ErrNoCaller) {
			return nil, fmt.Errorf("GetIdentity error = %v, want ErrNoCaller", err)
		}
		if _, err := ctx.Auth.ExecuteGuard("plain", nil); err == nil || err.Error() != "guards cannot be executed internally" {
			return nil, fmt.Errorf("ExecuteGuard error = %v, want guards cannot be executed internally", err)
		}
		_, err := ctx.Storage.GetUploadURL()
		if err == nil {
			return nil, errors.New("GetUploadURL error = nil, want an error")
		}
		return nil, ctx.Storage.DeleteFile("nope")
	}, Internal())

	_, err := e.ExecuteQuery("who", nil)
	if err == nil || err.Error() != "tether: this capability is not available in this context" {
		t.Fatalf("DeleteFile error = %v, want capability denied", err)
	}
}

func TestExecuteQueryUnknownName(t *testing.T) {
	e := newTestEngine(t)
	_, err := e.ExecuteQuery("missing", nil)
	if err == nil || err.Error() != "query not found" {
		t.Errorf("ExecuteQuery error = %v, want query not found", err)
	}
}

func TestExecuteQueryRejectsWrites(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterQuery("writer", func(ctx *QueryCtx) (any, error) {
		return nil, ctx.DB.Create(&testMessage{Body: "nope", RoomID: "lobby"}).Error
	})

	_, err := e.ExecuteQuery("writer", nil)
	if !errors.Is(err, errReadOnly) {
		t.Fatalf("ExecuteQuery write error = %v, want errReadOnly", err)
	}
	var count int64
	if err := e.db.Model(&testMessage{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("messages after rejected write = %d, want 0", count)
	}
}

func TestExecuteQueryDoesNotPushToSubscribers(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.RegisterQuery("item", func(ctx *QueryCtx) (any, error) { return "live", nil })
	subscribe(t, e, client, "item", "k", nil)
	drain(client)

	got, err := e.ExecuteQuery("item", nil)
	if err != nil || got != "live" {
		t.Fatalf("ExecuteQuery() = %#v, %v; want live, nil", got, err)
	}
	if msgs := drain(client); len(msgs) != 0 {
		t.Errorf("ExecuteQuery pushed %#v", msgs)
	}
}

func TestRegisteringANameTwicePanics(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterQuery("q", func(*QueryCtx) (any, error) { return nil, nil })
	e.RegisterMutation("m", func(*MutationCtx) (any, error) { return nil, nil })
	e.RegisterGuard("g", func(*GuardCtx) (any, error) { return true, nil })

	assertPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("%s: duplicate registration did not panic", name)
				return
			}
			if !strings.Contains(fmt.Sprint(r), "already registered") {
				t.Errorf("%s: panic = %v, want already registered", name, r)
			}
		}()
		fn()
	}
	assertPanic("query", func() {
		e.RegisterQuery("q", func(*QueryCtx) (any, error) { return nil, nil })
	})
	assertPanic("mutation", func() {
		e.RegisterMutation("m", func(*MutationCtx) (any, error) { return nil, nil })
	})
	assertPanic("guard", func() {
		e.RegisterGuard("g", func(*GuardCtx) (any, error) { return true, nil })
	})
}

func TestQueryReturningErrorSendsErrorFrameAndStaysLive(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&msgs)
		if len(msgs) == 0 {
			return nil, errors.New("room is empty")
		}
		return len(msgs), nil
	})
	e.RegisterMutation("createMessage", func(ctx *MutationCtx) (any, error) {
		ctx.DB.Create(&testMessage{Body: "hi", RoomID: "lobby"})
		return nil, nil
	})

	subscribe(t, e, client, "getMessages", "lobby-key", nil)
	msgs := drain(client)
	if len(msgs) != 1 {
		t.Fatalf("messages = %#v, want one error frame", msgs)
	}
	msg := msgs[0]
	if msg["type"] != "error" || msg["error"] != "room is empty" || msg["query_key"] != "lobby-key" || msg["timestamp"] == nil {
		t.Errorf("message = %#v, want timestamped error frame for lobby-key", msg)
	}

	if _, err := e.executeMutation("createMessage", map[string]interface{}{}, client.ID, "m1"); err != nil {
		t.Fatalf("executeMutation: %v", err)
	}
	var sawData bool
	for _, msg := range drain(client) {
		if msg["type"] == "query" && msg["query_key"] == "lobby-key" && msg["data"] == float64(1) {
			sawData = true
		}
	}
	if !sawData {
		t.Error("query that returned an error was not re-run after its dependencies changed")
	}
}

func TestExecuteQueryUnserializableParamsDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.RegisterQuery("noop", func(ctx *QueryCtx) (any, error) { return "ok", nil })
	sub := e.tracker.SubscribeToQuery(client.ID, "noop", "k", map[string]interface{}{})

	var err error
	mustNoPanic(t, "ExecuteQuery(bad params)", func() {
		_, err = e.executeQuery("noop", map[string]interface{}{"ch": make(chan int)}, sub)
	})
	if err == nil {
		t.Error("ExecuteQuery with unmarshalable params returned nil error")
	}
}

func TestExecuteQueryUnserializableResultDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.RegisterQuery("bad", func(ctx *QueryCtx) (any, error) { return make(chan int), nil })
	sub := e.tracker.SubscribeToQuery(client.ID, "bad", "k", map[string]interface{}{})

	var err error
	mustNoPanic(t, "ExecuteQuery(bad result)", func() {
		_, err = e.executeQuery("bad", map[string]interface{}{}, sub)
	})
	if err == nil {
		t.Error("ExecuteQuery with unmarshalable result returned nil error")
	}
}

func TestUnknownQueryDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	sub := e.tracker.SubscribeToQuery(client.ID, "missing", "k", map[string]interface{}{})

	mustNoPanic(t, "ExecuteQuery(unknown)", func() {
		_, err := e.executeQuery("missing", map[string]interface{}{}, sub)
		if err == nil {
			t.Error("ExecuteQuery(unknown) returned nil error")
		}
	})
}

func TestUnknownMutationDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	mustNoPanic(t, "ExecuteMutation(unknown)", func() {
		_, err := e.executeMutation("missing", map[string]interface{}{}, client.ID, "m1")
		if err == nil {
			t.Error("ExecuteMutation(unknown) returned nil error")
		}
	})
}

func TestMalformedSubscribeDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	mustNoPanic(t, "subscribe without fields", func() {
		_ = e.onReceiveMessage(client.ID, map[string]interface{}{"type": "subscribe"})
	})
	mustNoPanic(t, "subscribe with nil params", func() {
		_ = e.onReceiveMessage(client.ID, map[string]interface{}{
			"type":      "subscribe",
			"location":  "q",
			"params":    nil,
			"query_key": "k",
		})
	})
}

func TestMalformedMutationDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	mustNoPanic(t, "mutation without fields", func() {
		_ = e.onReceiveMessage(client.ID, map[string]interface{}{"type": "mutation"})
	})
}

func TestMalformedAuthDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	mustNoPanic(t, "auth without token", func() {
		_ = e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth"})
	})
}

func TestStaleInvalidationOrdersTimestamps(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var version atomic.Int64
	version.Store(1)
	var pause atomic.Bool
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseQuery := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseQuery()

	params := map[string]interface{}{}
	e.RegisterQuery("counter", func(ctx *QueryCtx) (any, error) {
		ctx.TrackCollection("widgets", "id", "1")
		v := version.Load()
		if pause.CompareAndSwap(true, false) {
			close(started)
			<-release
		}
		return map[string]interface{}{"v": v}, nil
	})

	subscribe(t, e, client, "counter", "k", params)
	initial := queryMessages(t, drain(client))
	if len(initial) != 1 {
		t.Fatalf("initial messages = %d, want 1", len(initial))
	}
	initialTS, ok := initial[0]["timestamp"].(float64)
	if !ok || initialTS == 0 {
		t.Fatalf("initial timestamp = %#v", initial[0]["timestamp"])
	}

	pause.Store(true)
	done := make(chan struct{})
	go func() {
		e.invalidateTags([]string{"widgets_id:1"}, "slow", "slow")
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("slow invalidation did not reach the query")
	}

	version.Store(2)
	e.invalidateTags([]string{"widgets_id:1"}, "fast", "fast")
	releaseQuery()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("slow invalidation did not finish")
	}

	msgs := queryMessages(t, drain(client))
	if len(msgs) != 2 {
		t.Fatalf("invalidation messages = %d, want 2: %v", len(msgs), msgs)
	}
	versionOf := func(msg map[string]interface{}) float64 {
		t.Helper()
		data, _ := msg["data"].(map[string]interface{})
		got, _ := data["v"].(float64)
		return got
	}
	timestampOf := func(msg map[string]interface{}) float64 {
		t.Helper()
		ts, ok := msg["timestamp"].(float64)
		if !ok {
			t.Fatalf("timestamp = %#v", msg["timestamp"])
		}
		return ts
	}
	if versionOf(msgs[0]) != 2 || versionOf(msgs[1]) != 1 {
		t.Fatalf("arrival order = [%v, %v], want [2, 1]", versionOf(msgs[0]), versionOf(msgs[1]))
	}
	tsNewer := timestampOf(msgs[0])
	tsOlder := timestampOf(msgs[1])
	if !(initialTS < tsOlder && tsOlder < tsNewer) {
		t.Fatalf("timestamps = subscribe %v, slow %v, fast %v; want subscribe < slow < fast", initialTS, tsOlder, tsNewer)
	}
}

func TestStaleExecutionDoesNotOverwriteDependencies(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var dep atomic.Value
	dep.Store("base")
	var pause atomic.Bool
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseQuery := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseQuery()

	var runs atomic.Int64
	e.RegisterQuery("widget", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		selected := dep.Load().(string)
		ctx.TrackCollection("widgets", "id", selected)
		if pause.CompareAndSwap(true, false) {
			close(started)
			<-release
		}
		return selected, nil
	})

	sub := subscribe(t, e, client, "widget", "k", nil)
	drain(client)
	runs.Store(0)

	pause.Store(true)
	dep.Store("data1")
	done := make(chan struct{})
	go func() {
		e.invalidateTags([]string{"widgets_id:base"}, "slow", "slow")
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("slow execution did not reach the query")
	}

	dep.Store("data2")
	e.invalidateTags([]string{"widgets_id:base"}, "fast", "fast")
	releaseQuery()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("slow execution did not finish")
	}

	if e.tracker.SubscriptionHasTag(sub.SubID, "widgets_id:data1") {
		t.Fatal("stale execution restored dependency data1")
	}
	if !e.tracker.SubscriptionHasTag(sub.SubID, "widgets_id:data2") {
		t.Fatal("newest execution's dependency data2 is missing")
	}
	if e.tracker.SubscriptionHasTag(sub.SubID, "widgets_id:base") {
		t.Fatal("initial dependency was left in place")
	}

	msgs := queryMessages(t, drain(client))
	if len(msgs) != 2 {
		t.Fatalf("invalidation messages = %d, want 2: %v", len(msgs), msgs)
	}
	timestampOf := func(msg map[string]interface{}) float64 {
		t.Helper()
		ts, ok := msg["timestamp"].(float64)
		if !ok {
			t.Fatalf("timestamp = %#v", msg["timestamp"])
		}
		return ts
	}
	if msgs[0]["data"] != "data2" || msgs[1]["data"] != "data1" {
		t.Fatalf("arrival order = [%v, %v], want [data2, data1]", msgs[0]["data"], msgs[1]["data"])
	}
	if !(timestampOf(msgs[1]) < timestampOf(msgs[0])) {
		t.Fatalf("timestamps = data2 %v, data1 %v; want data1 < data2", timestampOf(msgs[0]), timestampOf(msgs[1]))
	}

	before := runs.Load()
	e.invalidateTag("widgets_id:data2")
	if got := runs.Load() - before; got != 1 {
		t.Fatalf("invalidating data2 ran the query %d times, want 1", got)
	}
	before = runs.Load()
	e.invalidateTag("widgets_id:data1")
	if got := runs.Load() - before; got != 0 {
		t.Fatalf("invalidating stale data1 ran the query %d times, want 0", got)
	}
}

func TestQueryResultIncludesLocationAndQueryKey(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.RegisterQuery("getThing", func(ctx *QueryCtx) (any, error) {
		return map[string]interface{}{"ok": true, "p": ctx.Params["id"]}, nil
	})
	subscribe(t, e, client, "getThing", "thing-7", map[string]interface{}{"id": 7})

	msgs := queryMessages(t, drain(client))
	if len(msgs) != 1 {
		t.Fatalf("got %d query messages, want 1", len(msgs))
	}
	if msgs[0]["location"] != "getThing" {
		t.Errorf("location = %v, want getThing", msgs[0]["location"])
	}
	if msgs[0]["query_key"] != "thing-7" {
		t.Errorf("query_key = %v, want thing-7", msgs[0]["query_key"])
	}
	data, _ := msgs[0]["data"].(map[string]interface{})
	if data["p"] != float64(7) && data["p"] != 7 {
		t.Errorf("params were not passed through; data = %#v", msgs[0]["data"])
	}
}

func TestAuthMapsToTheAuthenticatedClientOnly(t *testing.T) {
	e := newTestEngine(t)
	alice := trackClient(t, e)
	bob := trackClient(t, e)

	e.SetAuth(&stubAuth{userID: "alice", expiresAt: time.Now().Add(time.Hour)})
	if err := e.onReceiveMessage(alice.ID, map[string]interface{}{"type": "auth", "token": "alice-token"}); err != nil {
		t.Fatalf("auth alice: %v", err)
	}

	aliceAuth, ok := e.tracker.GetAuth(alice.ID)
	if !ok {
		t.Fatalf("alice auth not found")
	}
	bobAuth, ok := e.tracker.GetAuth(bob.ID)
	if !ok {
		t.Fatalf("bob auth not found")
	}
	if aliceAuth.UserID != "alice" {
		t.Fatalf("alice auth = %+v, want userID alice", aliceAuth)
	}
	if bobAuth.UserID != "" {
		t.Errorf("bob auth leaked alice's identity: %+v", bobAuth)
	}

	msgs := drain(alice)
	var sawAuth bool
	for _, msg := range msgs {
		if msg["type"] == "auth" {
			sawAuth = true
			if msg["success"] != true {
				t.Errorf("auth success = %v, want true", msg["success"])
			}
			data, _ := msg["data"].(map[string]interface{})
			if data["user_id"] != "alice" {
				t.Errorf("auth data.user_id = %v, want alice", data["user_id"])
			}
		}
	}
	if !sawAuth {
		t.Error("alice did not receive an auth success message")
	}
	for _, msg := range drain(bob) {
		if msg["type"] == "auth" {
			t.Errorf("bob received alice's auth message: %v", msg)
		}
	}
}

func TestQueryExposesIdentityOfTheSubscribedClient(t *testing.T) {
	e := newTestEngine(t)
	alice := trackClient(t, e)
	bob := trackClient(t, e)
	anon := trackClient(t, e)

	e.tracker.SetAuth(alice.ID, "user-alice", time.Now().Add(time.Hour))
	e.tracker.SetAuth(bob.ID, "user-bob", time.Now().Add(time.Hour))

	e.RegisterQuery("me", func(ctx *QueryCtx) (any, error) {
		id, err := ctx.Auth.GetIdentity()
		if err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return map[string]interface{}{"id": id}, nil
	})

	subscribe(t, e, alice, "me", "alice", nil)
	subscribe(t, e, bob, "me", "bob", nil)
	subscribe(t, e, anon, "me", "anon", nil)

	assertMe := func(client *reactivity.Client, wantID string) {
		t.Helper()
		msgs := queryMessages(t, drain(client))
		if len(msgs) != 1 {
			t.Fatalf("got %d query messages, want 1: %v", len(msgs), msgs)
		}
		data, _ := msgs[0]["data"].(map[string]interface{})
		if data["id"] != wantID {
			t.Errorf("id = %v, want %q", data["id"], wantID)
		}
	}
	assertMe(alice, "user-alice")
	assertMe(bob, "user-bob")
	assertMe(anon, "")
}

func TestGetIdentityRegistersPermanentUserTag(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.tracker.SetAuth(client.ID, "user-7", time.Now().Add(time.Hour))

	e.RegisterQuery("me", func(ctx *QueryCtx) (any, error) {
		id, _ := ctx.Auth.GetIdentity()
		return id, nil
	})
	sub := subscribe(t, e, client, "me", "me", nil)

	if !hasSubscription(e.tracker.GetSubscriptionsToTag("*user_identity:user-7"), sub.SubID) {
		t.Fatal("GetIdentity did not register *user_identity:user-7")
	}

	// Permanent tags should survive a later query that does not call GetIdentity.
	e.queries["me"] = query{Func: func(ctx *QueryCtx) (any, error) { return "no-auth-call", nil }, Internal: false}
	if _, err := e.executeQuery("me", map[string]interface{}{}, sub); err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	if !hasSubscription(e.tracker.GetSubscriptionsToTag("*user_identity:user-7"), sub.SubID) {
		t.Error("permanent *user_identity tag was dropped on a later execution")
	}
}

func TestGuardIdentityDoesNotFingerprintTheQuery(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.tracker.SetAuth(client.ID, "user-7", time.Now().Add(time.Hour))

	e.RegisterGuard("allow", func(ctx *GuardCtx) (any, error) {
		id, _ := ctx.Auth.GetIdentity()
		ctx.TrackCollection("room_members", "user_id", id)
		return map[string]interface{}{"access": true}, nil
	})
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		res, err := ctx.Auth.ExecuteGuard("allow", map[string]interface{}{"room": "lobby"})
		if err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		ctx.TrackCollection("messages", "room_id", "lobby")
		return res, nil
	})
	sub := subscribe(t, e, client, "getMessages", "lobby", nil)

	if hasSubscription(e.tracker.GetSubscriptionsToTag("*user_identity:user-7"), sub.SubID) {
		t.Fatal("GetIdentity inside a guard tagged the query; matching clients cannot be batched")
	}
	if len(sub.LinkedSubIDs) != 1 {
		t.Fatalf("linked guards = %d, want 1", len(sub.LinkedSubIDs))
	}
	guardID := sub.LinkedSubIDs[0]
	if !hasSubscription(e.tracker.GetSubscriptionsToTag("*user_identity:user-7"), guardID) {
		t.Fatal("GetIdentity inside a guard did not register *user_identity on the guard")
	}
	if !hasSubscription(e.tracker.GetSubscriptionsToTag("room_members_user_id:user-7"), guardID) {
		t.Fatal("guard did not subscribe to room_members_user_id:user-7")
	}
	paramsJSON, err := json.Marshal(map[string]interface{}{"room": "lobby"})
	if err != nil {
		t.Fatalf("marshal guard params: %v", err)
	}
	paramsHash := strconv.FormatUint(xxhash.Sum64(paramsJSON), 10)
	if !hasSubscription(e.tracker.GetSubscriptionsToTag("*guard_allow_"+paramsHash+":{\"access\":true}"), sub.SubID) {
		t.Fatal("query is missing the guard fingerprint tag")
	}
}

func TestGuardsBatchQueriesOnInvalidation(t *testing.T) {
	e := newTestEngine(t)
	a := trackClient(t, e)
	b := trackClient(t, e)
	e.tracker.SetAuth(a.ID, "user-a", time.Now().Add(time.Hour))
	e.tracker.SetAuth(b.ID, "user-b", time.Now().Add(time.Hour))

	var guardRuns, queryRuns atomic.Int64
	e.RegisterGuard("allow", func(ctx *GuardCtx) (any, error) {
		guardRuns.Add(1)
		id, _ := ctx.Auth.GetIdentity()
		ctx.TrackCollection("room_members", "user_id", id)
		return map[string]interface{}{"ok": true}, nil
	})
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		queryRuns.Add(1)
		if _, err := ctx.Auth.ExecuteGuard("allow", map[string]interface{}{"room": "lobby"}); err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		ctx.TrackCollection("messages", "room_id", "lobby")
		return map[string]interface{}{"ok": true}, nil
	})
	subscribe(t, e, a, "getMessages", "a", map[string]interface{}{"room": "lobby"})
	subscribe(t, e, b, "getMessages", "b", map[string]interface{}{"room": "lobby"})
	if got, want := queryRuns.Load(), int64(2); got != want {
		t.Fatalf("initial query runs = %d, want %d", got, want)
	}
	if got, want := guardRuns.Load(), int64(2); got != want {
		t.Fatalf("initial guard runs = %d, want %d", got, want)
	}

	e.invalidateTag("messages_room_id:lobby")
	if got, want := queryRuns.Load(), int64(3); got != want {
		t.Errorf("query runs after invalidate = %d, want %d (one batched execution)", got, want)
	}
	if got, want := guardRuns.Load(), int64(2); got != want {
		t.Errorf("guard runs after query invalidate = %d, want %d (cached fingerprints)", got, want)
	}
}

func TestInvalidateTagStillBatchesClientsWithTheSameIdentity(t *testing.T) {
	e := newTestEngine(t)
	a := trackClient(t, e)
	b := trackClient(t, e)
	e.tracker.SetAuth(a.ID, "alice", time.Now().Add(time.Hour))
	e.tracker.SetAuth(b.ID, "alice", time.Now().Add(time.Hour))

	var runs atomic.Int64
	e.RegisterQuery("me", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		ctx.TrackCollection("settings", "key", "me")
		id, _ := ctx.Auth.GetIdentity()
		return id, nil
	})
	subscribe(t, e, a, "me", "a", map[string]interface{}{})
	subscribe(t, e, b, "me", "b", map[string]interface{}{})
	drain(a)
	drain(b)
	if got, want := runs.Load(), int64(2); got != want {
		t.Fatalf("initial runs = %d, want %d", got, want)
	}

	e.invalidateTag("settings_key:me")
	if got, want := runs.Load(), int64(3); got != want {
		t.Errorf("runs after invalidate = %d, want %d (one batched execution)", got, want)
	}
	if data, n := lastQueryData(t, a); n != 1 || data != "alice" {
		t.Errorf("client a got %d pushes, data = %v; want one push of alice", n, data)
	}
	if data, n := lastQueryData(t, b); n != 1 || data != "alice" {
		t.Errorf("client b got %d pushes, data = %v; want one push of alice", n, data)
	}
}

func TestInvalidateTagSplitsBatchWhenQueryBecomesIdentityDependent(t *testing.T) {
	e := newTestEngine(t)
	alice := trackClient(t, e)
	bob := trackClient(t, e)
	e.tracker.SetAuth(alice.ID, "alice", time.Now().Add(time.Hour))
	e.tracker.SetAuth(bob.ID, "bob", time.Now().Add(time.Hour))

	var private atomic.Bool
	e.RegisterQuery("visibility", func(ctx *QueryCtx) (any, error) {
		ctx.TrackCollection("settings", "key", "visibility")
		if !private.Load() {
			return "public", nil
		}
		id, _ := ctx.Auth.GetIdentity()
		return "private:" + id, nil
	})
	params := map[string]interface{}{"k": "visibility"}
	aliceSub := subscribe(t, e, alice, "visibility", "alice", params)
	bobSub := subscribe(t, e, bob, "visibility", "bob", params)
	if data, _ := lastQueryData(t, alice); data != "public" {
		t.Fatalf("alice initial data = %v, want public", data)
	}
	if data, _ := lastQueryData(t, bob); data != "public" {
		t.Fatalf("bob initial data = %v, want public", data)
	}
	if fp := e.tracker.GetAuthFingerprint(aliceSub); fp != "" {
		t.Fatalf("alice auth fingerprint = %q, want empty before the private branch", fp)
	}
	if fp := e.tracker.GetAuthFingerprint(bobSub); fp != "" {
		t.Fatalf("bob auth fingerprint = %q, want empty before the private branch", fp)
	}

	private.Store(true)
	e.invalidateTag("settings_key:visibility")

	assertPrivate := func(client *reactivity.Client, want string) {
		t.Helper()
		msgs := queryMessages(t, drain(client))
		if len(msgs) != 1 {
			t.Fatalf("got %d query pushes, want 1: %v", len(msgs), msgs)
		}
		if msgs[0]["data"] != want {
			t.Errorf("data = %v, want %q", msgs[0]["data"], want)
		}
	}
	assertPrivate(alice, "private:alice")
	assertPrivate(bob, "private:bob")

	if !hasSubscription(e.tracker.GetSubscriptionsToTag("*user_identity:alice"), aliceSub.SubID) {
		t.Error("alice is missing *user_identity:alice")
	}
	if hasSubscription(e.tracker.GetSubscriptionsToTag("*user_identity:bob"), aliceSub.SubID) {
		t.Error("alice tracked bob's identity tag")
	}
	if !hasSubscription(e.tracker.GetSubscriptionsToTag("*user_identity:bob"), bobSub.SubID) {
		t.Error("bob is missing *user_identity:bob")
	}
	if hasSubscription(e.tracker.GetSubscriptionsToTag("*user_identity:alice"), bobSub.SubID) {
		t.Error("bob tracked alice's identity tag")
	}
}

func TestGuardInvalidationRerunsAttachedQuery(t *testing.T) {
	e := newTestEngine(t)
	e.CreateTable(&testRoomMember{})
	client := trackClient(t, e)
	e.tracker.SetAuth(client.ID, "user-7", time.Now().Add(time.Hour))
	if err := e.db.Create(&testRoomMember{UserID: "user-7", RoomID: "lobby"}).Error; err != nil {
		t.Fatalf("seed membership: %v", err)
	}

	e.RegisterGuard("hasAccess", func(ctx *GuardCtx) (any, error) {
		id, _ := ctx.Auth.GetIdentity()
		ctx.TrackCollection("room_members", "user_id", id)
		var member testRoomMember
		if err := ctx.DB.Where("user_id = ? AND room_id = ?", id, ctx.Params["room"]).First(&member).Error; err != nil {
			return map[string]interface{}{"error": "forbidden"}, nil
		}
		return map[string]interface{}{"access": true}, nil
	})
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		hasAccess, err := ctx.Auth.ExecuteGuard("hasAccess", map[string]interface{}{"room": "lobby"})
		if err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		access, _ := hasAccess.(map[string]interface{})
		if errMsg, _ := access["error"].(string); errMsg != "" {
			return map[string]interface{}{"error": errMsg}, nil
		}
		return map[string]interface{}{"ok": true}, nil
	})
	subscribe(t, e, client, "getMessages", "lobby", nil)
	drain(client)

	if err := e.db.Delete(&testRoomMember{UserID: "user-7", RoomID: "lobby"}).Error; err != nil {
		t.Fatalf("revoke membership: %v", err)
	}
	msgs := queryMessages(t, drain(client))
	if len(msgs) != 1 {
		t.Fatalf("got %d query pushes after revoke, want 1: %v", len(msgs), msgs)
	}
	data, _ := msgs[0]["data"].(map[string]interface{})
	if data["error"] != "forbidden" {
		t.Errorf("after revoke data = %v, want error=forbidden", data)
	}
}

func TestGuardPanicOnReevaluationDoesNotStopInvalidation(t *testing.T) {
	e := newTestEngine(t)
	panicker := trackClient(t, e)
	other := trackClient(t, e)
	e.tracker.SetAuth(panicker.ID, "user-panic", time.Now().Add(time.Hour))
	e.tracker.SetAuth(other.ID, "user-ok", time.Now().Add(time.Hour))

	var panicGuardEvals, okGuardEvals atomic.Int64
	e.RegisterGuard("role", func(ctx *GuardCtx) (any, error) {
		id, _ := ctx.Auth.GetIdentity()
		ctx.TrackCollection("room_members", "user_id", "shared")
		if id == "user-panic" {
			if panicGuardEvals.Add(1) > 1 {
				panic("guard boom")
			}
			return "first", nil
		}
		return fmt.Sprintf("ok-%d", okGuardEvals.Add(1)), nil
	})
	e.RegisterQuery("who", func(ctx *QueryCtx) (any, error) {
		role, err := ctx.Auth.ExecuteGuard("role", map[string]interface{}{})
		if err != nil {
			return "ERROR", nil
		}
		return role, nil
	})
	subscribe(t, e, panicker, "who", "panic", nil)
	subscribe(t, e, other, "who", "ok", nil)
	drain(panicker)
	drain(other)

	e.invalidateTag("room_members_user_id:shared")

	if panicGuardEvals.Load() != 2 {
		t.Fatalf("panicking guard evals = %d, want 2 (initial plus reevaluation)", panicGuardEvals.Load())
	}
	if okGuardEvals.Load() != 2 {
		t.Fatalf("other guard evals = %d, want 2", okGuardEvals.Load())
	}
	if data, n := lastQueryData(t, other); n != 1 || data != "ok-2" {
		t.Fatalf("other client after guard panic: %d pushes, data = %v; want one push of ok-2", n, data)
	}
}

func TestFailedGuardReevaluationRevokesCachedGrant(t *testing.T) {
	cases := []struct {
		name string
		fail func() (any, error)
	}{
		{"panic", func() (any, error) { panic("guard boom") }},
		{"unmarshalable result", func() (any, error) { return func() {}, nil }},
		{"error", func() (any, error) { return nil, errors.New("guard down") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEngine(t)
			client := trackClient(t, e)
			e.tracker.SetAuth(client.ID, "user-1", time.Now().Add(time.Hour))

			var guardEvals atomic.Int64
			e.RegisterGuard("canRead", func(ctx *GuardCtx) (any, error) {
				ctx.TrackTable("permissions")
				if guardEvals.Add(1) > 1 {
					return tc.fail()
				}
				return true, nil
			})
			var secretVersion atomic.Int64
			secretVersion.Store(1)
			e.RegisterQuery("secret", func(ctx *QueryCtx) (any, error) {
				ctx.TrackTable("secrets")
				allowed, err := ctx.Auth.ExecuteGuard("canRead", map[string]interface{}{})
				if err != nil || allowed != true {
					return "DENIED", nil
				}
				return fmt.Sprintf("SECRET-%d", secretVersion.Load()), nil
			})
			sub := subscribe(t, e, client, "secret", "secret", nil)
			if data, n := lastQueryData(t, client); n != 1 || data != "SECRET-1" {
				t.Fatalf("initial push: %d pushes, data = %v; want one push of SECRET-1", n, data)
			}

			e.invalidateTag("table_permissions:mutated")
			if guardEvals.Load() != 2 {
				t.Fatalf("guard evals after permissions change = %d, want 2", guardEvals.Load())
			}
			if fp := e.tracker.GetAuthFingerprint(sub); strings.Contains(fp, "*guard_") {
				t.Fatalf("query kept guard fingerprint after failed revalidation: %q", fp)
			}

			secretVersion.Store(2)
			e.invalidateTag("table_secrets:mutated")
			if guardEvals.Load() != 3 {
				t.Errorf("guard evals after secrets change = %d, want 3 (query must revalidate)", guardEvals.Load())
			}
			for _, msg := range queryMessages(t, drain(client)) {
				if msg["data"] == "SECRET-2" {
					t.Fatalf("client received SECRET-2 after failed guard revalidation")
				}
			}
		})
	}
}

func TestFailedAuthDoesNotSetIdentityOrPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	auth := &stubAuth{err: errors.New("bad token")}
	e.SetAuth(auth)

	var err error
	mustNoPanic(t, "failed auth", func() {
		err = e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "nope"})
	})
	if err == nil {
		t.Error("failed auth returned nil error")
	}
	if !auth.sawDB {
		t.Error("VerifyToken was not given a database")
	}
	if !slices.Equal(auth.tokens, []string{"nope"}) {
		t.Errorf("VerifyToken tokens = %v, want [nope]", auth.tokens)
	}
	if got, ok := e.tracker.GetAuth(client.ID); !ok || got.UserID != "" {
		t.Errorf("failed auth left UserID = %+v", got)
	}

	var sawError bool
	for _, msg := range drain(client) {
		if msg["type"] == "error" {
			sawError = true
		}
		if msg["type"] == "auth" {
			t.Errorf("failed auth sent a success payload: %v", msg)
		}
	}
	if !sawError {
		t.Error("failed auth did not send an error message")
	}
}

func TestAuthSuccessEncodesUserIDAsJSON(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.SetAuth(&stubAuth{userID: `user "quoted"`, expiresAt: time.Now().Add(time.Hour)})

	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
		t.Fatalf("auth: %v", err)
	}

	var raw string
	select {
	case b := <-client.Send:
		raw = string(b)
	default:
		t.Fatal("no auth message")
	}
	var msg map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		t.Fatalf("auth success payload is not valid JSON: %s (%v)", raw, err)
	}
	data, _ := msg["data"].(map[string]interface{})
	if data["user_id"] != `user "quoted"` {
		t.Errorf("user_id = %v, want the raw authenticated id", data["user_id"])
	}
}

func TestOutboundFramesIncludeProtocolVersion(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.SetAuth(&stubAuth{userID: "user-1", expiresAt: time.Now().Add(time.Hour)})
	e.RegisterQuery("item", func(ctx *QueryCtx) (any, error) { return "ok", nil })
	e.RegisterQuery("missing", func(ctx *QueryCtx) (any, error) {
		return nil, errors.New("gone")
	})
	e.RegisterMutation("save", func(ctx *MutationCtx) (any, error) { return "saved", nil })
	e.RegisterMutation("deny", func(ctx *MutationCtx) (any, error) {
		return nil, errors.New("nope")
	})

	check := func(label string) {
		t.Helper()
		var msgs []map[string]interface{}
		if !waitUntil(t, time.Second, func() bool {
			msgs = append(msgs, drain(client)...)
			return len(msgs) > 0
		}) {
			t.Fatalf("%s: no frames", label)
		}
		for _, msg := range msgs {
			if msg["protocol_version"] != float64(e.protocolVersion) {
				t.Errorf("%s frame = %#v, want protocol_version %d", label, msg, e.protocolVersion)
			}
		}
	}

	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
		t.Fatalf("auth: %v", err)
	}
	check("auth success")

	if err := e.onReceiveMessage(client.ID, map[string]interface{}{
		"type": "subscribe", "location": "item", "params": map[string]interface{}{}, "query_key": "item",
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	check("query result")

	if err := e.onReceiveMessage(client.ID, map[string]interface{}{
		"type": "subscribe", "location": "missing", "params": map[string]interface{}{}, "query_key": "missing",
	}); err != nil {
		t.Fatalf("subscribe error query: %v", err)
	}
	check("query error")

	if err := e.onReceiveMessage(client.ID, map[string]interface{}{
		"type": "mutation", "location": "save", "params": map[string]interface{}{}, "mutation_id": "m1",
	}); err != nil {
		t.Fatalf("mutation: %v", err)
	}
	check("mutation result")

	if err := e.onReceiveMessage(client.ID, map[string]interface{}{
		"type": "mutation", "location": "deny", "params": map[string]interface{}{}, "mutation_id": "m2",
	}); err != nil {
		t.Fatalf("mutation error: %v", err)
	}
	check("mutation error")

	if err := e.onReceiveMessage(client.ID, map[string]interface{}{
		"type": "mutation", "location": "absent", "params": map[string]interface{}{}, "mutation_id": "m3",
	}); err == nil {
		t.Fatal("missing mutation returned nil error")
	}
	check("mutation failure")

	_ = e.onReceiveMessage(client.ID, map[string]interface{}{
		"type": "subscribe", "location": "absent", "params": map[string]interface{}{}, "query_key": "absent",
	})
	check("query failure")

	_ = e.onReceiveMessage(client.ID, map[string]interface{}{"type": "unsubscribe"})
	check("invalid unsubscribe")

	_ = e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth"})
	check("invalid auth")

	e.SetAuth(&stubAuth{err: errors.New("bad token")})
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "nope"}); err == nil {
		t.Fatal("failed auth returned nil error")
	}
	check("auth failure")

	expiresAt := time.Now().Add(time.Hour)
	e.tracker.SetAuth(client.ID, "user-1", expiresAt)
	e.tracker.ExpireAuth(client.ID, expiresAt)
	check("identity expired")
}

// syncBuffer is a bytes.Buffer safe for concurrent use; websocket handler
// goroutines keep logging while the test reads the captured output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestAuthFrameDoesNotLeakToken(t *testing.T) {
	const secret = "audit-fake-secret"

	var logs syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	e := newTestEngine(t)
	auth := &stubAuth{userID: "user-1", expiresAt: time.Now().Add(time.Hour)}
	e.SetAuth(auth)
	if err := e.Profiler().Start(); err != nil {
		t.Fatalf("start profiler: %v", err)
	}
	t.Cleanup(e.Profiler().Stop)

	srv := httptest.NewServer(http.HandlerFunc(e.Handle))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if err := conn.WriteJSON(map[string]interface{}{"type": "auth", "token": secret}); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var reply map[string]interface{}
	if err := conn.ReadJSON(&reply); err != nil {
		t.Fatalf("read auth: %v", err)
	}
	if reply["success"] != true {
		t.Fatalf("auth reply = %v", reply)
	}
	if !slices.Contains(auth.tokens, secret) {
		t.Fatalf("VerifyToken tokens = %v, want the original secret", auth.tokens)
	}

	metrics := e.Profiler().DumpMetricsAndFlush()
	encoded, err := json.Marshal(metrics)
	if err != nil {
		t.Fatalf("encode metrics: %v", err)
	}
	if strings.Contains(string(encoded), secret) {
		t.Fatalf("raw metrics contain the token: %s", encoded)
	}
	var sawAuth bool
	for _, metric := range metrics {
		if metric.Type != MetricTypeAuthentication {
			continue
		}
		sawAuth = true
		if metric.Name != "authentication" {
			t.Errorf("auth metric name = %q, want authentication", metric.Name)
		}
	}
	if !sawAuth {
		t.Fatal("profiler did not record an authentication metric")
	}

	logged := logs.String()
	if strings.Contains(logged, secret) {
		t.Fatalf("debug logs contain the token:\n%s", logged)
	}
	for _, want := range []string{"WS: Received message", "Received message"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("debug logs missing %q:\n%s", want, logged)
		}
	}
}

// Login/refresh mutations take credentials in params and return them in
// results; neither may reach a framework log at any level.
func TestParamsAndResultsDoNotLeakToLogs(t *testing.T) {
	const (
		paramSecret  = "audit-synthetic-param-secret"
		resultSecret = "audit-synthetic-result-secret"
	)

	var logs syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	e := newTestEngine(t)
	e.RegisterMutation("login", func(ctx *MutationCtx) (any, error) {
		return map[string]interface{}{"refresh_token": resultSecret}, nil
	})
	e.RegisterMutation("internalLogin", func(ctx *MutationCtx) (any, error) {
		return map[string]interface{}{"refresh_token": resultSecret}, nil
	}, Internal())
	e.RegisterQuery("session", func(ctx *QueryCtx) (any, error) {
		return map[string]interface{}{"refresh_token": resultSecret}, nil
	})
	client := trackClient(t, e)
	params := func() map[string]interface{} {
		return map[string]interface{}{"password": paramSecret, "nested": map[string]interface{}{"x": paramSecret}}
	}

	frames := []map[string]interface{}{
		{"type": "mutation", "location": "login", "params": params(), "mutation_id": "m1"},
		{"type": "mutation", "location": "missing", "params": params(), "mutation_id": "m2"},
		{"type": "mutation", "location": "login", "params": params()},
		{"type": "subscribe", "location": "session", "params": params(), "query_key": "q1"},
		{"type": "subscribe", "location": "missing", "params": params(), "query_key": "q2"},
		{"type": "subscribe", "location": "session", "params": paramSecret, "query_key": "q3"},
		{"type": "unsubscribe", "location": "session", "params": params()},
		{"type": map[string]interface{}{"password": paramSecret}, "params": params()},
		{"type": "auth", "token": map[string]interface{}{"password": paramSecret}},
	}
	for _, frame := range frames {
		_ = e.onReceiveMessage(client.ID, frame)
	}
	if _, err := e.executeMutationInternal("internalLogin", params()); err != nil {
		t.Fatalf("executeMutationInternal: %v", err)
	}
	wantLogs := []string{"Executed mutation", "Executing query", "Failed to execute mutation", "Failed to execute query", "Invalid message"}
	if !waitUntil(t, time.Second, func() bool {
		logged := logs.String()
		for _, want := range wantLogs {
			if !strings.Contains(logged, want) {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("debug logs missing expected lines:\n%s", logs.String())
	}
	drain(client)

	logged := logs.String()
	for _, secret := range []string{paramSecret, resultSecret} {
		if strings.Contains(logged, secret) {
			t.Fatalf("debug logs contain %q:\n%s", secret, logged)
		}
	}
	for _, want := range wantLogs {
		if !strings.Contains(logged, want) {
			t.Fatalf("debug logs missing %q:\n%s", want, logged)
		}
	}
}

func TestMutationAuthMatchesTheCallingClient(t *testing.T) {
	e := newTestEngine(t)
	authed := trackClient(t, e)
	anon := trackClient(t, e)
	e.tracker.SetAuth(authed.ID, "user-1", time.Now().Add(time.Hour))

	type seen struct {
		id string
	}
	var fromAuthed, fromAnon seen
	e.RegisterMutation("whoami", func(ctx *MutationCtx) (any, error) {
		id, _ := ctx.Auth.GetIdentity()
		return map[string]interface{}{"id": id}, nil
	})

	mustNoPanic(t, "authed mutation", func() {
		result, err := e.executeMutation("whoami", map[string]interface{}{}, authed.ID, "m1")
		if err != nil {
			t.Errorf("ExecuteMutation: %v", err)
		}
		data := result.(map[string]interface{})
		fromAuthed = seen{id: data["id"].(string)}
	})
	mustNoPanic(t, "anon mutation", func() {
		result, err := e.executeMutation("whoami", map[string]interface{}{}, anon.ID, "m2")
		if err != nil {
			t.Errorf("ExecuteMutation: %v", err)
		}
		data := result.(map[string]interface{})
		fromAnon = seen{id: data["id"].(string)}
	})

	if fromAuthed.id != "user-1" {
		t.Errorf("authenticated mutation saw %+v, want id=user-1", fromAuthed)
	}
	if fromAnon.id != "" {
		t.Errorf("unauthenticated mutation GetIdentity = %q, want empty", fromAnon.id)
	}
}

func TestAuthExpiryClearsOnlyThatClient(t *testing.T) {
	e := newTestEngine(t)
	expiring := trackClient(t, e)
	kept := trackClient(t, e)
	expiresAt := time.Now().Add(40 * time.Millisecond)

	e.SetAuth(&stubAuth{userID: "temp", expiresAt: expiresAt})
	if err := e.onReceiveMessage(expiring.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
		t.Fatalf("auth expiring client: %v", err)
	}
	e.tracker.SetAuth(kept.ID, "kept", time.Now().Add(time.Hour))

	if !waitUntil(t, time.Second, func() bool {
		auth, ok := e.tracker.GetAuth(expiring.ID)
		if !ok {
			return false
		}
		return auth.UserID == ""
	}) {
		t.Fatal("expired auth was not cleared")
	}
	if auth, ok := e.tracker.GetAuth(kept.ID); !ok || auth.UserID != "kept" {
		t.Errorf("other client's auth was cleared: %+v", auth)
	}
}

// registerSecretQuery wires up a query guarded by an identity check. The guard
// allows only allowedUser; the query tracks table_refresh:mutated so tests can
// force re-runs.
func registerSecretQuery(e *Engine, allowedUser string, guardRuns *atomic.Int64) {
	e.RegisterGuard("isAllowed", func(ctx *GuardCtx) (any, error) {
		guardRuns.Add(1)
		id, _ := ctx.Auth.GetIdentity()
		return id != "" && id == allowedUser, nil
	})
	registerSecretQueryHandler(e)
}

func registerSecretQueryHandler(e *Engine) {
	e.RegisterQuery("secret", func(ctx *QueryCtx) (any, error) {
		ctx.TrackCollection("table", "refresh", "mutated")
		allowed, err := ctx.Auth.ExecuteGuard("isAllowed", map[string]interface{}{})
		if err != nil {
			return "ERROR", nil
		}
		if ok, _ := allowed.(bool); ok {
			return "SECRET", nil
		}
		return "DENIED", nil
	})
}

func emptyParamsHash() string {
	paramsJSON, _ := json.Marshal(map[string]interface{}{})
	return strconv.FormatUint(xxhash.Sum64(paramsJSON), 10)
}

func lastQueryData(t *testing.T, client *reactivity.Client) (interface{}, int) {
	t.Helper()
	msgs := queryMessages(t, drain(client))
	if len(msgs) == 0 {
		return nil, 0
	}
	return msgs[len(msgs)-1]["data"], len(msgs)
}

func waitForQueryData(t *testing.T, client *reactivity.Client, want interface{}) bool {
	t.Helper()
	return waitUntil(t, time.Second, func() bool {
		data, n := lastQueryData(t, client)
		return n > 0 && data == want
	})
}

func TestAuthExpiryRevokesGuardedSubscription(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	var guardRuns atomic.Int64
	registerSecretQuery(e, "alice", &guardRuns)

	e.SetAuth(&stubAuth{userID: "alice", expiresAt: time.Now().Add(100 * time.Millisecond)})
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
		t.Fatalf("auth: %v", err)
	}
	subscribe(t, e, client, "secret", "secret", nil)
	if data, _ := lastQueryData(t, client); data != "SECRET" {
		t.Fatalf("initial data = %v, want SECRET", data)
	}

	if !waitForQueryData(t, client, "DENIED") {
		t.Fatal("expiry did not push DENIED to the guarded subscription")
	}
	if auth, _ := e.tracker.GetAuth(client.ID); auth.UserID != "" {
		t.Fatalf("auth after expiry = %+v, want cleared", auth)
	}

	e.invalidateTag("table_refresh:mutated")
	data, n := lastQueryData(t, client)
	if n != 1 || data != "DENIED" {
		t.Errorf("after mutation got %d pushes, last = %v; want 1 push of DENIED", n, data)
	}
	if got := guardRuns.Load(); got < 2 {
		t.Errorf("guard runs = %d, want the guard re-executed after expiry", got)
	}
}

func TestReauthAsDifferentUserRevokesGuardedSubscription(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	var guardRuns atomic.Int64
	registerSecretQuery(e, "alice", &guardRuns)

	auth := &stubAuth{userID: "alice", expiresAt: time.Now().Add(time.Hour)}
	e.SetAuth(auth)
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "alice"}); err != nil {
		t.Fatalf("auth alice: %v", err)
	}
	sub := subscribe(t, e, client, "secret", "secret", nil)
	if data, _ := lastQueryData(t, client); data != "SECRET" {
		t.Fatalf("initial data = %v, want SECRET", data)
	}

	auth.userID = "bob"
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "bob"}); err != nil {
		t.Fatalf("auth bob: %v", err)
	}
	if data, n := lastQueryData(t, client); n == 0 || data != "DENIED" {
		t.Fatalf("after reauth as bob last push = %v (%d pushes), want DENIED", data, n)
	}

	e.invalidateTag("table_refresh:mutated")
	if data, n := lastQueryData(t, client); n != 1 || data != "DENIED" {
		t.Errorf("after mutation got %d pushes, last = %v; want 1 push of DENIED", n, data)
	}
	if hasSubscription(e.tracker.GetSubscriptionsToTag("*guard_isAllowed_"+emptyParamsHash()+":true"), sub.SubID) {
		t.Error("alice's cached guard fingerprint survived reauth")
	}
}

func TestTokenRefreshForSameUserKeepsGuardedSubscription(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	var guardRuns atomic.Int64
	registerSecretQuery(e, "alice", &guardRuns)

	auth := &stubAuth{userID: "alice", expiresAt: time.Now().Add(60 * time.Millisecond)}
	e.SetAuth(auth)
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "first"}); err != nil {
		t.Fatalf("auth: %v", err)
	}
	subscribe(t, e, client, "secret", "secret", nil)
	drain(client)

	auth.expiresAt = time.Now().Add(time.Hour)
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "refreshed"}); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	time.Sleep(120 * time.Millisecond)

	if got, _ := e.tracker.GetAuth(client.ID); got.UserID != "alice" {
		t.Fatalf("first token's expiry cleared the refreshed auth: %+v", got)
	}
	e.invalidateTag("table_refresh:mutated")
	if data, n := lastQueryData(t, client); n == 0 || data != "SECRET" {
		t.Errorf("after refresh last push = %v (%d pushes), want SECRET", data, n)
	}
	if got := guardRuns.Load(); got != 1 {
		t.Errorf("guard runs = %d, want 1 (same identity keeps the cached decision)", got)
	}
}

func TestAuthExpiryRerunsIdentityQuery(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.RegisterQuery("me", func(ctx *QueryCtx) (any, error) {
		id, _ := ctx.Auth.GetIdentity()
		return id, nil
	})

	e.SetAuth(&stubAuth{userID: "alice", expiresAt: time.Now().Add(60 * time.Millisecond)})
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
		t.Fatalf("auth: %v", err)
	}
	sub := subscribe(t, e, client, "me", "me", nil)
	if data, _ := lastQueryData(t, client); data != "alice" {
		t.Fatalf("initial data = %v, want alice", data)
	}

	if !waitForQueryData(t, client, "") {
		t.Fatal("expiry did not re-run the identity query")
	}
	if hasSubscription(e.tracker.GetSubscriptionsToTag("*user_identity:alice"), sub.SubID) {
		t.Error("stale *user_identity:alice tag survived expiry")
	}
}

func TestAuthExpiryAfterDisconnectDoesNotPanic(t *testing.T) {
	if os.Getenv("TETHER_TEST_CHILD") == "1" {
		e := newTestEngine(t)
		client := reactivity.NewClient(nil)
		e.tracker.Track(client)
		e.SetAuth(&stubAuth{userID: "temp", expiresAt: time.Now().Add(20 * time.Millisecond)})
		if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
			t.Fatalf("auth: %v", err)
		}
		e.tracker.Untrack(client)
		time.Sleep(80 * time.Millisecond)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestAuthExpiryAfterDisconnectDoesNotPanic$")
	cmd.Env = append(os.Environ(), "TETHER_TEST_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("auth expiry timer crashed after the client disconnected: %v\n%s", err, out)
	}
}

func TestAuthExpiryQueryPanicIsRecovered(t *testing.T) {
	if os.Getenv("TETHER_TEST_CHILD") == "1" {
		e := newTestEngine(t)
		client := trackClient(t, e)
		var runs atomic.Int64
		e.RegisterQuery("me", func(ctx *QueryCtx) (any, error) {
			if runs.Add(1) > 1 {
				panic("expiry boom")
			}
			id, _ := ctx.Auth.GetIdentity()
			return id, nil
		})
		expiresAt := time.Now().Add(40 * time.Millisecond)
		e.SetAuth(&stubAuth{userID: "alice", expiresAt: expiresAt})
		if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
			t.Fatalf("auth: %v", err)
		}
		subscribe(t, e, client, "me", "me", nil)

		if !waitUntil(t, time.Second, func() bool {
			auth, ok := e.tracker.GetAuth(client.ID)
			return ok && auth.UserID == ""
		}) {
			t.Fatal("expired auth was not cleared")
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestAuthExpiryQueryPanicIsRecovered$")
	cmd.Env = append(os.Environ(), "TETHER_TEST_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("auth expiry timer crashed when the re-run panicked: %v\n%s", err, out)
	}
}

// registerBlockingSecretQuery registers the secret query with an isAllowed
// guard (allowing only alice) that tracks table_guard:mutated and parks its
// blockOn-th invocation after it has read the caller's identity. entered is
// closed once that invocation is parked; release lets it return.
func registerBlockingSecretQuery(t *testing.T, e *Engine, blockOn int64) (entered <-chan struct{}, release func()) {
	t.Helper()
	registerSecretQueryHandler(e)
	enteredCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(releaseCh) }) }
	t.Cleanup(release)
	var runs atomic.Int64
	e.RegisterGuard("isAllowed", func(ctx *GuardCtx) (any, error) {
		ctx.TrackTable("guard")
		id, _ := ctx.Auth.GetIdentity()
		if runs.Add(1) == blockOn {
			close(enteredCh)
			<-releaseCh
		}
		return id == "alice", nil
	})
	return enteredCh, release
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// assertGuardRevoked checks that sub holds only the post-expiry false decision
// and keeps serving DENIED, no matter which fingerprint map iteration picks.
func assertGuardRevoked(t *testing.T, e *Engine, client *reactivity.Client, sub *reactivity.Subscription) {
	t.Helper()
	if data, n := lastQueryData(t, client); n != 0 {
		t.Errorf("stale guard pushed %d results after expiry, last = %v", n, data)
	}
	prefix := "*guard_isAllowed_" + emptyParamsHash() + ":"
	if e.tracker.SubscriptionHasTag(sub.SubID, prefix+"true") {
		t.Error("stale guard restored alice's true fingerprint")
	}
	if !e.tracker.SubscriptionHasTag(sub.SubID, prefix+"false") {
		t.Error("post-expiry false fingerprint is missing")
	}
	if subs := e.tracker.GetSubscriptionsToTag("*user_identity:alice"); len(subs) != 0 {
		t.Errorf("%d subscriptions still depend on alice's identity after expiry", len(subs))
	}
	for i := 0; i < 5; i++ {
		e.invalidateTag("table_refresh:mutated")
		if data, n := lastQueryData(t, client); n != 1 || data != "DENIED" {
			t.Fatalf("invalidation %d got %d pushes, last = %v; want 1 push of DENIED", i, n, data)
		}
	}
}

func TestAuthExpiryDuringFirstGuardRejectsStaleResult(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	entered, release := registerBlockingSecretQuery(t, e, 1)

	e.SetAuth(&stubAuth{userID: "alice", expiresAt: time.Now().Add(100 * time.Millisecond)})
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
		t.Fatalf("auth: %v", err)
	}
	sub := e.tracker.SubscribeToQuery(client.ID, "secret", "secret", map[string]interface{}{})
	if sub == nil {
		t.Fatal("SubscribeToQuery returned nil")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := e.executeQuery("secret", sub.Params, sub); err != nil {
			t.Errorf("in-flight executeQuery: %v", err)
		}
	}()
	waitClosed(t, entered, "the first guard to start")

	if !waitForQueryData(t, client, "DENIED") {
		t.Fatal("expiry did not push DENIED while the first guard was in flight")
	}
	release()
	waitClosed(t, done, "the in-flight query to finish")

	assertGuardRevoked(t, e, client, sub)
}

func TestAuthExpiryDuringGuardReevaluationRejectsStaleResult(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	entered, release := registerBlockingSecretQuery(t, e, 2)

	e.SetAuth(&stubAuth{userID: "alice", expiresAt: time.Now().Add(150 * time.Millisecond)})
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
		t.Fatalf("auth: %v", err)
	}
	sub := subscribe(t, e, client, "secret", "secret", nil)
	if data, _ := lastQueryData(t, client); data != "SECRET" {
		t.Fatalf("initial data = %v, want SECRET", data)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.invalidateTag("table_guard:mutated")
	}()
	waitClosed(t, entered, "the guard re-evaluation to start")

	if !waitForQueryData(t, client, "DENIED") {
		t.Fatal("expiry did not push DENIED while the guard re-evaluation was in flight")
	}
	release()
	waitClosed(t, done, "the in-flight invalidation to finish")

	assertGuardRevoked(t, e, client, sub)
}

func TestTokenRefreshDuringFirstGuardKeepsResult(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	entered, release := registerBlockingSecretQuery(t, e, 1)

	e.SetAuth(&stubAuth{userID: "alice", expiresAt: time.Now().Add(time.Hour)})
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "first"}); err != nil {
		t.Fatalf("auth: %v", err)
	}
	sub := e.tracker.SubscribeToQuery(client.ID, "secret", "secret", map[string]interface{}{})
	if sub == nil {
		t.Fatal("SubscribeToQuery returned nil")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := e.executeQuery("secret", sub.Params, sub); err != nil {
			t.Errorf("in-flight executeQuery: %v", err)
		}
	}()
	waitClosed(t, entered, "the first guard to start")

	if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "refreshed"}); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	release()
	waitClosed(t, done, "the in-flight query to finish")

	if data, n := lastQueryData(t, client); n != 1 || data != "SECRET" {
		t.Errorf("after same-user refresh got %d pushes, last = %v; want 1 push of SECRET", n, data)
	}
	if !e.tracker.SubscriptionHasTag(sub.SubID, "*guard_isAllowed_"+emptyParamsHash()+":true") {
		t.Error("same-user refresh dropped the in-flight guard decision")
	}
}

func TestConcurrentFirstGuardRunsKeepOneResult(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.tracker.SetAuth(client.ID, "alice", time.Now().Add(time.Hour))

	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCh) }) }
	t.Cleanup(release)

	var runs atomic.Int64
	bothStarted := make(chan struct{})
	e.RegisterGuard("isAllowed", func(ctx *GuardCtx) (any, error) {
		n := runs.Add(1)
		if n <= 2 {
			if n == 2 {
				close(bothStarted)
			}
			<-releaseCh
		}
		return n, nil
	})
	e.RegisterQuery("secret", func(ctx *QueryCtx) (any, error) {
		result, err := ctx.Auth.ExecuteGuard("isAllowed", map[string]interface{}{})
		if err != nil {
			return "ERROR", nil
		}
		return result, nil
	})

	sub := e.tracker.SubscribeToQuery(client.ID, "secret", "secret", map[string]interface{}{})
	if sub == nil {
		t.Fatal("SubscribeToQuery returned nil")
	}

	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			if _, err := e.executeQuery("secret", sub.Params, sub); err != nil {
				t.Errorf("executeQuery: %v", err)
			}
		}()
	}
	waitClosed(t, bothStarted, "both first guard runs to start")
	release()
	wg.Wait()

	prefix := "*guard_isAllowed_" + emptyParamsHash() + ":"
	kept := 0
	for _, value := range []string{"1", "2"} {
		if e.tracker.SubscriptionHasTag(sub.SubID, prefix+value) {
			kept++
		}
	}
	if kept != 1 {
		t.Errorf("guard results kept = %d, want 1", kept)
	}
}

func TestStaleGuardExecutionDoesNotOverwriteState(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var dep atomic.Value
	dep.Store("data1")
	var result atomic.Int64
	result.Store(1)
	var pause atomic.Bool
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseGuard := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseGuard()

	var runs atomic.Int64
	e.RegisterGuard("allow", func(ctx *GuardCtx) (any, error) {
		runs.Add(1)
		selected := dep.Load().(string)
		decision := result.Load()
		ctx.TrackCollection("perms", "id", selected)
		if pause.CompareAndSwap(true, false) {
			close(started)
			<-release
		}
		return decision, nil
	})
	e.RegisterQuery("secret", func(ctx *QueryCtx) (any, error) {
		allowed, err := ctx.Auth.ExecuteGuard("allow", map[string]interface{}{})
		if err != nil {
			return "ERROR", nil
		}
		return allowed, nil
	})

	sub := subscribe(t, e, client, "secret", "k", nil)
	drain(client)
	if len(sub.LinkedSubIDs) != 1 {
		t.Fatalf("linked guards = %d, want 1", len(sub.LinkedSubIDs))
	}
	guardID := sub.LinkedSubIDs[0]
	runs.Store(0)

	pause.Store(true)
	done := make(chan struct{})
	go func() {
		e.invalidateTags([]string{"perms_id:data1"}, "slow", "slow")
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("slow guard reevaluation did not start")
	}

	dep.Store("data2")
	result.Store(2)
	e.invalidateTags([]string{"perms_id:data1"}, "fast", "fast")
	releaseGuard()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("slow guard reevaluation did not finish")
	}

	if e.tracker.SubscriptionHasTag(guardID, "perms_id:data1") {
		t.Fatal("stale guard reevaluation restored dependency data1")
	}
	if !e.tracker.SubscriptionHasTag(guardID, "perms_id:data2") {
		t.Fatal("newest guard reevaluation's dependency data2 is missing")
	}
	fingerprint := "*guard_allow_" + emptyParamsHash() + ":"
	if e.tracker.SubscriptionHasTag(sub.SubID, fingerprint+"1") {
		t.Fatal("stale guard reevaluation restored result 1")
	}
	if !e.tracker.SubscriptionHasTag(sub.SubID, fingerprint+"2") {
		t.Fatal("newest guard result 2 is missing")
	}

	before := runs.Load()
	e.invalidateTag("perms_id:data2")
	if got := runs.Load() - before; got != 1 {
		t.Fatalf("invalidating data2 ran the guard %d times, want 1", got)
	}
	before = runs.Load()
	e.invalidateTag("perms_id:data1")
	if got := runs.Load() - before; got != 0 {
		t.Fatalf("invalidating stale data1 ran the guard %d times, want 0", got)
	}
}

func TestOnReceiveMessageSubscribeAndMutation(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var queryRuns atomic.Int64
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		queryRuns.Add(1)
		room := ctx.Params["room"].(string)
		ctx.TrackCollection("messages", "room_id", room)
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", room).Find(&msgs)
		return len(msgs), nil
	})
	e.RegisterMutation("createMessage", func(ctx *MutationCtx) (any, error) {
		msg := testMessage{Body: ctx.Params["body"].(string), RoomID: ctx.Params["room"].(string)}
		if err := ctx.DB.Create(&msg).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return msg.ID, nil
	})

	mustNoPanic(t, "subscribe", func() {
		if err := e.onReceiveMessage(client.ID, map[string]interface{}{
			"type":      "subscribe",
			"location":  "getMessages",
			"params":    map[string]interface{}{"room": "lobby"},
			"query_key": "lobby",
		}); err != nil {
			t.Errorf("subscribe: %v", err)
		}
	})
	if !waitUntil(t, time.Second, func() bool {
		return len(drain(client)) > 0
	}) {
		t.Fatal("initial query result was not sent")
	}
	if queryRuns.Load() != 1 {
		t.Fatalf("query runs after subscribe = %d, want 1", queryRuns.Load())
	}

	mustNoPanic(t, "mutation", func() {
		if err := e.onReceiveMessage(client.ID, map[string]interface{}{
			"type":        "mutation",
			"location":    "createMessage",
			"params":      map[string]interface{}{"body": "hi", "room": "lobby"},
			"mutation_id": "m-ws",
		}); err != nil {
			t.Errorf("mutation: %v", err)
		}
	})
	if queryRuns.Load() != 2 {
		t.Errorf("query runs after mutation message = %d, want 2", queryRuns.Load())
	}

	var sawMutation, sawQuery bool
	for _, msg := range drain(client) {
		switch msg["type"] {
		case "mutation":
			sawMutation = true
			if msg["mutation_id"] != "m-ws" {
				t.Errorf("mutation_id = %v, want m-ws", msg["mutation_id"])
			}
		case "query":
			sawQuery = true
			if msg["data"] != float64(1) && msg["data"] != 1 {
				t.Errorf("query data = %#v, want 1", msg["data"])
			}
		}
	}
	if !sawMutation || !sawQuery {
		t.Errorf("sawMutation=%v sawQuery=%v, want both", sawMutation, sawQuery)
	}
}

func TestInitialSubscriptionsDoNotBlockEachOther(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseSlow := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseSlow()

	e.RegisterQuery("slow", func(ctx *QueryCtx) (any, error) {
		close(started)
		<-release
		return "slow", nil
	})
	e.RegisterQuery("fast", func(ctx *QueryCtx) (any, error) {
		return "fast", nil
	})

	subscribed := make(chan error, 1)
	go func() {
		subscribed <- e.onReceiveMessage(client.ID, map[string]interface{}{
			"type":      "subscribe",
			"location":  "slow",
			"params":    map[string]interface{}{},
			"query_key": "slow",
		})
	}()
	select {
	case err := <-subscribed:
		if err != nil {
			t.Fatalf("slow subscribe: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow subscribe blocked the connection")
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("slow query did not start")
	}

	if err := e.onReceiveMessage(client.ID, map[string]interface{}{
		"type":      "subscribe",
		"location":  "fast",
		"params":    map[string]interface{}{},
		"query_key": "fast",
	}); err != nil {
		t.Fatalf("fast subscribe: %v", err)
	}
	if !waitUntil(t, time.Second, func() bool {
		for _, msg := range queryMessages(t, drain(client)) {
			if msg["query_key"] == "fast" && msg["data"] == "fast" {
				return true
			}
		}
		return false
	}) {
		t.Fatal("fast query did not complete while the slow query was still running")
	}
	releaseSlow()
}

func TestUnsubscribeDropsInFlightInitialResult(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseQuery := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseQuery()

	e.RegisterQuery("slow", func(ctx *QueryCtx) (any, error) {
		close(started)
		<-release
		return "late", nil
	})
	params := map[string]interface{}{}
	subscribed := make(chan error, 1)
	go func() {
		subscribed <- e.onReceiveMessage(client.ID, map[string]interface{}{
			"type":      "subscribe",
			"location":  "slow",
			"params":    params,
			"query_key": "slow",
		})
	}()
	select {
	case err := <-subscribed:
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscribe blocked the connection")
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("query did not start")
	}

	if err := e.onReceiveMessage(client.ID, map[string]interface{}{
		"type":     "unsubscribe",
		"location": "slow",
		"params":   params,
	}); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	releaseQuery()
	e.Close()
	if got := queryMessages(t, drain(client)); len(got) != 0 {
		t.Fatalf("in-flight subscribe delivered after unsubscribe: %v", got)
	}
}

func TestUnsubscribeMessageStopsQueryPushes(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var queryRuns atomic.Int64
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		queryRuns.Add(1)
		room := ctx.Params["room"].(string)
		ctx.TrackCollection("messages", "room_id", room)
		return room, nil
	})

	params := map[string]interface{}{"room": "lobby"}
	if err := e.onReceiveMessage(client.ID, map[string]interface{}{
		"type":      "subscribe",
		"location":  "getMessages",
		"params":    params,
		"query_key": "lobby",
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	var initial []map[string]interface{}
	if !waitUntil(t, time.Second, func() bool {
		initial = append(initial, queryMessages(t, drain(client))...)
		return len(initial) >= 1
	}) {
		t.Fatalf("initial query pushes = %d, want 1", len(initial))
	}
	if len(initial) != 1 {
		t.Fatalf("initial query pushes = %d, want 1", len(initial))
	}
	if initial[0]["query_key"] != "lobby" {
		t.Fatalf("query_key = %v, want lobby", initial[0]["query_key"])
	}

	if err := e.onReceiveMessage(client.ID, map[string]interface{}{
		"type":      "unsubscribe",
		"location":  "getMessages",
		"params":    params,
		"query_key": "lobby",
	}); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}

	e.invalidateTag("messages_room_id:lobby")
	if got := queryMessages(t, drain(client)); len(got) != 0 {
		t.Errorf("query pushes after unsubscribe = %d, want 0", len(got))
	}
	if got := queryRuns.Load(); got != 1 {
		t.Errorf("query runs after unsubscribe = %d, want 1", got)
	}
}

func TestOnReceiveMessageUnknownTypeDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	mustNoPanic(t, "unknown type", func() {
		if err := e.onReceiveMessage(client.ID, map[string]interface{}{"type": "ping"}); err != nil {
			t.Errorf("unknown type returned error: %v", err)
		}
	})
}

func TestSetAllowedOrigins(t *testing.T) {
	e := newTestEngine(t)
	e.SetAllowedOrigins([]string{"https://app.example", "https://admin.example"})

	allowed := e.websocketHelper.CheckOrigin(&http.Request{Header: http.Header{"Origin": []string{"https://app.example"}}})
	denied := e.websocketHelper.CheckOrigin(&http.Request{Header: http.Header{"Origin": []string{"https://evil.example"}}})
	if !allowed {
		t.Error("allowed origin was rejected")
	}
	if denied {
		t.Error("unknown origin was accepted")
	}
}

func TestSetCheckOrigin(t *testing.T) {
	e := newTestEngine(t)
	e.SetCheckOrigin(func(r *http.Request) bool { return r.Header.Get("Origin") == "ok" })
	if !e.websocketHelper.CheckOrigin(&http.Request{Header: http.Header{"Origin": []string{"ok"}}}) {
		t.Error("custom CheckOrigin rejected a valid origin")
	}
}

func TestStorageRoutesUseCheckOrigin(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	e.SetStorage(store, "")
	e.SetAllowedOrigins([]string{"https://app.example"})

	upload, err := e.getUploadURL()
	if err != nil {
		t.Fatalf("getUploadURL: %v", err)
	}
	fileID := upload.FileID
	uploadPath := upload.UploadURL

	t.Run("preflight allowed", func(t *testing.T) {
		rec := serveStorage(e, http.MethodOptions, uploadPath, nil, map[string]string{
			"Origin":                         "https://app.example",
			"Access-Control-Request-Method":  "PUT",
			"Access-Control-Request-Headers": "content-type",
		})
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example" {
			t.Fatalf("Allow-Origin = %q", got)
		}
		if got := rec.Header().Get("Vary"); got != "Origin" {
			t.Fatalf("Vary = %q, want Origin", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(got, "PUT") {
			t.Fatalf("Allow-Methods = %q", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "content-type" {
			t.Fatalf("Allow-Headers = %q", got)
		}
	})

	t.Run("preflight denied", func(t *testing.T) {
		rec := serveStorage(e, http.MethodOptions, uploadPath, nil, map[string]string{
			"Origin": "https://evil.example",
		})
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("denied origin received Allow-Origin %q", got)
		}
		if got := rec.Header().Get("Vary"); got != "Origin" {
			t.Fatalf("Vary = %q, want Origin", got)
		}
	})

	t.Run("put denied keeps token", func(t *testing.T) {
		rec := serveStorage(e, http.MethodPut, uploadPath, strings.NewReader("no"), map[string]string{
			"Origin":       "https://evil.example",
			"Content-Type": "text/plain",
		})
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
		}
	})

	t.Run("put allowed", func(t *testing.T) {
		rec := serveStorage(e, http.MethodPut, uploadPath, strings.NewReader("hello"), map[string]string{
			"Origin":       "https://app.example",
			"Content-Type": "text/plain",
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example" {
			t.Fatalf("Allow-Origin = %q", got)
		}
		body, err := os.ReadFile(filepath.Join(store.UploadDir, fileID))
		if err != nil {
			t.Fatalf("read upload: %v", err)
		}
		if string(body) != "hello" {
			t.Fatalf("uploaded body = %q", body)
		}
	})

	t.Run("put without origin", func(t *testing.T) {
		next, err := e.getUploadURL()
		if err != nil {
			t.Fatalf("getUploadURL: %v", err)
		}
		path := next.UploadURL
		rec := serveStorage(e, http.MethodPut, path, strings.NewReader("server"), map[string]string{
			"Content-Type": "text/plain",
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("missing origin received Allow-Origin %q", got)
		}
		if got := rec.Header().Get("Vary"); got != "Origin" {
			t.Fatalf("Vary = %q, want Origin", got)
		}
	})

	downloadPath, err := e.getDownloadURL(fileID)
	if err != nil {
		t.Fatalf("getDownloadURL: %v", err)
	}

	t.Run("download allowed", func(t *testing.T) {
		rec := serveStorage(e, http.MethodGet, downloadPath, nil, map[string]string{
			"Origin": "https://app.example",
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example" {
			t.Fatalf("Allow-Origin = %q", got)
		}
		if got := rec.Header().Get("Vary"); got != "Origin" {
			t.Fatalf("Vary = %q, want Origin", got)
		}
		if rec.Body.String() != "hello" {
			t.Fatalf("download body = %q", rec.Body.String())
		}
	})

	t.Run("download without origin", func(t *testing.T) {
		rec := serveStorage(e, http.MethodGet, downloadPath, nil, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("missing origin received Allow-Origin %q", got)
		}
		if got := rec.Header().Get("Vary"); got != "Origin" {
			t.Fatalf("Vary = %q, want Origin", got)
		}
		if rec.Header().Get("Last-Modified") == "" {
			t.Fatal("download without origin has no Last-Modified")
		}
		if got := rec.Header().Get("Cache-Control"); got != "" {
			t.Fatalf("Cache-Control = %q, want empty so the response stays cacheable", got)
		}
		if rec.Body.String() != "hello" {
			t.Fatalf("download body = %q", rec.Body.String())
		}
	})

	t.Run("download denied", func(t *testing.T) {
		rec := serveStorage(e, http.MethodGet, downloadPath, nil, map[string]string{
			"Origin": "https://evil.example",
		})
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
		}
		if got := rec.Header().Get("Vary"); got != "Origin" {
			t.Fatalf("Vary = %q, want Origin", got)
		}
	})
}

func TestStorageRoutesDefaultToSameOrigin(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	e.SetStorage(store, "")

	upload, err := e.getUploadURL()
	if err != nil {
		t.Fatalf("getUploadURL: %v", err)
	}
	uploadPath := upload.UploadURL

	denied := serveStorage(e, http.MethodOptions, uploadPath, nil, map[string]string{
		"Origin": "https://app.example",
	})
	if denied.Code != http.StatusForbidden {
		t.Fatalf("cross-origin status = %d, want %d", denied.Code, http.StatusForbidden)
	}

	allowed := serveStorage(e, http.MethodPut, uploadPath, strings.NewReader("same"), map[string]string{
		"Origin":       "https://example.com",
		"Content-Type": "text/plain",
	})
	if allowed.Code != http.StatusOK {
		t.Fatalf("same-origin status = %d, body %s", allowed.Code, allowed.Body.String())
	}
	if got := allowed.Header().Get("Access-Control-Allow-Origin"); got != "https://example.com" {
		t.Fatalf("Allow-Origin = %q", got)
	}
}

func TestStorageRoutesCustomCheckOrigin(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	e.SetStorage(store, "")
	e.SetCheckOrigin(func(r *http.Request) bool {
		return strings.HasSuffix(r.Header.Get("Origin"), ".example")
	})

	upload, err := e.getUploadURL()
	if err != nil {
		t.Fatalf("getUploadURL: %v", err)
	}
	uploadPath := upload.UploadURL

	allowed := serveStorage(e, http.MethodOptions, uploadPath, nil, map[string]string{
		"Origin": "https://app.example",
	})
	if allowed.Code != http.StatusNoContent {
		t.Fatalf("allowed status = %d, want %d", allowed.Code, http.StatusNoContent)
	}

	denied := serveStorage(e, http.MethodOptions, uploadPath, nil, map[string]string{
		"Origin": "https://app.other",
	})
	if denied.Code != http.StatusForbidden {
		t.Fatalf("denied status = %d, want %d", denied.Code, http.StatusForbidden)
	}
}

func TestDeleteFileRequiresStoredObjectAndRejectsPathEscape(t *testing.T) {
	e := newTestEngine(t)
	root := t.TempDir()
	uploadDir := filepath.Join(root, "uploads")
	store, err := local.New(uploadDir)
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	recorder := &recordingStorage{local: store}
	e.SetStorage(recorder, "")

	sibling := filepath.Join(root, "outside.txt")
	if err := os.WriteFile(sibling, []byte("sibling"), 0o644); err != nil {
		t.Fatalf("write sibling: %v", err)
	}
	absTarget := filepath.Join(root, "absolute.txt")
	if err := os.WriteFile(absTarget, []byte("absolute"), 0o644); err != nil {
		t.Fatalf("write absolute target: %v", err)
	}

	storageCtx := &StorageCtx{DeleteFile: e.deleteFile}

	t.Run("parent path without metadata", func(t *testing.T) {
		err := storageCtx.DeleteFile("../outside.txt")
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			t.Fatalf("DeleteFile: %v, want record not found", err)
		}
		if len(recorder.deleted) != 0 {
			t.Fatalf("adapter Delete called with %v", recorder.deleted)
		}
		if _, err := os.Stat(sibling); err != nil {
			t.Fatalf("sibling file was removed: %v", err)
		}
	})

	t.Run("absolute path without metadata", func(t *testing.T) {
		err := storageCtx.DeleteFile(absTarget)
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			t.Fatalf("DeleteFile: %v, want record not found", err)
		}
		if len(recorder.deleted) != 0 {
			t.Fatalf("adapter Delete called with %v", recorder.deleted)
		}
		if _, err := os.Stat(absTarget); err != nil {
			t.Fatalf("absolute target was removed: %v", err)
		}
	})

	t.Run("nonexistent metadata", func(t *testing.T) {
		err := storageCtx.DeleteFile("missing-file")
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			t.Fatalf("DeleteFile: %v, want record not found", err)
		}
		if len(recorder.deleted) != 0 {
			t.Fatalf("adapter Delete called with %v", recorder.deleted)
		}
	})

	t.Run("stored parent path stays contained", func(t *testing.T) {
		if err := e.db.Create(&TetherStorage{
			ID:        "../outside.txt",
			Token:     "parent-token",
			Status:    "active",
			ExpiresAt: time.Now().Add(time.Hour),
		}).Error; err != nil {
			t.Fatalf("create storage row: %v", err)
		}
		err := storageCtx.DeleteFile("../outside.txt")
		if err == nil {
			t.Fatal("stored parent path delete returned nil")
		}
		if _, statErr := os.Stat(sibling); statErr != nil {
			t.Fatalf("sibling file was removed: %v", statErr)
		}
		var remaining int64
		if err := e.db.Model(&TetherStorage{}).Where("id = ?", "../outside.txt").Count(&remaining).Error; err != nil {
			t.Fatalf("count storage row: %v", err)
		}
		if remaining != 1 {
			t.Fatalf("storage rows = %d, want 1 after rejected delete", remaining)
		}
	})
}

func TestGetUploadURLUsesSetStorageDefaultsAndCallOptions(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	if err := e.SetStorage(store, "", storage.WithMaxBytes(1234), storage.WithExpiresIn(time.Hour)); err != nil {
		t.Fatalf("set storage: %v", err)
	}

	started := time.Now()
	upload, err := e.getUploadURL()
	if err != nil {
		t.Fatalf("getUploadURL: %v", err)
	}
	var row TetherStorage
	if err := e.db.Where("id = ?", upload.FileID).First(&row).Error; err != nil {
		t.Fatalf("load upload: %v", err)
	}
	if !strings.HasPrefix(upload.UploadURL, "/storage/upload/") {
		t.Fatalf("upload URL = %q, want /storage/upload/ prefix", upload.UploadURL)
	}
	if row.MaxBytes != 1234 {
		t.Fatalf("default MaxBytes = %d, want 1234", row.MaxBytes)
	}
	if row.ExpiresAt.Before(started.Add(time.Hour-time.Second)) || row.ExpiresAt.After(time.Now().Add(time.Hour+time.Second)) {
		t.Fatalf("default ExpiresAt = %s, want about 1h from now", row.ExpiresAt)
	}

	overridden, err := e.getUploadURL(storage.WithMaxBytes(99))
	if err != nil {
		t.Fatalf("getUploadURL override: %v", err)
	}
	row = TetherStorage{}
	if err := e.db.Where("id = ?", overridden.FileID).First(&row).Error; err != nil {
		t.Fatalf("load override: %v", err)
	}
	if row.MaxBytes != 99 {
		t.Fatalf("override MaxBytes = %d, want 99", row.MaxBytes)
	}
	if row.ExpiresAt.Before(started.Add(time.Hour-time.Second)) || row.ExpiresAt.After(time.Now().Add(time.Hour+time.Second)) {
		t.Fatalf("override ExpiresAt = %s, want the SetStorage default of about 1h", row.ExpiresAt)
	}
	if row.Public {
		t.Fatal("upload without storage.Public was public")
	}
}

func TestPublicFilesAreServedByID(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	if err := e.SetStorage(store, ""); err != nil {
		t.Fatalf("set storage: %v", err)
	}

	upload, err := e.getUploadURL(storage.Public())
	if err != nil {
		t.Fatalf("getUploadURL: %v", err)
	}
	put := serveStorage(e, http.MethodPut, upload.UploadURL, strings.NewReader("hello"), map[string]string{
		"Content-Type": "text/plain",
	})
	if put.Code != http.StatusOK {
		t.Fatalf("upload status = %d, body %s", put.Code, put.Body.String())
	}

	var row TetherStorage
	if err := e.db.Where("id = ?", upload.FileID).First(&row).Error; err != nil {
		t.Fatalf("load upload: %v", err)
	}
	if !row.Public || row.Status != "active" || row.MimeType != "text/plain" {
		t.Fatalf("public upload = %+v, want active public text/plain", row)
	}

	got := serveStorage(e, http.MethodGet, "/storage/public/"+upload.FileID, nil, nil)
	if got.Code != http.StatusOK || got.Body.String() != "hello" {
		t.Fatalf("public file status = %d, body %q", got.Code, got.Body.String())
	}
	if ct := got.Header().Get("Content-Type"); ct != "text/plain" {
		t.Fatalf("Content-Type = %q, want text/plain", ct)
	}
	if nosniff := got.Header().Get("X-Content-Type-Options"); nosniff != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", nosniff)
	}
	if disp := got.Header().Get("Content-Disposition"); disp != `inline; filename="`+upload.FileID+`"` {
		t.Fatalf("Content-Disposition = %q, want inline", disp)
	}

	download, err := e.getDownloadURL(upload.FileID)
	if err != nil {
		t.Fatalf("getDownloadURL: %v", err)
	}
	tokenDownload := serveStorage(e, http.MethodGet, download, nil, nil)
	if tokenDownload.Code != http.StatusOK || tokenDownload.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("token download status = %d, nosniff = %q", tokenDownload.Code, tokenDownload.Header().Get("X-Content-Type-Options"))
	}

	private, err := e.getUploadURL()
	if err != nil {
		t.Fatalf("private getUploadURL: %v", err)
	}
	privPut := serveStorage(e, http.MethodPut, private.UploadURL, strings.NewReader("secret"), map[string]string{
		"Content-Type": "text/plain",
	})
	if privPut.Code != http.StatusOK {
		t.Fatalf("private upload status = %d, body %s", privPut.Code, privPut.Body.String())
	}
	hidden := serveStorage(e, http.MethodGet, "/storage/public/"+private.FileID, nil, nil)
	if hidden.Code != http.StatusNotFound {
		t.Fatalf("private public-route status = %d, want 404", hidden.Code)
	}
	escaped := serveStorage(e, http.MethodGet, "/storage/public/../file/"+strings.TrimPrefix(download, "/storage/file/"), nil, nil)
	if escaped.Code != http.StatusNotFound || escaped.Body.String() == "hello" {
		t.Fatalf("escaped public path status = %d, body %q", escaped.Code, escaped.Body.String())
	}

	pending, err := e.getUploadURL(storage.Public())
	if err != nil {
		t.Fatalf("pending getUploadURL: %v", err)
	}
	notReady := serveStorage(e, http.MethodGet, "/storage/public/"+pending.FileID, nil, nil)
	if notReady.Code != http.StatusNotFound {
		t.Fatalf("pending public file status = %d, want 404", notReady.Code)
	}
	missing := serveStorage(e, http.MethodGet, "/storage/public/missing", nil, nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing public file status = %d, want 404", missing.Code)
	}

	svg, err := e.getUploadURL(storage.Public())
	if err != nil {
		t.Fatalf("svg getUploadURL: %v", err)
	}
	svgPut := serveStorage(e, http.MethodPut, svg.UploadURL, strings.NewReader("<svg></svg>"), map[string]string{
		"Content-Type": "image/svg+xml",
	})
	if svgPut.Code != http.StatusOK {
		t.Fatalf("svg upload status = %d, body %s", svgPut.Code, svgPut.Body.String())
	}
	svgGot := serveStorage(e, http.MethodGet, "/storage/public/"+svg.FileID, nil, nil)
	if svgGot.Code != http.StatusOK {
		t.Fatalf("svg status = %d, body %s", svgGot.Code, svgGot.Body.String())
	}
	if ct := svgGot.Header().Get("Content-Type"); ct != "image/svg+xml" {
		t.Fatalf("svg Content-Type = %q", ct)
	}
	if disp := svgGot.Header().Get("Content-Disposition"); disp != `attachment; filename="`+svg.FileID+`"` {
		t.Fatalf("svg Content-Disposition = %q, want attachment", disp)
	}
	if svgGot.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("svg response missing nosniff")
	}

	if err := e.db.Create(&TetherStorage{ID: "ghost", Status: "active", Public: true, MimeType: "text/plain"}).Error; err != nil {
		t.Fatalf("create ghost row: %v", err)
	}
	ghost := serveStorage(e, http.MethodGet, "/storage/public/ghost", nil, nil)
	if ghost.Code != http.StatusInternalServerError {
		t.Fatalf("missing bytes status = %d, want 500", ghost.Code)
	}
}

func TestPublicUploadDefaultAndCustomBasePath(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	if err := e.SetStorage(store, "/api/files", storage.Public()); err != nil {
		t.Fatalf("set storage: %v", err)
	}

	upload, err := e.getUploadURL()
	if err != nil {
		t.Fatalf("getUploadURL: %v", err)
	}
	var row TetherStorage
	if err := e.db.Where("id = ?", upload.FileID).First(&row).Error; err != nil {
		t.Fatalf("load upload: %v", err)
	}
	if !row.Public {
		t.Fatal("SetStorage Public() default left the upload private")
	}
	put := serveStorage(e, http.MethodPut, upload.UploadURL, strings.NewReader("shared"), map[string]string{
		"Content-Type": "text/plain",
	})
	if put.Code != http.StatusOK {
		t.Fatalf("upload status = %d, body %s", put.Code, put.Body.String())
	}

	got := serveStorage(e, http.MethodGet, "/api/files/public/"+upload.FileID, nil, nil)
	if got.Code != http.StatusOK || got.Body.String() != "shared" {
		t.Fatalf("custom public route status = %d, body %q", got.Code, got.Body.String())
	}
	missed := serveStorage(e, http.MethodGet, "/storage/public/"+upload.FileID, nil, nil)
	if missed.Body.String() == "shared" {
		t.Fatal("default public route served a custom-prefix file")
	}
}

func TestPutFile(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	if err := e.SetStorage(store, ""); err != nil {
		t.Fatalf("set storage: %v", err)
	}

	e.RegisterMutation("save", func(ctx *MutationCtx) (any, error) {
		return ctx.Storage.PutFile("text/plain", strings.NewReader("hello-from-mutation"), storage.WithMaxBytes(1), storage.WithExpiresIn(time.Second))
	}, Internal())
	saved, err := e.ExecuteMutation("save", nil)
	if err != nil {
		t.Fatalf("PutFile: %v", err)
	}
	fileID, ok := saved.(string)
	if !ok || fileID == "" {
		t.Fatalf("PutFile result = %#v, want a file ID", saved)
	}

	body, err := os.ReadFile(filepath.Join(store.UploadDir, fileID))
	if err != nil {
		t.Fatalf("read stored file: %v", err)
	}
	if string(body) != "hello-from-mutation" {
		t.Fatalf("stored body = %q", body)
	}
	var row TetherStorage
	if err := e.db.Where("id = ?", fileID).First(&row).Error; err != nil {
		t.Fatalf("load file: %v", err)
	}
	if row.Status != "active" || row.MimeType != "text/plain" || row.Public || row.MaxBytes != 0 || !row.ExpiresAt.IsZero() {
		t.Fatalf("file row = %+v, want active private text/plain with size and lifetime options ignored", row)
	}
	hidden := serveStorage(e, http.MethodGet, "/storage/public/"+fileID, nil, nil)
	if hidden.Code != http.StatusNotFound {
		t.Fatalf("private PutFile public-route status = %d, want 404", hidden.Code)
	}
	download, err := e.getDownloadURL(fileID)
	if err != nil {
		t.Fatalf("getDownloadURL: %v", err)
	}
	got := serveStorage(e, http.MethodGet, download, nil, nil)
	if got.Code != http.StatusOK || got.Body.String() != "hello-from-mutation" {
		t.Fatalf("download status = %d, body %q", got.Code, got.Body.String())
	}

	client := trackClient(t, e)
	e.RegisterMutation("savePublic", func(ctx *MutationCtx) (any, error) {
		return ctx.Storage.PutFile("text/plain", strings.NewReader("public-bytes"), storage.Public())
	})
	clientSaved, err := e.executeMutation("savePublic", nil, client.ID, "m-put")
	if err != nil {
		t.Fatalf("client PutFile: %v", err)
	}
	publicID, ok := clientSaved.(string)
	if !ok {
		t.Fatalf("client PutFile result = %#v, want a file ID", clientSaved)
	}
	publicGot := serveStorage(e, http.MethodGet, "/storage/public/"+publicID, nil, nil)
	if publicGot.Code != http.StatusOK || publicGot.Body.String() != "public-bytes" {
		t.Fatalf("public PutFile status = %d, body %q", publicGot.Code, publicGot.Body.String())
	}
	if disp := publicGot.Header().Get("Content-Disposition"); disp != `inline; filename="`+publicID+`"` {
		t.Fatalf("Content-Disposition = %q, want inline", disp)
	}

	e.RegisterMutation("remove", func(ctx *MutationCtx) (any, error) {
		return nil, ctx.Storage.DeleteFile(publicID)
	}, Internal())
	if _, err := e.ExecuteMutation("remove", nil); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	afterDelete := serveStorage(e, http.MethodGet, "/storage/public/"+publicID, nil, nil)
	if afterDelete.Code != http.StatusNotFound {
		t.Fatalf("deleted public file status = %d, want 404", afterDelete.Code)
	}
}

func TestPutFileHonorsPublicDefault(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	if err := e.SetStorage(store, "", storage.Public()); err != nil {
		t.Fatalf("set storage: %v", err)
	}
	e.RegisterMutation("save", func(ctx *MutationCtx) (any, error) {
		return ctx.Storage.PutFile("application/octet-stream", strings.NewReader("bin"))
	}, Internal())
	saved, err := e.ExecuteMutation("save", nil)
	if err != nil {
		t.Fatalf("PutFile: %v", err)
	}
	fileID := saved.(string)
	got := serveStorage(e, http.MethodGet, "/storage/public/"+fileID, nil, nil)
	if got.Code != http.StatusOK || got.Body.String() != "bin" {
		t.Fatalf("default-public file status = %d, body %q", got.Code, got.Body.String())
	}
	if disp := got.Header().Get("Content-Disposition"); disp != `attachment; filename="`+fileID+`"` {
		t.Fatalf("Content-Disposition = %q, want attachment", disp)
	}
	if got.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("public file response missing nosniff")
	}
}

func TestPutFileRequiresStorage(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterMutation("save", func(ctx *MutationCtx) (any, error) {
		_, err := ctx.Storage.PutFile("text/plain", strings.NewReader("nope"))
		return nil, err
	}, Internal())
	_, err := e.ExecuteMutation("save", nil)
	if err == nil || err.Error() != "storage not configured" {
		t.Fatalf("PutFile error = %v, want storage not configured", err)
	}
}

func TestPutFileDeniedInQueries(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	if err := e.SetStorage(store, ""); err != nil {
		t.Fatalf("set storage: %v", err)
	}
	e.RegisterQuery("save", func(ctx *QueryCtx) (any, error) {
		_, err := ctx.Storage.PutFile("text/plain", strings.NewReader("nope"))
		return nil, err
	})

	client := trackClient(t, e)
	sub := e.tracker.SubscribeToQuery(client.ID, "save", "k", map[string]interface{}{})
	raw, err := e.executeQuery("save", map[string]interface{}{}, sub)
	if err != nil {
		t.Fatalf("executeQuery: %v", err)
	}
	var msg map[string]interface{}
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("decode query frame: %v", err)
	}
	if msg["type"] != "error" || msg["error"] != "tether: this capability is not available in this context" {
		t.Fatalf("client query frame = %#v, want capability denied", msg)
	}
	_, err = e.ExecuteQuery("save", nil)
	if err == nil || err.Error() != "tether: this capability is not available in this context" {
		t.Fatalf("ExecuteQuery PutFile error = %v, want capability denied", err)
	}
	entries, err := os.ReadDir(store.UploadDir)
	if err != nil {
		t.Fatalf("read upload dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("query PutFile wrote %d files", len(entries))
	}
	var rows int64
	if err := e.db.Model(&TetherStorage{}).Count(&rows).Error; err != nil {
		t.Fatalf("count storage rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("storage rows = %d, want 0", rows)
	}
}

func TestMutationCtxCanRunMutationsAndQueries(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.tracker.SetAuth(client.ID, "alice", time.Now().Add(time.Hour))

	var runs atomic.Int64
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&msgs)
		return len(msgs), nil
	})
	subscribe(t, e, client, "getMessages", "lobby", nil)
	drain(client)

	e.RegisterQuery("hiddenRead", func(ctx *QueryCtx) (any, error) {
		if ctx.Params["room"] != "lobby" {
			return nil, fmt.Errorf("room = %#v, want lobby", ctx.Params["room"])
		}
		if _, err := ctx.Auth.GetIdentity(); !errors.Is(err, ErrNoCaller) {
			return nil, fmt.Errorf("GetIdentity error = %v, want ErrNoCaller", err)
		}
		err := ctx.DB.Create(&testMessage{Body: "nope", RoomID: "lobby"}).Error
		if !errors.Is(err, errReadOnly) {
			return nil, fmt.Errorf("query write error = %v, want errReadOnly", err)
		}
		var n int64
		if err := ctx.DB.Model(&testMessage{}).Where("room_id = ?", "lobby").Count(&n).Error; err != nil {
			return nil, err
		}
		return n, nil
	}, Internal())
	e.RegisterMutation("hiddenWrite", func(ctx *MutationCtx) (any, error) {
		if _, err := ctx.Auth.GetIdentity(); !errors.Is(err, ErrNoCaller) {
			return nil, fmt.Errorf("GetIdentity error = %v, want ErrNoCaller", err)
		}
		if _, err := ctx.Auth.ExecuteGuard("unused", nil); err == nil || err.Error() != "guards cannot be executed internally" {
			return nil, fmt.Errorf("ExecuteGuard error = %v", err)
		}
		if ctx.Params["body"] != "hi" {
			return nil, fmt.Errorf("body = %#v, want hi", ctx.Params["body"])
		}
		if err := ctx.DB.Create(&testMessage{Body: "hi", RoomID: "lobby"}).Error; err != nil {
			return nil, err
		}
		return "written", nil
	}, Internal())
	e.RegisterMutation("fails", func(ctx *MutationCtx) (any, error) {
		return nil, errors.New("boom")
	}, Internal())
	e.RegisterMutation("send", func(ctx *MutationCtx) (any, error) {
		id, err := ctx.Auth.GetIdentity()
		if err != nil || id != "alice" {
			return nil, fmt.Errorf("outer identity = %q, %v", id, err)
		}
		written, err := ctx.ExecuteMutation("hiddenWrite", map[string]interface{}{"body": "hi"})
		if err != nil {
			return nil, err
		}
		count, err := ctx.ExecuteQuery("hiddenRead", map[string]interface{}{"room": "lobby"})
		if err != nil {
			return nil, err
		}
		if _, err := ctx.ExecuteMutation("missing", nil); err == nil || err.Error() != "mutation not found" {
			return nil, fmt.Errorf("missing mutation error = %v", err)
		}
		if _, err := ctx.ExecuteQuery("missing", nil); err == nil || err.Error() != "query not found" {
			return nil, fmt.Errorf("missing query error = %v", err)
		}
		if _, err := ctx.ExecuteMutation("fails", nil); err == nil || err.Error() != "boom" {
			return nil, fmt.Errorf("nested mutation error = %v", err)
		}
		return map[string]any{"written": written, "count": count}, nil
	})

	if _, err := e.executeMutation("hiddenWrite", nil, client.ID, "direct"); err == nil || err.Error() != "mutation not found" {
		t.Fatalf("client call of internal mutation error = %v, want mutation not found", err)
	}

	result, err := e.executeMutation("send", nil, client.ID, "m1")
	if err != nil {
		t.Fatalf("executeMutation: %v", err)
	}
	got, ok := result.(map[string]any)
	if !ok || got["written"] != "written" || got["count"] != int64(1) {
		t.Fatalf("send result = %#v, want written and count 1", result)
	}
	if runs.Load() != 2 {
		t.Fatalf("query runs after nested mutation = %d, want 2", runs.Load())
	}
	var sawUpdate bool
	for _, msg := range queryMessages(t, drain(client)) {
		if msg["query_key"] == "lobby" && msg["data"] == float64(1) {
			sawUpdate = true
		}
		if msg["location"] == "hiddenRead" {
			t.Fatalf("nested query was pushed: %#v", msg)
		}
	}
	if !sawUpdate {
		t.Fatal("nested mutation write did not update subscribers")
	}
	var count int64
	if err := e.db.Model(&testMessage{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("messages = %d, want 1", count)
	}
}

func TestSetStorageBasePath(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	if err := e.SetStorage(store, "/api/files/"); err != nil {
		t.Fatalf("set storage: %v", err)
	}

	upload, err := e.getUploadURL()
	if err != nil {
		t.Fatalf("getUploadURL: %v", err)
	}
	if !strings.HasPrefix(upload.UploadURL, "/api/files/upload/") || strings.Contains(upload.UploadURL, "//") {
		t.Fatalf("upload URL = %q, want /api/files/upload/{token}", upload.UploadURL)
	}
	rec := serveStorage(e, http.MethodPut, upload.UploadURL, strings.NewReader("hello"), map[string]string{
		"Content-Type": "text/plain",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("custom upload status = %d, body %s", rec.Code, rec.Body.String())
	}

	other, err := e.getUploadURL()
	if err != nil {
		t.Fatalf("second getUploadURL: %v", err)
	}
	token := strings.TrimPrefix(other.UploadURL, "/api/files/upload/")
	missed := serveStorage(e, http.MethodPut, "/storage/upload/"+token, strings.NewReader("nope"), map[string]string{
		"Content-Type": "text/plain",
	})
	if missed.Code == http.StatusOK && missed.Body.Len() != 0 {
		t.Fatalf("default route handled a custom-prefix upload: status %d body %s", missed.Code, missed.Body.String())
	}
	var pending TetherStorage
	if err := e.db.Where("id = ?", other.FileID).First(&pending).Error; err != nil {
		t.Fatalf("load untouched upload: %v", err)
	}
	if pending.Status != "pending" {
		t.Fatalf("default route status = %q, want pending", pending.Status)
	}

	download, err := e.getDownloadURL(upload.FileID)
	if err != nil {
		t.Fatalf("getDownloadURL: %v", err)
	}
	if !strings.HasPrefix(download, "/api/files/file/") || strings.Contains(download, "//") {
		t.Fatalf("download URL = %q, want /api/files/file/{token}", download)
	}
	got := serveStorage(e, http.MethodGet, download, nil, nil)
	if got.Code != http.StatusOK || got.Body.String() != "hello" {
		t.Fatalf("custom download status = %d, body %q", got.Code, got.Body.String())
	}

	unset := newTestEngine(t)
	if err := unset.SetStorage(store, "files"); err == nil {
		t.Fatal("relative base path was accepted")
	}
	if err := unset.SetStorage(store, "/api/../secret"); err == nil {
		t.Fatal("base path containing .. was accepted")
	}
	rejected := serveStorage(unset, http.MethodPut, "/files/upload/token", strings.NewReader("x"), nil)
	if rejected.Code != http.StatusNotImplemented {
		t.Fatalf("storage after rejected base path status = %d, want %d", rejected.Code, http.StatusNotImplemented)
	}
}

func TestSetStorageTableErrorLeavesTheEngineRunning(t *testing.T) {
	e := newTestEngine(t)
	sqlDB, err := e.db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	if err := e.SetStorage(store, ""); err == nil {
		t.Fatal("SetStorage with a closed database returned nil")
	}
	if e.isClosed() {
		t.Fatal("SetStorage shut down the engine")
	}
	if e.storage != nil || e.storageBasePath != "" {
		t.Fatal("storage was enabled after a table error")
	}
}

func TestSetStorageAfterCloseReturnsErrEngineClosed(t *testing.T) {
	e := newTestEngine(t)
	e.Close()
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	if err := e.SetStorage(store, ""); !errors.Is(err, ErrEngineClosed) {
		t.Fatalf("SetStorage after Close = %v, want ErrEngineClosed", err)
	}
	if e.storage != nil {
		t.Fatal("storage was enabled on a closed engine")
	}
}

func TestDeleteFileRemovesStoredObject(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	e.SetStorage(store, "")

	upload, err := e.getUploadURL()
	if err != nil {
		t.Fatalf("getUploadURL: %v", err)
	}
	path := filepath.Join(store.UploadDir, upload.FileID)
	if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := os.WriteFile(path+".mime", []byte("text/plain"), 0o644); err != nil {
		t.Fatalf("write mime: %v", err)
	}

	if err := (&StorageCtx{DeleteFile: e.deleteFile}).DeleteFile(upload.FileID); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("stored file still present: %v", err)
	}
	var remaining int64
	if err := e.db.Model(&TetherStorage{}).Where("id = ?", upload.FileID).Count(&remaining).Error; err != nil {
		t.Fatalf("count storage row: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("storage rows = %d, want 0", remaining)
	}
}

type recordingStorage struct {
	local   *local.Storage
	deleted []string
}

func (r *recordingStorage) UploadStream(ctx context.Context, fileID string, contentType string, req *http.Request) error {
	return r.local.UploadStream(ctx, fileID, contentType, req)
}

func (r *recordingStorage) ServeFile(fileID string, w http.ResponseWriter, req *http.Request) error {
	return r.local.ServeFile(fileID, w, req)
}

func (r *recordingStorage) Delete(ctx context.Context, fileID string) error {
	r.deleted = append(r.deleted, fileID)
	return r.local.Delete(ctx, fileID)
}

func (r *recordingStorage) Name() string {
	return r.local.Name()
}

// cleanupStorage records whether upload metadata still existed when Delete ran,
// and can fail a chosen id without touching its bytes.
type cleanupStorage struct {
	local    *local.Storage
	db       *gorm.DB
	fail     map[string]error
	deleted  []string
	rowAlive []bool
}

func (c *cleanupStorage) UploadStream(ctx context.Context, fileID string, contentType string, r *http.Request) error {
	return c.local.UploadStream(ctx, fileID, contentType, r)
}

func (c *cleanupStorage) ServeFile(fileID string, w http.ResponseWriter, r *http.Request) error {
	return c.local.ServeFile(fileID, w, r)
}

func (c *cleanupStorage) Delete(ctx context.Context, fileID string) error {
	c.deleted = append(c.deleted, fileID)
	var n int64
	if err := c.db.Model(&TetherStorage{}).Where("id = ?", fileID).Count(&n).Error; err != nil {
		return err
	}
	c.rowAlive = append(c.rowAlive, n == 1)
	if err, ok := c.fail[fileID]; ok {
		return err
	}
	return c.local.Delete(ctx, fileID)
}

func (c *cleanupStorage) Name() string {
	return c.local.Name()
}

func TestCleanStorage(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	adapter := &cleanupStorage{
		local: store,
		db:    e.db,
		fail:  map[string]error{"delete-fail": errors.New("disk full")},
	}
	e.SetStorage(adapter, "")

	if !e.db.Migrator().HasIndex(&TetherStorage{}, "ExpiresAt") {
		t.Fatal("TetherStorage.ExpiresAt has no cleanup index")
	}
	if !e.db.Migrator().HasIndex(&TetherDownloadToken{}, "ExpiresAt") {
		t.Fatal("TetherDownloadToken.ExpiresAt has no cleanup index")
	}

	now := time.Date(2026, 9, 27, 17, 0, 0, 0, time.UTC)
	// Issued 30h ago with a 48h lifetime, so the old created_at < now-24h sweep would delete it.
	issued := now.Add(-30 * time.Hour)
	expires := now.Add(18 * time.Hour)

	type fixture struct {
		id         string
		status     string
		createdAt  time.Time
		expiresAt  time.Time
		bytes      bool
		wantRow    bool
		wantBytes  bool
		wantDelete bool
	}
	fixtures := []fixture{
		{id: "valid-pending", status: "pending", createdAt: issued, expiresAt: expires, wantRow: true},
		{id: "expired-pending", status: "pending", createdAt: now.Add(-2 * time.Hour), expiresAt: now.Add(-time.Hour), wantDelete: true},
		{id: "expired-pending-bytes", status: "pending", createdAt: now.Add(-2 * time.Hour), expiresAt: now.Add(-time.Hour), bytes: true, wantDelete: true},
		{id: "expired-uploading", status: "uploading", createdAt: now.Add(-2 * time.Hour), expiresAt: now.Add(-time.Hour), bytes: true, wantDelete: true},
		{id: "valid-uploading", status: "uploading", createdAt: issued, expiresAt: expires, bytes: true, wantRow: true, wantBytes: true},
		{id: "active-file", status: "active", createdAt: now.Add(-48 * time.Hour), expiresAt: now.Add(-24 * time.Hour), bytes: true, wantRow: true, wantBytes: true},
		{id: "delete-fail", status: "uploading", createdAt: now.Add(-2 * time.Hour), expiresAt: now.Add(-time.Hour), bytes: true, wantRow: true, wantBytes: true, wantDelete: true},
	}

	for _, fx := range fixtures {
		row := TetherStorage{
			ID:        fx.id,
			Token:     fx.id + "-token",
			Status:    fx.status,
			ExpiresAt: fx.expiresAt,
			CreatedAt: fx.createdAt,
		}
		if err := e.db.Create(&row).Error; err != nil {
			t.Fatalf("create %s: %v", fx.id, err)
		}
		if err := e.db.Model(&TetherStorage{}).Where("id = ?", fx.id).UpdateColumn("created_at", fx.createdAt).Error; err != nil {
			t.Fatalf("set created_at %s: %v", fx.id, err)
		}
		if fx.bytes {
			path := filepath.Join(store.UploadDir, fx.id)
			if err := os.WriteFile(path, []byte("partial"), 0o644); err != nil {
				t.Fatalf("write %s: %v", fx.id, err)
			}
		}
	}

	var aged TetherStorage
	if err := e.db.Where("id = ?", "valid-pending").First(&aged).Error; err != nil {
		t.Fatalf("load valid-pending: %v", err)
	}
	if !aged.CreatedAt.Before(now.Add(-24 * time.Hour)) {
		t.Fatalf("created_at = %v, want older than 24h", aged.CreatedAt)
	}

	if err := e.db.Create(&TetherDownloadToken{Token: "expired-download", FileID: "active-file", ExpiresAt: now.Add(-time.Hour)}).Error; err != nil {
		t.Fatalf("create expired download token: %v", err)
	}
	if err := e.db.Create(&TetherDownloadToken{Token: "live-download", FileID: "active-file", ExpiresAt: now.Add(time.Hour)}).Error; err != nil {
		t.Fatalf("create live download token: %v", err)
	}

	e.cleanStorage(now)

	for _, fx := range fixtures {
		var n int64
		if err := e.db.Model(&TetherStorage{}).Where("id = ?", fx.id).Count(&n).Error; err != nil {
			t.Fatalf("count %s: %v", fx.id, err)
		}
		if fx.wantRow && n != 1 {
			t.Errorf("%s rows = %d, want 1", fx.id, n)
		}
		if !fx.wantRow && n != 0 {
			t.Errorf("%s rows = %d, want 0", fx.id, n)
		}
		_, statErr := os.Stat(filepath.Join(store.UploadDir, fx.id))
		exists := statErr == nil
		if fx.wantBytes && !exists {
			t.Errorf("%s bytes were removed", fx.id)
		}
		if !fx.wantBytes && fx.bytes && exists {
			t.Errorf("%s bytes are still present", fx.id)
		}
		if !fx.wantBytes && !fx.bytes && exists {
			t.Errorf("%s unexpectedly has bytes", fx.id)
		}
		sawDelete := slices.Contains(adapter.deleted, fx.id)
		if sawDelete != fx.wantDelete {
			t.Errorf("%s adapter delete = %v, want %v", fx.id, sawDelete, fx.wantDelete)
		}
	}
	for i, alive := range adapter.rowAlive {
		if !alive {
			t.Errorf("adapter delete %s ran after metadata was removed", adapter.deleted[i])
		}
	}

	var expiredTokens, liveTokens int64
	if err := e.db.Model(&TetherDownloadToken{}).Where("token = ?", "expired-download").Count(&expiredTokens).Error; err != nil {
		t.Fatalf("count expired token: %v", err)
	}
	if expiredTokens != 0 {
		t.Errorf("expired download tokens = %d, want 0", expiredTokens)
	}
	if err := e.db.Model(&TetherDownloadToken{}).Where("token = ?", "live-download").Count(&liveTokens).Error; err != nil {
		t.Fatalf("count live token: %v", err)
	}
	if liveTokens != 1 {
		t.Errorf("live download tokens = %d, want 1", liveTokens)
	}
}

func TestGetDownloadURLRequiresStorage(t *testing.T) {
	e := newTestEngine(t)
	if _, err := e.getDownloadURL("missing"); err == nil {
		t.Fatal("getDownloadURL without storage returned nil")
	}
}

func TestGetDownloadURLOptionsAndCaching(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	if err := e.SetStorage(store, "/api/files", storage.WithDownloadExpiresIn(2*time.Hour), storage.UseCachedURLs()); err != nil {
		t.Fatalf("set storage: %v", err)
	}

	started := time.Now()
	cached, err := e.getDownloadURL("stable")
	if err != nil {
		t.Fatalf("getDownloadURL: %v", err)
	}
	if !strings.HasPrefix(cached, "/api/files/file/") {
		t.Fatalf("download URL = %q, want /api/files/file/ prefix", cached)
	}
	first := loadDownloadToken(t, e, "stable")
	if cached != "/api/files/file/"+first.Token {
		t.Fatalf("download URL = %q, want token %s", cached, first.Token)
	}
	assertExpiryAround(t, first.ExpiresAt, started, 2*time.Hour)

	again, err := e.getDownloadURL("stable", storage.WithDownloadExpiresIn(30*time.Minute))
	if err != nil {
		t.Fatalf("cached getDownloadURL: %v", err)
	}
	if again != cached {
		t.Fatalf("cached URL = %q, want %q", again, cached)
	}
	if n := countDownloadTokens(t, e, "stable"); n != 1 {
		t.Fatalf("cached download tokens = %d, want 1", n)
	}

	if err := e.db.Model(&TetherDownloadToken{}).Where("token = ?", first.Token).Update("expires_at", time.Now().Add(4*time.Minute)).Error; err != nil {
		t.Fatalf("shorten token lifetime: %v", err)
	}
	refreshedAt := time.Now()
	refreshed, err := e.getDownloadURL("stable", storage.WithDownloadExpiresIn(30*time.Minute))
	if err != nil {
		t.Fatalf("refresh getDownloadURL: %v", err)
	}
	if refreshed == cached {
		t.Fatal("URL expiring within 5 minutes was reused")
	}
	if n := countDownloadTokens(t, e, "stable"); n != 2 {
		t.Fatalf("download tokens after refresh = %d, want 2", n)
	}
	var refreshedToken TetherDownloadToken
	if err := e.db.Where("token = ?", strings.TrimPrefix(refreshed, "/api/files/file/")).First(&refreshedToken).Error; err != nil {
		t.Fatalf("load refreshed token: %v", err)
	}
	assertExpiryAround(t, refreshedToken.ExpiresAt, refreshedAt, 30*time.Minute)

	zeroAt := time.Now()
	zero, err := e.getDownloadURL("zero", storage.WithDownloadExpiresIn(0))
	if err != nil {
		t.Fatalf("zero lifetime getDownloadURL: %v", err)
	}
	if !strings.HasPrefix(zero, "/api/files/file/") {
		t.Fatalf("zero lifetime URL = %q", zero)
	}
	assertExpiryAround(t, loadDownloadToken(t, e, "zero").ExpiresAt, zeroAt, 15*time.Minute)
}

func TestGetDownloadURLWithoutCacheIssuesANewToken(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	if err := e.SetStorage(store, ""); err != nil {
		t.Fatalf("set storage: %v", err)
	}

	started := time.Now()
	first, err := e.getDownloadURL("file")
	if err != nil {
		t.Fatalf("getDownloadURL: %v", err)
	}
	second, err := e.getDownloadURL("file", storage.WithDownloadExpiresIn(time.Hour))
	if err != nil {
		t.Fatalf("second getDownloadURL: %v", err)
	}
	if first == second {
		t.Fatal("uncached getDownloadURL returned the same URL twice")
	}
	if !strings.HasPrefix(first, "/storage/file/") || !strings.HasPrefix(second, "/storage/file/") {
		t.Fatalf("URLs = %q, %q; want /storage/file/ prefix", first, second)
	}
	if n := countDownloadTokens(t, e, "file"); n != 2 {
		t.Fatalf("download tokens = %d, want 2", n)
	}
	var shorter, longer TetherDownloadToken
	if err := e.db.Where("token = ?", strings.TrimPrefix(first, "/storage/file/")).First(&shorter).Error; err != nil {
		t.Fatalf("load first token: %v", err)
	}
	if err := e.db.Where("token = ?", strings.TrimPrefix(second, "/storage/file/")).First(&longer).Error; err != nil {
		t.Fatalf("load second token: %v", err)
	}
	assertExpiryAround(t, shorter.ExpiresAt, started, 15*time.Minute)
	assertExpiryAround(t, longer.ExpiresAt, started, time.Hour)
}

func TestDownloadURLExpiryRerunsSubscribedQuery(t *testing.T) {
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	if err := e.SetStorage(store, "/api/files", storage.WithDownloadExpiresIn(time.Hour)); err != nil {
		t.Fatalf("set storage: %v", err)
	}

	var expiringRuns, liveRuns atomic.Int64
	e.RegisterQuery("expiringFile", func(ctx *QueryCtx) (any, error) {
		expiringRuns.Add(1)
		return ctx.Storage.GetDownloadURL("expiring", storage.UseCachedURLs())
	})
	e.RegisterQuery("liveFile", func(ctx *QueryCtx) (any, error) {
		liveRuns.Add(1)
		return ctx.Storage.GetDownloadURL("live")
	})

	client := trackClient(t, e)
	expiringSub := subscribe(t, e, client, "expiringFile", "expiring", nil)
	liveSub := subscribe(t, e, client, "liveFile", "live", nil)
	pushed := queryData(t, drain(client))
	if len(pushed) != 2 {
		t.Fatalf("initial query pushes = %d, want 2", len(pushed))
	}

	expiringToken := loadDownloadToken(t, e, "expiring")
	liveToken := loadDownloadToken(t, e, "live")
	expiringTag := "~storage_token:" + expiringToken.Token
	liveTag := "~storage_token:" + liveToken.Token
	if !e.tracker.SubscriptionHasTag(expiringSub.SubID, expiringTag) {
		t.Fatalf("expiring subscription missing %s", expiringTag)
	}
	if !e.tracker.SubscriptionHasTag(liveSub.SubID, liveTag) {
		t.Fatalf("live subscription missing %s", liveTag)
	}
	if pushed["expiringFile"] != "/api/files/file/"+expiringToken.Token {
		t.Fatalf("expiring URL = %v", pushed["expiringFile"])
	}

	e.cleanStorage(time.Now())
	if expiringRuns.Load() != 1 || liveRuns.Load() != 1 {
		t.Fatalf("runs before expiry = %d and %d, want 1 and 1", expiringRuns.Load(), liveRuns.Load())
	}

	if err := e.db.Model(&TetherDownloadToken{}).Where("token = ?", expiringToken.Token).Update("expires_at", time.Now().Add(-time.Minute)).Error; err != nil {
		t.Fatalf("expire token: %v", err)
	}
	refreshedAt := time.Now()
	e.cleanStorage(time.Now())

	if expiringRuns.Load() != 2 {
		t.Fatalf("expiring query runs = %d, want 2", expiringRuns.Load())
	}
	if liveRuns.Load() != 1 {
		t.Fatalf("live query runs = %d, want 1", liveRuns.Load())
	}
	if e.tracker.SubscriptionHasTag(expiringSub.SubID, expiringTag) {
		t.Fatal("subscription still tracks the expired token")
	}
	if !e.tracker.SubscriptionHasTag(liveSub.SubID, liveTag) {
		t.Fatal("live subscription lost its token tag")
	}

	var gone int64
	if err := e.db.Model(&TetherDownloadToken{}).Where("token = ?", expiringToken.Token).Count(&gone).Error; err != nil {
		t.Fatalf("count expired token: %v", err)
	}
	if gone != 0 {
		t.Fatalf("expired tokens = %d, want 0", gone)
	}
	replacement := loadDownloadToken(t, e, "expiring")
	if replacement.Token == expiringToken.Token {
		t.Fatal("expired token was reused")
	}
	replacementTag := "~storage_token:" + replacement.Token
	if !e.tracker.SubscriptionHasTag(expiringSub.SubID, replacementTag) {
		t.Fatalf("subscription missing %s", replacementTag)
	}
	assertExpiryAround(t, replacement.ExpiresAt, refreshedAt, time.Hour)

	got := queryData(t, drain(client))
	if got["expiringFile"] != "/api/files/file/"+replacement.Token {
		t.Fatalf("refreshed URL = %v, want %s", got["expiringFile"], replacement.Token)
	}
	if _, ok := got["liveFile"]; ok {
		t.Fatal("live query was pushed after another file's URL expired")
	}
}

func loadDownloadToken(t *testing.T, e *Engine, fileID string) TetherDownloadToken {
	t.Helper()
	var tokens []TetherDownloadToken
	if err := e.db.Where("file_id = ?", fileID).Order("expires_at desc").Find(&tokens).Error; err != nil {
		t.Fatalf("load download tokens for %s: %v", fileID, err)
	}
	if len(tokens) == 0 {
		t.Fatalf("no download token for %s", fileID)
	}
	return tokens[0]
}

func countDownloadTokens(t *testing.T, e *Engine, fileID string) int64 {
	t.Helper()
	var n int64
	if err := e.db.Model(&TetherDownloadToken{}).Where("file_id = ?", fileID).Count(&n).Error; err != nil {
		t.Fatalf("count download tokens for %s: %v", fileID, err)
	}
	return n
}

func assertExpiryAround(t *testing.T, got, start time.Time, d time.Duration) {
	t.Helper()
	if got.Before(start.Add(d-time.Second)) || got.After(time.Now().Add(d+time.Second)) {
		t.Fatalf("expires at %s, want about %s after %s", got, d, start)
	}
}

func queryData(t *testing.T, msgs []map[string]interface{}) map[string]interface{} {
	t.Helper()
	out := map[string]interface{}{}
	for _, msg := range queryMessages(t, msgs) {
		location, _ := msg["location"].(string)
		out[location] = msg["data"]
	}
	return out
}

func serveStorage(e *Engine, method, path string, body io.Reader, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, body)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	e.StorageHandler(rec, req)
	return rec
}

func TestSubscribeUnknownQueryViaMessageDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	mustNoPanic(t, "subscribe missing query", func() {
		_ = e.onReceiveMessage(client.ID, map[string]interface{}{
			"type":      "subscribe",
			"location":  "does-not-exist",
			"params":    map[string]interface{}{},
			"query_key": "k",
		})
	})
}

func TestExecuteQueryWithoutTrackedClientDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterQuery("q", func(ctx *QueryCtx) (any, error) { return "ok", nil })
	ghost := &reactivity.Subscription{
		SubID:    "ghost",
		Client:   &reactivity.Client{ID: "missing", Send: make(chan []byte, 1)},
		Query:    "q",
		QueryKey: "k",
		Params:   map[string]interface{}{},
	}
	mustNoPanic(t, "ExecuteQuery untracked client", func() {
		_, _ = e.executeQuery("q", map[string]interface{}{}, ghost)
	})
}

func TestDefaultAuthVerifyToken(t *testing.T) {
	var a defaultAuth
	userID, expiresAt, err := a.VerifyToken(context.Background(), nil, "token")
	if err != nil {
		t.Errorf("defaultAuth.VerifyToken error = %v", err)
	}
	if userID != "" {
		t.Errorf("defaultAuth userID = %q, want empty", userID)
	}
	if !expiresAt.IsZero() {
		t.Errorf("defaultAuth expiresAt = %v, want zero", expiresAt)
	}
}

func newConcurrentTestEngine(t *testing.T) *Engine {
	t.Helper()
	var db *gorm.DB
	if *usePostgres {
		db = newPostgresTestDB(t)
	} else {
		path := filepath.Join(t.TempDir(), "tether.db")
		dsn := path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
		var err error
		db, err = gorm.Open(sqlite.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		if err != nil {
			t.Fatalf("open sqlite: %v", err)
		}
		sqlDB, err := db.DB()
		if err != nil {
			t.Fatalf("sql db: %v", err)
		}
		sqlDB.SetMaxOpenConns(1)
		sqlDB.SetMaxIdleConns(1)
		t.Cleanup(func() { _ = sqlDB.Close() })
	}

	e, err := NewEngine(db)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}
	t.Cleanup(e.Close)
	e.CreateTable(&testMessage{})
	e.SetCheckOrigin(func(*http.Request) bool { return true })
	return e
}

type e2eRole int

const (
	e2eMutator e2eRole = iota
	e2eWatcher
	e2eDropper
)

type e2eBarriers struct {
	subscribed    sync.WaitGroup
	goMutate      chan struct{}
	readyToRevoke sync.WaitGroup
	goRevoke      chan struct{}
}

type e2eInbox struct {
	mu        sync.Mutex
	queryHits int
	queryKey  string
	lastData  []interface{}
	acks      map[string]map[string]interface{}
}

func newE2EInbox() *e2eInbox {
	return &e2eInbox{acks: make(map[string]map[string]interface{})}
}

func (in *e2eInbox) read(conn *websocket.Conn) {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var msg map[string]interface{}
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		in.mu.Lock()
		switch msg["type"] {
		case "query":
			in.queryHits++
			if k, ok := msg["query_key"].(string); ok {
				in.queryKey = k
			}
			data, _ := msg["data"].([]interface{})
			// Messages are only created in this test, so a shorter payload is a
			// stale query result that finished after a newer one.
			if len(data) >= len(in.lastData) {
				in.lastData = data
			}
		case "mutation":
			id, _ := msg["mutation_id"].(string)
			in.acks[id] = msg
		}
		in.mu.Unlock()
	}
}

func (in *e2eInbox) snapshot() (queryHits int, queryKey string, last []interface{}, acks map[string]map[string]interface{}) {
	in.mu.Lock()
	defer in.mu.Unlock()
	last = append([]interface{}(nil), in.lastData...)
	acks = make(map[string]map[string]interface{}, len(in.acks))
	for k, v := range in.acks {
		acks[k] = v
	}
	return in.queryHits, in.queryKey, last, acks
}

func waitPred(ctx context.Context, pred func() bool) bool {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if pred() {
			return true
		}
		select {
		case <-ctx.Done():
			return pred()
		case <-ticker.C:
		}
	}
}

func e2eMessageBodies(data []interface{}) map[string]struct{} {
	out := make(map[string]struct{}, len(data))
	for _, item := range data {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		body, _ := m["Body"].(string)
		if body == "" {
			continue
		}
		out[body] = struct{}{}
	}
	return out
}

func e2eBodiesMatch(got, want map[string]struct{}) bool {
	if len(got) != len(want) {
		return false
	}
	for body := range want {
		if _, ok := got[body]; !ok {
			return false
		}
	}
	return true
}

func e2eBodyDiff(got, want map[string]struct{}) (missing, extra []string) {
	for body := range want {
		if _, ok := got[body]; !ok {
			missing = append(missing, body)
		}
	}
	for body := range got {
		if _, ok := want[body]; !ok {
			extra = append(extra, body)
		}
	}
	slices.Sort(missing)
	slices.Sort(extra)
	return missing, extra
}

type e2eClientCfg struct {
	id            int
	role          e2eRole
	room          string
	queryKey      string
	mutationsEach int
	wantBodies    map[string]struct{}
	bodyOf        func(mutatorID, n int) string
}

func runE2EClient(ctx context.Context, wsURL string, cfg e2eClientCfg, barriers *e2eBarriers) (err error) {
	var readyOnce sync.Once
	signalReady := func() { readyOnce.Do(func() { barriers.subscribed.Done() }) }
	defer signalReady()

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("client %d: dial: %w", cfg.id, err)
	}
	defer conn.Close()
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	inbox := newE2EInbox()
	go inbox.read(conn)

	if err := conn.WriteJSON(map[string]interface{}{
		"type":      "subscribe",
		"location":  "getMessages",
		"params":    map[string]interface{}{"room": cfg.room},
		"query_key": cfg.queryKey,
	}); err != nil {
		return fmt.Errorf("client %d: subscribe: %w", cfg.id, err)
	}

	if !waitPred(ctx, func() bool {
		hits, key, _, _ := inbox.snapshot()
		return hits >= 1 && key == cfg.queryKey
	}) {
		hits, key, data, _ := inbox.snapshot()
		return fmt.Errorf("client %d: did not receive initial query response (hits=%d query_key=%q data=%v)", cfg.id, hits, key, data)
	}

	signalReady()
	select {
	case <-barriers.goMutate:
	case <-ctx.Done():
		return fmt.Errorf("client %d: %w", cfg.id, ctx.Err())
	}

	if cfg.role == e2eDropper {
		return nil
	}

	if cfg.role == e2eMutator {
		wantIDs := make(map[string]string, cfg.mutationsEach)
		for n := 0; n < cfg.mutationsEach; n++ {
			mutID := fmt.Sprintf("m-%d-%d", cfg.id, n)
			body := cfg.bodyOf(cfg.id, n)
			wantIDs[mutID] = body
			if err := conn.WriteJSON(map[string]interface{}{
				"type":        "mutation",
				"location":    "createMessage",
				"params":      map[string]interface{}{"body": body, "room": cfg.room},
				"mutation_id": mutID,
			}); err != nil {
				return fmt.Errorf("client %d: mutation %s: %w", cfg.id, mutID, err)
			}
		}
		if !waitPred(ctx, func() bool {
			_, _, _, acks := inbox.snapshot()
			for id := range wantIDs {
				if _, ok := acks[id]; !ok {
					return false
				}
			}
			return true
		}) {
			_, _, _, acks := inbox.snapshot()
			var got []string
			for id := range acks {
				got = append(got, id)
			}
			slices.Sort(got)
			return fmt.Errorf("client %d: missing mutation acks: got %d/%d %v", cfg.id, len(acks), len(wantIDs), got)
		}
		_, _, _, acks := inbox.snapshot()
		for mutID, body := range wantIDs {
			ack := acks[mutID]
			if ack["location"] != "createMessage" {
				return fmt.Errorf("client %d: ack %s location = %v, want createMessage", cfg.id, mutID, ack["location"])
			}
			data, _ := ack["data"].(map[string]interface{})
			if data["error"] != nil {
				return fmt.Errorf("client %d: mutation %s error: %v", cfg.id, mutID, data["error"])
			}
			if data["Body"] != body {
				return fmt.Errorf("client %d: mutation %s Body = %v, want %s", cfg.id, mutID, data["Body"], body)
			}
			if data["RoomID"] != cfg.room {
				return fmt.Errorf("client %d: mutation %s RoomID = %v, want %s", cfg.id, mutID, data["RoomID"], cfg.room)
			}
		}
		for mutID := range acks {
			if _, ok := wantIDs[mutID]; !ok {
				return fmt.Errorf("client %d: received unexpected mutation_id %q", cfg.id, mutID)
			}
		}
	}

	if !waitPred(ctx, func() bool {
		_, _, data, _ := inbox.snapshot()
		return e2eBodiesMatch(e2eMessageBodies(data), cfg.wantBodies)
	}) {
		hits, _, data, _ := inbox.snapshot()
		got := e2eMessageBodies(data)
		missing, extra := e2eBodyDiff(got, cfg.wantBodies)
		return fmt.Errorf("client %d: query did not converge to all room %q messages (hits=%d got=%d want=%d missing=%v extra=%v)",
			cfg.id, cfg.room, hits, len(got), len(cfg.wantBodies), missing, extra)
	}

	_, key, data, _ := inbox.snapshot()
	if key != cfg.queryKey {
		return fmt.Errorf("client %d: query_key = %q, want %q", cfg.id, key, cfg.queryKey)
	}
	for _, item := range data {
		m, ok := item.(map[string]interface{})
		if !ok {
			return fmt.Errorf("client %d: query item is %T, want object", cfg.id, item)
		}
		if m["RoomID"] != cfg.room {
			return fmt.Errorf("client %d: query leaked RoomID %v into room %s", cfg.id, m["RoomID"], cfg.room)
		}
	}
	return nil
}

func TestConcurrentWebsocketClientsEndToEnd(t *testing.T) {
	const (
		nMutators     = 24
		nWatchers     = 16
		nDroppers     = 10
		mutationsEach = 5
	)
	nClients := nMutators + nWatchers + nDroppers

	e := newConcurrentTestEngine(t)
	e.Profiler().Start()
	defer func() {
		metrics := e.Profiler().DumpMetricsAndFlush()
		t.Log(SanitizeMetrics(metrics))
	}()
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		room := ctx.Params["room"].(string)
		ctx.TrackCollection("messages", "room_id", room)
		var msgs []testMessage
		if err := ctx.DB.Where("room_id = ?", room).Order("id").Find(&msgs).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return msgs, nil
	})
	e.RegisterMutation("createMessage", func(ctx *MutationCtx) (any, error) {
		msg := testMessage{
			Body:   ctx.Params["body"].(string),
			RoomID: ctx.Params["room"].(string),
		}
		if err := ctx.DB.Create(&msg).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return msg, nil
	})

	srv := httptest.NewServer(http.HandlerFunc(e.Handle))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/"

	roomOf := func(id int) string {
		if id%2 == 0 {
			return "alpha"
		}
		return "bravo"
	}
	bodyOf := func(mutatorID, n int) string {
		return fmt.Sprintf("c%d-n%d", mutatorID, n)
	}

	wantBodies := map[string]map[string]struct{}{
		"alpha": {},
		"bravo": {},
	}
	for id := 0; id < nMutators; id++ {
		for n := 0; n < mutationsEach; n++ {
			wantBodies[roomOf(id)][bodyOf(id, n)] = struct{}{}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	barriers := &e2eBarriers{goMutate: make(chan struct{})}
	barriers.subscribed.Add(nClients)
	errCh := make(chan error, nClients)

	startClient := func(id int, role e2eRole) {
		go func() {
			errCh <- runE2EClient(ctx, wsURL, e2eClientCfg{
				id:            id,
				role:          role,
				room:          roomOf(id),
				queryKey:      fmt.Sprintf("client-%d", id),
				mutationsEach: mutationsEach,
				wantBodies:    wantBodies[roomOf(id)],
				bodyOf:        bodyOf,
			}, barriers)
		}()
	}
	for id := 0; id < nMutators; id++ {
		startClient(id, e2eMutator)
	}
	for i := 0; i < nWatchers; i++ {
		startClient(nMutators+i, e2eWatcher)
	}
	for i := 0; i < nDroppers; i++ {
		startClient(nMutators+nWatchers+i, e2eDropper)
	}

	subscribed := make(chan struct{})
	go func() {
		barriers.subscribed.Wait()
		close(subscribed)
	}()
	select {
	case <-subscribed:
	case <-ctx.Done():
		for {
			select {
			case err := <-errCh:
				if err != nil {
					t.Error(err)
				}
			default:
				t.Fatal("timed out waiting for all clients to subscribe and receive an initial query result")
			}
		}
	}
	close(barriers.goMutate)

	for i := 0; i < nClients; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			for {
				select {
				case err := <-errCh:
					if err != nil {
						t.Error(err)
					}
				default:
					t.Fatal("timed out waiting for concurrent clients to finish")
				}
			}
		}
	}
}

// Access-token auth fixtures used by the concurrent GetIdentity end-to-end test.
// A later Guard-based variant can reuse the same schema and token lookup.
type testUser struct {
	ID   string `gorm:"primaryKey"`
	Name string `gorm:"not null"`
}

func (testUser) TableName() string { return "users" }

type testAccessToken struct {
	Token     string    `gorm:"primaryKey"`
	UserID    string    `gorm:"not null"`
	ExpiresAt time.Time `gorm:"not null"`
}

func (testAccessToken) TableName() string { return "access_tokens" }

type testRoomMember struct {
	UserID string `gorm:"primaryKey" tether:"track"`
	RoomID string `gorm:"primaryKey"`
}

func (testRoomMember) TableName() string { return "room_members" }

type testAuthoredMessage struct {
	ID       uint   `gorm:"primaryKey"`
	Body     string `gorm:"not null"`
	RoomID   string `tether:"track"`
	AuthorID string
}

func (testAuthoredMessage) TableName() string { return "messages" }

// accessTokenAuth looks up opaque access tokens in the database. It is intentionally
// simple: the goal is to exercise Tether's auth plumbing, not a production token scheme.
type accessTokenAuth struct{}

func (accessTokenAuth) VerifyToken(ctx context.Context, db *gorm.DB, token string) (string, time.Time, error) {
	if token == "" {
		return "", time.Time{}, errors.New("missing token")
	}
	var row testAccessToken
	if err := db.Where("token = ?", token).First(&row).Error; err != nil {
		return "", time.Time{}, err
	}
	if time.Now().After(row.ExpiresAt) {
		return "", time.Time{}, errors.New("token expired")
	}
	var user testUser
	if err := db.Where("id = ?", row.UserID).First(&user).Error; err != nil {
		return "", time.Time{}, err
	}
	return user.ID, row.ExpiresAt, nil
}

func newConcurrentAuthTestEngine(t *testing.T) *Engine {
	t.Helper()
	e := newConcurrentTestEngine(t)
	e.CreateTable(&testUser{})
	e.CreateTable(&testAccessToken{})
	e.CreateTable(&testRoomMember{})
	e.CreateTable(&testAuthoredMessage{})
	e.SetAuth(accessTokenAuth{})
	return e
}

type e2eAuthRole int

const (
	e2eAuthMutator e2eAuthRole = iota
	e2eAuthWatcher
	e2eAuthDropper
	e2eAuthBadToken
	e2eAuthUnauthed
	e2eAuthOutsider
	e2eAuthRevoked
)

type authE2EInbox struct {
	mu           sync.Mutex
	queryHits    int
	queryKey     string
	lastIdentity string
	lastUserName string
	lastError    string
	lastMessages []interface{}
	acks         map[string]map[string]interface{}
	auth         map[string]interface{}
	errors       []map[string]interface{}
}

func newAuthE2EInbox() *authE2EInbox {
	return &authE2EInbox{acks: make(map[string]map[string]interface{})}
}

func (in *authE2EInbox) read(conn *websocket.Conn) {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var msg map[string]interface{}
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		in.mu.Lock()
		switch msg["type"] {
		case "query":
			in.queryHits++
			if k, ok := msg["query_key"].(string); ok {
				in.queryKey = k
			}
			data, _ := msg["data"].(map[string]interface{})
			in.lastError, _ = data["error"].(string)
			in.lastIdentity, _ = data["identity"].(string)
			in.lastUserName, _ = data["user_name"].(string)
			msgs, _ := data["messages"].([]interface{})
			// Messages are only created in this test, so a shorter payload is a
			// stale query result that finished after a newer one. An error
			// payload is a later authorization change and always wins.
			if in.lastError != "" || len(msgs) >= len(in.lastMessages) {
				in.lastMessages = msgs
			}
		case "mutation":
			id, _ := msg["mutation_id"].(string)
			in.acks[id] = msg
		case "auth":
			in.auth = msg
		case "error":
			in.errors = append(in.errors, msg)
		}
		in.mu.Unlock()
	}
}

type authE2ESnap struct {
	queryHits int
	queryKey  string
	identity  string
	userName  string
	queryErr  string
	messages  []interface{}
	acks      map[string]map[string]interface{}
	auth      map[string]interface{}
	errors    []map[string]interface{}
}

func (in *authE2EInbox) snapshot() authE2ESnap {
	in.mu.Lock()
	defer in.mu.Unlock()
	s := authE2ESnap{
		queryHits: in.queryHits,
		queryKey:  in.queryKey,
		identity:  in.lastIdentity,
		userName:  in.lastUserName,
		queryErr:  in.lastError,
		messages:  append([]interface{}(nil), in.lastMessages...),
		acks:      make(map[string]map[string]interface{}, len(in.acks)),
		auth:      in.auth,
		errors:    append([]map[string]interface{}(nil), in.errors...),
	}
	for k, v := range in.acks {
		s.acks[k] = v
	}
	return s
}

type authE2EClientCfg struct {
	id            int
	role          e2eAuthRole
	room          string
	queryKey      string
	userID        string
	userName      string
	token         string
	mutationsEach int
	wantBodies    map[string]struct{}
	bodyOf        func(mutatorID, n int) string
	// skipQueryIdentity omits checks that getMessages includes identity and
	// user_name. Guard-backed queries cannot return per-user fields without
	// splitting the batched result.
	skipQueryIdentity bool
}

func authorIDForMutator(mutatorID int) string {
	return fmt.Sprintf("user-%d", mutatorID)
}

func runAuthE2EClient(ctx context.Context, wsURL string, cfg authE2EClientCfg, barriers *e2eBarriers) (err error) {
	var readyOnce sync.Once
	signalReady := func() { readyOnce.Do(func() { barriers.subscribed.Done() }) }
	defer signalReady()
	signalRevokeReady := func() {}
	switch cfg.role {
	case e2eAuthMutator, e2eAuthWatcher, e2eAuthRevoked:
		var revokeOnce sync.Once
		signalRevokeReady = func() { revokeOnce.Do(func() { barriers.readyToRevoke.Done() }) }
		defer signalRevokeReady()
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("client %d: dial: %w", cfg.id, err)
	}
	defer conn.Close()
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	inbox := newAuthE2EInbox()
	go inbox.read(conn)

	if cfg.role == e2eAuthBadToken {
		if err := conn.WriteJSON(map[string]interface{}{"type": "auth", "token": cfg.token}); err != nil {
			return fmt.Errorf("client %d: auth: %w", cfg.id, err)
		}
		if !waitPred(ctx, func() bool {
			s := inbox.snapshot()
			return s.auth != nil || len(s.errors) > 0
		}) {
			return fmt.Errorf("client %d: did not receive an auth failure", cfg.id)
		}
		s := inbox.snapshot()
		if s.auth != nil {
			return fmt.Errorf("client %d: invalid token was accepted: %v", cfg.id, s.auth)
		}
		if len(s.errors) == 0 {
			return fmt.Errorf("client %d: invalid token did not produce an error message", cfg.id)
		}
		return nil
	}

	if cfg.role != e2eAuthUnauthed {
		if err := conn.WriteJSON(map[string]interface{}{"type": "auth", "token": cfg.token}); err != nil {
			return fmt.Errorf("client %d: auth: %w", cfg.id, err)
		}
		if !waitPred(ctx, func() bool {
			s := inbox.snapshot()
			return s.auth != nil || len(s.errors) > 0
		}) {
			return fmt.Errorf("client %d: did not receive an auth response", cfg.id)
		}
		s := inbox.snapshot()
		if len(s.errors) > 0 {
			return fmt.Errorf("client %d: auth failed: %v", cfg.id, s.errors)
		}
		if s.auth["success"] != true {
			return fmt.Errorf("client %d: auth success = %v, want true", cfg.id, s.auth["success"])
		}
		data, _ := s.auth["data"].(map[string]interface{})
		if data["user_id"] != cfg.userID {
			return fmt.Errorf("client %d: auth user_id = %v, want %s", cfg.id, data["user_id"], cfg.userID)
		}
	}

	if err := conn.WriteJSON(map[string]interface{}{
		"type":      "subscribe",
		"location":  "getMessages",
		"params":    map[string]interface{}{"room": cfg.room},
		"query_key": cfg.queryKey,
	}); err != nil {
		return fmt.Errorf("client %d: subscribe: %w", cfg.id, err)
	}

	wantQueryErr := ""
	switch cfg.role {
	case e2eAuthUnauthed:
		wantQueryErr = "unauthenticated"
	case e2eAuthOutsider:
		wantQueryErr = "forbidden"
	}
	if !waitPred(ctx, func() bool {
		s := inbox.snapshot()
		if s.queryHits < 1 || s.queryKey != cfg.queryKey || s.queryErr != wantQueryErr {
			return false
		}
		if cfg.skipQueryIdentity {
			return true
		}
		if cfg.role == e2eAuthUnauthed {
			return s.identity == ""
		}
		return s.identity == cfg.userID
	}) {
		s := inbox.snapshot()
		return fmt.Errorf("client %d: initial query mismatch (hits=%d query_key=%q identity=%q error=%q wantError=%q data=%v)",
			cfg.id, s.queryHits, s.queryKey, s.identity, s.queryErr, wantQueryErr, s.messages)
	}

	signalReady()
	select {
	case <-barriers.goMutate:
	case <-ctx.Done():
		return fmt.Errorf("client %d: %w", cfg.id, ctx.Err())
	}

	if cfg.role == e2eAuthDropper {
		return nil
	}

	sendDeniedMutation := func(wantErr string) error {
		mutID := fmt.Sprintf("denied-%d", cfg.id)
		if err := conn.WriteJSON(map[string]interface{}{
			"type":     "mutation",
			"location": "createMessage",
			"params": map[string]interface{}{
				"body":      "should-not-land",
				"room":      cfg.room,
				"author_id": "spoofed",
			},
			"mutation_id": mutID,
		}); err != nil {
			return fmt.Errorf("client %d: mutation %s: %w", cfg.id, mutID, err)
		}
		if !waitPred(ctx, func() bool {
			_, ok := inbox.snapshot().acks[mutID]
			return ok
		}) {
			return fmt.Errorf("client %d: missing denied mutation ack", cfg.id)
		}
		data, _ := inbox.snapshot().acks[mutID]["data"].(map[string]interface{})
		var gotErr interface{}
		if data != nil {
			gotErr = data["error"]
		}
		if gotErr != wantErr {
			return fmt.Errorf("client %d: denied mutation error = %v, want %s", cfg.id, gotErr, wantErr)
		}
		if data != nil && (data["Body"] != nil || data["AuthorID"] != nil) {
			return fmt.Errorf("client %d: denied mutation created a row: %v", cfg.id, data)
		}
		return nil
	}

	switch cfg.role {
	case e2eAuthUnauthed:
		return sendDeniedMutation("unauthenticated")
	case e2eAuthOutsider:
		return sendDeniedMutation("forbidden")
	}

	if cfg.role == e2eAuthMutator {
		wantIDs := make(map[string]string, cfg.mutationsEach)
		for n := 0; n < cfg.mutationsEach; n++ {
			mutID := fmt.Sprintf("m-%d-%d", cfg.id, n)
			body := cfg.bodyOf(cfg.id, n)
			wantIDs[mutID] = body
			if err := conn.WriteJSON(map[string]interface{}{
				"type":     "mutation",
				"location": "createMessage",
				"params": map[string]interface{}{
					"body":      body,
					"room":      cfg.room,
					"author_id": "spoofed-author",
					"user_id":   "spoofed-user",
				},
				"mutation_id": mutID,
			}); err != nil {
				return fmt.Errorf("client %d: mutation %s: %w", cfg.id, mutID, err)
			}
		}
		if !waitPred(ctx, func() bool {
			acks := inbox.snapshot().acks
			for id := range wantIDs {
				if _, ok := acks[id]; !ok {
					return false
				}
			}
			return true
		}) {
			acks := inbox.snapshot().acks
			var got []string
			for id := range acks {
				got = append(got, id)
			}
			slices.Sort(got)
			return fmt.Errorf("client %d: missing mutation acks: got %d/%d %v", cfg.id, len(acks), len(wantIDs), got)
		}
		acks := inbox.snapshot().acks
		for mutID, body := range wantIDs {
			ack := acks[mutID]
			if ack["location"] != "createMessage" {
				return fmt.Errorf("client %d: ack %s location = %v, want createMessage", cfg.id, mutID, ack["location"])
			}
			data, _ := ack["data"].(map[string]interface{})
			if data["error"] != nil {
				return fmt.Errorf("client %d: mutation %s error: %v", cfg.id, mutID, data["error"])
			}
			if data["Body"] != body {
				return fmt.Errorf("client %d: mutation %s Body = %v, want %s", cfg.id, mutID, data["Body"], body)
			}
			if data["RoomID"] != cfg.room {
				return fmt.Errorf("client %d: mutation %s RoomID = %v, want %s", cfg.id, mutID, data["RoomID"], cfg.room)
			}
			if data["AuthorID"] != cfg.userID {
				return fmt.Errorf("client %d: mutation %s AuthorID = %v, want %s (identity, not client-supplied author_id)", cfg.id, mutID, data["AuthorID"], cfg.userID)
			}
		}
		for mutID := range acks {
			if _, ok := wantIDs[mutID]; !ok {
				return fmt.Errorf("client %d: received unexpected mutation_id %q", cfg.id, mutID)
			}
		}
	}

	if !waitPred(ctx, func() bool {
		s := inbox.snapshot()
		if s.queryErr != "" || !e2eBodiesMatch(e2eMessageBodies(s.messages), cfg.wantBodies) {
			return false
		}
		if cfg.skipQueryIdentity {
			return true
		}
		return s.identity == cfg.userID && s.userName == cfg.userName
	}) {
		s := inbox.snapshot()
		got := e2eMessageBodies(s.messages)
		missing, extra := e2eBodyDiff(got, cfg.wantBodies)
		return fmt.Errorf("client %d: query did not converge with identity %q (hits=%d identity=%q user_name=%q error=%q got=%d want=%d missing=%v extra=%v)",
			cfg.id, cfg.userID, s.queryHits, s.identity, s.userName, s.queryErr, len(got), len(cfg.wantBodies), missing, extra)
	}

	if cfg.role == e2eAuthRevoked {
		hitsAfterAuth := inbox.snapshot().queryHits
		signalRevokeReady()
		select {
		case <-barriers.goRevoke:
		case <-ctx.Done():
			return fmt.Errorf("client %d: %w", cfg.id, ctx.Err())
		}
		if !waitPred(ctx, func() bool {
			s := inbox.snapshot()
			if s.queryHits <= hitsAfterAuth || s.queryErr != "forbidden" {
				return false
			}
			if cfg.skipQueryIdentity {
				return true
			}
			return s.identity == cfg.userID
		}) {
			s := inbox.snapshot()
			return fmt.Errorf("client %d: did not de-auth after room access was removed (hits=%d identity=%q error=%q)",
				cfg.id, s.queryHits, s.identity, s.queryErr)
		}
		return nil
	}

	s := inbox.snapshot()
	if s.queryKey != cfg.queryKey {
		return fmt.Errorf("client %d: query_key = %q, want %q", cfg.id, s.queryKey, cfg.queryKey)
	}
	if !cfg.skipQueryIdentity {
		if s.identity != cfg.userID {
			return fmt.Errorf("client %d: query identity = %q, want %q", cfg.id, s.identity, cfg.userID)
		}
		if s.userName != cfg.userName {
			return fmt.Errorf("client %d: query user_name = %q, want %q", cfg.id, s.userName, cfg.userName)
		}
	}
	if s.queryErr != "" {
		return fmt.Errorf("client %d: query error = %q, want empty", cfg.id, s.queryErr)
	}
	for _, item := range s.messages {
		m, ok := item.(map[string]interface{})
		if !ok {
			return fmt.Errorf("client %d: query item is %T, want object", cfg.id, item)
		}
		if m["RoomID"] != cfg.room {
			return fmt.Errorf("client %d: query leaked RoomID %v into room %s", cfg.id, m["RoomID"], cfg.room)
		}
		body, _ := m["Body"].(string)
		wantAuthor, ok := authorIDFromBody(body)
		if !ok {
			return fmt.Errorf("client %d: unexpected message body %q", cfg.id, body)
		}
		if m["AuthorID"] != wantAuthor {
			return fmt.Errorf("client %d: message %q AuthorID = %v, want %s", cfg.id, body, m["AuthorID"], wantAuthor)
		}
	}
	return nil
}

func authorIDFromBody(body string) (string, bool) {
	var mutatorID, n int
	if _, err := fmt.Sscanf(body, "c%d-n%d", &mutatorID, &n); err != nil {
		return "", false
	}
	return authorIDForMutator(mutatorID), true
}

// TestConcurrentWebsocketAuthGetIdentityEndToEnd is the authenticated counterpart of
// TestConcurrentWebsocketClientsEndToEnd. Each client presents an access token that
// VerifyToken looks up in the database. Queries use ctx.Auth.GetIdentity and mutations
// use ctx.AuthCtx.GetIdentity, then compare that identity against users/memberships in
// the DB. Queries also track room_members by user id, so deleting some members mid-run
// re-fires those clients from an authorized result into a forbidden state. A later
// test will cover the same scenario with Guard functions.
func TestConcurrentWebsocketAuthGetIdentityEndToEnd(t *testing.T) {
	const (
		nMutators     = 24
		nWatchers     = 16
		nDroppers     = 10
		nRevoked      = 6
		nOutsiders    = 4
		nBadTokens    = 4
		nUnauthed     = 4
		mutationsEach = 5
	)
	nMembers := nMutators + nWatchers + nDroppers + nRevoked
	nClients := nMembers + nOutsiders + nBadTokens + nUnauthed

	e := newConcurrentAuthTestEngine(t)
	e.Profiler().Start()
	defer func() {
		metrics := e.Profiler().DumpMetricsAndFlush()
		t.Log(SanitizeMetrics(metrics))
	}()
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		id, err := ctx.Auth.GetIdentity()
		if err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		if id == "" {
			return map[string]interface{}{"error": "unauthenticated"}, nil
		}
		ctx.TrackCollection("room_members", "user_id", id)
		var user testUser
		if err := ctx.DB.Where("id = ?", id).First(&user).Error; err != nil {
			return map[string]interface{}{"error": "unknown user", "identity": id}, nil
		}
		room, _ := ctx.Params["room"].(string)
		var member testRoomMember
		if err := ctx.DB.Where("user_id = ? AND room_id = ?", id, room).First(&member).Error; err != nil {
			return map[string]interface{}{"error": "forbidden", "identity": id, "user_name": user.Name}, nil
		}
		ctx.TrackCollection("messages", "room_id", room)
		msgs := make([]testAuthoredMessage, 0)
		if err := ctx.DB.Where("room_id = ?", room).Order("id").Find(&msgs).Error; err != nil {
			return map[string]interface{}{"error": err.Error(), "identity": id, "user_name": user.Name}, nil
		}
		return map[string]interface{}{
			"identity":  id,
			"user_name": user.Name,
			"messages":  msgs,
		}, nil
	})
	e.RegisterMutation("createMessage", func(ctx *MutationCtx) (any, error) {
		id, err := ctx.Auth.GetIdentity()
		if err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		if id == "" {
			return map[string]interface{}{"error": "unauthenticated"}, nil
		}
		var user testUser
		if err := ctx.DB.Where("id = ?", id).First(&user).Error; err != nil {
			return map[string]interface{}{"error": "unknown user"}, nil
		}
		room, _ := ctx.Params["room"].(string)
		var member testRoomMember
		if err := ctx.DB.Where("user_id = ? AND room_id = ?", id, room).First(&member).Error; err != nil {
			return map[string]interface{}{"error": "forbidden"}, nil
		}
		msg := testAuthoredMessage{
			Body:     ctx.Params["body"].(string),
			RoomID:   room,
			AuthorID: id,
		}
		if err := ctx.DB.Create(&msg).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return msg, nil
	})

	expiresAt := time.Now().Add(time.Hour)
	users := make([]testUser, 0, nMembers+nOutsiders)
	tokens := make([]testAccessToken, 0, nMembers+nOutsiders)
	members := make([]testRoomMember, 0, nMembers+nOutsiders)
	userNameOf := func(id int) string { return fmt.Sprintf("User %d", id) }
	tokenOf := func(id int) string { return fmt.Sprintf("tok-%d", id) }
	roomOf := func(id int) string {
		if id%2 == 0 {
			return "alpha"
		}
		return "bravo"
	}
	bodyOf := func(mutatorID, n int) string {
		return fmt.Sprintf("c%d-n%d", mutatorID, n)
	}

	for id := 0; id < nMembers; id++ {
		uid := authorIDForMutator(id)
		users = append(users, testUser{ID: uid, Name: userNameOf(id)})
		tokens = append(tokens, testAccessToken{Token: tokenOf(id), UserID: uid, ExpiresAt: expiresAt})
		members = append(members, testRoomMember{UserID: uid, RoomID: roomOf(id)})
	}
	for i := 0; i < nOutsiders; i++ {
		uid := fmt.Sprintf("outsider-%d", i)
		users = append(users, testUser{ID: uid, Name: fmt.Sprintf("Outsider %d", i)})
		tokens = append(tokens, testAccessToken{Token: fmt.Sprintf("tok-outsider-%d", i), UserID: uid, ExpiresAt: expiresAt})
		members = append(members, testRoomMember{UserID: uid, RoomID: "gamma"})
	}
	if err := e.db.Create(&users).Error; err != nil {
		t.Fatalf("seed users: %v", err)
	}
	if err := e.db.Create(&tokens).Error; err != nil {
		t.Fatalf("seed access tokens: %v", err)
	}
	if err := e.db.Create(&members).Error; err != nil {
		t.Fatalf("seed room members: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(e.Handle))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/"

	wantBodies := map[string]map[string]struct{}{
		"alpha": {},
		"bravo": {},
	}
	for id := 0; id < nMutators; id++ {
		for n := 0; n < mutationsEach; n++ {
			wantBodies[roomOf(id)][bodyOf(id, n)] = struct{}{}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	barriers := &e2eBarriers{goMutate: make(chan struct{}), goRevoke: make(chan struct{})}
	barriers.subscribed.Add(nClients)
	barriers.readyToRevoke.Add(nMutators + nWatchers + nRevoked)
	errCh := make(chan error, nClients)

	startClient := func(id int, role e2eAuthRole, room, userID, userName, token string, want map[string]struct{}) {
		go func() {
			errCh <- runAuthE2EClient(ctx, wsURL, authE2EClientCfg{
				id:            id,
				role:          role,
				room:          room,
				queryKey:      fmt.Sprintf("client-%d", id),
				userID:        userID,
				userName:      userName,
				token:         token,
				mutationsEach: mutationsEach,
				wantBodies:    want,
				bodyOf:        bodyOf,
			}, barriers)
		}()
	}
	for id := 0; id < nMutators; id++ {
		startClient(id, e2eAuthMutator, roomOf(id), authorIDForMutator(id), userNameOf(id), tokenOf(id), wantBodies[roomOf(id)])
	}
	for i := 0; i < nWatchers; i++ {
		id := nMutators + i
		startClient(id, e2eAuthWatcher, roomOf(id), authorIDForMutator(id), userNameOf(id), tokenOf(id), wantBodies[roomOf(id)])
	}
	for i := 0; i < nDroppers; i++ {
		id := nMutators + nWatchers + i
		startClient(id, e2eAuthDropper, roomOf(id), authorIDForMutator(id), userNameOf(id), tokenOf(id), wantBodies[roomOf(id)])
	}
	for i := 0; i < nRevoked; i++ {
		id := nMutators + nWatchers + nDroppers + i
		startClient(id, e2eAuthRevoked, roomOf(id), authorIDForMutator(id), userNameOf(id), tokenOf(id), wantBodies[roomOf(id)])
	}
	for i := 0; i < nOutsiders; i++ {
		id := nMembers + i
		startClient(id, e2eAuthOutsider, "alpha", fmt.Sprintf("outsider-%d", i), fmt.Sprintf("Outsider %d", i), fmt.Sprintf("tok-outsider-%d", i), nil)
	}
	for i := 0; i < nBadTokens; i++ {
		id := nMembers + nOutsiders + i
		startClient(id, e2eAuthBadToken, "alpha", "", "", fmt.Sprintf("no-such-token-%d", i), nil)
	}
	for i := 0; i < nUnauthed; i++ {
		id := nMembers + nOutsiders + nBadTokens + i
		startClient(id, e2eAuthUnauthed, "alpha", "", "", "", nil)
	}

	subscribed := make(chan struct{})
	go func() {
		barriers.subscribed.Wait()
		close(subscribed)
	}()
	select {
	case <-subscribed:
	case <-ctx.Done():
		for {
			select {
			case err := <-errCh:
				if err != nil {
					t.Error(err)
				}
			default:
				t.Fatal("timed out waiting for all clients to authenticate and receive an initial query result")
			}
		}
	}
	close(barriers.goMutate)

	readyToRevoke := make(chan struct{})
	go func() {
		barriers.readyToRevoke.Wait()
		close(readyToRevoke)
	}()
	select {
	case <-readyToRevoke:
	case <-ctx.Done():
		for {
			select {
			case err := <-errCh:
				if err != nil {
					t.Error(err)
				}
			default:
				t.Fatal("timed out waiting for authenticated clients to converge before revoking room access")
			}
		}
	}
	for i := 0; i < nRevoked; i++ {
		id := nMutators + nWatchers + nDroppers + i
		member := testRoomMember{UserID: authorIDForMutator(id), RoomID: roomOf(id)}
		if err := e.db.Delete(&member).Error; err != nil {
			t.Fatalf("revoke room access for %s: %v", member.UserID, err)
		}
	}
	close(barriers.goRevoke)

	for i := 0; i < nClients; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for concurrent authenticated clients to finish")
		}
	}
}

func TestConcurrentWebsocketAuthGuardsEndToEnd(t *testing.T) {
	const (
		nMutators     = 24
		nWatchers     = 16
		nDroppers     = 10
		nRevoked      = 6
		nOutsiders    = 4
		nBadTokens    = 4
		nUnauthed     = 4
		mutationsEach = 5
	)
	nMembers := nMutators + nWatchers + nDroppers + nRevoked
	nClients := nMembers + nOutsiders + nBadTokens + nUnauthed

	e := newConcurrentAuthTestEngine(t)
	e.Profiler().Start()
	defer func() {
		metrics := e.Profiler().DumpMetricsAndFlush()
		t.Log(SanitizeMetrics(metrics))
	}()
	e.RegisterGuard("hasAccess", func(ctx *GuardCtx) (any, error) {
		id, err := ctx.Auth.GetIdentity()
		if err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		if id == "" {
			return map[string]interface{}{"error": "unauthenticated"}, nil
		}
		var user testUser
		if err := ctx.DB.Where("id = ?", id).First(&user).Error; err != nil {
			return map[string]interface{}{"error": "unknown user"}, nil
		}
		room, _ := ctx.Params["room"].(string)
		var member testRoomMember
		ctx.TrackCollection("room_members", "user_id", id)
		if err := ctx.DB.Where("user_id = ? AND room_id = ?", id, room).First(&member).Error; err != nil {
			return map[string]interface{}{"error": "forbidden"}, nil
		}
		return map[string]interface{}{"access": true}, nil
	})
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		hasAccess, err := ctx.Auth.ExecuteGuard("hasAccess", map[string]interface{}{"room": ctx.Params["room"]})
		if err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		access, _ := hasAccess.(map[string]interface{})
		if errMsg, _ := access["error"].(string); errMsg != "" {
			return map[string]interface{}{"error": errMsg}, nil
		}
		if access["access"] != true {
			return map[string]interface{}{"error": "forbidden"}, nil
		}
		room := ctx.Params["room"].(string)
		ctx.TrackCollection("messages", "room_id", room)
		msgs := make([]testAuthoredMessage, 0)
		if err := ctx.DB.Where("room_id = ?", room).Order("id").Find(&msgs).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return map[string]interface{}{
			"messages": msgs,
		}, nil
	})
	e.RegisterMutation("createMessage", func(ctx *MutationCtx) (any, error) {
		id, err := ctx.Auth.GetIdentity()
		if err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		if id == "" {
			return map[string]interface{}{"error": "unauthenticated"}, nil
		}
		var user testUser
		if err := ctx.DB.Where("id = ?", id).First(&user).Error; err != nil {
			return map[string]interface{}{"error": "unknown user"}, nil
		}
		room, _ := ctx.Params["room"].(string)
		var member testRoomMember
		if err := ctx.DB.Where("user_id = ? AND room_id = ?", id, room).First(&member).Error; err != nil {
			return map[string]interface{}{"error": "forbidden"}, nil
		}
		msg := testAuthoredMessage{
			Body:     ctx.Params["body"].(string),
			RoomID:   room,
			AuthorID: id,
		}
		if err := ctx.DB.Create(&msg).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}, nil
		}
		return msg, nil
	})

	expiresAt := time.Now().Add(time.Hour)
	users := make([]testUser, 0, nMembers+nOutsiders)
	tokens := make([]testAccessToken, 0, nMembers+nOutsiders)
	members := make([]testRoomMember, 0, nMembers+nOutsiders)
	userNameOf := func(id int) string { return fmt.Sprintf("User %d", id) }
	tokenOf := func(id int) string { return fmt.Sprintf("tok-%d", id) }
	roomOf := func(id int) string {
		if id%2 == 0 {
			return "alpha"
		}
		return "bravo"
	}
	bodyOf := func(mutatorID, n int) string {
		return fmt.Sprintf("c%d-n%d", mutatorID, n)
	}

	for id := 0; id < nMembers; id++ {
		uid := authorIDForMutator(id)
		users = append(users, testUser{ID: uid, Name: userNameOf(id)})
		tokens = append(tokens, testAccessToken{Token: tokenOf(id), UserID: uid, ExpiresAt: expiresAt})
		members = append(members, testRoomMember{UserID: uid, RoomID: roomOf(id)})
	}
	for i := 0; i < nOutsiders; i++ {
		uid := fmt.Sprintf("outsider-%d", i)
		users = append(users, testUser{ID: uid, Name: fmt.Sprintf("Outsider %d", i)})
		tokens = append(tokens, testAccessToken{Token: fmt.Sprintf("tok-outsider-%d", i), UserID: uid, ExpiresAt: expiresAt})
		members = append(members, testRoomMember{UserID: uid, RoomID: "gamma"})
	}
	if err := e.db.Create(&users).Error; err != nil {
		t.Fatalf("seed users: %v", err)
	}
	if err := e.db.Create(&tokens).Error; err != nil {
		t.Fatalf("seed access tokens: %v", err)
	}
	if err := e.db.Create(&members).Error; err != nil {
		t.Fatalf("seed room members: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(e.Handle))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/"

	wantBodies := map[string]map[string]struct{}{
		"alpha": {},
		"bravo": {},
	}
	for id := 0; id < nMutators; id++ {
		for n := 0; n < mutationsEach; n++ {
			wantBodies[roomOf(id)][bodyOf(id, n)] = struct{}{}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	barriers := &e2eBarriers{goMutate: make(chan struct{}), goRevoke: make(chan struct{})}
	barriers.subscribed.Add(nClients)
	barriers.readyToRevoke.Add(nMutators + nWatchers + nRevoked)
	errCh := make(chan error, nClients)

	startClient := func(id int, role e2eAuthRole, room, userID, userName, token string, want map[string]struct{}) {
		go func() {
			errCh <- runAuthE2EClient(ctx, wsURL, authE2EClientCfg{
				id:                id,
				role:              role,
				room:              room,
				queryKey:          fmt.Sprintf("client-%d", id),
				userID:            userID,
				userName:          userName,
				token:             token,
				mutationsEach:     mutationsEach,
				wantBodies:        want,
				bodyOf:            bodyOf,
				skipQueryIdentity: true,
			}, barriers)
		}()
	}
	for id := 0; id < nMutators; id++ {
		startClient(id, e2eAuthMutator, roomOf(id), authorIDForMutator(id), userNameOf(id), tokenOf(id), wantBodies[roomOf(id)])
	}
	for i := 0; i < nWatchers; i++ {
		id := nMutators + i
		startClient(id, e2eAuthWatcher, roomOf(id), authorIDForMutator(id), userNameOf(id), tokenOf(id), wantBodies[roomOf(id)])
	}
	for i := 0; i < nDroppers; i++ {
		id := nMutators + nWatchers + i
		startClient(id, e2eAuthDropper, roomOf(id), authorIDForMutator(id), userNameOf(id), tokenOf(id), wantBodies[roomOf(id)])
	}
	for i := 0; i < nRevoked; i++ {
		id := nMutators + nWatchers + nDroppers + i
		startClient(id, e2eAuthRevoked, roomOf(id), authorIDForMutator(id), userNameOf(id), tokenOf(id), wantBodies[roomOf(id)])
	}
	for i := 0; i < nOutsiders; i++ {
		id := nMembers + i
		startClient(id, e2eAuthOutsider, "alpha", fmt.Sprintf("outsider-%d", i), fmt.Sprintf("Outsider %d", i), fmt.Sprintf("tok-outsider-%d", i), nil)
	}
	for i := 0; i < nBadTokens; i++ {
		id := nMembers + nOutsiders + i
		startClient(id, e2eAuthBadToken, "alpha", "", "", fmt.Sprintf("no-such-token-%d", i), nil)
	}
	for i := 0; i < nUnauthed; i++ {
		id := nMembers + nOutsiders + nBadTokens + i
		startClient(id, e2eAuthUnauthed, "alpha", "", "", "", nil)
	}

	subscribed := make(chan struct{})
	go func() {
		barriers.subscribed.Wait()
		close(subscribed)
	}()
	select {
	case <-subscribed:
	case <-ctx.Done():
		for {
			select {
			case err := <-errCh:
				if err != nil {
					t.Error(err)
				}
			default:
				t.Fatal("timed out waiting for all clients to authenticate and receive an initial query result")
			}
		}
	}
	close(barriers.goMutate)

	readyToRevoke := make(chan struct{})
	go func() {
		barriers.readyToRevoke.Wait()
		close(readyToRevoke)
	}()
	select {
	case <-readyToRevoke:
	case <-ctx.Done():
		for {
			select {
			case err := <-errCh:
				if err != nil {
					t.Error(err)
				}
			default:
				t.Fatal("timed out waiting for authenticated clients to converge before revoking room access")
			}
		}
	}
	for i := 0; i < nRevoked; i++ {
		id := nMutators + nWatchers + nDroppers + i
		member := testRoomMember{UserID: authorIDForMutator(id), RoomID: roomOf(id)}
		if err := e.db.Delete(&member).Error; err != nil {
			t.Fatalf("revoke room access for %s: %v", member.UserID, err)
		}
	}
	close(barriers.goRevoke)

	for i := 0; i < nClients; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for concurrent authenticated clients to finish")
		}
	}
}
