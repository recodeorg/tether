package tether

import (
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
	"github.com/recodeorg/tether/reactivity"
	"github.com/recodeorg/tether/storage"
	"github.com/recodeorg/tether/storage/local"
	"github.com/recodeorg/tether/utilities"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// postgresTestDSN is the server used when tests are run with -postgres.
const postgresTestDSN = "host=cheetah user=postgres password=secret dbname=mydb port=5432 sslmode=disable"

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

func (a *stubAuth) VerifyToken(db *gorm.DB, token string) (string, time.Time, error) {
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
	sqlDB.SetMaxOpenConns(64)
	sqlDB.SetMaxIdleConns(20)
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
	e := NewEngine(newTestDB(t))
	e.CreateTable("messages", &testMessage{})
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
	if _, err := e.ExecuteQuery(query, params, sub, true); err != nil {
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
			_ = NewEngine(newTestDB(t))
		})
	}
}

func TestRegisterQueryAndGetDependentQueries(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} { return nil }, []string{"messages"})
	e.RegisterQuery("countMessages", func(ctx *QueryCtx) interface{} { return nil }, []string{"messages", "rooms"})

	got := e.GetDependentQueries("messages")
	if !slices.Contains(got, "getMessages") || !slices.Contains(got, "countMessages") {
		t.Errorf("GetDependentQueries(messages) = %v, want both registered queries", got)
	}
	gotRooms := e.GetDependentQueries("rooms")
	if !slices.Contains(gotRooms, "countMessages") {
		t.Errorf("GetDependentQueries(rooms) = %v, want [countMessages]", gotRooms)
	}
	if got := e.GetDependentQueries("missing"); len(got) != 0 {
		t.Errorf("GetDependentQueries(missing) = %v, want empty", got)
	}
}

func TestTrackCollectionAndTrackTableTagFormat(t *testing.T) {
	ctx := &QueryCtx{}
	ctx.TrackCollection("messages", "room_id", "lobby")
	ctx.TrackCollection("messages", "room_id", 5)
	ctx.TrackTable("messages")

	want := []string{"messages_room_id:lobby", "messages_room_id:5", "table_messages:mutated"}
	if !slices.Equal(ctx.Dependencies, want) {
		t.Errorf("Dependencies = %v, want %v", ctx.Dependencies, want)
	}

	guard := &GuardCtx{}
	guard.TrackCollection("room_members", "user_id", "user-7")
	guard.TrackTable("room_members")
	wantGuard := []string{"room_members_user_id:user-7", "table_room_members:mutated"}
	if !slices.Equal(guard.Dependencies, wantGuard) {
		t.Errorf("GuardCtx.Dependencies = %v, want %v", guard.Dependencies, wantGuard)
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
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		runs.Add(1)
		roomID := ctx.Params["room"].(string)
		ctx.TrackCollection("messages", "room_id", roomID)
		var msgs []testMessage
		if err := ctx.DB.Where("room_id = ?", roomID).Find(&msgs).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return msgs
	}, nil)

	subscribe(t, e, client, "getMessages", "lobby", map[string]interface{}{"room": "lobby"})
	if got := runs.Load(); got != 1 {
		t.Fatalf("query runs after subscribe = %d, want 1", got)
	}
	drain(client)

	e.RegisterMutation("createMessage", func(ctx *MutationCtx) interface{} {
		msg := testMessage{Body: ctx.Params["body"].(string), RoomID: ctx.Params["room"].(string)}
		if err := ctx.DB.Create(&msg).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return msg
	})
	if _, err := e.ExecuteMutation("createMessage", map[string]interface{}{"body": "hi", "room": "lobby"}, client.ID, "m1"); err != nil {
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
	e.RegisterQuery("getLobby", func(ctx *QueryCtx) interface{} {
		lobbyRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&msgs)
		return msgs
	}, nil)
	e.RegisterQuery("getOther", func(ctx *QueryCtx) interface{} {
		otherRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "other")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "other").Find(&msgs)
		return msgs
	}, nil)

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
	e.RegisterQuery("byID", func(ctx *QueryCtx) interface{} {
		pkRuns.Add(1)
		var got testMessage
		ctx.DB.First(&got, msg.ID)
		return got
	}, nil)
	e.RegisterQuery("byRoom", func(ctx *QueryCtx) interface{} {
		colRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&msgs)
		return msgs
	}, nil)
	e.RegisterQuery("otherRoom", func(ctx *QueryCtx) interface{} {
		otherRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "other")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "other").Find(&msgs)
		return msgs
	}, nil)

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
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&msgs)
		return msgs
	}, nil)
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

func TestMovingRecordInvalidatesOldAndNewCollections(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	msg := testMessage{Body: "moving", RoomID: "old"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("seed Create: %v", err)
	}

	var oldRuns, newRuns atomic.Int64
	e.RegisterQuery("oldRoom", func(ctx *QueryCtx) interface{} {
		oldRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "old")
		return "old"
	}, nil)
	e.RegisterQuery("newRoom", func(ctx *QueryCtx) interface{} {
		newRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "new")
		return "new"
	}, nil)
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
	e.RegisterQuery("oldRoom", func(ctx *QueryCtx) interface{} {
		oldRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "old")
		return "old"
	}, nil)
	e.RegisterQuery("newRoom", func(ctx *QueryCtx) interface{} {
		newRuns.Add(1)
		ctx.TrackCollection("messages", "room_id", "new")
		return "new"
	}, nil)
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
	e.RegisterQuery("byID", func(ctx *QueryCtx) interface{} {
		runs.Add(1)
		var got testMessage
		ctx.DB.First(&got, msg.ID)
		return got
	}, nil)
	subscribe(t, e, client, "byID", "id", nil)

	if err := e.db.Model(&testMessage{}).Where("id = ?", msg.ID).Updates(map[string]interface{}{"body": "patched"}).Error; err != nil {
		t.Fatalf("Updates: %v", err)
	}
	if got := runs.Load(); got != 2 {
		t.Errorf("query runs after map Updates = %d, want 2", got)
	}
}

func TestAutoTrackRecordsLoadedIDs(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	msg := testMessage{Body: "tracked", RoomID: "lobby"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("seed Create: %v", err)
	}

	e.RegisterQuery("getOne", func(ctx *QueryCtx) interface{} {
		var got testMessage
		ctx.DB.First(&got, msg.ID)
		return got
	}, nil)
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

	e.RegisterQuery("getAll", func(ctx *QueryCtx) interface{} {
		var got []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&got)
		return got
	}, nil)
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

	e.RegisterQuery("getNote", func(ctx *QueryCtx) interface{} {
		var got testNote
		ctx.DB.First(&got, note.NoteID)
		return got
	}, nil)
	sub := subscribe(t, e, client, "getNote", "note", nil)

	tag := fmt.Sprintf("notes:%v", note.NoteID)
	if !hasSubscription(e.tracker.GetSubscriptionsToTag(tag), sub.SubID) {
		t.Errorf("auto-track did not record primary key tag %q for a model whose PK is not named ID", tag)
	}
}

func TestTrackTableIsInvalidatedByMutations(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var runs atomic.Int64
	e.RegisterQuery("allMessages", func(ctx *QueryCtx) interface{} {
		runs.Add(1)
		ctx.TrackTable("messages")
		var msgs []testMessage
		ctx.DB.Find(&msgs)
		return msgs
	}, nil)
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
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		return []testMessage{}
	}, nil)
	subscribe(t, e, a, "getMessages", "a", map[string]interface{}{"who": "a"})
	subscribe(t, e, b, "getMessages", "b", map[string]interface{}{"who": "b"})
	drain(a)
	drain(b)

	e.InvalidateTag("messages_room_id:lobby")
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
		e.InvalidateTag("messages:999")
	})
}

func TestMutationOnOneClientPushesQueryToSubscribersOnly(t *testing.T) {
	e := newTestEngine(t)
	subscriber := trackClient(t, e)
	mutator := trackClient(t, e)

	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&msgs)
		return len(msgs)
	}, nil)
	e.RegisterMutation("createMessage", func(ctx *MutationCtx) interface{} {
		msg := testMessage{Body: "hi", RoomID: "lobby"}
		ctx.DB.Create(&msg)
		return msg.ID
	})

	subscribe(t, e, subscriber, "getMessages", "lobby", nil)
	drain(subscriber)
	drain(mutator)

	if _, err := e.ExecuteMutation("createMessage", map[string]interface{}{}, mutator.ID, "mut-1"); err != nil {
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
	e.RegisterQuery("brokenFind", func(ctx *QueryCtx) interface{} {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		err := ctx.DB.Where("not_a_column = 1").Find(&msgs).Error
		if err == nil {
			t.Error("expected GORM error from invalid column")
		}
		return map[string]interface{}{"error": err.Error()}
	}, nil)

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
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Find(&msgs)
		return msgs
	}, nil)
	subscribe(t, e, client, "getMessages", "lobby", nil)
	drain(client)

	e.RegisterMutation("badCreate", func(ctx *MutationCtx) interface{} {
		err := ctx.DB.Exec("INSERT INTO messages (not_a_column) VALUES (1)").Error
		if err == nil {
			return "unexpected success"
		}
		return map[string]interface{}{"error": err.Error()}
	})

	var result interface{}
	var err error
	mustNoPanic(t, "failing mutation", func() {
		result, err = e.ExecuteMutation("badCreate", map[string]interface{}{}, client.ID, "m-bad")
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

func TestExecuteQueryUnserializableParamsDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.RegisterQuery("noop", func(ctx *QueryCtx) interface{} { return "ok" }, nil)
	sub := e.tracker.SubscribeToQuery(client.ID, "noop", "k", map[string]interface{}{})

	var err error
	mustNoPanic(t, "ExecuteQuery(bad params)", func() {
		_, err = e.ExecuteQuery("noop", map[string]interface{}{"ch": make(chan int)}, sub, true)
	})
	if err == nil {
		t.Error("ExecuteQuery with unmarshalable params returned nil error")
	}
}

func TestExecuteQueryUnserializableResultDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.RegisterQuery("bad", func(ctx *QueryCtx) interface{} { return make(chan int) }, nil)
	sub := e.tracker.SubscribeToQuery(client.ID, "bad", "k", map[string]interface{}{})

	var err error
	mustNoPanic(t, "ExecuteQuery(bad result)", func() {
		_, err = e.ExecuteQuery("bad", map[string]interface{}{}, sub, true)
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
		_, err := e.ExecuteQuery("missing", map[string]interface{}{}, sub, true)
		if err == nil {
			t.Error("ExecuteQuery(unknown) returned nil error")
		}
	})
}

func TestUnknownMutationDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	mustNoPanic(t, "ExecuteMutation(unknown)", func() {
		_, err := e.ExecuteMutation("missing", map[string]interface{}{}, client.ID, "m1")
		if err == nil {
			t.Error("ExecuteMutation(unknown) returned nil error")
		}
	})
}

func TestMalformedSubscribeDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	mustNoPanic(t, "subscribe without fields", func() {
		_ = e.OnReceiveMessage(client.ID, map[string]interface{}{"type": "subscribe"})
	})
	mustNoPanic(t, "subscribe with nil params", func() {
		_ = e.OnReceiveMessage(client.ID, map[string]interface{}{
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
		_ = e.OnReceiveMessage(client.ID, map[string]interface{}{"type": "mutation"})
	})
}

func TestMalformedAuthDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	mustNoPanic(t, "auth without token", func() {
		_ = e.OnReceiveMessage(client.ID, map[string]interface{}{"type": "auth"})
	})
}

func TestQueryHashSkipsUnchangedPushUnlessForced(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	e.RegisterQuery("const", func(ctx *QueryCtx) interface{} {
		return map[string]interface{}{"n": 1}
	}, nil)
	sub := subscribe(t, e, client, "const", "k", map[string]interface{}{"p": 1})
	drain(client)

	if _, err := e.ExecuteQuery("const", map[string]interface{}{"p": 1}, sub, false); err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	if got := drain(client); len(got) != 0 {
		t.Errorf("unchanged query with forceSend=false pushed %d messages, want 0", len(got))
	}

	if _, err := e.ExecuteQuery("const", map[string]interface{}{"p": 1}, sub, true); err != nil {
		t.Fatalf("ExecuteQuery force: %v", err)
	}
	if got := drain(client); len(got) != 1 {
		t.Errorf("unchanged query with forceSend=true pushed %d messages, want 1", len(got))
	}
}

func TestQueryResultIncludesLocationAndQueryKey(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	e.RegisterQuery("getThing", func(ctx *QueryCtx) interface{} {
		return map[string]interface{}{"ok": true, "p": ctx.Params["id"]}
	}, nil)
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
	if err := e.OnReceiveMessage(alice.ID, map[string]interface{}{"type": "auth", "token": "alice-token"}); err != nil {
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

	e.RegisterQuery("me", func(ctx *QueryCtx) interface{} {
		id, err := ctx.Auth.GetIdentity()
		if err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return map[string]interface{}{"id": id}
	}, nil)

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

	e.RegisterQuery("me", func(ctx *QueryCtx) interface{} {
		id, _ := ctx.Auth.GetIdentity()
		return id
	}, nil)
	sub := subscribe(t, e, client, "me", "me", nil)

	if !hasSubscription(e.tracker.GetSubscriptionsToTag("*user_identity:user-7"), sub.SubID) {
		t.Fatal("GetIdentity did not register *user_identity:user-7")
	}

	// Permanent tags should survive a later query that does not call GetIdentity.
	e.queries["me"] = Query{Func: func(ctx *QueryCtx) interface{} { return "no-auth-call" }, Internal: false}
	if _, err := e.ExecuteQuery("me", map[string]interface{}{}, sub, true); err != nil {
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

	e.RegisterGuard("allow", func(ctx *GuardCtx) interface{} {
		id, _ := ctx.Auth.GetIdentity()
		ctx.TrackCollection("room_members", "user_id", id)
		return map[string]interface{}{"access": true}
	})
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		res, err := ctx.Auth.ExecuteGuard("allow", map[string]interface{}{"room": "lobby"})
		if err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		ctx.TrackCollection("messages", "room_id", "lobby")
		return res
	}, nil)
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
	e.RegisterGuard("allow", func(ctx *GuardCtx) interface{} {
		guardRuns.Add(1)
		id, _ := ctx.Auth.GetIdentity()
		ctx.TrackCollection("room_members", "user_id", id)
		return map[string]interface{}{"ok": true}
	})
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		queryRuns.Add(1)
		if _, err := ctx.Auth.ExecuteGuard("allow", map[string]interface{}{"room": "lobby"}); err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		ctx.TrackCollection("messages", "room_id", "lobby")
		return map[string]interface{}{"ok": true}
	}, nil)
	subscribe(t, e, a, "getMessages", "a", map[string]interface{}{"room": "lobby"})
	subscribe(t, e, b, "getMessages", "b", map[string]interface{}{"room": "lobby"})
	if got, want := queryRuns.Load(), int64(2); got != want {
		t.Fatalf("initial query runs = %d, want %d", got, want)
	}
	if got, want := guardRuns.Load(), int64(2); got != want {
		t.Fatalf("initial guard runs = %d, want %d", got, want)
	}

	e.InvalidateTag("messages_room_id:lobby")
	if got, want := queryRuns.Load(), int64(3); got != want {
		t.Errorf("query runs after invalidate = %d, want %d (one batched execution)", got, want)
	}
	if got, want := guardRuns.Load(), int64(2); got != want {
		t.Errorf("guard runs after query invalidate = %d, want %d (cached fingerprints)", got, want)
	}
}

func TestGuardInvalidationRerunsAttachedQuery(t *testing.T) {
	e := newTestEngine(t)
	e.CreateTable("room_members", &testRoomMember{})
	client := trackClient(t, e)
	e.tracker.SetAuth(client.ID, "user-7", time.Now().Add(time.Hour))
	if err := e.db.Create(&testRoomMember{UserID: "user-7", RoomID: "lobby"}).Error; err != nil {
		t.Fatalf("seed membership: %v", err)
	}

	e.RegisterGuard("hasAccess", func(ctx *GuardCtx) interface{} {
		id, _ := ctx.Auth.GetIdentity()
		ctx.TrackCollection("room_members", "user_id", id)
		var member testRoomMember
		if err := ctx.DB.Where("user_id = ? AND room_id = ?", id, ctx.Params["room"]).First(&member).Error; err != nil {
			return map[string]interface{}{"error": "forbidden"}
		}
		return map[string]interface{}{"access": true}
	})
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		hasAccess, err := ctx.Auth.ExecuteGuard("hasAccess", map[string]interface{}{"room": "lobby"})
		if err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		access, _ := hasAccess.(map[string]interface{})
		if errMsg, _ := access["error"].(string); errMsg != "" {
			return map[string]interface{}{"error": errMsg}
		}
		return map[string]interface{}{"ok": true}
	}, nil)
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

func TestFailedAuthDoesNotSetIdentityOrPanic(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)
	auth := &stubAuth{err: errors.New("bad token")}
	e.SetAuth(auth)

	var err error
	mustNoPanic(t, "failed auth", func() {
		err = e.OnReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "nope"})
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

	if err := e.OnReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
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

func TestMutationAuthMatchesTheCallingClient(t *testing.T) {
	e := newTestEngine(t)
	authed := trackClient(t, e)
	anon := trackClient(t, e)
	e.tracker.SetAuth(authed.ID, "user-1", time.Now().Add(time.Hour))

	type seen struct {
		id string
	}
	var fromAuthed, fromAnon seen
	e.RegisterMutation("whoami", func(ctx *MutationCtx) interface{} {
		id, _ := ctx.Auth.GetIdentity()
		return map[string]interface{}{"id": id}
	})

	mustNoPanic(t, "authed mutation", func() {
		result, err := e.ExecuteMutation("whoami", map[string]interface{}{}, authed.ID, "m1")
		if err != nil {
			t.Errorf("ExecuteMutation: %v", err)
		}
		data := result.(map[string]interface{})
		fromAuthed = seen{id: data["id"].(string)}
	})
	mustNoPanic(t, "anon mutation", func() {
		result, err := e.ExecuteMutation("whoami", map[string]interface{}{}, anon.ID, "m2")
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
	if err := e.OnReceiveMessage(expiring.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
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

func TestAuthExpiryAfterDisconnectDoesNotPanic(t *testing.T) {
	if os.Getenv("TETHER_TEST_CHILD") == "1" {
		e := newTestEngine(t)
		client := reactivity.NewClient(nil)
		e.tracker.Track(client)
		e.SetAuth(&stubAuth{userID: "temp", expiresAt: time.Now().Add(20 * time.Millisecond)})
		if err := e.OnReceiveMessage(client.ID, map[string]interface{}{"type": "auth", "token": "t"}); err != nil {
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

func TestOnReceiveMessageSubscribeAndMutation(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var queryRuns atomic.Int64
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		queryRuns.Add(1)
		room := ctx.Params["room"].(string)
		ctx.TrackCollection("messages", "room_id", room)
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", room).Find(&msgs)
		return len(msgs)
	}, nil)
	e.RegisterMutation("createMessage", func(ctx *MutationCtx) interface{} {
		msg := testMessage{Body: ctx.Params["body"].(string), RoomID: ctx.Params["room"].(string)}
		if err := ctx.DB.Create(&msg).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return msg.ID
	})

	mustNoPanic(t, "subscribe", func() {
		if err := e.OnReceiveMessage(client.ID, map[string]interface{}{
			"type":      "subscribe",
			"location":  "getMessages",
			"params":    map[string]interface{}{"room": "lobby"},
			"query_key": "lobby",
		}); err != nil {
			t.Errorf("subscribe: %v", err)
		}
	})
	if queryRuns.Load() != 1 {
		t.Fatalf("query runs after subscribe = %d, want 1", queryRuns.Load())
	}
	drain(client)

	mustNoPanic(t, "mutation", func() {
		if err := e.OnReceiveMessage(client.ID, map[string]interface{}{
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

func TestUnsubscribeMessageStopsQueryPushes(t *testing.T) {
	e := newTestEngine(t)
	client := trackClient(t, e)

	var queryRuns atomic.Int64
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		queryRuns.Add(1)
		room := ctx.Params["room"].(string)
		ctx.TrackCollection("messages", "room_id", room)
		return room
	}, nil)

	params := map[string]interface{}{"room": "lobby"}
	if err := e.OnReceiveMessage(client.ID, map[string]interface{}{
		"type":      "subscribe",
		"location":  "getMessages",
		"params":    params,
		"query_key": "lobby",
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	initial := queryMessages(t, drain(client))
	if len(initial) != 1 {
		t.Fatalf("initial query pushes = %d, want 1", len(initial))
	}
	if initial[0]["query_key"] != "lobby" {
		t.Fatalf("query_key = %v, want lobby", initial[0]["query_key"])
	}

	if err := e.OnReceiveMessage(client.ID, map[string]interface{}{
		"type":      "unsubscribe",
		"location":  "getMessages",
		"params":    params,
		"query_key": "lobby",
	}); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}

	e.InvalidateTag("messages_room_id:lobby")
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
		if err := e.OnReceiveMessage(client.ID, map[string]interface{}{"type": "ping"}); err != nil {
			t.Errorf("unknown type returned error: %v", err)
		}
	})
}

func TestOnConnectAndOnDisconnect(t *testing.T) {
	e := newTestEngine(t)
	if err := e.OnConnect("c1"); err != nil {
		t.Errorf("OnConnect: %v", err)
	}
	if err := e.OnDisconnect("c1"); err != nil {
		t.Errorf("OnDisconnect: %v", err)
	}
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
	store := local.NewLocalStorage(t.TempDir())
	e.UseStorage(store)
	e.SetAllowedOrigins([]string{"https://app.example"})

	upload, err := e.getUploadURL(storage.UploadOptions{})
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
		next, err := e.getUploadURL(storage.UploadOptions{})
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
	})
}

func TestStorageRoutesDefaultToSameOrigin(t *testing.T) {
	e := newTestEngine(t)
	store := local.NewLocalStorage(t.TempDir())
	e.UseStorage(store)

	upload, err := e.getUploadURL(storage.UploadOptions{})
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
	store := local.NewLocalStorage(t.TempDir())
	e.UseStorage(store)
	e.SetCheckOrigin(func(r *http.Request) bool {
		return strings.HasSuffix(r.Header.Get("Origin"), ".example")
	})

	upload, err := e.getUploadURL(storage.UploadOptions{})
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
		_ = e.OnReceiveMessage(client.ID, map[string]interface{}{
			"type":      "subscribe",
			"location":  "does-not-exist",
			"params":    map[string]interface{}{},
			"query_key": "k",
		})
	})
}

func TestExecuteQueryWithoutTrackedClientDoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterQuery("q", func(ctx *QueryCtx) interface{} { return "ok" }, nil)
	ghost := &reactivity.Subscription{
		SubID:    "ghost",
		Client:   &reactivity.Client{ID: "missing", Send: make(chan []byte, 1)},
		Query:    "q",
		QueryKey: "k",
		Params:   map[string]interface{}{},
	}
	mustNoPanic(t, "ExecuteQuery untracked client", func() {
		_, _ = e.ExecuteQuery("q", map[string]interface{}{}, ghost, true)
	})
}

func TestDefaultAuthVerifyToken(t *testing.T) {
	var a defaultAuth
	userID, expiresAt, err := a.VerifyToken(nil, "token")
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

	e := NewEngine(db)
	e.CreateTable("messages", &testMessage{})
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
	e.Profiler.Start()
	defer func() {
		metrics := e.Profiler.DumpMetricsAndFlush()
		t.Log(utilities.SanitizeMetrics(metrics))
	}()
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		room := ctx.Params["room"].(string)
		ctx.TrackCollection("messages", "room_id", room)
		var msgs []testMessage
		if err := ctx.DB.Where("room_id = ?", room).Order("id").Find(&msgs).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return msgs
	}, nil)
	e.RegisterMutation("createMessage", func(ctx *MutationCtx) interface{} {
		msg := testMessage{
			Body:   ctx.Params["body"].(string),
			RoomID: ctx.Params["room"].(string),
		}
		if err := ctx.DB.Create(&msg).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return msg
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

func (accessTokenAuth) VerifyToken(db *gorm.DB, token string) (string, time.Time, error) {
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
	e.CreateTable("users", &testUser{})
	e.CreateTable("access_tokens", &testAccessToken{})
	e.CreateTable("room_members", &testRoomMember{})
	e.CreateTable("messages", &testAuthoredMessage{})
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
	e.Profiler.Start()
	defer func() {
		metrics := e.Profiler.DumpMetricsAndFlush()
		t.Log(utilities.SanitizeMetrics(metrics))
	}()
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		id, err := ctx.Auth.GetIdentity()
		if err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		if id == "" {
			return map[string]interface{}{"error": "unauthenticated"}
		}
		ctx.TrackCollection("room_members", "user_id", id)
		var user testUser
		if err := ctx.DB.Where("id = ?", id).First(&user).Error; err != nil {
			return map[string]interface{}{"error": "unknown user", "identity": id}
		}
		room, _ := ctx.Params["room"].(string)
		var member testRoomMember
		if err := ctx.DB.Where("user_id = ? AND room_id = ?", id, room).First(&member).Error; err != nil {
			return map[string]interface{}{"error": "forbidden", "identity": id, "user_name": user.Name}
		}
		ctx.TrackCollection("messages", "room_id", room)
		msgs := make([]testAuthoredMessage, 0)
		if err := ctx.DB.Where("room_id = ?", room).Order("id").Find(&msgs).Error; err != nil {
			return map[string]interface{}{"error": err.Error(), "identity": id, "user_name": user.Name}
		}
		return map[string]interface{}{
			"identity":  id,
			"user_name": user.Name,
			"messages":  msgs,
		}
	}, nil)
	e.RegisterMutation("createMessage", func(ctx *MutationCtx) interface{} {
		id, err := ctx.Auth.GetIdentity()
		if err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		if id == "" {
			return map[string]interface{}{"error": "unauthenticated"}
		}
		var user testUser
		if err := ctx.DB.Where("id = ?", id).First(&user).Error; err != nil {
			return map[string]interface{}{"error": "unknown user"}
		}
		room, _ := ctx.Params["room"].(string)
		var member testRoomMember
		if err := ctx.DB.Where("user_id = ? AND room_id = ?", id, room).First(&member).Error; err != nil {
			return map[string]interface{}{"error": "forbidden"}
		}
		msg := testAuthoredMessage{
			Body:     ctx.Params["body"].(string),
			RoomID:   room,
			AuthorID: id,
		}
		if err := ctx.DB.Create(&msg).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return msg
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
	e.Profiler.Start()
	defer func() {
		metrics := e.Profiler.DumpMetricsAndFlush()
		t.Log(utilities.SanitizeMetrics(metrics))
	}()
	e.RegisterGuard("hasAccess", func(ctx *GuardCtx) interface{} {
		id, err := ctx.Auth.GetIdentity()
		if err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		if id == "" {
			return map[string]interface{}{"error": "unauthenticated"}
		}
		var user testUser
		if err := ctx.DB.Where("id = ?", id).First(&user).Error; err != nil {
			return map[string]interface{}{"error": "unknown user"}
		}
		room, _ := ctx.Params["room"].(string)
		var member testRoomMember
		ctx.TrackCollection("room_members", "user_id", id)
		if err := ctx.DB.Where("user_id = ? AND room_id = ?", id, room).First(&member).Error; err != nil {
			return map[string]interface{}{"error": "forbidden"}
		}
		return map[string]interface{}{"access": true}
	})
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		hasAccess, err := ctx.Auth.ExecuteGuard("hasAccess", map[string]interface{}{"room": ctx.Params["room"]})
		if err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		access, _ := hasAccess.(map[string]interface{})
		if errMsg, _ := access["error"].(string); errMsg != "" {
			return map[string]interface{}{"error": errMsg}
		}
		if access["access"] != true {
			return map[string]interface{}{"error": "forbidden"}
		}
		room := ctx.Params["room"].(string)
		ctx.TrackCollection("messages", "room_id", room)
		msgs := make([]testAuthoredMessage, 0)
		if err := ctx.DB.Where("room_id = ?", room).Order("id").Find(&msgs).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return map[string]interface{}{
			"messages": msgs,
		}
	}, nil)
	e.RegisterMutation("createMessage", func(ctx *MutationCtx) interface{} {
		id, err := ctx.Auth.GetIdentity()
		if err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		if id == "" {
			return map[string]interface{}{"error": "unauthenticated"}
		}
		var user testUser
		if err := ctx.DB.Where("id = ?", id).First(&user).Error; err != nil {
			return map[string]interface{}{"error": "unknown user"}
		}
		room, _ := ctx.Params["room"].(string)
		var member testRoomMember
		if err := ctx.DB.Where("user_id = ? AND room_id = ?", id, room).First(&member).Error; err != nil {
			return map[string]interface{}{"error": "forbidden"}
		}
		msg := testAuthoredMessage{
			Body:     ctx.Params["body"].(string),
			RoomID:   room,
			AuthorID: id,
		}
		if err := ctx.DB.Create(&msg).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return msg
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
