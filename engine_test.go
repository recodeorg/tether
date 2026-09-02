package tether

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/recodeorg/tether/reactivity"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

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

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	return newTestEngineWithType(t, "sqlite")
}

func newTestEngineWithType(t *testing.T, dbType string) *Engine {
	t.Helper()
	e := NewEngine(newTestDB(t), dbType)
	e.CreateTable("messages", &testMessage{})
	return e
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

func TestNewEngineRejectsInvalidDBType(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewEngine(\"mysql\") did not panic")
		}
	}()
	NewEngine(newTestDB(t), "mysql")
}

func TestNewEngineAcceptsSQLiteAndPostgres(t *testing.T) {
	for _, dbType := range []string{"sqlite", "postgres"} {
		t.Run(dbType, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("NewEngine(%q) panicked: %v", dbType, r)
				}
			}()
			_ = NewEngine(newTestDB(t), dbType)
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
	// auto-tracked row id, so one Save that emits both tags re-runs it twice.
	if got := colRuns.Load(); got != 3 {
		t.Errorf("collection query runs after Update = %d, want 3", got)
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
	// Delete emits both the PK tag and the collection tag; this query listens to both.
	if got := runs.Load(); got != 3 {
		t.Errorf("query runs after Delete = %d, want 3", got)
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

func TestPostgresEngineDoesNotInvalidateOnSQLiteCallbacks(t *testing.T) {
	e := newTestEngineWithType(t, "postgres")
	client := trackClient(t, e)

	var runs atomic.Int64
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) interface{} {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&msgs)
		return msgs
	}, nil)
	subscribe(t, e, client, "getMessages", "lobby", nil)

	if err := e.db.Create(&testMessage{Body: "hi", RoomID: "lobby"}).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("postgres-typed engine query runs after Create = %d, want 1 (callbacks should no-op)", got)
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
	if aliceAuth == nil || aliceAuth.UserID != "alice" {
		t.Fatalf("alice auth = %+v, want userID alice", aliceAuth)
	}
	if bobAuth == nil || bobAuth.UserID != "" {
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
	e.queries["me"] = func(ctx *QueryCtx) interface{} { return "no-auth-call" }
	if _, err := e.ExecuteQuery("me", map[string]interface{}{}, sub, true); err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	if !hasSubscription(e.tracker.GetSubscriptionsToTag("*user_identity:user-7"), sub.SubID) {
		t.Error("permanent *user_identity tag was dropped on a later execution")
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
		id, _ := ctx.AuthCtx.GetIdentity()
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
		return auth != nil && auth.UserID == ""
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
