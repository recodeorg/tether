package tether

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/jackc/pgx/v5"
	"github.com/recodeorg/tether/internal/reactivity"
	"github.com/recodeorg/tether/utilities"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newPooledEngine opens a file-backed WAL database (or the Postgres test
// schema) with a pool of maxConns, so re-run queries use real separate
// connections instead of a shared in-memory one.
func newPooledEngine(t *testing.T, maxConns int) *Engine {
	t.Helper()
	var db *gorm.DB
	if *usePostgres {
		db = newPostgresTestDB(t)
	} else {
		path := filepath.Join(t.TempDir(), "tether.db")
		var err error
		db, err = gorm.Open(sqlite.Open(path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		if err != nil {
			t.Fatalf("open sqlite: %v", err)
		}
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	sqlDB.SetMaxOpenConns(maxConns)
	sqlDB.SetMaxIdleConns(maxConns)
	t.Cleanup(func() { _ = sqlDB.Close() })
	e, err := NewEngine(db)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}
	t.Cleanup(e.Close)
	e.CreateTable(&testMessage{})
	return e
}

// subscribeToBody stores a message with Body "old" and subscribes a client to
// a query returning that message's body.
func subscribeToBody(t *testing.T, e *Engine) (*reactivity.Client, testMessage, *atomic.Int64) {
	t.Helper()
	msg := testMessage{Body: "old", RoomID: "lobby"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}
	runs := &atomic.Int64{}
	e.RegisterQuery("getBody", func(ctx *QueryCtx) interface{} {
		runs.Add(1)
		var row testMessage
		if err := ctx.DB.First(&row, msg.ID).Error; err != nil {
			return "error: " + err.Error()
		}
		return row.Body
	})
	client := trackClient(t, e)
	subscribe(t, e, client, "getBody", "body", nil)
	if data, n := lastQueryData(t, client); n != 1 || data != "old" {
		t.Fatalf("initial push = %v (%d messages), want \"old\"", data, n)
	}
	return client, msg, runs
}

func storedBody(t *testing.T, e *Engine, id uint) string {
	t.Helper()
	var row testMessage
	if err := e.db.First(&row, id).Error; err != nil {
		t.Fatalf("load message: %v", err)
	}
	return row.Body
}

func TestTransactionCommitPublishesCommittedData(t *testing.T) {
	e := newPooledEngine(t, 4)
	client, msg, runs := subscribeToBody(t, e)

	err := e.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&msg).Update("body", "new").Error; err != nil {
			return err
		}
		if got := runs.Load(); got != 1 {
			t.Errorf("query runs before commit = %d, want 1", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}

	if got := storedBody(t, e, msg.ID); got != "new" {
		t.Fatalf("stored body = %q, want \"new\"", got)
	}
	data, n := lastQueryData(t, client)
	if n != 1 || data != "new" {
		t.Errorf("pushes after commit = %d, last = %v; want one push of \"new\"", n, data)
	}
}

func TestTransactionDeleteByIDPublishesCollectionCountOnCommit(t *testing.T) {
	e := newPooledEngine(t, 4)
	client := trackClient(t, e)

	msg := testMessage{Body: "bye", RoomID: "r"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}

	var runs atomic.Int64
	e.RegisterQuery("countRoom", func(ctx *QueryCtx) interface{} {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "r")
		var n int64
		if err := ctx.DB.Model(&testMessage{}).Where("room_id = ?", "r").Count(&n).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return n
	})
	subscribe(t, e, client, "countRoom", "count", nil)
	drain(client)

	err := e.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(&testMessage{}, msg.ID).Error; err != nil {
			return err
		}
		if got := runs.Load(); got != 1 {
			t.Errorf("count runs before commit = %d, want 1", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	if got := runs.Load(); got != 2 {
		t.Errorf("count runs after commit = %d, want 2", got)
	}
	data, n := lastQueryData(t, client)
	if n != 1 || data != float64(0) {
		t.Errorf("count push after commit = %v (%d messages), want one push of 0", data, n)
	}
}

func TestTransactionDeleteByIDRollbackDoesNotPublish(t *testing.T) {
	e := newPooledEngine(t, 4)
	client := trackClient(t, e)

	msg := testMessage{Body: "bye", RoomID: "r"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}

	var runs atomic.Int64
	e.RegisterQuery("countRoom", func(ctx *QueryCtx) interface{} {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "r")
		var n int64
		if err := ctx.DB.Model(&testMessage{}).Where("room_id = ?", "r").Count(&n).Error; err != nil {
			return map[string]interface{}{"error": err.Error()}
		}
		return n
	})
	subscribe(t, e, client, "countRoom", "count", nil)
	drain(client)

	abort := errors.New("abort")
	err := e.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(&testMessage{}, msg.ID).Error; err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatalf("Transaction error = %v, want %v", err, abort)
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("count runs after rollback = %d, want 1", got)
	}
	if _, n := lastQueryData(t, client); n != 0 {
		t.Errorf("count pushes after rollback = %d, want 0", n)
	}
	var left int64
	if err := e.db.Model(&testMessage{}).Where("id = ?", msg.ID).Count(&left).Error; err != nil {
		t.Fatalf("count remaining: %v", err)
	}
	if left != 1 {
		t.Fatalf("row count after rollback = %d, want 1", left)
	}
}

func TestTransactionRollbackDiscardsInvalidation(t *testing.T) {
	e := newPooledEngine(t, 4)
	client, msg, runs := subscribeToBody(t, e)

	abort := errors.New("abort")
	err := e.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&msg).Update("body", "new").Error; err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatalf("Transaction error = %v, want %v", err, abort)
	}

	if got := storedBody(t, e, msg.ID); got != "old" {
		t.Fatalf("stored body = %q, want \"old\"", got)
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("query runs after rollback = %d, want 1", got)
	}
	if _, n := lastQueryData(t, client); n != 0 {
		t.Errorf("pushes after rollback = %d, want 0", n)
	}
}

func TestTransactionCommitWithSingleConnectionPool(t *testing.T) {
	e := newPooledEngine(t, 1)
	client, msg, _ := subscribeToBody(t, e)

	done := make(chan error, 1)
	go func() {
		done <- e.db.Transaction(func(tx *gorm.DB) error {
			return tx.Model(&msg).Update("body", "new").Error
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Transaction: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("transaction deadlocked waiting for the only pooled connection")
	}

	data, n := lastQueryData(t, client)
	if n != 1 || data != "new" {
		t.Errorf("pushes after commit = %d, last = %v; want one push of \"new\"", n, data)
	}
}

func TestImplicitTransactionPublishesCommittedData(t *testing.T) {
	e := newPooledEngine(t, 4)
	client, msg, _ := subscribeToBody(t, e)

	if err := e.db.Model(&msg).Update("body", "new").Error; err != nil {
		t.Fatalf("Update: %v", err)
	}
	data, n := lastQueryData(t, client)
	if n != 1 || data != "new" {
		t.Errorf("pushes after update = %d, last = %v; want one push of \"new\"", n, data)
	}
}

func TestConnectionUpdatePublishesCommittedData(t *testing.T) {
	e := newPooledEngine(t, 4)
	client, msg, _ := subscribeToBody(t, e)

	err := e.db.Connection(func(conn *gorm.DB) error {
		return conn.Model(&msg).Update("body", "new").Error
	})
	if err != nil {
		t.Fatalf("Connection: %v", err)
	}
	data, n := lastQueryData(t, client)
	if n != 1 || data != "new" {
		t.Errorf("pushes after update = %d, last = %v; want one push of \"new\"", n, data)
	}
}

func TestConnectionTransactionCommitPublishesCommittedData(t *testing.T) {
	e := newPooledEngine(t, 4)
	client, msg, runs := subscribeToBody(t, e)

	err := e.db.Connection(func(conn *gorm.DB) error {
		return conn.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&msg).Update("body", "new").Error; err != nil {
				return err
			}
			if got := runs.Load(); got != 1 {
				t.Errorf("query runs before commit = %d, want 1", got)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("Connection: %v", err)
	}

	if got := storedBody(t, e, msg.ID); got != "new" {
		t.Fatalf("stored body = %q, want \"new\"", got)
	}
	data, n := lastQueryData(t, client)
	if n != 1 || data != "new" {
		t.Errorf("pushes after commit = %d, last = %v; want one push of \"new\"", n, data)
	}
}

func TestConnectionTransactionRollbackDiscardsInvalidation(t *testing.T) {
	e := newPooledEngine(t, 4)
	client, msg, runs := subscribeToBody(t, e)

	abort := errors.New("abort")
	err := e.db.Connection(func(conn *gorm.DB) error {
		return conn.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&msg).Update("body", "new").Error; err != nil {
				return err
			}
			return abort
		})
	})
	if !errors.Is(err, abort) {
		t.Fatalf("Connection error = %v, want %v", err, abort)
	}

	if got := storedBody(t, e, msg.ID); got != "old" {
		t.Fatalf("stored body = %q, want \"old\"", got)
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("query runs after rollback = %d, want 1", got)
	}
	if _, n := lastQueryData(t, client); n != 0 {
		t.Errorf("pushes after rollback = %d, want 0", n)
	}
}

type recordingTx struct {
	calls []string
}

func (r *recordingTx) PrepareContext(context.Context, string) (*sql.Stmt, error) {
	return nil, errors.New("unused")
}

func (r *recordingTx) ExecContext(_ context.Context, query string, args ...interface{}) (sql.Result, error) {
	r.calls = append(r.calls, fmt.Sprint(query, args))
	return nil, nil
}

func (r *recordingTx) QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error) {
	return nil, errors.New("unused")
}

func (r *recordingTx) QueryRowContext(context.Context, string, ...interface{}) *sql.Row {
	return nil
}

func (r *recordingTx) StmtContext(_ context.Context, stmt *sql.Stmt) *sql.Stmt { return stmt }

func (r *recordingTx) Commit() error {
	r.calls = append(r.calls, "commit")
	return nil
}

func (r *recordingTx) Rollback() error {
	r.calls = append(r.calls, "rollback")
	return nil
}

func TestPostgresNotifyIsSentInsideTheTransaction(t *testing.T) {
	e := &Engine{
		dbType:      "postgres",
		EphemeralID: "sender",
		tracker:     reactivity.NewTracker(),
		Profiler:    utilities.NewProfiler(func(string) {}),
	}

	committed := &recordingTx{}
	tx := &trackedTx{Tx: committed, ctx: context.Background()}
	tx.queue(e, []string{"messages:1", "messages_room_id:lobby"}, "exec", "action")
	tx.queue(e, []string{"messages:1"}, "exec2", "action2")
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	want := []string{`SELECT pg_notify('tether_sync', $1)[{"sender":"sender","tags":["messages:1","messages_room_id:lobby"]}]`, "commit"}
	if strings.Join(committed.calls, "\n") != strings.Join(want, "\n") {
		t.Errorf("commit calls = %q, want %q", committed.calls, want)
	}

	rolledBack := &recordingTx{}
	tx = &trackedTx{Tx: rolledBack, ctx: context.Background()}
	tx.queue(e, []string{"messages:1"}, "exec", "action")
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if len(rolledBack.calls) != 1 || rolledBack.calls[0] != "rollback" {
		t.Errorf("rollback calls = %q, want only rollback", rolledBack.calls)
	}
}

func TestNotifyPayloadsChunkUnderLimit(t *testing.T) {
	var tags []string
	for i := 0; i < 2000; i++ {
		tags = append(tags, fmt.Sprintf("messages_room_id:a,b|\"%d\"<>", i))
	}
	oversized := strings.Repeat("x", maxNotifyPayload)
	payloads := notifyPayloads("sender", append(tags, oversized))
	if len(payloads) < 2 {
		t.Fatalf("payloads = %d, want the tags split across several notifications", len(payloads))
	}
	var got []string
	for _, payload := range payloads {
		if len(payload) >= maxNotifyPayload {
			t.Errorf("payload length %d, want < %d", len(payload), maxNotifyPayload)
		}
		var msg notifyMessage
		if err := json.Unmarshal([]byte(payload), &msg); err != nil {
			t.Fatalf("payload is not valid JSON: %v", err)
		}
		if msg.Sender != "sender" {
			t.Fatalf("payload sender = %q, want %q", msg.Sender, "sender")
		}
		got = append(got, msg.Tags...)
	}
	if !slices.Equal(got, tags) {
		t.Errorf("payloads carried %d tags, want the %d input tags in order without the oversized one", len(got), len(tags))
	}
	if payloads := notifyPayloads("sender", nil); len(payloads) != 0 {
		t.Errorf("notifyPayloads(no tags) = %q, want none", payloads)
	}
}

func TestPostgresNotifyDeliveredOnlyOnCommit(t *testing.T) {
	if !*usePostgres {
		t.Skip("requires -postgres")
	}
	e := newPooledEngine(t, 4)
	msg := testMessage{Body: "old", RoomID: "lobby"}
	if err := e.db.Create(&msg).Error; err != nil {
		t.Fatalf("Create: %v", err)
	}

	ctx := context.Background()
	listener, err := pgx.Connect(ctx, postgresTestDSN)
	if err != nil {
		t.Fatalf("connect listener: %v", err)
	}
	defer listener.Close(ctx)
	if _, err := listener.Exec(ctx, "LISTEN tether_sync"); err != nil {
		t.Fatalf("LISTEN: %v", err)
	}
	nextFromEngine := func(timeout time.Duration) (string, bool) {
		waitCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		for {
			n, err := listener.WaitForNotification(waitCtx)
			if err != nil {
				return "", false
			}
			var msg notifyMessage
			if json.Unmarshal([]byte(n.Payload), &msg) == nil && msg.Sender == e.EphemeralID {
				return n.Payload, true
			}
		}
	}

	_ = e.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&msg).Update("body", "rolled back").Error; err != nil {
			return err
		}
		return errors.New("abort")
	})
	if payload, ok := nextFromEngine(500 * time.Millisecond); ok {
		t.Errorf("notification after rollback = %q, want none", payload)
	}

	if err := e.db.Transaction(func(tx *gorm.DB) error {
		return tx.Model(&msg).Update("body", "new").Error
	}); err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	payload, ok := nextFromEngine(5 * time.Second)
	if !ok {
		t.Fatal("no notification after commit")
	}
	if want := fmt.Sprintf("messages:%d", msg.ID); !strings.Contains(payload, want) {
		t.Errorf("notification payload = %q, want it to include %q", payload, want)
	}

	_ = e.db.Connection(func(conn *gorm.DB) error {
		return conn.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&msg).Update("body", "rolled back").Error; err != nil {
				return err
			}
			return errors.New("abort")
		})
	})
	if payload, ok := nextFromEngine(500 * time.Millisecond); ok {
		t.Errorf("notification after pinned rollback = %q, want none", payload)
	}

	if err := e.db.Connection(func(conn *gorm.DB) error {
		return conn.Transaction(func(tx *gorm.DB) error {
			return tx.Model(&msg).Update("body", "pinned").Error
		})
	}); err != nil {
		t.Fatalf("Connection: %v", err)
	}
	payload, ok = nextFromEngine(5 * time.Second)
	if !ok {
		t.Fatal("no notification after pinned commit")
	}
	if want := fmt.Sprintf("messages:%d", msg.ID); !strings.Contains(payload, want) {
		t.Errorf("pinned notification payload = %q, want it to include %q", payload, want)
	}
}
