package tether

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"unsafe"

	"gorm.io/gorm"
)

// PostgreSQL rejects NOTIFY payloads of 8000 bytes or more.
const maxNotifyPayload = 8000

// trackedPool wraps the engine's connection pool so every transaction GORM
// begins through it (explicit db.Transaction/Begin and GORM's implicit
// per-statement transaction) can hold invalidations until it commits.
type trackedPool struct {
	gorm.ConnPool
}

func (p *trackedPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	var (
		conn gorm.ConnPool
		err  error
	)
	switch beginner := p.ConnPool.(type) {
	case gorm.TxBeginner:
		var tx *sql.Tx
		tx, err = beginner.BeginTx(ctx, opts)
		conn = tx
	case gorm.ConnPoolBeginner:
		conn, err = beginner.BeginTx(ctx, opts)
	default:
		return nil, gorm.ErrInvalidTransaction
	}
	if err != nil {
		return nil, err
	}
	tx, ok := conn.(gorm.Tx)
	if !ok {
		return conn, nil
	}
	return &trackedTx{Tx: tx, pool: p, ctx: ctx}, nil
}

func (p *trackedPool) GetDBConn() (*sql.DB, error) {
	switch pool := p.ConnPool.(type) {
	case *sql.DB:
		return pool, nil
	case gorm.GetDBConnector:
		return pool.GetDBConn()
	}
	return nil, gorm.ErrInvalidDB
}

type pendingInvalidation struct {
	tags       []string
	seen       map[string]struct{}
	execID     string
	actionName string
}

// trackedTx collects invalidation tags written inside a transaction. They are
// published after a successful Commit and discarded on Rollback, so re-run
// queries (which use other pooled connections) observe the committed data.
type trackedTx struct {
	gorm.Tx
	pool *trackedPool
	ctx  context.Context
	invalidations
}

func (t *trackedTx) GetDBConn() (*sql.DB, error) {
	return t.pool.GetDBConn()
}

// invalidations collects tags written inside one transaction.
type invalidations struct {
	mu      sync.Mutex
	pending map[*Engine]*pendingInvalidation
}

func (t *invalidations) queue(e *Engine, tags []string, execID, actionName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pending == nil {
		t.pending = make(map[*Engine]*pendingInvalidation)
	}
	p, ok := t.pending[e]
	if !ok {
		p = &pendingInvalidation{seen: make(map[string]struct{})}
		t.pending[e] = p
	}
	if p.execID == "" {
		p.execID, p.actionName = execID, actionName
	}
	for _, tag := range tags {
		if _, dup := p.seen[tag]; dup {
			continue
		}
		p.seen[tag] = struct{}{}
		p.tags = append(p.tags, tag)
	}
}

func (t *invalidations) take() map[*Engine]*pendingInvalidation {
	t.mu.Lock()
	defer t.mu.Unlock()
	pending := t.pending
	t.pending = nil
	return pending
}

func publishPending(pending map[*Engine]*pendingInvalidation) {
	for e, p := range pending {
		e.invalidateTags(p.tags, p.execID, p.actionName)
	}
}

func (t *trackedTx) Commit() error {
	pending := t.take()
	// NOTIFY inside the transaction is delivered by PostgreSQL only on commit.
	for e, p := range pending {
		e.notifyRemote(t.ctx, t.Tx, p.tags)
	}
	if err := t.Tx.Commit(); err != nil {
		return err
	}
	publishPending(pending)
	return nil
}

func (t *trackedTx) Rollback() error {
	t.take()
	return t.Tx.Rollback()
}

// sqlTxHook publishes invalidations when a raw *sql.Tx commits. db.Connection
// checks out a *sql.Conn and GORM begins its transaction with Conn.BeginTx,
// which returns that *sql.Tx instead of a trackedTx. Callbacks then run on a
// cloned statement, while Commit is invoked on the statement Begin returned,
// so replacing the callback's ConnPool cannot see the commit. The driver
// transaction is the one object both sides share.
type sqlTxHook struct {
	driver.Tx
	invalidations
}

func (h *sqlTxHook) Commit() error {
	if err := h.Tx.Commit(); err != nil {
		h.take()
		return err
	}
	publishPending(h.take())
	return nil
}

func (h *sqlTxHook) Rollback() error {
	h.take()
	return h.Tx.Rollback()
}

// sqlTxOf returns the raw transaction behind a pinned connection, if any.
func sqlTxOf(conn gorm.ConnPool) *sql.Tx {
	if prepared, ok := conn.(*gorm.PreparedStmtTX); ok {
		conn = prepared.Tx
	}
	tx, _ := conn.(*sql.Tx)
	return tx
}

// sqlTxHookOf returns the commit hook for tx, installing it on first use.
// A nil result means the transaction could not be hooked; the caller publishes
// immediately rather than dropping the invalidation.
func sqlTxHookOf(tx *sql.Tx) *sqlTxHook {
	field := reflect.ValueOf(tx).Elem().FieldByName("txi")
	if !field.IsValid() || field.Kind() != reflect.Interface || field.IsNil() {
		slog.Error("Failed to defer invalidation until the pinned transaction commits")
		return nil
	}
	// txi is unexported, so the field Value refuses Interface. NewAt views the
	// same memory as an ordinary driver.Tx.
	current := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
	if hook, ok := current.Interface().(*sqlTxHook); ok {
		return hook
	}
	inner, ok := current.Interface().(driver.Tx)
	if !ok {
		slog.Error("Failed to defer invalidation until the pinned transaction commits")
		return nil
	}
	hook := &sqlTxHook{Tx: inner}
	current.Set(reflect.ValueOf(driver.Tx(hook)))
	return hook
}

// trackTransactions installs trackedPool under db. With PrepareStmt the
// prepared-statement layer stays outermost (drivers type-check it) and the
// tracked pool goes beneath it.
func trackTransactions(db *gorm.DB) {
	if prepared, ok := db.ConnPool.(*gorm.PreparedStmtDB); ok {
		if _, done := prepared.ConnPool.(*trackedPool); !done {
			prepared.ConnPool = &trackedPool{ConnPool: prepared.ConnPool}
		}
		return
	}
	if _, done := db.ConnPool.(*trackedPool); done {
		return
	}
	pool := &trackedPool{ConnPool: db.ConnPool}
	if db.Statement.ConnPool == db.ConnPool {
		db.Statement.ConnPool = pool
	}
	db.ConnPool = pool
}

func trackedTxOf(conn gorm.ConnPool) *trackedTx {
	if prepared, ok := conn.(*gorm.PreparedStmtTX); ok {
		conn = prepared.Tx
	}
	tx, _ := conn.(*trackedTx)
	return tx
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// notifyRemote tells other instances about tags over pg_notify. conn must be
// the connection that performed the write so the notification shares its
// transaction.
func (e *Engine) notifyRemote(ctx context.Context, conn execer, tags []string) {
	if e.dbType != "postgres" {
		return
	}
	for _, payload := range notifyPayloads(e.EphemeralID, tags) {
		if _, err := conn.ExecContext(ctx, "SELECT pg_notify('tether_sync', $1)", payload); err != nil {
			slog.Error("Failed to send PostgreSQL notification", "error", err)
		}
	}
}

// notifyMessage is the JSON body of a tether_sync notification.
type notifyMessage struct {
	Sender string   `json:"sender"`
	Tags   []string `json:"tags"`
}

// notifyPayloads encodes tags as notifyMessage JSON, split across as many
// payloads as needed for each to fit in a single NOTIFY.
func notifyPayloads(senderID string, tags []string) []string {
	// Assembled from individually encoded pieces so each chunk's exact size
	// is known before a tag is added; must stay in sync with notifyMessage.
	sender, _ := json.Marshal(senderID)
	prefix := `{"sender":` + string(sender) + `,"tags":[`
	const suffix = "]}"
	var payloads []string
	var b strings.Builder
	for _, tag := range tags {
		encoded, _ := json.Marshal(tag)
		if len(prefix)+len(encoded)+len(suffix) >= maxNotifyPayload {
			slog.Error("Tag too large to send via PostgreSQL notification", "tag", tag)
			continue
		}
		if b.Len() > 0 && b.Len()+1+len(encoded)+len(suffix) >= maxNotifyPayload {
			b.WriteString(suffix)
			payloads = append(payloads, b.String())
			b.Reset()
		}
		if b.Len() == 0 {
			b.WriteString(prefix)
		} else {
			b.WriteByte(',')
		}
		b.Write(encoded)
	}
	if b.Len() > 0 {
		b.WriteString(suffix)
		payloads = append(payloads, b.String())
	}
	return payloads
}
