package tether

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

var errReadOnly = errors.New("tether: database writes are not allowed in queries or guards; use a mutation instead")

// readOnlyPool is the connection pool behind QueryCtx.DB and GuardCtx.DB.
// Every statement GORM sends passes through it (builders, Raw, Exec, Row,
// Rows, Scan, migrator calls, transactions), and only statements accepted by
// checkReadOnlySQL reach the database. The restriction is attached to the
// pool rather than the context, so ctx.DB.WithContext cannot drop it.
//
// It deliberately does not implement gorm.GetDBConnector: ctx.DB.DB() and
// ctx.DB.Connection must not hand out the engine's unrestricted *sql.DB.
type readOnlyPool struct {
	pool     gorm.ConnPool
	postgres bool
}

func (p *readOnlyPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if err := checkReadOnlySQL(query, p.postgres, false); err != nil {
		return nil, err
	}
	return p.pool.PrepareContext(ctx, query)
}

func (p *readOnlyPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if err := checkReadOnlySQL(query, p.postgres, false); err != nil {
		return nil, err
	}
	return p.pool.ExecContext(ctx, query, args...)
}

func (p *readOnlyPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if err := checkReadOnlySQL(query, p.postgres, false); err != nil {
		return nil, err
	}
	return p.pool.QueryContext(ctx, query, args...)
}

func (p *readOnlyPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if err := checkReadOnlySQL(query, p.postgres, false); err != nil {
		return deniedRow(ctx, err)
	}
	return p.pool.QueryRowContext(ctx, query, args...)
}

// BeginTx always opens a READ ONLY transaction, which PostgreSQL enforces
// itself. SQLite has no read-only transaction mode, so there the statement
// check is the only barrier.
func (p *readOnlyPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	readOnly := &sql.TxOptions{ReadOnly: true}
	if opts != nil {
		readOnly.Isolation = opts.Isolation
	}
	var (
		conn gorm.ConnPool
		err  error
	)
	switch beginner := p.pool.(type) {
	case gorm.TxBeginner:
		var tx *sql.Tx
		if tx, err = beginner.BeginTx(ctx, readOnly); err == nil {
			conn = tx
		}
	case gorm.ConnPoolBeginner:
		conn, err = beginner.BeginTx(ctx, readOnly)
	default:
		return nil, gorm.ErrInvalidTransaction
	}
	if err != nil {
		return nil, err
	}
	tx, ok := conn.(gorm.Tx)
	if !ok {
		if committer, ok := conn.(gorm.TxCommitter); ok {
			_ = committer.Rollback()
		}
		return nil, gorm.ErrInvalidTransaction
	}
	return &readOnlyTx{tx: tx, postgres: p.postgres}, nil
}

// readOnlyTx is a transaction begun from readOnlyPool. It applies the same
// statement check, plus the savepoint statements GORM issues for nested
// db.Transaction calls.
type readOnlyTx struct {
	tx       gorm.Tx
	postgres bool
}

func (t *readOnlyTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if err := checkReadOnlySQL(query, t.postgres, true); err != nil {
		return nil, err
	}
	return t.tx.PrepareContext(ctx, query)
}

func (t *readOnlyTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if err := checkReadOnlySQL(query, t.postgres, true); err != nil {
		return nil, err
	}
	return t.tx.ExecContext(ctx, query, args...)
}

func (t *readOnlyTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if err := checkReadOnlySQL(query, t.postgres, true); err != nil {
		return nil, err
	}
	return t.tx.QueryContext(ctx, query, args...)
}

func (t *readOnlyTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if err := checkReadOnlySQL(query, t.postgres, true); err != nil {
		return deniedRow(ctx, err)
	}
	return t.tx.QueryRowContext(ctx, query, args...)
}

func (t *readOnlyTx) StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt {
	return t.tx.StmtContext(ctx, stmt)
}

func (t *readOnlyTx) Commit() error   { return t.tx.Commit() }
func (t *readOnlyTx) Rollback() error { return t.tx.Rollback() }

func isReadOnlyConn(conn gorm.ConnPool) bool {
	switch conn.(type) {
	case *readOnlyPool, *readOnlyTx:
		return true
	}
	return false
}

// unwrapPreparedReadOnly runs first in every GORM callback chain. A
// PrepareStmt session wraps ctx.DB's pool in GORM's prepared-statement layer,
// and that layer's statement cache is shared with mutations: a cached UPDATE
// would run without ever reaching readOnlyPool. Read-only statements are sent
// unprepared instead.
func unwrapPreparedReadOnly(db *gorm.DB) {
	switch conn := db.Statement.ConnPool.(type) {
	case *gorm.PreparedStmtDB:
		if pool, ok := conn.ConnPool.(*readOnlyPool); ok {
			db.Statement.ConnPool = pool
		}
	case *gorm.PreparedStmtTX:
		if tx, ok := conn.Tx.(*readOnlyTx); ok {
			db.Statement.ConnPool = tx
		}
	}
}

// deniedDB exists only to build *sql.Row values that carry an error, which
// database/sql offers no other way to construct. Its connector never opens a
// connection; it returns the error stored in the context instead.
var deniedDB = sql.OpenDB(deniedConnector{})

type deniedErrKey struct{}

type deniedConnector struct{}

func (deniedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err, ok := ctx.Value(deniedErrKey{}).(error); ok {
		return nil, err
	}
	return nil, errReadOnly
}

func (deniedConnector) Driver() driver.Driver { return deniedDriver{} }

type deniedDriver struct{}

func (deniedDriver) Open(string) (driver.Conn, error) { return nil, errReadOnly }

func deniedRow(ctx context.Context, err error) *sql.Row {
	return deniedDB.QueryRowContext(context.WithValue(ctx, deniedErrKey{}, err), "")
}

// checkReadOnlySQL accepts exactly one SELECT, WITH, VALUES or EXPLAIN
// statement (optionally followed by a semicolon) that contains none of INSERT,
// UPDATE, DELETE, MERGE, INTO or a FOR SHARE/UPDATE locking clause, and no
// EXPLAIN ANALYZE. That rules out DDL, data-modifying CTEs, SELECT INTO,
// upserts, transaction control and PRAGMA/SET. Inside a transaction,
// SAVEPOINT, RELEASE and ROLLBACK TO are accepted as well.
//
// Keywords are matched as words outside comments, string literals, quoted
// identifiers and dollar-quoted bodies. Input where the database might split
// those differently than this scanner does is rejected.
//
// Functions with side effects called from a SELECT (nextval, user-defined
// functions) are not detected.
func checkReadOnlySQL(query string, postgres, inTx bool) error {
	words, err := sqlWords(query, postgres)
	if err != nil {
		return err
	}
	if len(words) == 0 {
		return nil
	}
	first := words[0]
	if inTx {
		switch {
		case first == "SAVEPOINT", first == "RELEASE":
			return nil
		case first == "ROLLBACK" && len(words) > 1 && words[1] == "TO":
			return nil
		}
	}
	switch first {
	case "SELECT", "WITH", "VALUES", "EXPLAIN":
	default:
		return fmt.Errorf("%w (only SELECT, WITH, VALUES and EXPLAIN statements can run here, got %s)", errReadOnly, first)
	}
	for i, word := range words {
		switch word {
		case "INSERT", "UPDATE", "DELETE", "MERGE", "INTO":
			return fmt.Errorf("%w (statement contains %s)", errReadOnly, word)
		case "ANALYZE", "ANALYSE":
			if first == "EXPLAIN" {
				return fmt.Errorf("%w (EXPLAIN %s executes the statement)", errReadOnly, word)
			}
		case "FOR":
			if i+1 < len(words) && (words[i+1] == "SHARE" || words[i+1] == "KEY") {
				return fmt.Errorf("%w (statement takes row locks)", errReadOnly)
			}
		}
	}
	return nil
}

// sqlWords returns the upper-cased bare words of query in order, skipping
// comments, literals, quoted identifiers and punctuation.
func sqlWords(query string, postgres bool) ([]string, error) {
	var words []string
	ended := false
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c <= ' ':
			i++
			continue
		case c == '-' && strings.HasPrefix(query[i:], "--"):
			// PostgreSQL also ends a line comment at a bare \r; SQLite does
			// not, so ending there is the stricter reading for SQLite.
			end := strings.IndexAny(query[i:], "\r\n")
			if end < 0 {
				return words, nil
			}
			i += end + 1
			continue
		case c == '/' && strings.HasPrefix(query[i:], "/*"):
			// PostgreSQL nests block comments and SQLite does not, so any
			// comment containing another opener is ambiguous.
			end := strings.Index(query[i+2:], "*/")
			if end < 0 {
				return nil, fmt.Errorf("%w (unterminated comment)", errReadOnly)
			}
			if strings.Contains(query[i+2:i+2+end], "/*") {
				return nil, fmt.Errorf("%w (nested comment)", errReadOnly)
			}
			i += 2 + end + 2
			continue
		}
		if ended {
			return nil, fmt.Errorf("%w (multiple statements)", errReadOnly)
		}

		var ok bool
		switch {
		case c == ';':
			ended = true
			i++
			ok = true
		case c == '\'':
			i, ok = skipStringLiteral(query, i+1, false, postgres)
		case c == '"':
			i, ok = skipQuoted(query, i+1, '"')
		case c == '`' && !postgres:
			i, ok = skipQuoted(query, i+1, '`')
		case c == '[' && !postgres:
			end := strings.IndexByte(query[i:], ']')
			i, ok = i+end+1, end >= 0
		case c == '$' && postgres:
			if tag := dollarQuoteTag(query[i:]); tag != "" {
				end := strings.Index(query[i+len(tag):], tag)
				i, ok = i+len(tag)+end+len(tag), end >= 0
			} else {
				i, ok = i+1, true
			}
		case isSQLDigit(c):
			i, ok = skipNumber(query, i), true
		case isSQLIdentStart(c):
			start := i
			for i < len(query) && isSQLIdentChar(query[i]) {
				i++
			}
			word := query[start:i]
			if postgres && (word == "E" || word == "e") && i < len(query) && query[i] == '\'' {
				i, ok = skipStringLiteral(query, i+1, true, postgres)
			} else {
				words = append(words, upperASCII(word))
				ok = true
			}
		default:
			i++
			ok = true
		}
		if !ok {
			return nil, fmt.Errorf("%w (unterminated or ambiguous literal)", errReadOnly)
		}
	}
	return words, nil
}

// skipStringLiteral returns the index just past the literal whose body starts
// at i. With PostgreSQL's standard_conforming_strings off, a backslash before
// a quote escapes it; that setting is not visible here, so a plain PostgreSQL
// literal with a backslash before any quote is rejected.
func skipStringLiteral(query string, i int, backslashEscapes, postgres bool) (int, bool) {
	for i < len(query) {
		switch query[i] {
		case '\\':
			if backslashEscapes {
				i += 2
				continue
			}
		case '\'':
			if postgres && !backslashEscapes && query[i-1] == '\\' {
				return 0, false
			}
			if i+1 < len(query) && query[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1, true
		}
		i++
	}
	return 0, false
}

func skipQuoted(query string, i int, quote byte) (int, bool) {
	for i < len(query) {
		if query[i] == quote {
			if i+1 < len(query) && query[i+1] == quote {
				i += 2
				continue
			}
			return i + 1, true
		}
		i++
	}
	return 0, false
}

// dollarQuoteTag returns the opening $tag$ (or $$) at the start of s, or ""
// if s starts with a positional parameter or a lone $.
func dollarQuoteTag(s string) string {
	j := 1
	if j < len(s) && isSQLIdentStart(s[j]) {
		for j < len(s) && (isSQLIdentStart(s[j]) || isSQLDigit(s[j])) {
			j++
		}
	}
	if j < len(s) && s[j] == '$' {
		return s[:j+1]
	}
	return ""
}

// skipNumber consumes digits, '.', '_' and a signed exponent. Letters that
// follow (0x1F, 1abc) are scanned as a separate word.
func skipNumber(query string, i int) int {
	for i < len(query) && (isSQLDigit(query[i]) || query[i] == '.' || query[i] == '_') {
		i++
	}
	if i < len(query) && (query[i] == 'e' || query[i] == 'E') {
		j := i + 1
		if j < len(query) && (query[j] == '+' || query[j] == '-') {
			j++
		}
		if j < len(query) && isSQLDigit(query[j]) {
			for i = j; i < len(query) && isSQLDigit(query[i]); i++ {
			}
		}
	}
	return i
}

func isSQLDigit(c byte) bool { return c >= '0' && c <= '9' }

func isSQLIdentStart(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || c >= 0x80
}

func isSQLIdentChar(c byte) bool {
	return isSQLIdentStart(c) || isSQLDigit(c) || c == '$'
}

func upperASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 'a' + 'A'
		}
	}
	return string(b)
}
