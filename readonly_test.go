package tether

import (
	"errors"
	"testing"
)

func TestCheckReadOnlySQL(t *testing.T) {
	const (
		both = iota
		postgresOnly
		sqliteOnly
	)
	cases := []struct {
		sql     string
		dialect int
		inTx    bool
		allowed bool
	}{
		{sql: "", allowed: true},
		{sql: "  -- only a comment", allowed: true},
		{sql: "SELECT 1", allowed: true},
		{sql: "\n\tselect * from messages;", allowed: true},
		{sql: "SELECT 1; -- done", allowed: true},
		{sql: "/* dashboard */ SELECT 1", allowed: true},
		{sql: "SELECT 'UPDATE x; DELETE FROM y' AS s", allowed: true},
		{sql: "SELECT 'it''s' AS s", allowed: true},
		{sql: `SELECT "update", "a""b" FROM t`, allowed: true},
		{sql: "WITH m AS (SELECT 1) SELECT * FROM m", allowed: true},
		{sql: "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n) SELECT x FROM n", allowed: true},
		{sql: "VALUES (1), (2)", allowed: true},
		{sql: "EXPLAIN SELECT 1", allowed: true},
		{sql: "EXPLAIN QUERY PLAN SELECT 1", allowed: true},
		{sql: "SELECT updated_at, deleted_at FROM t", allowed: true},
		{sql: "SELECT replace(body, 'a', 'b') FROM t", allowed: true},
		{sql: "SELECT 1e5, 1.5e-3, .5, 0x1F", allowed: true},
		{sql: "SELECT * FROM t WHERE a = ? AND b = $1", allowed: true},
		{sql: "SELECT substring(body FROM 1 FOR 3) FROM t", allowed: true},
		{sql: "SELECT $$DELETE FROM t$$", dialect: postgresOnly, allowed: true},
		{sql: "SELECT $body$ ; UPDATE t $body$", dialect: postgresOnly, allowed: true},
		{sql: `SELECT E'it\'s UPDATE'`, dialect: postgresOnly, allowed: true},
		{sql: "SELECT a$b FROM t", dialect: postgresOnly, allowed: true},
		{sql: "(SELECT 1) UNION (SELECT 2)", dialect: postgresOnly, allowed: true},
		{sql: `SELECT * FROM t WHERE p LIKE '50\%'`, dialect: postgresOnly, allowed: true},
		{sql: "SELECT [update] FROM t", dialect: sqliteOnly, allowed: true},
		{sql: "SELECT `delete` FROM t", dialect: sqliteOnly, allowed: true},
		{sql: `SELECT 'C:\' AS path`, dialect: sqliteOnly, allowed: true},
		{sql: "SAVEPOINT sp1", inTx: true, allowed: true},
		{sql: "ROLLBACK TO SAVEPOINT sp1", inTx: true, allowed: true},
		{sql: "RELEASE SAVEPOINT sp1", inTx: true, allowed: true},

		{sql: "/* application comment */ UPDATE messages SET body = 'changed'"},
		{sql: "UPDATE messages SET body = 'changed' RETURNING id"},
		{sql: "-- note\nDELETE FROM t"},
		{sql: "-- note\rDELETE FROM t"},
		{sql: "SELECT 1; UPDATE t SET a = 1"},
		{sql: "SELECT 1 /* c */; UPDATE t SET a = 1"},
		{sql: "SELECT 1;;"},
		{sql: "WITH t AS (SELECT 1) UPDATE messages SET body = 'x'"},
		{sql: "WITH u AS (DELETE FROM t RETURNING *) SELECT * FROM u"},
		{sql: "WITH t AS (SELECT 1) INSERT INTO x SELECT * FROM t"},
		{sql: "WITH t AS (SELECT 1) REPLACE INTO x SELECT * FROM t"},
		{sql: "SELECT * INTO copy FROM t"},
		{sql: "SELECT 1INTO copy"},
		{sql: "SELECT * FROM t FOR UPDATE"},
		{sql: "SELECT * FROM t FOR NO KEY UPDATE"},
		{sql: "SELECT * FROM t FOR SHARE"},
		{sql: "SELECT * FROM t FOR KEY SHARE"},
		{sql: "EXPLAIN ANALYZE DELETE FROM t"},
		{sql: "EXPLAIN (ANALYZE) SELECT 1"},
		{sql: "EXPLAIN ANALYSE SELECT 1"},
		{sql: "CREATE TABLE x (id int)"},
		{sql: "CREATE TEMP TABLE x AS SELECT 1"},
		{sql: "DROP TABLE t"},
		{sql: "ALTER TABLE t ADD COLUMN c int"},
		{sql: "TRUNCATE t"},
		{sql: "REPLACE INTO t VALUES (1)"},
		{sql: "INSERT OR REPLACE INTO t VALUES (1)"},
		{sql: "PRAGMA user_version = 1"},
		{sql: "SET default_transaction_read_only = off"},
		{sql: "BEGIN"},
		{sql: "COMMIT"},
		{sql: "SAVEPOINT sp1"},
		{sql: "COMMIT", inTx: true},
		{sql: "ROLLBACK", inTx: true},
		{sql: "ATTACH DATABASE 'x.db' AS x"},
		{sql: "VACUUM"},
		{sql: "COPY t FROM STDIN"},
		{sql: "CALL p()"},
		{sql: "LOCK TABLE t"},
		{sql: "GRANT SELECT ON t TO u"},
		{sql: "SELECT 'unterminated"},
		{sql: `SELECT "unterminated`},
		{sql: "/* unterminated SELECT 1"},
		{sql: "/* a /* b */ */ SELECT 1"},
		{sql: `SELECT '\' ' UPDATE t SET a = 1 --'`, dialect: postgresOnly},
		{sql: `SELECT E'\' ' ; UPDATE t SET a = 1`, dialect: postgresOnly},
		{sql: "SELECT $$ unterminated", dialect: postgresOnly},
		{sql: "DO $$ BEGIN END $$", dialect: postgresOnly},
		{sql: "SELECT [unterminated", dialect: sqliteOnly},
		{sql: "SELECT `unterminated", dialect: sqliteOnly},
	}
	for _, tc := range cases {
		for _, postgres := range []bool{false, true} {
			if tc.dialect == postgresOnly && !postgres || tc.dialect == sqliteOnly && postgres {
				continue
			}
			err := checkReadOnlySQL(tc.sql, postgres, tc.inTx)
			if tc.allowed && err != nil {
				t.Errorf("postgres=%v inTx=%v %q: rejected: %v", postgres, tc.inTx, tc.sql, err)
			}
			if !tc.allowed && !errors.Is(err, errReadOnly) {
				t.Errorf("postgres=%v inTx=%v %q: err = %v, want the read-only error", postgres, tc.inTx, tc.sql, err)
			}
		}
	}
}
