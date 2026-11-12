//go:build !notest

// Package pgtestdb provides utilities for creating temporary Postgres databases in tests.
// Each database is automatically dropped at t.Cleanup.
package pgtestdb

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultDBURL  = "postgres://localhost:5432/postgres"
	dbNamePrefix  = "pgtestdb_"
	cleanupSuffix = "_requires_cleanup"
)

// TB is the subset of testing.T needed by this package.
type TB interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Cleanup(func())
}

// execer is satisfied by both *pgx.Conn and *pgxpool.Pool.
type execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// TestDB is a temporary Postgres database created for a single test.
// It is automatically dropped at t.Cleanup.
type TestDB struct {
	pool *pgxpool.Pool
	name string
	url  string
}

// Pool returns the pgxpool.Pool connected to the test database.
func (tdb *TestDB) Pool() *pgxpool.Pool { return tdb.pool }

// Name returns the generated name of the test database.
func (tdb *TestDB) Name() string { return tdb.name }

// URL returns the full connection URL for the test database.
func (tdb *TestDB) URL() string { return tdb.url }

// Option configures how New creates the test database.
type Option func(*config)

type config struct {
	dbURL      string
	template   string
	statements []string
}

// WithTemplate clones an existing Postgres template DB (CREATE DATABASE x TEMPLATE y).
func WithTemplate(name string) Option {
	return func(c *config) {
		c.template = name
	}
}

// WithStatements runs SQL statements against the new DB after creation.
// Multiple calls accumulate statements in order.
func WithStatements(stmts ...string) Option {
	return func(c *config) {
		c.statements = append(c.statements, stmts...)
	}
}

// WithDBURL sets the connection URL used to reach PostgreSQL. When it is not
// set, TEST_DATABASE_URL is used, falling back to
// postgres://localhost:5432/postgres.
func WithDBURL(dbURL string) Option {
	return func(c *config) {
		c.dbURL = dbURL
	}
}

// dropDBResultHook lets tests in this package simulate a DROP DATABASE failure.
// It receives the real result and returns the result to use.
var dropDBResultHook = func(actual bool) bool { return actual }

// New creates a temporary Postgres database for the duration of t.
//
// The database is automatically dropped at t.Cleanup. The connection URL comes
// from WithDBURL, then TEST_DATABASE_URL, then
// postgres://localhost:5432/postgres. Any databases left over from prior failed
// runs (identified by a _requires_cleanup suffix) are cleaned up before the new
// database is created.
func New(t TB, opts ...Option) *TestDB {
	t.Helper()
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	ctx := context.Background()
	baseURL := resolveDBURL(cfg.dbURL)
	adminConn := adminConnect(ctx, t, baseURL)
	t.Cleanup(func() { _ = adminConn.Close(ctx) }) // registered 1st: runs last (LIFO)

	if _, err := adminConn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf(
			"pgtestdb: failed to connect to postgres: %+v\n"+
				"Make sure PostgreSQL is running and that TEST_DATABASE_URL or WithDBURL points at it.",
			err,
		)
	}

	cleanUpOldDBs(ctx, t, adminConn)

	dbName := dbNamePrefix + fmt.Sprintf("%x", rand.Uint64())

	// registered 2nd: runs before adminConn.Close, so adminConn is still open here.
	t.Cleanup(func() { tryDropDB(t, adminConn, dbName) })

	ExecOrFail(t, adminConn, createDatabaseSQL(dbName, cfg.template))

	testURL := buildURL(t, baseURL, dbName)
	pool, err := pgxpool.New(ctx, testURL)
	fatalOnError(t, err, "pgtestdb: create pool for test db")
	t.Cleanup(func() { pool.Close() }) // registered 3rd: runs first (LIFO), before tryDropDB

	for _, stmt := range cfg.statements {
		ExecOrFail(t, pool, stmt)
	}

	return &TestDB{pool: pool, name: dbName, url: testURL}
}

// ExecOrFail runs stmt on exec, calling t.Fatalf on error.
// exec may be a *pgxpool.Pool or a *pgx.Conn.
func ExecOrFail(t TB, exec execer, stmt string, args ...any) {
	t.Helper()
	_, err := exec.Exec(context.Background(), stmt, args...)
	fatalOnError(t, err, "failed to run statement "+stmt)
}

// ExecOrError runs stmt on exec, calling t.Errorf on error. Returns true on success.
// exec may be a *pgxpool.Pool or a *pgx.Conn.
func ExecOrError(t TB, exec execer, stmt string, args ...any) bool {
	t.Helper()
	_, err := exec.Exec(context.Background(), stmt, args...)
	if err != nil {
		t.Errorf("failed to run statement %s: %+v", stmt, err)
		return false
	}
	return true
}

func adminConnect(ctx context.Context, t TB, baseURL string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, buildURL(t, baseURL, ""))
	fatalOnError(t, err, "pgtestdb: admin connect")
	return conn
}

// resolveDBURL applies the connection URL precedence: WithDBURL, then
// TEST_DATABASE_URL, then defaultDBURL.
func resolveDBURL(optionURL string) string {
	if optionURL != "" {
		return optionURL
	}
	if envURL := os.Getenv("TEST_DATABASE_URL"); envURL != "" {
		return envURL
	}
	return defaultDBURL
}

// buildURL returns the connection URL for the given database name.
// If dbName is empty, the path from baseURL is kept (suitable for admin connections).
func buildURL(t TB, baseURL, dbName string) string {
	t.Helper()
	u, err := url.Parse(baseURL)
	fatalOnError(t, err, "pgtestdb: parse database URL")
	q := u.Query()
	if dbName != "" {
		u.Path = "/" + dbName
		// pgx lets the dbname/database query aliases override the path, so
		// drop them; otherwise the pool would connect to the base database
		// while only the unused generated database is dropped.
		q.Del("dbname")
		q.Del("database")
	}
	if !q.Has("sslmode") {
		q.Set("sslmode", "disable")
	}
	u.RawQuery = encodeQuery(q)
	return u.String()
}

// encodeQuery encodes q as a query string using PostgreSQL URI encoding.
// url.Values.Encode emits "+" for spaces, which pgx parses literally, so spaces
// are rewritten to %20. Literal plus signs are unaffected: Encode already
// escapes them as %2B, so every "+" left in the encoded output was a space.
func encodeQuery(q url.Values) string {
	return strings.ReplaceAll(q.Encode(), "+", "%20")
}

func fatalOnError(t TB, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %+v", msg, err)
	}
}

// cleanupMarkerName returns the database name that the marker datname points
// at. It reports false unless datname has the literal pgtestdb_ prefix and
// _requires_cleanup suffix, so a LIKE pattern that over-matches can never cause
// an unrelated database to be dropped.
func cleanupMarkerName(datname string) (string, bool) {
	if !strings.HasPrefix(datname, dbNamePrefix) || !strings.HasSuffix(datname, cleanupSuffix) {
		return "", false
	}
	return strings.TrimSuffix(datname, cleanupSuffix), true
}

func getDBNamesToCleanup(ctx context.Context, t TB, conn *pgx.Conn) []string {
	t.Helper()
	// The underscores are intentionally unescaped: LIKE treats them as
	// single-character wildcards, so this may match a superset of the marker
	// names. cleanupMarkerName checks the literal prefix and suffix before
	// anything is dropped, which keeps the pattern simple and safe.
	pattern := dbNamePrefix + "%" + cleanupSuffix
	rows, err := conn.Query(ctx, `SELECT datname FROM pg_database WHERE datname LIKE $1`, pattern)
	fatalOnError(t, err, "pgtestdb: query for DBs to clean up")
	defer rows.Close()

	var names []string
	for rows.Next() {
		var datname string
		fatalOnError(t, rows.Scan(&datname), "pgtestdb: scan DB name")
		if name, ok := cleanupMarkerName(datname); ok {
			names = append(names, name)
		}
	}
	fatalOnError(t, rows.Err(), "pgtestdb: read DB names for cleanup")
	return names
}

func cleanUpOldDBs(ctx context.Context, t TB, conn *pgx.Conn) {
	t.Helper()
	for _, name := range getDBNamesToCleanup(ctx, t, conn) {
		ExecOrFail(t, conn, dropDatabaseSQL(name))
		ExecOrFail(t, conn, dropDatabaseSQL(name+cleanupSuffix))
	}
}

func tryDropDB(t TB, adminConn *pgx.Conn, dbName string) {
	t.Helper()
	if !dropDBResultHook(ExecOrError(t, adminConn, dropDatabaseSQL(dbName))) {
		// The drop failed — likely because the test left connections open.
		// Create a marker DB so the next run knows to clean it up.
		ExecOrError(t, adminConn, createDatabaseSQL(dbName+cleanupSuffix, ""))
	}
}

// createDatabaseSQL builds a CREATE DATABASE statement with quoted identifiers.
// DDL cannot take bind parameters, so names must be sanitized.
func createDatabaseSQL(name, template string) string {
	sql := "CREATE DATABASE " + pgx.Identifier{name}.Sanitize()
	if template != "" {
		sql += " TEMPLATE " + pgx.Identifier{template}.Sanitize()
	}
	return sql
}

// dropDatabaseSQL builds a DROP DATABASE IF EXISTS statement with a quoted identifier.
func dropDatabaseSQL(name string) string {
	return "DROP DATABASE IF EXISTS " + pgx.Identifier{name}.Sanitize()
}
