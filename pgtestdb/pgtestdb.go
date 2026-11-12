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
	"time"

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

// poolCloseTimeout bounds how long cleanup waits for pgxpool.Close to return.
// Close blocks until every acquired connection is returned, which never
// happens when a test exits while holding a transaction. Without the bound the
// remaining cleanups (drop attempt, marker creation, admin close) would never
// run and the suite would hang until its global timeout.
var poolCloseTimeout = 5 * time.Second

// poolCloseErrorMessage explains a closePool timeout: the pool is still holding
// an acquired connection, which almost always means a test leaked one.
const poolCloseErrorMessage = "Failed to close pgtestdb pool, this usually means a test kept an open connection/transaction even after it finished which blocked closing of the pool."

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

	testURL := buildURL(t, baseURL, dbName, true)
	pool, err := pgxpool.New(ctx, testURL)
	fatalOnError(t, err, "pgtestdb: create pool for test db")
	// registered 3rd: runs first (LIFO), before tryDropDB. The close is
	// bounded so a test that exits while holding a transaction cannot keep the
	// drop attempt, marker creation, and admin close from running.
	t.Cleanup(func() { closePool(t, pool) })

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

// closePool closes pool without letting a stuck connection hold up the rest of
// the cleanup chain. pgxpool.Close waits for acquired connections to be
// returned; a test that exited through Fatal while holding an uncommitted
// transaction never returns them. Cleanup therefore waits at most
// poolCloseTimeout and then continues, leaving the abandoned connections to be
// torn down when the process exits. Hitting the timeout is reported as a test
// error, since an open connection/transaction survived its test.
func closePool(t TB, pool *pgxpool.Pool) {
	t.Helper()
	closed := make(chan struct{})
	go func() {
		pool.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(poolCloseTimeout):
		t.Errorf("%s", poolCloseErrorMessage)
	}
}

func adminConnect(ctx context.Context, t TB, baseURL string) *pgx.Conn {
	t.Helper()
	// Reuse buildURL so the admin connection gets the same sslmode default as
	// the generated test database while retaining the base URL's database.
	return connect(ctx, t, buildURL(t, baseURL, "", false))
}

// connect opens a single connection from a URL. It parses with pgxpool so
// pgxpool-only query parameters are consumed instead of being forwarded to the
// server as startup parameters.
func connect(ctx context.Context, t TB, url string) *pgx.Conn {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(url)
	fatalOnError(t, err, "pgtestdb: parse database URL")
	conn, err := pgx.ConnectConfig(ctx, cfg.ConnConfig)
	fatalOnError(t, err, "pgtestdb: connect")
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

// buildURL returns the connection URL for the generated database dbName.
//
// It rewrites baseURL textually instead of parsing and re-encoding it with
// net/url. url.Parse rejects percent-encoded hosts such as the escaped Unix
// socket directory %2Fvar%2Frun%2Fpostgresql, even though pgx accepts them.
// The authority is therefore copied verbatim and query parameters are kept raw,
// which also preserves pgx-specific syntax ("+" is a literal plus sign, not a
// space) that net/url would rewrite.
func buildURL(t TB, baseURL, dbName string, stripDBAliases bool) string {
	t.Helper()
	scheme, rest, ok := strings.Cut(baseURL, "://")
	if !ok {
		t.Fatalf("pgtestdb: database URL must start with scheme://")
	}

	// The authority runs to the first path separator or query delimiter.
	// Userinfo ends at the first '@' before the path; a '?' before that '@'
	// belongs to the credentials (pgx accepts it there), not to the query.
	// Everything up to the authority's end is kept exactly as given: userinfo,
	// IPv6 literals, comma-separated hosts, and percent-encoded Unix sockets.
	authorityStart := 0
	if at := strings.IndexByte(rest, '@'); at >= 0 {
		if slash := strings.IndexByte(rest, '/'); slash < 0 || at < slash {
			authorityStart = at + 1
		}
	}
	end := strings.IndexAny(rest[authorityStart:], "/?")
	if end < 0 {
		end = len(rest)
	} else {
		end += authorityStart
	}
	authority, rest := rest[:end], rest[end:]

	// Preserve the base path for the admin connection (empty dbName); a
	// generated database name replaces it for the test connection. Only the
	// path portion is searched: a slash in a pathless URL's query string is not
	// a database path.
	basePath := "/"
	pathEnd := strings.IndexByte(rest, '?')
	if pathEnd < 0 {
		pathEnd = len(rest)
	}
	if slash := strings.IndexByte(rest[:pathEnd], '/'); slash >= 0 {
		basePath = rest[slash:pathEnd]
	}

	// The generated database name replaces the base path; only the query is
	// carried over.
	var rawQuery string
	if i := strings.IndexByte(rest, '?'); i >= 0 {
		rawQuery = rest[i+1:]
	}
	rawQuery, sawSSL := rewriteQuery(rawQuery, stripDBAliases)
	if !sawSSL {
		// pgx maps the ssl=true alias to sslmode=require, and an explicit
		// sslmode wins over the alias, so adding sslmode=disable when neither
		// was given must not override a requested TLS mode.
		if rawQuery != "" {
			rawQuery += "&"
		}
		rawQuery += "sslmode=disable"
	}

	// dbName is generated from a fixed prefix and hex digits, so it needs no
	// path escaping. An empty dbName keeps the base URL's database.
	path := basePath
	if dbName != "" {
		path = "/" + dbName
	}
	u := scheme + "://" + authority + path
	if rawQuery != "" {
		u += "?" + rawQuery
	}
	return u
}

// rewriteQuery rebuilds rawQuery for the generated database. Every parameter is
// preserved verbatim -- including ordering, duplicates, percent-encoding, and
// literal "+" signs that pgx reads as plus signs rather than spaces -- except
// dbname/database, which pgx lets override the path, so they are dropped to
// keep the generated database authoritative. Keys are percent-decoded only to
// recognize those parameters; the raw pair is emitted unchanged.
//
// sawSSL reports whether an ssl or sslmode key was present, so the caller can
// avoid adding a default that would override the requested TLS mode.
func rewriteQuery(rawQuery string, stripDBAliases bool) (rewritten string, sawSSL bool) {
	kept := make([]string, 0, strings.Count(rawQuery, "&")+1)
	for _, param := range strings.Split(rawQuery, "&") {
		if param == "" {
			continue
		}
		rawKey, _, _ := strings.Cut(param, "=")
		// pgx trims surrounding raw ASCII spaces before decoding a key.
		rawKey = strings.Trim(rawKey, " ")
		// PathUnescape percent-decodes without turning "+" into a space.
		key, err := url.PathUnescape(rawKey)
		if err != nil {
			// pgx rejects the same malformed escape when it parses the URL.
			key = rawKey
		}
		if stripDBAliases && (strings.EqualFold(key, "dbname") || strings.EqualFold(key, "database")) {
			continue
		}
		if strings.EqualFold(key, "ssl") || strings.EqualFold(key, "sslmode") {
			sawSSL = true
		}
		kept = append(kept, param)
	}
	return strings.Join(kept, "&"), sawSSL
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
		// A leftover database can still have connections attached (a test that
		// died holding one, or a concurrent process). DROP DATABASE fails in
		// that case; keep the target and its marker for a later run and move
		// on, so one busy leftover cannot stop this test from getting its own
		// database.
		if _, err := conn.Exec(ctx, dropDatabaseSQL(name)); err != nil {
			continue
		}
		// Only forget the marker once the database it points at is gone;
		// otherwise the marker would be the only record of the leftover.
		_, _ = conn.Exec(ctx, dropDatabaseSQL(name+cleanupSuffix))
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
