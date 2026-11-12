//go:build !notest

package pgtestdb

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestPgtestdb(t *testing.T) {
	t.Run("NewWithStatements", func(t *testing.T) {
		tdb := New(t, WithStatements("CREATE TABLE testtable (id bigint)"))
		ExecOrError(t, tdb.Pool(), "SELECT * FROM testtable LIMIT 1")
		assert.NotEmpty(t, tdb.Name())
		assert.NotEmpty(t, tdb.URL())
	})

	t.Run("NewWithPoolOptionsInDBURL", func(t *testing.T) {
		base := resolveDBURL("")
		sep := "?"
		if strings.Contains(base, "?") {
			sep = "&"
		}
		// pool_max_conns is a pgxpool option: it must be consumed by the pool
		// configuration and never forwarded to the server as a startup
		// parameter by the admin connection.
		tdb := New(t, WithDBURL(base+sep+"pool_max_conns=2"))

		assert.NotEmpty(t, tdb.Name())
		ExecOrError(t, tdb.Pool(), "SELECT 1")
	})

	t.Run("PoolCloseDoesNotBlockCleanup", func(t *testing.T) {
		ctx := context.Background()

		oldTimeout := poolCloseTimeout
		poolCloseTimeout = 100 * time.Millisecond
		defer func() { poolCloseTimeout = oldTimeout }()

		fakeT := &fakeTestingT{}
		tdb := New(fakeT, WithDBURL(resolveDBURL("")))
		if fakeT.fatal {
			t.Fatalf("New failed: %s", fakeT.errMsg)
		}

		// Hold an acquired connection in an uncommitted transaction, which is
		// what a test that exited through Fatal can leave behind. pgxpool.Close
		// waits for it to be returned.
		conn, err := tdb.Pool().Acquire(ctx)
		fatalOnError(t, err, "acquire leaked connection")
		tx, err := conn.Begin(ctx)
		fatalOnError(t, err, "begin leaked transaction")

		// The pool close is registered last, so LIFO cleanup runs it first.
		start := time.Now()
		fakeT.cleanups[len(fakeT.cleanups)-1]()
		closeElapsed := time.Since(start)
		// Capture the message now: the remaining cleanups run against the busy
		// database and legitimately report their own errors, which overwrite
		// fakeT.errMsg.
		closeErrMsg := fakeT.errMsg
		for i := len(fakeT.cleanups) - 2; i >= 0; i-- {
			fakeT.cleanups[i]()
		}

		assert.False(t, fakeT.fatal, "cleanup must not abort: %s", fakeT.errMsg)
		assert.Less(t, closeElapsed, time.Second,
			"pool close must not wait indefinitely for an abandoned transaction")
		assert.Contains(t, closeErrMsg, poolCloseErrorMessage,
			"a pool close timeout must be reported as a test error")

		admin := adminConnect(ctx, t, resolveDBURL(""))
		defer func() { _ = admin.Close(ctx) }()

		assert.True(t, dbExists(ctx, t, admin, tdb.Name()+cleanupSuffix),
			"a database left busy by an abandoned transaction must be marked")

		// Return the connection so the background Close can finish, then remove
		// the leftovers this subtest created.
		fatalOnError(t, tx.Rollback(ctx), "rollback leaked transaction")
		conn.Release()
		waitForNoConnections(ctx, t, admin, tdb.Name())
		cleanUpOldDBs(ctx, t, admin)
	})

	t.Run("ExecOrError", func(t *testing.T) {
		tdb := New(t)
		fakeT := &fakeTestingT{}
		ExecOrError(fakeT, tdb.Pool(), "SELECT * FROM nonexistingtable")
		assert.Contains(t, fakeT.errMsg, "failed to run statement")
		assert.False(t, fakeT.fatal)

		fakeT = &fakeTestingT{}
		ExecOrError(fakeT, tdb.Pool(), "SELECT 1")
		assert.Empty(t, fakeT.errMsg)
	})

	t.Run("ExecOrFail", func(t *testing.T) {
		tdb := New(t)
		fakeT := &fakeTestingT{}
		ExecOrFail(fakeT, tdb.Pool(), "SELECT * FROM nonexistingtable")
		assert.Contains(t, fakeT.errMsg, "failed to run statement")
		assert.True(t, fakeT.fatal)

		fakeT = &fakeTestingT{}
		ExecOrFail(fakeT, tdb.Pool(), "SELECT 1")
		assert.Empty(t, fakeT.errMsg)
	})

	t.Run("fatalOnError", func(t *testing.T) {
		fakeT := &fakeTestingT{}
		fatalOnError(fakeT, fmt.Errorf("something went wrong"), "prefix")
		assert.Contains(t, fakeT.errMsg, "prefix")
		assert.Contains(t, fakeT.errMsg, "something went wrong")
		assert.True(t, fakeT.fatal)

		fakeT = &fakeTestingT{}
		fatalOnError(fakeT, nil, "prefix")
		assert.Empty(t, fakeT.errMsg)
	})

	t.Run("getDBNamesToCleanupAndCleanUpOldDBs", func(t *testing.T) {
		ctx := context.Background()
		conn := adminConnect(ctx, t, resolveDBURL(""))
		defer func() { _ = conn.Close(ctx) }()

		dbName := dbNamePrefix + fmt.Sprintf("%x", rand.Uint64())
		ExecOrFail(t, conn, createDatabaseSQL(dbName+cleanupSuffix, ""))

		// The LIKE pattern intentionally leaves underscores unescaped, so the
		// decoy matches the query but not cleanupMarkerName's literal checks.
		decoy := dbNamePrefix + "prod_requiresXcleanup"
		ExecOrFail(t, conn, createDatabaseSQL(decoy, ""))
		defer func() { _, _ = conn.Exec(ctx, dropDatabaseSQL(decoy)) }()

		names := getDBNamesToCleanup(ctx, t, conn)
		assert.Contains(t, names, dbName)
		assert.NotContains(t, names, decoy)
		assert.True(t, dbExists(ctx, t, conn, decoy))

		cleanUpOldDBs(ctx, t, conn)

		assert.NotContains(t, getDBNamesToCleanup(ctx, t, conn), dbName)
		assert.False(t, dbExists(ctx, t, conn, dbName))
		assert.True(t, dbExists(ctx, t, conn, decoy), "cleanup must not drop a database that only resembles a marker")
	})

	t.Run("cleanUpOldDBsKeepsBusyDatabase", func(t *testing.T) {
		ctx := context.Background()
		conn := adminConnect(ctx, t, resolveDBURL(""))
		defer func() { _ = conn.Close(ctx) }()

		// A leftover from a run that died while still connected.
		dbName := dbNamePrefix + fmt.Sprintf("%x", rand.Uint64())
		ExecOrFail(t, conn, createDatabaseSQL(dbName, ""))
		ExecOrFail(t, conn, createDatabaseSQL(dbName+cleanupSuffix, ""))
		busy := connect(ctx, t, buildURL(t, resolveDBURL(""), dbName))

		// A second leftover that is not busy: cleanup must still reach it.
		freeName := dbNamePrefix + fmt.Sprintf("%x", rand.Uint64())
		ExecOrFail(t, conn, createDatabaseSQL(freeName+cleanupSuffix, ""))

		cleanUpOldDBs(ctx, t, conn)

		assert.True(t, dbExists(ctx, t, conn, dbName), "busy database must be retained")
		assert.True(t, dbExists(ctx, t, conn, dbName+cleanupSuffix), "busy database must keep its marker")
		assert.False(t, dbExists(ctx, t, conn, freeName), "cleanup must continue past a busy database")
		assert.False(t, dbExists(ctx, t, conn, freeName+cleanupSuffix))

		fatalOnError(t, busy.Close(ctx), "pgtestdb: close leftover connection")
		cleanUpOldDBs(ctx, t, conn)
	})

	t.Run("cleanupMarkerName", func(t *testing.T) {
		name, ok := cleanupMarkerName(dbNamePrefix + "abc" + cleanupSuffix)
		assert.True(t, ok)
		assert.Equal(t, dbNamePrefix+"abc", name)

		for _, datname := range []string{
			dbNamePrefix + "prod_requiresXcleanup",
			dbNamePrefix + "abc",
			"other" + cleanupSuffix,
			"plain",
		} {
			_, ok := cleanupMarkerName(datname)
			assert.False(t, ok, datname)
		}
	})

	t.Run("tryDropDB", func(t *testing.T) {
		t.Run("WhenDropFails", func(t *testing.T) {
			ctx := context.Background()
			conn := adminConnect(ctx, t, resolveDBURL(""))
			defer func() { _ = conn.Close(ctx) }()
			defer cleanUpOldDBs(ctx, t, conn)

			dbName := dbNamePrefix + fmt.Sprintf("%x", rand.Uint64())
			ExecOrFail(t, conn, createDatabaseSQL(dbName, ""))

			old := dropDBResultHook
			dropDBResultHook = func(_ bool) bool { return false }
			defer func() { dropDBResultHook = old }()

			tryDropDB(t, conn, dbName)

			assert.Contains(t, getDBNamesToCleanup(ctx, t, conn), dbName)
		})

		t.Run("WhenDropSucceeds", func(t *testing.T) {
			ctx := context.Background()
			conn := adminConnect(ctx, t, resolveDBURL(""))
			defer func() { _ = conn.Close(ctx) }()
			defer cleanUpOldDBs(ctx, t, conn)

			dbName := dbNamePrefix + fmt.Sprintf("%x", rand.Uint64())
			ExecOrFail(t, conn, createDatabaseSQL(dbName, ""))

			tryDropDB(t, conn, dbName)

			assert.NotContains(t, getDBNamesToCleanup(ctx, t, conn), dbName)
		})
	})

	t.Run("createDatabaseSQL", func(t *testing.T) {
		hostile := `x"; DROP DATABASE y; --`
		quoted := `"x""; DROP DATABASE y; --"`

		sql := createDatabaseSQL(hostile, "")
		assert.Equal(t, "CREATE DATABASE "+quoted, sql)
		assert.NotContains(t, sql, hostile)

		sql = createDatabaseSQL(hostile, hostile)
		assert.Equal(t, "CREATE DATABASE "+quoted+" TEMPLATE "+quoted, sql)
		assert.NotContains(t, sql, hostile)

		assert.Equal(t, `CREATE DATABASE "plain"`, createDatabaseSQL("plain", ""))
	})

	t.Run("dropDatabaseSQL", func(t *testing.T) {
		hostile := `x"; DROP DATABASE y; --`
		sql := dropDatabaseSQL(hostile)
		assert.Equal(t, `DROP DATABASE IF EXISTS "x""; DROP DATABASE y; --"`, sql)
		assert.NotContains(t, sql, hostile)
	})

	t.Run("resolveDBURL", func(t *testing.T) {
		t.Run("OptionWins", func(t *testing.T) {
			t.Setenv("TEST_DATABASE_URL", "postgres://env-host:5432/envdb")
			assert.Equal(t, "postgres://option-host:5433/optdb",
				resolveDBURL("postgres://option-host:5433/optdb"))
		})

		t.Run("EnvUsedWhenNoOption", func(t *testing.T) {
			t.Setenv("TEST_DATABASE_URL", "postgres://env-host:5432/envdb")
			assert.Equal(t, "postgres://env-host:5432/envdb", resolveDBURL(""))
		})

		t.Run("DefaultWhenNeitherIsSet", func(t *testing.T) {
			t.Setenv("TEST_DATABASE_URL", "")
			assert.Equal(t, defaultDBURL, resolveDBURL(""))
		})
	})

	t.Run("WithDBURL", func(t *testing.T) {
		t.Setenv("TEST_DATABASE_URL", "postgres://env-host:5432/envdb?sslmode=require")
		got := buildURL(t, resolveDBURL("postgres://option-host:5433/optdb"), "pgtestdb_abc")
		assert.NotContains(t, got, "env-host")
		assert.Contains(t, got, "sslmode=disable")

		cfg, err := pgx.ParseConfig(got)
		assert.NoError(t, err)
		assert.Equal(t, "option-host", cfg.Host)
		assert.Equal(t, uint16(5433), cfg.Port)
		assert.Equal(t, "pgtestdb_abc", cfg.Database)
	})

	t.Run("buildURL", func(t *testing.T) {
		t.Run("KeepsSSLMODEFromEnv", func(t *testing.T) {
			t.Setenv("TEST_DATABASE_URL", "postgres://localhost:5432/postgres?sslmode=require")
			got := buildURL(t, resolveDBURL(""), "pgtestdb_abc")
			assert.Contains(t, got, "/pgtestdb_abc?")
			assert.Contains(t, got, "sslmode=require")
			assert.NotContains(t, got, "sslmode=disable")
		})

		t.Run("AddsDisableWhenAbsent", func(t *testing.T) {
			t.Setenv("TEST_DATABASE_URL", "postgres://localhost:5432/postgres")
			got := buildURL(t, resolveDBURL(""), "pgtestdb_abc")
			assert.Equal(t, "postgres://localhost:5432/pgtestdb_abc?sslmode=disable", got)
		})

		t.Run("DefaultEnvUsesDisable", func(t *testing.T) {
			t.Setenv("TEST_DATABASE_URL", "")
			got := buildURL(t, resolveDBURL(""), "pgtestdb_abc")
			assert.Equal(t, "postgres://localhost:5432/pgtestdb_abc?sslmode=disable", got)
		})

		t.Run("HonorsSSLTrueAlias", func(t *testing.T) {
			base := "postgres://localhost:5432/postgres?ssl=true"
			got := buildURL(t, base, "pgtestdb_abc")

			assert.Contains(t, got, "ssl=true")
			assert.NotContains(t, got, "sslmode=", "must not override the ssl=true alias")

			cfg, err := pgx.ParseConfig(got)
			assert.NoError(t, err)
			assert.NotNil(t, cfg.TLSConfig, "pgx must still select TLS for ssl=true")
		})

		t.Run("HonorsExplicitSSLModeWithSSLTrue", func(t *testing.T) {
			base := "postgres://localhost:5432/postgres?ssl=true&sslmode=verify-full"
			got := buildURL(t, base, "pgtestdb_abc")

			assert.Contains(t, got, "ssl=true")
			assert.Contains(t, got, "sslmode=verify-full")
			assert.NotContains(t, got, "sslmode=disable")
		})

		t.Run("PreservesSemicolonInQueryValue", func(t *testing.T) {
			base := "postgres://localhost:5432/postgres?password=a;b&sslmode=disable"
			got := buildURL(t, base, "pgtestdb_abc")

			assert.Contains(t, got, "password=a;b")
			cfg, err := pgx.ParseConfig(got)
			assert.NoError(t, err)
			assert.Equal(t, "a;b", cfg.Password)
		})

		t.Run("PreservesSSLOrdering", func(t *testing.T) {
			base := "postgres://localhost:5432/postgres?sslmode=disable&ssl=true"
			got := buildURL(t, base, "pgtestdb_abc")

			assert.Contains(t, got, "sslmode=disable&ssl=true")
			cfg, err := pgx.ParseConfig(got)
			assert.NoError(t, err)
			assert.NotNil(t, cfg.TLSConfig, "the final ssl=true alias must win")
		})

		t.Run("AdminURLKeepsDatabaseAndDefaultsSSL", func(t *testing.T) {
			t.Setenv("PGSSLMODE", "require")
			got := buildURL(t, "postgres://localhost:5432/postgres", "")
			assert.Equal(t, "postgres://localhost:5432/postgres?sslmode=disable", got)
		})

		t.Run("DropsAliasesWithRawKeySpaces", func(t *testing.T) {
			base := "postgres://localhost:5432/postgres?dbname =shared& database=other&sslmode=disable"
			got := buildURL(t, base, "pgtestdb_abc")

			cfg, err := pgxpool.ParseConfig(got)
			assert.NoError(t, err)
			assert.Equal(t, "pgtestdb_abc", cfg.ConnConfig.Database)
		})

		t.Run("KeepsUserinfoQuestionMark", func(t *testing.T) {
			base := "postgres://user:p?ss@localhost:5432/postgres"
			got := buildURL(t, base, "pgtestdb_abc")

			assert.Contains(t, got, "postgres://user:p?ss@localhost:5432/pgtestdb_abc?")
			cfg, err := pgx.ParseConfig(got)
			assert.NoError(t, err)
			assert.Equal(t, "p?ss", cfg.Password)
			assert.Equal(t, "pgtestdb_abc", cfg.Database)
		})

		t.Run("KeepsEscapedUnixSocketHost", func(t *testing.T) {
			// net/url rejects %2F in the host, but pgx follows libpq and
			// percent-decodes the authority, so the raw rewrite must keep it.
			base := "postgres://user@%2Fvar%2Frun%2Fpostgresql/postgres"
			got := buildURL(t, base, "pgtestdb_abc")

			assert.Equal(t,
				"postgres://user@%2Fvar%2Frun%2Fpostgresql/pgtestdb_abc?sslmode=disable", got)

			cfg, err := pgxpool.ParseConfig(got)
			assert.NoError(t, err, "pgxpool must accept the escaped Unix socket host")
			assert.Equal(t, "/var/run/postgresql", cfg.ConnConfig.Host)
			assert.Equal(t, "user", cfg.ConnConfig.User)
			assert.Equal(t, "pgtestdb_abc", cfg.ConnConfig.Database)
		})

		t.Run("KeepsAuthorityVerbatim", func(t *testing.T) {
			for _, authority := range []string{
				"host1:5433,host2:5434",
				"[::1]:5432",
				"user:p%40ss@[::1]:5432",
			} {
				got := buildURL(t, "postgres://"+authority+"/postgres", "pgtestdb_abc")
				assert.Contains(t, got, "postgres://"+authority+"/pgtestdb_abc?")

				_, err := pgxpool.ParseConfig(got)
				assert.NoError(t, err, "pgxpool must accept authority %q", authority)
			}
		})

		t.Run("PreservesLiteralPlusInQuery", func(t *testing.T) {
			base := "postgres://localhost:5432/postgres?password=a+b&sslmode=disable"
			got := buildURL(t, base, "pgtestdb_abc")

			// pgx reads a raw "+" as a literal plus sign, so the raw
			// parameter is kept as is rather than re-encoded.
			assert.Contains(t, got, "password=a+b")
			assert.NotContains(t, got, "a%20b")
			assert.NotContains(t, got, "%2B")

			cfg, err := pgx.ParseConfig(got)
			assert.NoError(t, err)
			assert.Equal(t, "a+b", cfg.Password)
		})

		t.Run("DropsDBNameAliases", func(t *testing.T) {
			base := "postgres://localhost:5432/other?dbname=shared&database=other&%64bname=encoded&DataBase=mixed&sslmode=require"
			got := buildURL(t, base, "pgtestdb_abc")

			assert.Contains(t, got, "/pgtestdb_abc?")
			assert.Contains(t, got, "sslmode=require")
			for _, dropped := range []string{"dbname", "database", "%64bname", "DataBase"} {
				assert.NotContains(t, got, dropped)
			}

			cfg, err := pgx.ParseConfig(got)
			assert.NoError(t, err)
			assert.Equal(t, "pgtestdb_abc", cfg.Database)
		})

		t.Run("PreservesPercentEncoding", func(t *testing.T) {
			base := "postgres://testuser:a%20b@localhost:5432/postgres?options=-c%20search_path%3Dpublic&application_name=my%20app"
			got := buildURL(t, base, "pgtestdb_abc")

			assert.Contains(t, got, "a%20b")
			assert.Contains(t, got, "my%20app")
			assert.NotContains(t, got, "+")

			cfg, err := pgx.ParseConfig(got)
			assert.NoError(t, err)
			assert.Equal(t, "my app", cfg.RuntimeParams["application_name"])
			assert.Equal(t, "-c search_path=public", cfg.RuntimeParams["options"])
		})

		t.Run("KeepsLiteralPlusEncoded", func(t *testing.T) {
			base := "postgres://localhost:5432/postgres?options=-c%20x%3D1&application_name=a%2Bb"
			got := buildURL(t, base, "pgtestdb_abc")

			assert.Contains(t, got, "application_name=a%2Bb")
			assert.NotContains(t, got, "a+b")

			cfg, err := pgx.ParseConfig(got)
			assert.NoError(t, err)
			assert.Equal(t, "a+b", cfg.RuntimeParams["application_name"])
		})

		t.Run("PreservesOrderingAndDuplicates", func(t *testing.T) {
			base := "postgres://localhost:5432/postgres?application_name=first&x=1&application_name=last"
			got := buildURL(t, base, "pgtestdb_abc")

			assert.Contains(t,
				got,
				"/pgtestdb_abc?application_name=first&x=1&application_name=last&sslmode=disable")

			cfg, err := pgx.ParseConfig(got)
			assert.NoError(t, err)
			assert.Equal(t, "last", cfg.RuntimeParams["application_name"])
		})

		t.Run("PreservesPoolOptions", func(t *testing.T) {
			base := "postgres://localhost:5432/postgres?pool_max_conns=7&pool_min_conns=2&sslmode=disable"
			got := buildURL(t, base, "pgtestdb_abc")

			assert.Contains(t, got, "pool_max_conns=7")
			assert.Contains(t, got, "pool_min_conns=2")

			cfg, err := pgxpool.ParseConfig(got)
			assert.NoError(t, err)
			assert.Equal(t, int32(7), cfg.MaxConns)
			assert.Equal(t, int32(2), cfg.MinConns)
			assert.Equal(t, "pgtestdb_abc", cfg.ConnConfig.Database)
		})
	})
}

// dbExists reports whether the server has a database named name.
func dbExists(ctx context.Context, t TB, conn *pgx.Conn, name string) bool {
	t.Helper()
	var exists bool
	err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", name).Scan(&exists)
	fatalOnError(t, err, "pgtestdb: check DB exists")
	return exists
}

// waitForNoConnections blocks until no backend is connected to dbName, so a
// database left busy by a background pool close can be dropped.
func waitForNoConnections(ctx context.Context, t TB, conn *pgx.Conn, dbName string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var count int
		err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname = $1", dbName).Scan(&count)
		fatalOnError(t, err, "pgtestdb: count connections")
		if count == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pgtestdb: timed out waiting for connections to %s to close", dbName)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type fakeTestingT struct {
	errMsg   string
	fatal    bool
	cleanups []func()
}

func (f *fakeTestingT) Cleanup(c func())                  { f.cleanups = append(f.cleanups, c) }
func (f *fakeTestingT) Errorf(format string, args ...any) { f.errMsg = fmt.Sprintf(format, args...) }
func (f *fakeTestingT) Fatalf(format string, args ...any) { f.Errorf(format, args...); f.fatal = true }
func (f *fakeTestingT) Helper()                           {}
