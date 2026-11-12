//go:build !notest

package pgtestdb

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPgtestdb(t *testing.T) {
	t.Run("NewWithStatements", func(t *testing.T) {
		tdb := New(t, WithStatements("CREATE TABLE testtable (id bigint)"))
		ExecOrError(t, tdb.Pool(), "SELECT * FROM testtable LIMIT 1")
		assert.NotEmpty(t, tdb.Name())
		assert.NotEmpty(t, tdb.URL())
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

		assert.Contains(t, getDBNamesToCleanup(ctx, t, conn), dbName)

		cleanUpOldDBs(ctx, t, conn)

		assert.NotContains(t, getDBNamesToCleanup(ctx, t, conn), dbName)
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
		u, err := url.Parse(buildURL(t, resolveDBURL("postgres://option-host:5433/optdb"), "pgtestdb_abc"))
		assert.NoError(t, err)
		assert.Equal(t, "option-host:5433", u.Host)
		assert.Equal(t, "/pgtestdb_abc", u.Path)
		assert.Equal(t, "disable", u.Query().Get("sslmode"))
	})

	t.Run("buildURL", func(t *testing.T) {
		t.Run("KeepsSSLMODEFromEnv", func(t *testing.T) {
			t.Setenv("TEST_DATABASE_URL", "postgres://localhost:5432/postgres?sslmode=require")
			u, err := url.Parse(buildURL(t, resolveDBURL(""), "pgtestdb_abc"))
			assert.NoError(t, err)
			assert.Equal(t, "require", u.Query().Get("sslmode"))
			assert.Equal(t, "/pgtestdb_abc", u.Path)
		})

		t.Run("AddsDisableWhenAbsent", func(t *testing.T) {
			t.Setenv("TEST_DATABASE_URL", "postgres://localhost:5432/postgres")
			u, err := url.Parse(buildURL(t, resolveDBURL(""), "pgtestdb_abc"))
			assert.NoError(t, err)
			assert.Equal(t, "disable", u.Query().Get("sslmode"))
		})

		t.Run("DefaultEnvUsesDisable", func(t *testing.T) {
			t.Setenv("TEST_DATABASE_URL", "")
			u, err := url.Parse(buildURL(t, resolveDBURL(""), ""))
			assert.NoError(t, err)
			assert.Equal(t, "disable", u.Query().Get("sslmode"))
			assert.Equal(t, "/postgres", u.Path)
		})
	})
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
