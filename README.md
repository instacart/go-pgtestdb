# `pgtestdb`

`pgtestdb` creates an isolated PostgreSQL database for a Go test. Each database gets a random name, a `pgxpool.Pool`, and a connection URL. The database and pool are registered with the test's cleanup lifecycle automatically.

Use it when a test needs a real PostgreSQL database rather than a mock or an in-memory substitute.

Pre-1.0, maintained on a best-effort basis. See [Project status](#project-status).

## Requirements

The test process must be able to connect to PostgreSQL and create and drop databases.

The connection URL is resolved in this order:

1. `WithDBURL(...)` passed to `New`
2. the `TEST_DATABASE_URL` environment variable
3. the built-in default, `postgres://localhost:5432/postgres`

Pass the URL explicitly when the test should not depend on the environment:

```go
tdb := pgtestdb.New(t, pgtestdb.WithDBURL("postgres://user:password@db-host:5432/postgres"))
```

`pgtestdb` rewrites the URL textually rather than decoding and re-encoding it, so PostgreSQL URI semantics are preserved exactly. The scheme, the authority (credentials, IPv6 literals, comma-separated hosts, and percent-encoded Unix socket paths such as `%2Fvar%2Frun%2Fpostgresql`), and every remaining query parameter are copied through unchanged: a literal `+` stays a plus sign and `%XX` escapes are untouched. It replaces the database path for the test database, drops any `dbname` or `database` query parameter that would otherwise override that path, and defaults `sslmode` to `disable` when the URL specifies neither `sslmode` nor the `ssl` alias (so `ssl=true` still means `sslmode=require`).

## Leftover databases

Every test database is dropped at cleanup. If the drop fails — typically because a test left a
connection or an open transaction behind — the library records a marker database named
`<name>_requires_cleanup` instead of losing track of it. The next run drops any marked database
together with the database it points at, so abandoned test databases do not accumulate across
runs and fill the disk. A marked database that is still in use cannot be dropped: cleanup keeps
both the database and its marker for a later run and carries on with the next one, so it never
prevents a new test from getting its own database.

## Creating a test database

Create a database from scratch and run setup statements with `WithStatements`:

```go
package users_test

import (
	"testing"

	"github.com/instacart/go-pgtestdb/pgtestdb"
)

func TestUsers(t *testing.T) {
	tdb := pgtestdb.New(t, pgtestdb.WithStatements(
		`CREATE TABLE users (id bigint PRIMARY KEY, name text NOT NULL)`,
		`INSERT INTO users (id, name) VALUES (1, 'Ada')`,
	))

	pgtestdb.ExecOrFail(t, tdb.Pool(), `SELECT 1`)
	// Use tdb.Pool() for queries against the isolated database.
}
```

`WithStatements` options run in the order supplied. Multiple `WithStatements` options append to the same ordered list.

To clone an existing database, use `WithTemplate`:

```go
tdb := pgtestdb.New(t, pgtestdb.WithTemplate("myapp_template"))
```

The template must already exist and must be a trusted PostgreSQL database identifier. PostgreSQL must be able to create the new database from that template. `WithStatements` can also be supplied with `WithTemplate` to apply additional setup after the clone.

## `TestDB` API

| API | Description |
| --- | --- |
| `Pool()` | Returns the `*pgxpool.Pool` connected to the isolated database. |
| `Name()` | Returns the generated database name. |
| `URL()` | Returns the full connection URL for the isolated database. |

`URL()` is useful when the code under test expects a `database/sql` connection. Register that connection for cleanup as well, and close it before the test database is dropped:

```go
import (
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/instacart/go-pgtestdb/pgtestdb"
)

func openTestDB(t *testing.T) *sql.DB {
	tdb := pgtestdb.New(t)
	db, err := sql.Open("pgx", tdb.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
```

## Statement helpers

`ExecOrFail` and `ExecOrError` accept either a `*pgxpool.Pool` or a `*pgx.Conn`:

```go
pgtestdb.ExecOrFail(t, tdb.Pool(), `CREATE TABLE events (id bigint PRIMARY KEY)`)

ok := pgtestdb.ExecOrError(t, tdb.Pool(), `INSERT INTO events (id) VALUES (1)`)
if !ok {
    t.Fatal("insert failed")
}
```

`ExecOrFail` calls `Fatalf` when the statement fails. `ExecOrError` calls `Errorf`, returns `false` on failure, and returns `true` on success. Both helpers use a background context.

## Cleanup behavior

`pgtestdb.New` registers cleanup callbacks on the supplied test object. Cleanup happens in this order:

1. Close the test pool (waiting only briefly, so an abandoned transaction cannot stall the rest of cleanup).
2. Drop the generated database.
3. Close the admin connection.

Close any additional connections opened by the test before cleanup runs. If dropping a database fails, usually because a connection is still open, the `<name>_requires_cleanup` marker and retry behavior described in [Leftover databases](#leftover-databases) apply.

## Development

Requires Go 1.25 or later.

The tests require a reachable PostgreSQL server. Set `TEST_DATABASE_URL` to point at it, or start a local server and use the default `postgres://localhost:5432/postgres`:

```bash
TEST_DATABASE_URL=postgres://localhost:5432/postgres make test
```

`make test` runs `go test ./...`.

## Project status

A variant of this library is used at Instacart. We're publishing it so others
can use, adapt, and build on the ideas.

This is not an officially supported Instacart product and comes with no
commitment to ongoing maintenance. Fixes and improvements are made on a
best-effort basis, and the repository may be archived in the future.

The API is pre-1.0 and may change between minor versions; breaking changes
will be noted in the release notes.

Licensed under the Apache License 2.0. See [LICENSE](LICENSE).
