package integration

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// The desired schema and the upgrade history must install the same objects.
func TestDeclarativeSchemaMatchesMigrations(t *testing.T) {
	t.Parallel()
	ddl, err := os.ReadFile("../../schema.sql")
	if err != nil {
		t.Fatal(err)
	}

	legacy, container := newTestDatabase(t)
	ctx := t.Context()
	if _, err := legacy.Exec(ctx, `CREATE DATABASE declarative`); err != nil {
		t.Fatal(err)
	}

	cfg := legacy.Config()
	cfg.ConnConfig.Database = "declarative"
	declared, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer declared.Close()
	if _, err := declared.Exec(ctx, string(ddl)); err != nil {
		t.Fatal(err)
	}

	want, got := dumpSchema(t, container, "outbox"), dumpSchema(t, container, "declarative")
	if got != want {
		t.Fatalf("declarative schema:\n%s\nlegacy migrations:\n%s", got, want)
	}
}

func dumpSchema(t *testing.T, container *postgres.PostgresContainer, database string) string {
	t.Helper()
	code, output, err := container.Exec(t.Context(), []string{
		"pg_dump", "-U", "test", "--schema-only", "--no-owner", "--no-privileges", database,
	})
	if err != nil {
		t.Fatal(err)
	}
	dump, err := io.ReadAll(output)
	if err != nil || code != 0 {
		t.Fatalf("pg_dump %s: exit %d, %v\n%s", database, code, err, dump)
	}

	// Recent pg_dump versions randomize the psql restriction key each run.
	var schema strings.Builder
	for line := range strings.SplitSeq(string(dump), "\n") {
		if !strings.HasPrefix(line, `\restrict `) && !strings.HasPrefix(line, `\unrestrict `) {
			schema.WriteString(line)
			schema.WriteByte('\n')
		}
	}
	return schema.String()
}
