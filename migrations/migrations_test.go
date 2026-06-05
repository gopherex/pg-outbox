package migrations

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/gopherex/pg-outbox/message"
)

func TestFS(t *testing.T) {
	entries, err := fs.ReadDir(FS(), ".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) < 1 {
		t.Fatalf("expected at least 1 migration file, got %d", len(entries))
	}
}

func TestUp(t *testing.T) {
	ms := Up()
	if len(ms) != 1 {
		t.Fatalf("expected 1 up migration, got %d", len(ms))
	}
	if !strings.Contains(ms[0], "CREATE TABLE") || !strings.Contains(ms[0], message.TableName) {
		t.Fatalf("first migration missing CREATE TABLE %s:\n%s", message.TableName, ms[0])
	}
	if strings.Contains(ms[0], "DROP TABLE") {
		t.Fatalf("up migration must not contain DROP TABLE")
	}
}
