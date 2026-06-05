// Package migrations exposes the embedded outbox SQL migrations.
package migrations

import (
	"embed"
	"io/fs"
	"sort"
	"strings"
)

//go:embed *.sql
var fsys embed.FS

// FS returns the embedded migration files, for use with golang-migrate (iofs
// source), goose, atlas, etc.
//
// The SQL is schema-unqualified: to install into a non-default schema, set
// search_path on the migration connection.
func FS() embed.FS { return fsys }

// Up returns the contents of every *.sql file, ordered by filename. Each entry
// is ready to pass to Exec for callers that apply migrations without a dedicated
// tool. Migrations are forward-only; there are no down files.
func Up() []string {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		panic(err) // embedded FS is always present; a failure is a build bug
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	out := make([]string, 0, len(names))
	for _, n := range names {
		b, err := fsys.ReadFile(n)
		if err != nil {
			panic(err)
		}
		out = append(out, string(b))
	}
	return out
}
