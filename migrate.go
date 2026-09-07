package skein

import (
	"cmp"
	"context"
	"fmt"
	"io/fs"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mbeoliero/skein/internal/store"
	"github.com/mbeoliero/skein/migrations"
)

// schemaVersion is the migration this build expects; Start refuses anything else.
const schemaVersion = 1

// Migrate creates the schema and applies pending migrations. It is explicit and
// serial by design: run it from the release step, not from Start.
func Migrate(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	ms, err := loadMigrations()
	if err != nil {
		return err
	}
	return store.Open(pool, cmp.Or(schema, defaultSchema)).Migrate(ctx, ms)
}

func loadMigrations() ([]store.Migration, error) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return nil, err
	}
	var out []store.Migration
	for _, en := range entries {
		name := en.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(name, "_")
		v, err := strconv.Atoi(prefix)
		if !ok || err != nil {
			return nil, fmt.Errorf("skein: migration %q is not NNNNN_name.sql", name)
		}
		b, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			return nil, err
		}
		out = append(out, store.Migration{Version: v, SQL: string(b)})
	}
	slices.SortFunc(out, func(a, b store.Migration) int { return cmp.Compare(a.Version, b.Version) })
	if len(out) == 0 || out[len(out)-1].Version != schemaVersion {
		return nil, fmt.Errorf("skein: embedded migrations end at %d, schemaVersion is %d", out[len(out)-1].Version, schemaVersion)
	}
	return out, nil
}
