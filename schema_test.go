package skein

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSchemaValidationAtPublicEntrypoints(t *testing.T) {
	t.Parallel()
	pool, err := pgxpool.New(t.Context(), "postgres:///postgres?host=/tmp")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, tc := range []struct {
		name   string
		schema string
	}{
		{name: "nul_alias", schema: "tenant\x00a"},
		{name: "invalid_utf8", schema: "tenant\xff"},
		{name: "truncated_ascii", schema: strings.Repeat("a", 64)},
		{name: "truncated_unicode", schema: strings.Repeat("界", 22)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(pool, Config{Schema: tc.schema}); err == nil {
				t.Error("New accepted an invalid schema")
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			err := Migrate(ctx, pool, tc.schema)
			if err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "schema") {
				t.Errorf("Migrate must reject schema before database I/O, got %v", err)
			}
		})
	}
	for _, schema := range []string{"", "tenant", `tenant "one"`, strings.Repeat("a", 63), strings.Repeat("界", 21)} {
		t.Run("valid_"+schema, func(t *testing.T) {
			if _, err := New(pool, Config{Schema: schema}); err != nil {
				t.Fatalf("New rejected valid schema: %v", err)
			}
		})
	}
}
