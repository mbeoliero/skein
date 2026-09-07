// Package migrations holds the embedded schema files applied by skein.Migrate.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
