package oids

import (
	"testing"
)

func TestIDString(t *testing.T) {
	tests := []struct {
		id   ID
		want string
	}{
		{Bool, "bool"},
		{Text, "text"},
		{Jsonb, "jsonb"},
		{Uuid, "uuid"},
		{PgLsn, "pg_lsn"},
		{TextArray, "_text"},
		{JsonbArray, "_jsonb"},
		{PgLsnArray, "_pg_lsn"},
		{ID(123456789), "oids.ID(123456789)"},
	}

	for _, tt := range tests {
		if got := tt.id.String(); got != tt.want {
			t.Errorf("%v.String() = %q, want %q", tt.id, got, tt.want)
		}
	}
}
