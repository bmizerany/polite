package polite

import (
	"strings"
	"testing"
	"time"

	"blake.io/polite/internal/testutil"
	"blake.io/polite/oids"
	"kr.dev/diff"
)

func TestDecodeTimeRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   time.Time
	}{
		{
			name: "utc",
			in:   time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC),
		},
		{
			name: "utc-4",
			in:   time.Date(2024, 7, 8, 9, 10, 11, 123456789, time.FixedZone("UTC-4", -4*60*60)),
		},
		{
			name: "utc+14",
			in:   time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.FixedZone("UTC+14", 14*60*60)),
		},
		{
			name: "utc-12",
			in:   time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.FixedZone("UTC-12", -12*60*60)),
		},
		{
			name: "pg-epoch",
			in:   time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "pre-1970",
			in:   time.Date(1960, 2, 3, 4, 5, 6, 789123456, time.FixedZone("UTC+9", 9*60*60)),
		},
		{
			name: "far-future",
			in:   time.Date(2500, 1, 2, 3, 4, 5, 123456789, time.FixedZone("UTC-11", -11*60*60)),
		},
		{
			name: "far-future-beyond-unixmicro-limit",
			in:   time.Date(294260, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "far-past",
			in:   time.Date(1500, 1, 2, 3, 4, 5, 123456789, time.FixedZone("UTC+11", 11*60*60)),
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cn := newSession(t)
			got := decodeQueryTimes(t, cn, `SELECT $1::timestamp, $2::timestamptz`, tt.in, tt.in)
			assertTime(t, got[0], wantTimestamp(tt.in))
			assertTime(t, got[1], wantTimestamptz(tt.in))
		})
	}
}

func TestDecodeTimeIgnoresSessionTimeZoneAcrossStatements(t *testing.T) {
	t.Parallel()

	t.Run("timestamp", func(t *testing.T) {
		cn := newSession(t)
		ctx := t.Context()

		// Trailing comments produce distinct cache keys,
		// giving each query its own prepared statement.
		const a = `SELECT '2024-06-01 12:00:00'::timestamp -- a`
		const b = `SELECT '2024-06-01 12:00:00'::timestamp -- b`

		want := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

		if err := cn.Exec(ctx, `SET TimeZone TO 'America/New_York'`); err != nil {
			t.Fatal(err)
		}
		assertTime(t, decodeQueryTimes(t, cn, a)[0], want)

		if err := cn.Exec(ctx, `SET TimeZone TO 'America/Los_Angeles'`); err != nil {
			t.Fatal(err)
		}
		assertTime(t, decodeQueryTimes(t, cn, b)[0], want)

		// Re-run A (cache hit). The session GUC does not affect
		// timestamp decoding, so the value is unchanged.
		assertTime(t, decodeQueryTimes(t, cn, a)[0], want)
	})

	t.Run("timestamptz", func(t *testing.T) {
		cn := newSession(t)
		ctx := t.Context()

		const a = `SELECT '2024-06-01 12:00:00-04'::timestamptz -- a`
		const b = `SELECT '2024-06-01 12:00:00-04'::timestamptz -- b`

		// Binary timestamptz always decodes as UTC regardless
		// of session timezone.
		want := time.Date(2024, 6, 1, 16, 0, 0, 0, time.UTC)

		if err := cn.Exec(ctx, `SET TimeZone TO 'America/New_York'`); err != nil {
			t.Fatal(err)
		}
		assertTime(t, decodeQueryTimes(t, cn, a)[0], want)

		if err := cn.Exec(ctx, `SET TimeZone TO 'America/Los_Angeles'`); err != nil {
			t.Fatal(err)
		}
		assertTime(t, decodeQueryTimes(t, cn, b)[0], want)
		assertTime(t, decodeQueryTimes(t, cn, a)[0], want)
	})
}

func TestTimestampMicrosecondRoundingInSQL(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	ctx := t.Context()

	cases := []struct {
		name string
		in   time.Time
		want string
	}{
		{
			name: "round-down",
			in:   time.Date(2024, 1, 2, 3, 4, 5, 123456499, time.UTC),
			want: `timestamp '2024-01-02 03:04:05.123456'`,
		},
		{
			name: "round-up",
			in:   time.Date(2024, 1, 2, 3, 4, 5, 123456500, time.UTC),
			want: `timestamp '2024-01-02 03:04:05.123457'`,
		},
		{
			name: "no-change",
			in:   time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC),
			want: `timestamp '2024-01-02 03:04:05.123456'`,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			sql := `WITH x AS (SELECT $1::timestamp AS ts) SELECT ts = ` + tt.want + ` FROM x`
			for row := range cn.Query(ctx, sql, tt.in) {
				got, err := row.Column(0).Bytes()
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != 1 || got[0] != 1 {
					t.Fatalf("got %v, want true", got)
				}
			}
		})
	}
}

func TestDecodeTimeFromPostgres(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		// want is built per-test in the session TZ to handle
		// timestamptz literals, which Postgres interprets in the
		// session zone before storing as a UTC instant. For plain
		// timestamp values the session zone is irrelevant; the loc
		// argument is ignored.
		want func(*time.Location) time.Time
	}{
		{
			name: "timestamp-basic",
			sql:  `SELECT '2021-01-01 00:00:00'::timestamp`,
			want: func(*time.Location) time.Time {
				return time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
			},
		},
		{
			name: "timestamp-pg-epoch",
			sql:  `SELECT TIMESTAMP '2000-01-01 00:00:00'`,
			want: func(*time.Location) time.Time {
				return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			},
		},
		{
			name: "timestamp-far-future",
			sql:  `SELECT TIMESTAMP '2500-01-02 03:04:05.123457'`,
			want: func(*time.Location) time.Time {
				return time.Date(2500, 1, 2, 3, 4, 5, 123457000, time.UTC)
			},
		},
		{
			name: "timestamp-far-future-beyond-unixmicro-limit",
			sql:  `SELECT TIMESTAMP '294260-01-01 00:00:00'`,
			want: func(*time.Location) time.Time {
				return time.Date(294260, 1, 1, 0, 0, 0, 0, time.UTC)
			},
		},
		{
			name: "timestamp-far-past",
			sql:  `SELECT TIMESTAMP '1500-01-02 03:04:05.123457'`,
			want: func(*time.Location) time.Time {
				return time.Date(1500, 1, 2, 3, 4, 5, 123457000, time.UTC)
			},
		},
		{
			name: "timestamptz-timezone",
			sql:  `SELECT TIMESTAMPTZ '2024-07-08 09:10:11.123457'`,
			want: func(loc *time.Location) time.Time {
				return time.Date(2024, 7, 8, 9, 10, 11, 123457000, loc).UTC()
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cn := newSession(t)
			defer checkReady(t, cn) // ensure we leave in a good state
			loc := currentTimeZone(t, cn)
			got := decodeQueryTimes(t, cn, tt.sql)
			assertTime(t, got[0], tt.want(loc))
		})
	}
}

func TestDecodeTimeHandlesNegativeRemainder(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	var got time.Time
	rows := 0
	for row := range cn.Query(t.Context(), `SELECT TIMESTAMPTZ '1999-12-31 23:59:59.999999+00'`) {
		rows++
		var err error
		got, err = row.Column(0).DecodeTime()
		if err != nil {
			t.Fatal(err)
		}
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}

	want := time.Date(1999, 12, 31, 23, 59, 59, 999999000, time.UTC)
	diff.Test(t, t.Errorf, got, want)
}

func TestDecodeTimeRejectsUnsupportedValues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{name: "time", sql: `SELECT localtime`, wantErr: "unsupported type"},
		{name: "date", sql: `SELECT current_date`, wantErr: "unsupported type"},
		{name: "text", sql: `SELECT 'hello'::text`, wantErr: "unsupported type"},
		{name: "int8", sql: `SELECT 42::int8`, wantErr: "unsupported type"},
		{name: "timestamp-infinity", sql: `SELECT TIMESTAMP 'infinity'`, wantErr: "infinity is not supported"},
		{name: "timestamp-neg-infinity", sql: `SELECT TIMESTAMP '-infinity'`, wantErr: "infinity is not supported"},
		{name: "timestamptz-infinity", sql: `SELECT TIMESTAMPTZ 'infinity'`, wantErr: "infinity is not supported"},
		{name: "timestamptz-neg-infinity", sql: `SELECT TIMESTAMPTZ '-infinity'`, wantErr: "infinity is not supported"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cn := newSession(t)
			ctx := t.Context()
			rows := 0
			for row := range cn.Query(ctx, tt.sql) {
				rows++
				if _, err := row.Column(0).DecodeTime(); err == nil {
					t.Fatalf("DecodeTime() error = nil for %s", tt.sql)
				} else if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("DecodeTime() error = %q, want substring %q", err, tt.wantErr)
				}
			}
			if rows != 1 {
				t.Fatalf("rows = %d, want 1", rows)
			}
		})
	}
}

func TestDecodeTimeRejectsInvalidLength(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		typ  oids.ID
	}{
		{name: "timestamp", typ: oids.Timestamp},
		{name: "timestamptz", typ: oids.Timestamptz},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			col := Column{
				typeID: tt.typ,
				data:   []byte{1, 2, 3, 4},
			}

			if _, err := col.DecodeTime(); err == nil {
				t.Fatal("DecodeTime() error = nil; want invalid length error")
			} else if !strings.Contains(err.Error(), "invalid length") {
				t.Fatalf("DecodeTime() error = %q; want invalid length", err)
			}
		})
	}
}

func currentTimeZone(t *testing.T, cn *Conn) *time.Location {
	t.Helper()

	for row := range cn.Query(t.Context(), `SHOW TimeZone`) {
		tz, err := row.Column(0).Text()
		if err != nil {
			t.Fatal(err)
		}
		loc, err := time.LoadLocation(tz)
		if err != nil {
			t.Fatal(err)
		}
		return loc
	}

	t.Fatal("expected TimeZone row")
	return nil
}

func decodeQueryTimes(t *testing.T, cn *Conn, sql string, args ...any) []time.Time {
	t.Helper()

	var got []time.Time
	rows := 0
	for row := range cn.Query(t.Context(), sql, args...) {
		rows++
		for col := range row.Columns() {
			ts, err := col.DecodeTime()
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, ts)
		}
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	return got
}

// wantTimestamp mirrors appendArg/DecodeTime semantics: polite pins
// time.Time to timestamptz, so a $1::timestamp cast goes through the
// session's TimeZone GUC. The newSession default is UTC, so the wall
// clock matches the input's UTC instant.
func wantTimestamp(in time.Time) time.Time {
	return in.UTC().Round(time.Microsecond)
}

func wantTimestamptz(in time.Time) time.Time {
	return in.UTC().Round(time.Microsecond)
}

func assertTime(t *testing.T, got, want time.Time) {
	t.Helper()

	if !got.Equal(want) {
		t.Fatalf("time = %v, want %v", got, want)
	}
	if got.Location().String() != want.Location().String() {
		t.Fatalf("location = %v, want %v", got.Location(), want.Location())
	}
}

func TestDialNow(t *testing.T) {
	check := testutil.Checker(t)

	cases := []struct {
		name string
		cfg  *Config
		want func(*testing.T, *Conn)
	}{
		{
			name: "custom_now",
			cfg: &Config{
				Now: func() time.Time {
					return time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)
				},
			},
			want: func(t *testing.T, cn *Conn) {
				for _, sql := range []string{
					`SELECT now()`,
					`SELECT statement_timestamp()`,
					`SELECT transaction_timestamp()`,
					`SELECT clock_timestamp()`,
				} {
					for row := range cn.Query(t.Context(), sql) {
						ts, err := row.Column(0).DecodeTime()
						check(err, nil)
						want := time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)
						if !ts.Equal(want) {
							t.Errorf("%s got %v; want %v", sql, ts, want)
						}
					}
				}
			},
		},
		{
			name: "nil",
			want: func(t *testing.T, cn *Conn) {
				for row := range cn.Query(t.Context(), `SELECT current_setting('polite.go_time', true)`) {
					got, err := row.Column(0).Bytes()
					if err != nil {
						t.Fatal(err)
					}
					if got != nil {
						t.Fatalf("polite.go_time = %q; want unset", got)
					}
				}
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cn, err := Dial(t.Context(), testDSN, tt.cfg)
			check(err, nil)
			defer cn.Close()
			tt.want(t, cn)
		})
	}
}
