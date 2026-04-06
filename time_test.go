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
			loc := currentTimeZone(t, cn)
			got := decodeQueryTimes(t, cn, `SELECT $1::timestamp, $2::timestamptz`, tt.in, tt.in)
			assertTime(t, got[0], wantTimestampInLocation(loc, tt.in))
			assertTime(t, got[1], wantTimestamptz(tt.in))
		})
	}
}

func TestDecodeTimeTracksSessionTimeZone(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	startLoc := currentTimeZone(t, cn)
	start := time.Date(2024, 1, 2, 3, 4, 5, 123456789, startLoc)
	assertTime(t, decodeQueryTimes(t, cn, `SELECT $1::timestamp`, start)[0], wantTimestampInLocation(startLoc, start))

	if err := cn.Exec(t.Context(), `SET TimeZone TO 'America/Los_Angeles'`); err != nil {
		t.Fatal(err)
	}

	la, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}

	other := time.Date(2024, 7, 8, 9, 10, 11, 123456789, time.UTC)
	assertTime(t, decodeQueryTimes(t, cn, `SELECT $1::timestamp`, other)[0], wantTimestampInLocation(la, other))

	same := time.Date(2024, 7, 8, 9, 10, 11, 123456789, la)
	assertTime(t, decodeQueryTimes(t, cn, `SELECT $1::timestamp`, same)[0], wantTimestampInLocation(la, same))
}

func TestLoadTimeZoneOffset(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		text    string
		want    int
		wantErr string
	}{
		{name: "offset-hour", text: "+02", want: 2 * 60 * 60},
		{name: "offset-hour-negative", text: "-08", want: -8 * 60 * 60},
		{name: "offset-hour-minute", text: "+02:30", want: 2*60*60 + 30*60},
		{name: "offset-hour-minute-negative", text: "-07:30", want: -(7*60*60 + 30*60)},
		{name: "offset-hour-minute-second", text: "+02:30:15", want: 2*60*60 + 30*60 + 15},
		{name: "offset-hour-minute-second-negative", text: "-07:30:15", want: -(7*60*60 + 30*60 + 15)},
		{name: "offset-compact-hour-minute", text: "+0230", want: 2*60*60 + 30*60},
		{name: "offset-compact-hour-minute-negative", text: "-0730", want: -(7*60*60 + 30*60)},
		{name: "offset-compact-hour", text: "+02", want: 2 * 60 * 60},
		{name: "offset-zero", text: "Z", want: 0},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadTimeZoneOffset(tt.text)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("loadTimeZoneOffset(%q) = %d, want %d", tt.text, got, tt.want)
			}
		})
	}
}

func TestTimeZoneOffsetLayouts(t *testing.T) {
	t.Parallel()

	got := timeZoneOffsetLayouts()
	want := []string{"Z07:00:00", "Z07:00", "Z0700", "Z07"}
	diff.Test(t, t.Errorf, got, want)
}

func TestLoadLocation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		text string
	}{
		{name: "iana", text: "America/Los_Angeles"},
		{name: "posix", text: "EST5EDT"},
		{name: "utc", text: "UTC"},
		{name: "gmt", text: "GMT"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			loc, err := loadLocation(tt.text)
			if err != nil {
				t.Fatal(err)
			}
			if loc.String() != tt.text {
				t.Fatalf("loadLocation(%q) = %q, want %q", tt.text, loc.String(), tt.text)
			}
		})
	}
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
			for row, err := range cn.Query(ctx, sql, tt.in) {
				if err != nil {
					t.Fatal(err)
				}
				got := row.Column(0).Bytes()
				if len(got) != 1 || got[0] != 1 {
					t.Fatalf("got %v, want true", got)
				}
			}
		})
	}
}

func TestTimeArgRejectsUnknownAndNonTemporalParameterTypes(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn)

	in := time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.UTC)
	cases := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "text",
			sql:     `SELECT $1`,
			wantErr: "time.Time requires timestamp or timestamptz parameter type; got text",
		},
		{
			name:    "int8",
			sql:     `SELECT $1::int8`,
			wantErr: "time.Time requires timestamp or timestamptz parameter type; got int8",
		},
		{
			name:    "numeric",
			sql:     `SELECT $1::numeric`,
			wantErr: "time.Time requires timestamp or timestamptz parameter type; got numeric",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := cn.Exec(t.Context(), tt.sql, in)
			if err == nil {
				t.Fatalf("Exec(%q) error = nil", tt.sql)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Exec(%q) error = %q, want substring %q", tt.sql, err, tt.wantErr)
			}
		})
	}
}

func TestDecodeTimeFromPostgres(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want func(*time.Location) time.Time
	}{
		{
			name: "timestamp-basic",
			sql:  `SELECT '2021-01-01 00:00:00'::timestamp`,
			want: func(loc *time.Location) time.Time {
				return time.Date(2021, 1, 1, 0, 0, 0, 0, loc)
			},
		},
		{
			name: "timestamp-pg-epoch",
			sql:  `SELECT TIMESTAMP '2000-01-01 00:00:00'`,
			want: func(loc *time.Location) time.Time {
				return time.Date(2000, 1, 1, 0, 0, 0, 0, loc)
			},
		},
		{
			name: "timestamp-far-future",
			sql:  `SELECT TIMESTAMP '2500-01-02 03:04:05.123457'`,
			want: func(loc *time.Location) time.Time {
				return time.Date(2500, 1, 2, 3, 4, 5, 123457000, loc)
			},
		},
		{
			name: "timestamp-far-future-beyond-unixmicro-limit",
			sql:  `SELECT TIMESTAMP '294260-01-01 00:00:00'`,
			want: func(loc *time.Location) time.Time {
				return time.Date(294260, 1, 1, 0, 0, 0, 0, loc)
			},
		},
		{
			name: "timestamp-far-past",
			sql:  `SELECT TIMESTAMP '1500-01-02 03:04:05.123457'`,
			want: func(loc *time.Location) time.Time {
				return time.Date(1500, 1, 2, 3, 4, 5, 123457000, loc)
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

	col := Column{
		typeID: oids.Timestamp,
		data:   []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	}

	got, err := col.DecodeTime()
	if err != nil {
		t.Fatal(err)
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
			for row, err := range cn.Query(ctx, tt.sql) {
				if err != nil {
					t.Fatal(err)
				}
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

	for row, err := range cn.Query(t.Context(), `SHOW TimeZone`) {
		if err != nil {
			t.Fatal(err)
		}
		loc, err := loadLocation(row.Column(0).Text())
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
	for row, err := range cn.Query(t.Context(), sql, args...) {
		if err != nil {
			t.Fatal(err)
		}
		rows++
		got = make([]time.Time, len(row.columns))
		for i := range row.columns {
			ts, err := row.Column(i).DecodeTime()
			if err != nil {
				t.Fatal(err)
			}
			got[i] = ts
		}
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	return got
}

// wantTimestampInLocation mirrors appendArg/DecodeTime semantics for timestamp:
// preserve wall-clock fields and round only the subsecond component to the
// nearest PostgreSQL microsecond.
func wantTimestampInLocation(loc *time.Location, in time.Time) time.Time {
	t := in.Round(time.Microsecond)
	return time.Date(
		in.Year(),
		in.Month(),
		in.Day(),
		in.Hour(),
		in.Minute(),
		in.Second(),
		t.Nanosecond(),
		loc,
	)
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

func TestUseGoTime(t *testing.T) {
	check := testutil.Checker(t)

	cn0, err := Dial(t.Context(), testDSN, &Config{UseGoTime: true})
	check(err, nil)
	defer cn0.Close()

	err = cn0.Exec(t.Context(), `SET polite.go_time = '2024-01-02T03:04:05.123456Z'`)
	check(err, nil)

	cn1, err := Dial(t.Context(), testDSN, &Config{UseGoTime: true})
	check(err, nil)
	defer cn1.Close()

	err = cn1.Exec(t.Context(), `SET polite.go_time = '2025-01-02T03:04:05.123456Z'`)
	check(err, nil)

	checkTime := func(cn *Conn, want time.Time) {
		for _, sql := range []string{
			`SELECT now()`,
			`SELECT statement_timestamp()`,
			`SELECT transaction_timestamp()`,
			`SELECT clock_timestamp()`,
		} {
			for row, err := range cn.Query(t.Context(), sql) {
				check(err, nil)
				ts, err := row.Column(0).DecodeTime()
				check(err, nil)
				if !ts.Equal(want) {
					t.Errorf("%s got %v; want %v", sql, ts, want)
				}
			}
		}
	}

	checkTime(cn0, time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC))
	checkTime(cn1, time.Date(2025, 1, 2, 3, 4, 5, 123456000, time.UTC))
}
