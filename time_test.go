package polite

import (
	"strings"
	"testing"
	"time"

	"blake.io/polite/internal/testutil"
	"blake.io/polite/oids"
	"kr.dev/diff"
)

func TestRowDecodeTime(t *testing.T) {
	t.Parallel()

	check := testutil.Checker(t)
	cn := newSession(t)
	defer checkReady(t, cn) // ensure we leave in a good state

	ctx := t.Context()
	var got []time.Time
	for row, err := range cn.Query(ctx, `SELECT '2021-01-01 00:00:00'::timestamp`) {
		check(err, nil)
		ts, err := row.Column(0).DecodeTime()
		check(err, nil)
		got = append(got, ts)
	}
	want := []time.Time{time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)}
	diff.Test(t, t.Errorf, got, want)
}

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
			ctx := t.Context()
			want := tt.in.UTC().Round(time.Microsecond)
			var got []time.Time
			for row, err := range cn.Query(ctx, `SELECT $1::timestamp, $2::timestamptz`, tt.in, tt.in) {
				if err != nil {
					t.Fatal(err)
				}
				for i := range 2 {
					ts, err := row.Column(i).DecodeTime()
					if err != nil {
						t.Fatal(err)
					}
					if ts.Location() != time.UTC {
						t.Fatalf("column %d location = %v, want UTC", i, ts.Location())
					}
					got = append(got, ts)
				}
			}
			wantTimes := []time.Time{want, want}
			diff.Test(t, t.Errorf, got, wantTimes)
		})
	}
}

func TestDecodeTimeFromPostgres(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want time.Time
	}{
		{
			name: "timestamp-pg-epoch",
			sql:  `SELECT TIMESTAMP '2000-01-01 00:00:00'`,
			want: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "timestamp-far-future",
			sql:  `SELECT TIMESTAMP '2500-01-02 03:04:05.123457'`,
			want: time.Date(2500, 1, 2, 3, 4, 5, 123457000, time.UTC),
		},
		{
			name: "timestamp-far-future-beyond-unixmicro-limit",
			sql:  `SELECT TIMESTAMP '294260-01-01 00:00:00'`,
			want: time.Date(294260, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "timestamp-far-past",
			sql:  `SELECT TIMESTAMP '1500-01-02 03:04:05.123457'`,
			want: time.Date(1500, 1, 2, 3, 4, 5, 123457000, time.UTC),
		},
		{
			name: "timestamptz-timezone",
			sql:  `SELECT TIMESTAMPTZ '2024-07-08 09:10:11.123457-04'`,
			want: time.Date(2024, 7, 8, 13, 10, 11, 123457000, time.UTC),
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cn := newSession(t)
			ctx := t.Context()
			var got []time.Time
			for row, err := range cn.Query(ctx, tt.sql) {
				if err != nil {
					t.Fatal(err)
				}
				ts, err := row.Column(0).DecodeTime()
				if err != nil {
					t.Fatal(err)
				}
				if ts.Location() != time.UTC {
					t.Fatalf("location = %v, want UTC", ts.Location())
				}
				got = append(got, ts)
			}
			want := []time.Time{tt.want}
			diff.Test(t, t.Errorf, got, want)
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
