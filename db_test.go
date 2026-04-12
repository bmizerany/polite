package polite

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"iter"
	"math"
	"net"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"blake.io/polite/febe"
	"blake.io/polite/internal/testutil"
	"blake.io/polite/oids"
	"kr.dev/diff"
)

// This is added for tests that need to check error handling. It comes
// standard with all sessions created with newSession.
const (
	sqlCreateRaiseNotice = `
		CREATE OR REPLACE FUNCTION raise_notice() RETURNS integer
		   LANGUAGE plpgsql AS
		$$BEGIN
		   RAISE NOTICE 'boom notice';
		   RETURN 42;
		END;$$;
	`
	sqlCreateRaiseError = `
		CREATE OR REPLACE FUNCTION raise_error() RETURNS integer
		   LANGUAGE plpgsql AS
		$$BEGIN
		   RAISE EXCEPTION 'boom error';
		   RETURN 42;
		END;$$;
	`
)

// createDB creates a new database for the test using its name as the
// prefix. It returns the DSN for the new database.
func createDB(t testing.TB) string {
	t.Helper()

	cn, err := Dial(t.Context(), testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}

	tname := strings.ReplaceAll(t.Name(), "/", "_")
	dbname := fmt.Sprintf("gotest_%s_%s", tname, rand.Text()[:6])
	dbname = strings.ToLower(dbname)
	if err := cn.Exec(t.Context(), "CREATE DATABASE "+dbname); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(testDSN)
	if err == nil && u.Scheme != "" {
		u2 := *u
		u2.Path = "/" + dbname
		dsn := u2.String()
		t.Logf("newConn: dsn: %q", dsn)
		return dsn
	}

	dsn := testDSN + " dbname=" + dbname
	t.Logf("newConn: dsn: %q", dsn)
	return dsn
}

// newSessionDB opens and returns a database connection to mainDSN.
//
// Users that need a clean database should use createDB instead.
func newSessionDB(t testing.TB) *Conn {
	ctx := t.Context()
	dsn := createDB(t)
	if t.Failed() {
		t.FailNow()
	}

	cn, err := Dial(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cn.Close(); err != nil {
			if errors.Is(err, net.ErrClosed) || errors.Is(err, febe.ErrTerminated) {
				return
			}
			t.Fatal(err)
		}
	})
	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	return cn
}

var funcsCreated atomic.Bool

func newSession(t testing.TB) *Conn {
	ctx := t.Context()
	cn, err := Dial(ctx, testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cn.Close(); err != nil {
			if errors.Is(err, net.ErrClosed) || errors.Is(err, febe.ErrTerminated) {
				return
			}
			t.Fatal(err)
		}
	})

	if funcsCreated.Swap(true) == true {
		return cn
	}

	for _, sql := range []string{
		sqlCreateRaiseNotice,
		sqlCreateRaiseError,
	} {
		if err := cn.Exec(t.Context(), sql); err != nil {
			t.Fatal(err)
		}
	}
	return cn
}

func TestQuery(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	for range 3 {
		const q = `SELECT 1, 2, 'three'`
		var got [][]any
		for row, err := range cn.Query(t.Context(), q) {
			if err != nil {
				t.Fatal(err)
			}
			v0, err := row.Column(0).DecodeInt()
			if err != nil {
				t.Fatal(err)
			}
			v1, err := row.Column(1).DecodeInt()
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, []any{
				v0,
				v1,
				row.Column(2).Text(),
			})
		}
		want := [][]any{{int64(1), int64(2), "three"}}
		diff.Test(t, t.Errorf, got, want)
	}
}

func TestQueryAllocations(t *testing.T) {
	cn := newSession(t)
	allocs := testing.AllocsPerRun(1000, func() {
		const q = `SELECT 1, 'two'`
		for row, err := range cn.Query(t.Context(), q) {
			if err != nil {
				t.Fatal(err)
			}
			if _, err := row.Column(0).DecodeInt(); err != nil {
				t.Fatal(err)
			}
			_ = row.Column(1).Text()
		}
	})

	if allocs > 0 {
		t.Errorf("unexpected allocations: %.2f", allocs)
	}
}

func BenchmarkQueryAllocations(b *testing.B) {
	cn := newSession(b)
	ctx := b.Context()

	run := func() {
		const q = `SELECT 1, 'two'`
		for row, err := range cn.Query(ctx, q) {
			if err != nil {
				b.Fatal(err)
			}
			if _, err := row.Column(0).DecodeInt(); err != nil {
				b.Fatal(err)
			}
			_ = row.Column(1).Text()
		}
	}

	run()

	b.ReportAllocs()
	for b.Loop() {
		run()
	}
}

func TestDeadlineIsolation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	c, err := Dial(ctx, testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}

	cancel()

	if err := c.Exec(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("unexpected error after context cancel: %v", err)
	}
}

func TestConnClose(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	if err := cn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cn.Close(); err != nil {
		t.Fatalf("second Close() = %v; want nil", err)
	}

	// Ensure we can't use the connection after closing.
	if err := cn.Exec(t.Context(), "SELECT 1"); !errors.Is(err, febe.ErrTerminated) {
		t.Fatalf("query(SELECT 1) = %v; want %v", err, febe.ErrTerminated)
	}
}

type closeFailConn struct {
	writeErr error
	closeErr error
	closed   bool
}

func (c *closeFailConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *closeFailConn) Write(p []byte) (int, error)      { return len(p), c.writeErr }
func (c *closeFailConn) Close() error                     { c.closed = true; return c.closeErr }
func (c *closeFailConn) LocalAddr() net.Addr              { return dummyAddr("local") }
func (c *closeFailConn) RemoteAddr() net.Addr             { return dummyAddr("remote") }
func (c *closeFailConn) SetDeadline(time.Time) error      { return nil }
func (c *closeFailConn) SetReadDeadline(time.Time) error  { return nil }
func (c *closeFailConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr string

func (a dummyAddr) Network() string { return string(a) }
func (a dummyAddr) String() string  { return string(a) }

func TestConnCloseFlushErrorStillClosesUnderlyingConn(t *testing.T) {
	t.Parallel()

	flushErr := errors.New("flush")
	closeErr := errors.New("close")
	raw := &closeFailConn{
		writeErr: flushErr,
		closeErr: closeErr,
	}
	cn := &Conn{
		cn: raw,
		fc: febe.New(raw, raw),
	}

	err := cn.Close()
	if !errors.Is(err, flushErr) {
		t.Fatalf("Close() = %v; want %v", err, flushErr)
	}
	if !raw.closed {
		t.Fatal("underlying connection was not closed")
	}
}

func TestConnCloseIgnoresNetErrClosed(t *testing.T) {
	t.Parallel()

	raw := &closeFailConn{
		writeErr: net.ErrClosed,
	}
	cn := &Conn{
		cn: raw,
		fc: febe.New(raw, raw),
	}

	if err := cn.Close(); err != nil {
		t.Fatalf("Close() = %v; want nil", err)
	}
	if !raw.closed {
		t.Fatal("underlying connection was not closed")
	}
}

func TestConnCloseIgnoresRawCloseError(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("close")
	raw := &closeFailConn{
		closeErr: closeErr,
	}
	cn := &Conn{
		cn: raw,
		fc: febe.New(raw, raw),
	}

	if err := cn.Close(); err != nil {
		t.Fatalf("Close() = %v; want nil", err)
	}
	if !raw.closed {
		t.Fatal("underlying connection was not closed")
	}
}

func TestRowClose(t *testing.T) {
	t.Parallel()

	const q = `SELECT 1, 2`

	cn := newSession(t)

	t.Run("with remaining values", func(t *testing.T) {
		for _, err := range cn.Query(t.Context(), q) {
			if err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("without remaining values", func(t *testing.T) {
		for _, err := range cn.Query(t.Context(), q) {
			if err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestQueryExtended(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	const q = `
		SELECT $1
		UNION ALL
		SELECT $2
	`
	var got []string
	for row, err := range cn.Query(t.Context(), q, "hello", "world") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, row.Column(0).Text())
	}

	want := []string{"hello", "world"}
	diff.Test(t, t.Errorf, got, want)
}

func TestQueryExtendedBytes(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	var got []string
	for row, err := range cn.Query(t.Context(), "SELECT $1::bytea", []byte{0x12, 0x34, 0x35}) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(row.Column(0).Bytes()))
	}

	diff.Test(t, t.Errorf, got, []string{"\x12\x345"})
}

func TestQueryExtendedNullBytes(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	for row, err := range cn.Query(t.Context(), "SELECT $1::bytea", nil) {
		if err != nil {
			t.Fatal(err)
		}
		if got := row.Column(0).Bytes(); got != nil {
			t.Fatalf("unexpected non-nil got: %v", got)
		}
		return
	}

	t.Fatal("expected 1 row")
}

func TestQueryExtendedUnsupportedType(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	t.Cleanup(func() { checkReady(t, cn) })

	var got []error
	for _, err := range cn.Query(t.Context(), "SELECT $1", true) {
		got = append(got, err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 error; got %d", len(got))
	}
	if got[0] == nil || !strings.Contains(got[0].Error(), "polite:") {
		t.Fatalf("expected polite error; got %v", got[0])
	}
}

func TestQueryExtendedUnsignedOverflow(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	var got string
	for row, err := range cn.Query(t.Context(), "SELECT $1", uint64(math.MaxUint64)) {
		if err != nil {
			t.Fatal(err)
		}
		got = row.Column(0).Text()
	}
	if got != "18446744073709551615" {
		t.Fatalf("got %q want %q", got, "18446744073709551615")
	}
}

func TestQueryExtendedComplex(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	t.Cleanup(func() { checkReady(t, cn) })

	var got []error
	for _, err := range cn.Query(t.Context(), "SELECT $1", complex128(1+2i)) {
		got = append(got, err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 error; got %d", len(got))
	}
	if got[0] == nil || !strings.Contains(got[0].Error(), "polite:") {
		t.Fatalf("expected polite error; got %v", got[0])
	}
}

func collectRows(t testing.TB, rows iter.Seq2[*Row, error]) [][]string {
	var got [][]string
	for row, err := range rows {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, collectValues(row))
	}
	return got
}

func collectValues(row *Row) []string {
	var got []string
	for col := range row.Columns() {
		got = append(got, string(col.Bytes()))
	}
	return got
}

func TestRowColumn(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn)

	t.Run("basic", func(t *testing.T) {
		for row, err := range cn.Query(t.Context(), `SELECT 11::int4, 'two'::text`) {
			if err != nil {
				t.Fatal(err)
			}

			col := row.Column(1)
			if got := col.Text(); got != "two" {
				t.Fatalf("column 1 = %q, want %q", got, "two")
			}

			col = row.Column(0)
			got, err := col.DecodeInt()
			if err != nil {
				t.Fatal(err)
			}
			if got != 11 {
				t.Fatalf("column 0 = %v, want 11", got)
			}

			col = row.Column(0)
			got, err = col.DecodeInt()
			if err != nil {
				t.Fatal(err)
			}
			if got != 11 {
				t.Fatalf("column 0 repeated = %v, want 11", got)
			}
			return
		}
		t.Fatal("expected 1 row")
	})

	t.Run("negative_and_zero", func(t *testing.T) {
		for row, err := range cn.Query(t.Context(), `SELECT -7::int2, 0::int2, -123456::int4, 0::int4, -1234567890123::int8, 0::int8`) {
			if err != nil {
				t.Fatal(err)
			}

			var got []int64
			for col := range row.Columns() {
				n, err := col.DecodeInt()
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, n)
			}

			want := []int64{-7, 0, -123456, 0, -1234567890123, 0}
			diff.Test(t, t.Errorf, got, want)
			return
		}
		t.Fatal("expected 1 signed integer row")
	})
}

func TestRowColumnPanic(t *testing.T) {
	t.Parallel()

	t.Run("column_index", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			index     int
			wantPanic bool
		}{
			{name: "negative_index", index: -1, wantPanic: true},
			{name: "zero", index: 0},
			{name: "past_end", index: 1, wantPanic: true},
		} {
			func() {
				cn := newSession(t)
				if tc.wantPanic {
					defer func() {
						if r := recover(); r == nil {
							t.Fatalf("expected %s panic", tc.name)
						}
					}()
				}

				for row, err := range cn.Query(t.Context(), `SELECT 11::int4`) {
					if err != nil {
						t.Fatal(err)
					}
					_ = row.Column(tc.index)
					if !tc.wantPanic {
						continue
					}
					t.Fatalf("expected %s panic", tc.name)
				}

				if tc.wantPanic {
					t.Fatal("expected 1 row")
				}
			}()
		}
	})
}

func TestZeroColumn(t *testing.T) {
	t.Parallel()

	var col Column
	if got := col.Bytes(); got != nil {
		t.Fatalf("Bytes() = %v; want nil", got)
	}
	if got := col.Name(); got != "" {
		t.Fatalf("Name = %q; want empty", got)
	}
	if got := col.Size(); got != 0 {
		t.Fatalf("Size = %d; want 0", got)
	}
	if got := col.Type(); got != 0 {
		t.Fatalf("Type = %v; want 0", got)
	}
}

func TestQueryAll(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	var got [][]string
	for row, err := range cn.Query(t.Context(), "SELECT * FROM generate_series(1, 5)") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, collectValues(row))
	}
	want := [][]string{
		{"\x00\x00\x00\x01"},
		{"\x00\x00\x00\x02"},
		{"\x00\x00\x00\x03"},
		{"\x00\x00\x00\x04"},
		{"\x00\x00\x00\x05"},
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestQueryBreakInAll(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	t.Cleanup(func() { checkReady(t, cn) })
	var got [][]string
	for row, err := range cn.Query(t.Context(), "SELECT * FROM generate_series(1, 5)") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, collectValues(row))
		break
	}
	want := [][]string{
		{"\x00\x00\x00\x01"},
	}
	diff.Test(t, t.Errorf, got, want)
}

func TestQueryBreakCleanupErrorIsTraced(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cn, err := Dial(ctx, testDSN, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cn.cn.Close()
	})

	startCleanup := make(chan struct{})
	done := make(chan struct{})
	go func() {
		<-startCleanup
		_ = cn.cn.Close()
		close(done)
	}()

	var observed error
	traceCtx := febe.WithTrace(ctx, &febe.Trace{
		ObservedError: func(ctx context.Context, err error) {
			observed = err
		},
	})

	for row, err := range cn.Query(traceCtx, "SELECT * FROM generate_series(1, 100000)") {
		if err != nil {
			t.Fatal(err)
		}
		_ = row
		close(startCleanup)
		break
	}
	<-done

	if observed == nil {
		t.Fatal("expected ObservedError during cleanup")
	}
	var fe *febe.Error
	if errors.As(observed, &fe) {
		t.Fatalf("ObservedError = %T; want non-message cleanup error", observed)
	}

	if err := cn.Exec(ctx, "SELECT 1"); err == nil {
		t.Fatal("expected Exec after cleanup failure to fail")
	}
}

func TestQueryEmptyQueryResponse(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	t.Cleanup(func() {
		checkReady(t, cn)
	})

	ctx := t.Context()

	var got []error
	for _, err := range cn.Query(ctx, "") {
		got = append(got, err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 error; got %d", len(got))
	}
	if !errors.Is(got[0], febe.ErrEmptyQuery) {
		t.Fatalf("expected ErrEmptyQuery; got %v", got[0])
	}
}

func TestQueryExtendedNumbers(t *testing.T) {
	t.Parallel()

	cn := newSession(t)

	const q = `
		SELECT
			encode(int2send($1::int2), 'hex'),
			encode(int2send($2::int2), 'hex'),
			encode(int4send($3::int4), 'hex'),
			encode(int4send($4::int4), 'hex'),
			encode(int8send($5::int8), 'hex'),
			encode(int8send($6::int8), 'hex'),
			encode(int8send($7::int8), 'hex'),
			encode(float4send($8::real), 'hex'),
			encode(float8send($9::double precision), 'hex')
	`

	args := []any{
		int8(-7),
		uint8(200),
		int32(-123456),
		uint16(60000),
		int64(1234567890123),
		int(-1234567890123),
		uint32(math.MaxUint32),
		float32(1.5),
		float64(-2.25),
	}

	var got [][]string
	for row, err := range cn.Query(t.Context(), q, args...) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, collectValues(row))
	}

	want := [][]string{{
		"fff9",
		"00c8",
		"fffe1dc0",
		"0000ea60",
		"0000011f71fb04cb",
		"fffffee08e04fb35",
		"00000000ffffffff",
		"3fc00000",
		"c002000000000000",
	}}
	diff.Test(t, t.Errorf, got, want)
}

func TestEncodeArg(t *testing.T) {
	type customInt int64

	tests := []struct {
		name    string
		dst     []byte
		arg     any
		oid     oids.ID
		wantFmt int16
		want    []byte
		wantErr string
	}{
		{
			name:    "nil arg",
			dst:     []byte("unused"),
			arg:     nil,
			want:    nil,
			wantFmt: formatBinary,
		},
		{
			name:    "nil bytes arg",
			dst:     []byte("unused"),
			arg:     []byte(nil),
			want:    nil,
			wantFmt: formatBinary,
		},
		{
			name:    "byte slice arg",
			dst:     []byte("prefix:"),
			arg:     []byte("hello"),
			want:    []byte{'p', 'r', 'e', 'f', 'i', 'x', ':', 'h', 'e', 'l', 'l', 'o'},
			wantFmt: formatBinary,
		},
		{
			name:    "string arg",
			dst:     []byte("prefix:"),
			arg:     "hello",
			want:    []byte{'p', 'r', 'e', 'f', 'i', 'x', ':', 'h', 'e', 'l', 'l', 'o'},
			wantFmt: formatBinary,
		},
		{
			name:    "int8 arg",
			arg:     int8(-8),
			want:    []byte{0xff, 0xf8},
			wantFmt: formatBinary,
		},
		{
			name:    "uint8 arg",
			arg:     uint8(200),
			want:    []byte{0x00, 0xc8},
			wantFmt: formatBinary,
		},
		{
			name:    "int16 arg",
			arg:     int16(-123),
			want:    []byte{0xff, 0x85},
			wantFmt: formatBinary,
		},
		{
			name:    "uint16 arg",
			arg:     uint16(60000),
			want:    []byte{0x00, 0x00, 0xea, 0x60},
			wantFmt: formatBinary,
		},
		{
			name:    "int32 arg",
			arg:     int32(-123456789),
			want:    []byte{0xf8, 0xa4, 0x32, 0xeb},
			wantFmt: formatBinary,
		},
		{
			name:    "float32 arg",
			arg:     float32(1.5),
			want:    []byte{0x3f, 0xc0, 0x00, 0x00},
			wantFmt: formatBinary,
		},
		{
			name:    "int64 arg",
			arg:     int64(-7),
			want:    []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf9},
			wantFmt: formatBinary,
		},
		{
			name:    "float64 arg",
			arg:     float64(-2.25),
			want:    []byte{0xc0, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			wantFmt: formatBinary,
		},
		{
			name:    "time arg",
			arg:     time.Date(2024, 2, 3, 4, 5, 6, 987654321, time.FixedZone("UTC+2", 2*60*60)),
			oid:     oids.Timestamptz,
			want:    []byte{0x00, 0x02, 0xb3, 0x6f, 0x6d, 0x20, 0x2a, 0x86},
			wantFmt: formatBinary,
		},
		{
			name:    "time arg far-future beyond unixmicro limit",
			arg:     time.Date(294260, 1, 1, 0, 0, 0, 0, time.UTC),
			oid:     oids.Timestamptz,
			want:    []byte{0x7f, 0xfe, 0x17, 0x5f, 0xd1, 0x2b, 0xe0, 0x00},
			wantFmt: formatBinary,
		},
		{
			name:    "time arg unknown oid",
			arg:     time.Date(2024, 2, 3, 4, 5, 6, 987654321, time.UTC),
			wantErr: "time.Time requires timestamp or timestamptz parameter type; got unknown",
			wantFmt: formatBinary,
		},
		{
			name:    "time arg int8 oid",
			arg:     time.Date(2024, 2, 3, 4, 5, 6, 987654321, time.UTC),
			oid:     oids.Numeric,
			wantErr: "time.Time requires timestamp or timestamptz parameter type; got numeric",
			wantFmt: formatBinary,
		},
		{
			name:    "int arg",
			arg:     int(-123456),
			want:    []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0x1d, 0xc0},
			wantFmt: formatBinary,
		},
		{
			name:    "uint32 arg",
			arg:     uint32(math.MaxUint32),
			want:    []byte{0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff},
			wantFmt: formatBinary,
		},
		{
			name:    "uint arg",
			arg:     uint(42),
			want:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a},
			wantFmt: formatBinary,
		},
		{
			name:    "uint64 arg",
			arg:     uint64(42),
			want:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a},
			wantFmt: formatBinary,
		},
		{
			name:    "unsupported arg type bool",
			arg:     true,
			wantErr: "polite: unsupported argument type bool",
			wantFmt: formatBinary,
		},
		{
			name:    "unsupported alias int",
			arg:     customInt(7),
			wantErr: "polite: unsupported argument type",
			wantFmt: formatBinary,
		},
		{
			name:    "unsigned overflow",
			arg:     uint64(math.MaxUint64),
			want:    []byte("18446744073709551615"),
			wantFmt: formatText,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			got, gotFmt, err := appendArg(tt.dst, tt.arg, tt.oid, nil)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("got error %v", err)
				}
				if !bytes.Equal(got, tt.want) {
					t.Fatalf("got=%v want=%v", got, tt.want)
				}
				if gotFmt != tt.wantFmt {
					t.Fatalf("format got=%v want=%v", gotFmt, tt.wantFmt)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got err %v want substring %q", err, tt.wantErr)
			}
			if tt.want != nil {
				t.Fatalf("expected no successful value; got %v", got)
			}
		})
	}
}

func TestAppendArgUintOverflow(t *testing.T) {
	t.Parallel()

	u := ^uint(0)
	if uint64(u) <= maxInt64 {
		t.Skip("uint does not overflow signed 64-bit on this platform")
	}

	got, gotFmt, err := appendArg(nil, u, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if gotFmt != formatText {
		t.Fatalf("format got=%v want=%v", gotFmt, formatText)
	}
	want := fmt.Appendf(nil, "%d", uint64(u))
	if !bytes.Equal(got, want) {
		t.Fatalf("got=%q want=%q", got, want)
	}
}

func TestQueryExtendedMixedArguments(t *testing.T) {
	t.Parallel()

	check := testutil.Checker(t)
	cn := newSession(t)

	in := time.Date(2026, 1, 2, 3, 4, 5, 987654321, time.FixedZone("UTC+3", 3*60*60))
	wantTime := wantTimestamptz(in)

	const q = `
		SELECT
			$1::text,
			$2::bytea,
			$3::timestamptz,
			$4::int8,
			$5::double precision
	`

	var gotText string
	var gotBytes []byte
	var gotTime time.Time
	var gotInt int64
	var gotFloat []byte
	for row, err := range cn.Query(t.Context(), q, "hello", []byte("world"), in, uint(42), float64(3.5)) {
		check(err, nil)
		gotText = row.Column(0).Text()
		gotBytes = row.Column(1).Bytes()
		gotTime, err = row.Column(2).DecodeTime()
		check(err, nil)
		gotInt, err = row.Column(3).DecodeInt()
		check(err, nil)
		gotFloat = row.Column(4).Bytes()
	}

	diff.Test(t, t.Errorf, gotText, "hello")
	diff.Test(t, t.Errorf, string(gotBytes), "world")
	diff.Test(t, t.Errorf, gotTime, wantTime)
	diff.Test(t, t.Errorf, gotInt, int64(42))
	diff.Test(t, t.Errorf, gotFloat, []byte{0x40, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
}

func TestExecLimit1(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	var got []byte
	ctx := febe.WithTrace(t.Context(), &febe.Trace{
		ReceivedHeader: func(ctx context.Context, h febe.MessageHeader) {
			got = append(got, h.Type())
		},
	})
	if err := cn.Exec(ctx, "SELECT generate_series(1, 3)"); err != nil {
		t.Fatal(err)
	}

	want := []byte("1tTZ2DDDCZ")
	diff.Test(t, t.Errorf, got, want)
}

func TestExec(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	if err := cn.Exec(t.Context(), "CREATE TEMP TABLE foo (bar INT)"); err != nil {
		t.Fatal(err)
	}
	if err := cn.Exec(t.Context(), "INSERT INTO foo VALUES (1)"); err != nil {
		t.Fatal(err)
	}
}

func TestExecExtended(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	if err := cn.Exec(t.Context(), "CREATE TEMP TABLE foo_any (name TEXT, payload BYTEA, n BIGINT)"); err != nil {
		t.Fatal(err)
	}
	if err := cn.Exec(t.Context(), "INSERT INTO foo_any VALUES ($1, $2, $3)", "hello", []byte("world"), uint32(7)); err != nil {
		t.Fatal(err)
	}

	got := collectRows(t, cn.Query(t.Context(), "SELECT name, payload, n FROM foo_any"))
	diff.Test(t, t.Errorf, got, [][]string{{"hello", "world", "\x00\x00\x00\x00\x00\x00\x00\a"}})
}

func TestErrors(t *testing.T) {
	ctx := t.Context()
	t.Run("prepare error", func(t *testing.T) {
		t.Parallel()
		cn := newSession(t)
		err := cn.Exec(ctx, "SELECT !")
		if err == nil {
			t.Fatal("expected error")
		}
		var fe *febe.Error
		if !errors.As(err, &fe) {
			t.Fatalf("got %T, want *febe.Error", err)
		}
		if !strings.Contains(fe.Message, "syntax error") {
			t.Fatalf("got %v, want syntax error", err)
		}

		// ensure we recover
		if err := cn.Exec(ctx, "SELECT 1"); err != nil {
			t.Fatalf("SELECT 1 failed: %v", err)
		}
	})

	t.Run("exec error", func(t *testing.T) {
		t.Parallel()
		cn := newSession(t)
		err := cn.Exec(ctx, "SELECT raise_error()")
		if err == nil {
			t.Fatal("expected error")
		}
		var fe *febe.Error
		if !errors.As(err, &fe) {
			t.Fatalf("got %T, want *febe.Error", err)
		}
		if !strings.Contains(fe.Message, "boom error") {
			t.Fatalf("got %v, want boom", err)
		}

		// ensure we recover
		if err := cn.Exec(ctx, "SELECT 1"); err != nil {
			t.Fatalf("SELECT 1 failed: %v", err)
		}
	})

	t.Run("terminal error is sticky", func(t *testing.T) {
		ctx := t.Context()
		cn, err := Dial(ctx, testDSN, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := cn.cn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Fatal(err)
			}
		})

		terminal := cn.Exec(ctx, "SELECT pg_terminate_backend(pg_backend_pid())")
		if terminal == nil {
			t.Fatal("expected error")
		}

		sticky := cn.fc.Err()
		if sticky == nil {
			t.Fatal("expected sticky terminal error")
		}
		if !errors.Is(terminal, sticky) {
			t.Fatalf("Query(pg_terminate_backend) = %v; want error matching %v", terminal, sticky)
		}

		err = cn.Exec(ctx, "SELECT 1")
		if !errors.Is(err, sticky) {
			t.Fatalf("Exec(SELECT 1) = %v; want error matching %v", err, sticky)
		}
	})
}

func TestNotice(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn) // ensure we leave in a good state

	notice := func(ctx context.Context) {
		t.Helper()
		if err := cn.Exec(ctx, "SELECT raise_notice()"); err != nil {
			t.Fatal(err)
		}
	}

	ctx := t.Context()

	// no handler
	notice(ctx)
	checkReady(t, cn) // check notice is dropped

	var got []*febe.Notice
	ctx = febe.WithTrace(ctx, &febe.Trace{
		ReceivedNotice: func(ctx context.Context, notice *febe.Notice) {
			got = append(got, notice)
		},
	})

	notice(ctx)
	want := []*febe.Notice{
		{
			Severity:  "NOTICE",
			Verbosity: "NOTICE",
			Message:   "boom notice",
		},
	}
	keep := diff.KeepFields[febe.Notice]("Severity", "Verbosity", "Message")
	diff.Test(t, t.Errorf, got, want, keep)
}

func TestParameterStatus(t *testing.T) {
	t.Parallel()

	cn := newSessionDB(t)
	defer checkReady(t, cn) // ensure we leave in a good state

	var got []febe.Parameter
	ctx := febe.WithTrace(t.Context(), &febe.Trace{
		ReceivedParameterStatus: func(ctx context.Context, p febe.Parameter) {
			got = append(got, p)
		},
	})

	if err := cn.Exec(ctx, "SET datestyle TO postgres, dmy"); err != nil {
		t.Fatal(err)
	}

	checkReady(t, cn) // flush out parameter status if not received by Exec

	diff.Test(t, t.Errorf, got, []febe.Parameter{
		{Name: "DateStyle", Value: "Postgres, DMY"},
	})
}

func TestParameterStatusTraceCompatibility(t *testing.T) {
	t.Parallel()

	cn := newSessionDB(t)
	defer checkReady(t, cn)

	var got int
	ctx := febe.WithTrace(t.Context(), &febe.Trace{
		ReceivedParameterStatus: func(ctx context.Context, p febe.Parameter) {
			got++
		},
	})

	if err := cn.Exec(ctx, "SET datestyle TO postgres, dmy"); err != nil {
		t.Fatal(err)
	}
	checkReady(t, cn)
	if got == 0 {
		t.Fatal("expected ReceivedParameterStatus callback to fire")
	}
}

func TestStatus(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn)

	if got := cn.Status(); got != febe.ReadyStatusIdle {
		t.Fatalf("initial Status() = %q; want %q", got, febe.ReadyStatusIdle)
	}

	if err := cn.Exec(t.Context(), "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if got := cn.Status(); got != febe.ReadyStatusBusy {
		t.Fatalf("after BEGIN Status() = %q; want %q", got, febe.ReadyStatusBusy)
	}

	if err := cn.Exec(t.Context(), "SELECT 1/0"); err == nil {
		t.Fatal("expected error from SELECT 1/0")
	}
	if got := cn.Status(); got != febe.ReadyStatusFailed {
		t.Fatalf("after failed statement Status() = %q; want %q", got, febe.ReadyStatusFailed)
	}

	if err := cn.Exec(t.Context(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if got := cn.Status(); got != febe.ReadyStatusIdle {
		t.Fatalf("after ROLLBACK Status() = %q; want %q", got, febe.ReadyStatusIdle)
	}
}

func TestByteaString(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn) // ensure we leave in a good state

	ctx := t.Context()
	var got []string
	for row, err := range cn.Query(ctx, "SELECT '\x12\x345'::bytea") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, collectValues(row)...)
	}
	want := []string{"\x12\x345"}
	diff.Test(t, t.Errorf, got, want)
}

func TestRowDecodeFloat(t *testing.T) {
	t.Parallel()

	check := testutil.Checker(t)
	cn := newSession(t)
	defer checkReady(t, cn) // ensure we leave in a good state

	ctx := t.Context()
	var got []float64
	for row, err := range cn.Query(ctx, `SELECT 1.5::real, -2.25::double precision`) {
		check(err, nil)
		for col := range row.Columns() {
			f, err := col.DecodeFloat()
			check(err, nil)
			got = append(got, f)
		}
	}
	want := []float64{1.5, -2.25}
	diff.Test(t, t.Errorf, got, want)
}

func TestColumnType(t *testing.T) {
	t.Parallel()

	cn := newSession(t)
	defer checkReady(t, cn)

	ctx := t.Context()
	var got []struct {
		Name string
		Size int
		Type oids.ID
	}
	for row, err := range cn.Query(ctx, `SELECT 'hello'::text AS text, 42::int4 AS int4, now()::timestamptz AS timestamptz`) {
		if err != nil {
			t.Fatal(err)
		}
		for col := range row.Columns() {
			got = append(got, struct {
				Name string
				Size int
				Type oids.ID
			}{
				Name: col.Name(),
				Size: col.Size(),
				Type: col.Type(),
			})
		}
	}

	want := []struct {
		Name string
		Size int
		Type oids.ID
	}{
		{Name: "text", Size: -1, Type: oids.Text},
		{Name: "int4", Size: 4, Type: oids.Int4},
		{Name: "timestamptz", Size: 8, Type: oids.Timestamptz},
	}
	diff.Test(t, t.Errorf, got, want)
}

func checkReady(t *testing.T, c *Conn) {
	t.Helper()
	if t.Failed() {
		return
	}
	if got := c.Status(); got != febe.ReadyStatusIdle {
		t.Fatalf("Status() = %q; want %q", got, febe.ReadyStatusIdle)
	}
	if err := c.Exec(t.Context(), "SELECT 1"); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		t.Fatal(err)
	}
}

var testDSN = cmp.Or(
	os.Getenv("DATABASE_URL"),
	"dbname=postgres host=/tmp port=5411 sslmode=disable user=postgres password=postgres",
)
