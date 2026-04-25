package polite

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"blake.io/polite/dsn"
	"blake.io/polite/febe"
	"blake.io/polite/lru"
	"blake.io/polite/oids"
)

// pgQueryCanceledCode is the SQLSTATE returned by PostgreSQL when a query is
// cancelled via CancelRequest (query_canceled).
const pgQueryCanceledCode = "57014"

// Errors
var (
	ErrTLSRefused     = errors.New("polite: TLS refused by backend")
	ErrCopyFormat     = errors.New("polite: unsupported COPY format: must use `... WITH (FORMAT binary)`")
	ErrCopyFrom       = errors.New("polite: COPY FROM is active; use Exec(ctx, \"\", args...) to send rows")
	ErrBusy           = errors.New("polite: a Query range is active; complete or break it before calling Exec")
	ErrClosed         = errors.New("polite: closed")
	ErrTooManyWaiters = errors.New("polite: pool: too many waiters")
)

var errDone = errors.New("polite: done")

type statement struct {
	name string
	in   febe.ParameterDescriptionResponse
	out  febe.RowDescriptionResponse
}

// Conn is a PostgreSQL connection. Obtain one from [Dial] or [Pool.Get]; the
// zero value is not usable.
//
// Conn is not safe for concurrent use by multiple goroutines.
// Use [Pool] for concurrent access from multiple goroutines.
//
// Conn prepares SQL on first use and caches up to a fixed number of prepared
// statements per connection by exact SQL text, evicting older entries when
// the cache is full. Calls with the same SQL reuse the cached statement.
//
// This statement cache is used by [Conn.Query] and [Conn.Exec].
//
// If the context passed to [Conn.Query] or [Conn.Exec] is canceled or times
// out while a query is in flight, Conn opens a separate connection to the
// backend and sends a CancelRequest authenticated with the BackendKeyData
// captured at startup. The original connection drains the resulting
// ErrorResponse (SQLSTATE 57014) and ReadyForQuery and remains usable. The
// error returned from Query/Exec satisfies errors.Is for the context's error
// ([context.Canceled] or [context.DeadlineExceeded]).
type Conn struct {
	cn net.Conn
	fc *febe.Conn

	now      func() time.Time
	timeZone *time.Location

	// Redial parameters captured at Dial. They allow opening a fresh
	// side connection to issue a CancelRequest using the same network,
	// address, and TLS configuration as the original.
	dial      func(ctx context.Context, network, addr string) (net.Conn, error)
	network   string
	addr      string
	dsnOpts   *dsn.Options
	tlsConfig *tls.Config

	nstmt int
	cache *lru.Cache[string, *statement]

	// current row
	row *Row

	// COPY state
	inCopyFrom           bool
	copyHeaderWritten    bool
	copyOutHeaderPending bool

	// busy is true while a Query iterator is active. It prevents
	// re-entrant Query/Exec calls on the same Conn, which would
	// collide with the in-flight row stream.
	busy bool

	// syncPending is true after a Sync has been flushed to the backend
	// but the matching ReadyForQuery has not yet been consumed. It is the
	// single source of truth for whether the connection needs to be
	// drained before the next query.
	syncPending bool

	// closed is true after the caller invoked Close. It distinguishes a
	// client-initiated Terminate from a backend-initiated one so that
	// ErrClosed is only reported for the former.
	closed bool
}

// flushSync writes a Sync and flushes. On success, the backend owes the
// connection exactly one ReadyForQuery; [Conn.readReadyForQuery] consumes it.
func (c *Conn) flushSync() error {
	c.fc.WriteSync()
	if err := c.fc.Flush(); err != nil {
		return err
	}
	c.syncPending = true
	return nil
}

// readReadyForQuery consumes the pending ReadyForQuery. Callers must have
// already read the 'Z' header; this reads the body.
func (c *Conn) readReadyForQuery() error {
	_, err := c.fc.ReadReadyForQuery()
	c.syncPending = false
	return err
}

// Close sends Terminate, flushes, and closes the underlying connection. It
// closes the connection even if Terminate cannot be sent.
//
// After Close, all Conn methods return [ErrClosed]. A Terminate received
// from the backend surfaces as [febe.ErrTerminated]; transport failures and
// FATAL backend errors surface their own error.
//
// Close returns the error from closing the underlying connection.
func (c *Conn) Close() error {
	c.closed = true
	c.cn.SetDeadline(time.Now().Add(100 * time.Millisecond))
	defer c.cn.Close()
	c.fc.WriteTerminate()
	c.fc.Flush()
	return c.cn.Close()
}

// Idle reports whether the connection is ready for the next query: the
// transport is healthy, no COPY is in progress, and the backend is at
// ReadyForQuery outside any transaction block. It returns false in every
// other state (mid-query, in a transaction, failed transaction, or
// terminal transport error).
func (c *Conn) Idle() bool {
	return c.fc.Err() == nil && !c.busy && !c.inCopyFrom && c.fc.Status().Idle()
}

// Notifications returns an iterator over the [febe.Notification]s received
// on cn. Only channels the connection has subscribed to with LISTEN produce
// notifications; issue LISTEN via [Conn.Exec] before ranging.
//
// Notifications returns only when the caller breaks out of the range loop or
// when the connection becomes terminal (protocol or transport error). No
// error is returned directly; both cases look the same from the caller's
// side. To distinguish them, attach a [febe.Trace] with a ReceivedError
// callback via [febe.WithTrace].
//
// Typical usage with LISTEN:
//
//	cn.Exec(ctx, "LISTEN chat")
//	for n := range polite.Notifications(ctx, cn) {
//		fmt.Printf("%s: %s\n", n.Channel, n.Payload)
//	}
func Notifications(ctx context.Context, cn *Conn) iter.Seq[*febe.Notification] {
	return func(yield func(*febe.Notification) bool) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		ctx = febe.WithTrace(ctx, &febe.Trace{
			ReceivedNotification: func(_ context.Context, n *febe.Notification) {
				if !yield(n) {
					cancel()
				}
			},
		})
		for ctx.Err() == nil {
			if _, err := cn.fc.ReadHeader(ctx); err != nil {
				return
			}
		}
	}
}

// Exec executes a query that is not intended to return rows, such as an
// INSERT, UPDATE, or DELETE. It returns only an error.
//
// When a COPY FROM is in progress, calling Exec with an empty sql string sends
// a single COPY data row encoded from args.
func (c *Conn) Exec(ctx context.Context, sql string, args ...any) error {
	if c.inCopyFrom && sql == "" {
		return c.copyFromRow(args...)
	}
	for row := range c.Query(ctx, sql, args...) {
		if _, err := row.Column(0).Bytes(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) copyFromRow(args ...any) error {
	values := make([][]byte, len(args))
	scratch := make([]byte, 0, len(args)*8)
	for i, arg := range args {
		start := len(scratch)
		encoded, _, err := appendArg(scratch, arg, 0)
		if err != nil {
			return err
		}
		if encoded == nil {
			values[i] = nil
			continue
		}
		scratch = encoded
		values[i] = scratch[start:]
	}
	err := c.fc.WriteCopyDataRow(values, !c.copyHeaderWritten)
	if err != nil {
		return err
	}
	c.copyHeaderWritten = true
	return nil
}

var (
	formatText   int16 = 0
	formatBinary int16 = 1
)

const maxInt64 = uint64(^uint64(0) >> 1)

func formatGoTime(t time.Time) string {
	return t.UTC().Round(time.Microsecond).Format("2006-01-02 15:04:05.999999Z07:00")
}

func appendBinaryArg(dst []byte, arg any) ([]byte, int16, error) {
	dst, err := binary.Append(dst, binary.BigEndian, arg)
	if err != nil {
		return nil, 0, err
	}
	return dst, formatBinary, nil
}

func appendArg(dst []byte, arg any, oid oids.ID) ([]byte, int16, error) {
	switch v := arg.(type) {
	case nil:
		return nil, formatBinary, nil
	// Variable-width values.
	case []byte:
		if v == nil {
			return nil, formatBinary, nil
		}
		return append(dst, v...), formatBinary, nil
	case string:
		return append(dst, v...), formatBinary, nil
	// 2-byte wire values.
	case uint8:
		return appendBinaryArg(dst, int16(v))
	case int8:
		return appendBinaryArg(dst, int16(v))
	case int16:
		return appendBinaryArg(dst, v)
	// 4-byte wire values.
	case uint16:
		return appendBinaryArg(dst, int32(v))
	case int32, float32:
		return appendBinaryArg(dst, v)
	// 8-byte wire values.
	case time.Time:
		// PostgreSQL timestamps are represented at microsecond
		// precision. Round instead of truncate so we choose the
		// nearest representable PostgreSQL timestamp; truncate would
		// always bias earlier than round.
		switch oid {
		case oids.Timestamp:
			v = time.Date(
				v.Year(),
				v.Month(),
				v.Day(),
				v.Hour(),
				v.Minute(),
				v.Second(),
				v.Round(time.Microsecond).Nanosecond(),
				time.UTC,
			)
		case oids.Timestamptz:
			v = v.UTC()
		default:
			return nil, formatBinary, fmt.Errorf("polite: time.Time requires timestamp or timestamptz parameter type; got %s", oid)
		}
		return appendBinaryArg(dst, timeToPGMicroseconds(v))
	case uint32:
		return appendBinaryArg(dst, int64(v))
	case uint:
		u := uint64(v)
		if u > maxInt64 {
			return strconv.AppendUint(dst, u, 10), formatText, nil
		}
		return appendBinaryArg(dst, int64(u))
	case uint64:
		// Encode as NUMERIC text only for values that overflow signed 64-bit.
		if v > maxInt64 {
			return strconv.AppendUint(dst, v, 10), formatText, nil
		}
		return appendBinaryArg(dst, int64(v))
	case int:
		return appendBinaryArg(dst, int64(v))
	case int64, float64:
		return appendBinaryArg(dst, v)
	default:
		return nil, formatBinary, fmt.Errorf("polite: unsupported argument type %T", v)
	}
}

func (c *Conn) updateGoTime(ctx context.Context) error {
	if c.now == nil {
		return nil
	}
	now := c.now
	c.now = nil // prevent infinite recursion during the internal set_config query.
	defer func() { c.now = now }()
	return c.Exec(ctx, "SELECT set_config('polite.go_time', $1, false)", formatGoTime(now()))
}

func (c *Conn) handleParameterStatus(p febe.Parameter) {
	if p.Name != "TimeZone" {
		return
	}
	loc, err := loadLocation(p.Value)
	if err != nil {
		return
	}
	c.timeZone = loc
}

func loadLocation(name string) (*time.Location, error) {
	loc, err := time.LoadLocation(name)
	if err == nil {
		return loc, nil
	}
	if offset, err := loadTimeZoneOffset(name); err == nil {
		return time.FixedZone(name, offset), nil
	}
	return nil, err
}

func loadTimeZoneOffset(name string) (int, error) {
	for _, layout := range timeZoneOffsetLayouts() {
		ts, err := time.Parse(layout, name)
		if err != nil {
			continue
		}
		_, offset := ts.Zone()
		return offset, nil
	}
	return 0, fmt.Errorf("unsupported timezone %q", name)
}

func (c *Conn) initGoTime(ctx context.Context) error {
	for _, sql := range []string{
		"CREATE SCHEMA IF NOT EXISTS polite",
		"CREATE OR REPLACE FUNCTION polite.now() RETURNS timestamptz LANGUAGE sql STABLE AS $$ SELECT current_setting('polite.go_time', false)::timestamptz $$",
		"CREATE OR REPLACE FUNCTION polite.transaction_timestamp() RETURNS timestamptz LANGUAGE sql STABLE AS $$ SELECT current_setting('polite.go_time', false)::timestamptz $$",
		"CREATE OR REPLACE FUNCTION polite.statement_timestamp() RETURNS timestamptz LANGUAGE sql STABLE AS $$ SELECT current_setting('polite.go_time', false)::timestamptz $$",
		"CREATE OR REPLACE FUNCTION polite.clock_timestamp() RETURNS timestamptz LANGUAGE sql VOLATILE AS $$ SELECT current_setting('polite.go_time', false)::timestamptz $$",
		"SELECT set_config('search_path', 'polite,pg_catalog,' || current_setting('search_path'), false)",
	} {
		if err := c.Exec(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) prepare(ctx context.Context, portal, sql string, args ...any) (*statement, error) {
	stmt, err := c.getStmt(ctx, sql)
	if err != nil {
		return nil, err
	}

	bindArgs := make([][]byte, len(args))
	in := make([]int16, len(args))
	scratch := make([]byte, 0, len(args)*8)
	for i, arg := range args {
		start := len(scratch)
		oid := oids.ID(0)
		if i < len(stmt.in.OIDs) {
			oid = stmt.in.OIDs[i]
		}
		encoded, format, err := appendArg(scratch, arg, oid)
		if err != nil {
			return nil, err
		}
		if encoded == nil {
			bindArgs[i] = nil
			in[i] = formatBinary
			continue
		}
		in[i] = format
		scratch = encoded
		bindArgs[i] = scratch[start:]
	}

	fp := &febe.FormatParams{
		In:  in,
		Out: []int16{formatBinary}, // return all columns in binary format
	}
	if err := c.fc.WriteBind(portal, stmt.name, bindArgs, fp); err != nil {
		return nil, err
	}
	return stmt, nil
}

func (c *Conn) newStmtName() string {
	stmt := strconv.Itoa(c.nstmt)
	c.nstmt++
	return stmt
}

// stmtCacheMax bounds the per-connection prepared-statement cache. Server-
// side prepared statements created for evicted entries persist until the
// connection closes.
const stmtCacheMax = 256

func (c *Conn) getStmt(ctx context.Context, sql string) (*statement, error) {
	if c.cache == nil {
		c.cache = lru.New[string, *statement](stmtCacheMax)
	}
	var loadErr error
	stmt := c.cache.Get(sql, func() *statement {
		s, err := c.prepareStmt(ctx, sql)
		if err != nil {
			loadErr = err
			return nil
		}
		return s
	})
	if loadErr != nil {
		c.cache.Delete(sql)
		return nil, loadErr
	}
	return stmt, nil
}

func (c *Conn) prepareStmt(ctx context.Context, sql string) (*statement, error) {
	stmt := &statement{name: c.newStmtName()}

	c.fc.WriteParse(stmt.name, sql)
	c.fc.WriteDescribe('S', stmt.name)
	if err := c.flushSync(); err != nil {
		return nil, err
	}

	for {
		h, err := c.fc.ReadHeader(ctx)
		if err != nil {
			return nil, err
		}
		switch h.Type() {
		case '1':
			continue
		case 't':
			pd, err := c.fc.ReadParameterDescription()
			if err != nil {
				return nil, err
			}
			stmt.in = pd
		case 'T':
			rd, err := c.fc.ReadRowDescription()
			if err != nil {
				return nil, err
			}
			stmt.out = rd
		case 'n':
			// NoData signals the query is guranateed to return
			// no rows. There may be value in using this
			// information, but it is not clearn ATM, since we
			// know we can infer there is no data if there are
			// no columns in the row description since it we
			// an empty row description.
			//
			// We can safely swallow this header-only message
			// for now.
			continue
		case 'Z':
			if err := c.readReadyForQuery(); err != nil {
				return nil, err
			}
			return stmt, nil
		default:
			return nil, fmt.Errorf("prepareStmt: unexpected message type: %q for: %s", h.Type(), sql)
		}
	}
}

// Query executes sql and returns an iterator over its result rows.
// Each yielded [Row] carries either column data or an error. Errors are
// surfaced through [Column] methods: calling [Column.Bytes], [Column.Text],
// [Column.DecodeInt], [Column.DecodeFloat], or [Column.DecodeTime] on a
// column from an error row returns the underlying error. Use
// [Row.Column](0).[Column.Bytes]() as the canonical zero-allocation error
// check.
//
// A Query range is active until the loop ends or the caller breaks. While a
// range is active, further calls to [Conn.Query] or [Conn.Exec] on the same
// Conn return [ErrBusy]; end the range before calling either again.
//
// When COPY FROM enters copy mode, Query yields a [Row] whose columns
// report [ErrCopyFrom].
func (c *Conn) Query(ctx context.Context, sql string, args ...any) iter.Seq[*Row] {
	return func(yield func(*Row) bool) {
		emit := func(r *Row) bool {
			if c.closed && r.err != nil && errors.Is(r.err, febe.ErrTerminated) {
				r.err = ErrClosed
			}
			r.err = c.wrapCtxErr(ctx, r.err)
			return yield(r)
		}
		if c.busy {
			emit(&Row{err: ErrBusy})
			return
		}
		if err := ctx.Err(); err != nil {
			emit(&Row{err: err})
			return
		}
		if c.now != nil {
			if err := c.updateGoTime(ctx); err != nil {
				emit(&Row{err: err})
				return
			}
		}
		c.busy = true
		defer func() { c.busy = false }()

		stopWatcher := c.startCancelWatcher(ctx)
		defer stopWatcher()

		stmt, err := c.prepare(ctx, "", sql, args...)
		if err != nil {
			emit(&Row{err: c.recover(ctx, err)})
			return
		}
		if err := c.execAndSync(""); err != nil {
			emit(&Row{err: c.recover(ctx, err)})
			return
		}
		for {
			row, err := c.nextRow(ctx, stmt.out.Fields)
			if err != nil {
				if errors.Is(err, errDone) {
					return
				}
				emit(&Row{err: c.recover(ctx, err)})
				return
			}
			if row.err != nil { // COPY FROM signal row
				if !emit(row) {
					c.finalizeCopyFrom(ctx)
					return
				}
				if err := c.finalizeCopyFrom(ctx); err != nil {
					emit(&Row{err: err})
				}
				return
			}
			if !emit(row) {
				c.recover(ctx, nil)
				return
			}
		}
	}
}

func (c *Conn) execAndSync(portal string) error {
	c.fc.WriteExecute(portal, 0)
	return c.flushSync()
}

// Row is a view of the current result row.
//
// Use [Row.Column] to read a single column, or [Row.Columns] to range over
// all of them. A row may carry an error instead of data; the error surfaces
// through the [Column] decoders. Columns borrow the row's storage and are
// valid only while that row is current.
type Row struct {
	data    []byte
	columns []Column
	err     error
}

// Columns returns an iterator over the columns of the row. If the row
// carries an error, Columns yields a single [Column] whose decoders all
// return that error.
func (r *Row) Columns() iter.Seq[Column] {
	return func(yield func(Column) bool) {
		if r.err != nil {
			yield(Column{err: r.err})
			return
		}
		for i := range r.columns {
			if !yield(r.Column(i)) {
				return
			}
		}
	}
}

// Column returns the column at index i. If the row carries an error, the
// returned [Column] carries the same error regardless of i. Otherwise
// Column panics if i is out of range.
//
// The returned Column borrows the row's storage and is valid only while
// that row is current.
func (r *Row) Column(i int) Column {
	if r.err != nil {
		return Column{err: r.err}
	}
	return r.columns[i]
}

// Column is one column of the current [Row].
//
// Values arrive in PostgreSQL's binary wire format. [Column.DecodeInt],
// [Column.DecodeFloat], and [Column.DecodeTime] decode that format directly.
// For types without a built-in decoder, [Column.Bytes] returns a view of the
// raw wire bytes, borrowed from the row's storage.
type Column struct {
	ordinal int
	name    string
	size    int
	typeID  oids.ID
	data    []byte

	timeZone *time.Location
	err      error
}

// Ordinal returns the column's zero-based position in the row.
func (c Column) Ordinal() int { return c.ordinal }

// Name returns the column name.
func (c Column) Name() string { return c.name }

// Size returns the column's type width in bytes, as reported by the backend
// from pg_type.typlen. Fixed-width types return their byte width (e.g. 4 for
// int4, 8 for int8). Variable-width types (text, bytea, numeric, all array
// types, and other varlena types) return -1.
func (c Column) Size() int { return c.size }

// Type returns the column's PostgreSQL type OID.
func (c Column) Type() oids.ID { return c.typeID }

// Bytes returns the raw wire bytes. The slice borrows the row's storage and
// is valid only while that row is current.
func (c Column) Bytes() ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.data, nil
}

// Text returns the column bytes as a string. Unlike [Column.Bytes], the
// returned string is a copy and remains valid after the row advances.
func (c Column) Text() (string, error) {
	if c.err != nil {
		return "", c.err
	}
	return string(c.data), nil
}

// DecodeInt decodes the column as a signed integer from PostgreSQL's binary
// wire format. It accepts the four fixed integer widths — 1, 2, 4, or 8
// bytes — and returns the value as int64. Any other width is reported as an
// error rather than guessed at.
func (c Column) DecodeInt() (int64, error) {
	if c.err != nil {
		return 0, c.err
	}
	v := c.data
	// TODO(bmizerany): test coverage for all cases
	switch len(v) {
	case 1:
		return int64(int8(v[0])), nil
	case 2:
		return int64(int16(binary.BigEndian.Uint16(v))), nil
	case 4:
		return int64(int32(binary.BigEndian.Uint32(v))), nil
	case 8:
		return int64(binary.BigEndian.Uint64(v)), nil
	default:
		return 0, fmt.Errorf("DecodeInt: invalid length: %d", len(v))
	}
}

// DecodeFloat decodes the column as an IEEE-754 floating-point value from
// PostgreSQL's binary wire format. It accepts the two fixed widths, 4 bytes
// (float4) and 8 bytes (float8), and returns the value as float64. Any
// other width is reported as an error.
func (c Column) DecodeFloat() (float64, error) {
	if c.err != nil {
		return 0, c.err
	}
	v := c.data
	switch len(v) {
	case 4:
		return float64(math.Float32frombits(binary.BigEndian.Uint32(v))), nil
	case 8:
		return math.Float64frombits(binary.BigEndian.Uint64(v)), nil
	default:
		return 0, fmt.Errorf("DecodeFloat: invalid length: %d", len(v))
	}
}

var (
	pgEpoch     = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	pgEpochUnix = pgEpoch.Unix()
)

var timeZoneOffsetLayoutsList = []string{
	"Z07:00:00",
	"Z07:00",
	"Z0700",
	"Z07",
}

func timeZoneOffsetLayouts() []string { return timeZoneOffsetLayoutsList }

const (
	pgTimestampInfinity    = int64(^uint64(0) >> 1)
	pgTimestampNegInfinity = -pgTimestampInfinity - 1
)

func timeToPGMicroseconds(t time.Time) int64 {
	t = t.UTC().Round(time.Microsecond)
	sec := t.Unix() - pgEpochUnix
	return sec*1_000_000 + int64(t.Nanosecond()/1_000)
}

func pgMicrosecondsToTime(microseconds int64) time.Time {
	sec := microseconds / 1_000_000
	usec := microseconds % 1_000_000
	if usec < 0 {
		sec--
		usec += 1_000_000
	}
	return time.Unix(pgEpochUnix+sec, usec*1_000).UTC()
}

// DecodeTime decodes the column as a microsecond-precision timestamp. A
// timestamptz is returned in UTC; a timestamp is returned in the session's
// time zone, or UTC when the session has none. PostgreSQL's positive and
// negative infinity sentinels are reported as an error rather than mapped
// to a Go time value.
func (c Column) DecodeTime() (time.Time, error) {
	if c.err != nil {
		return time.Time{}, c.err
	}
	switch c.Type() {
	case oids.Timestamp, oids.Timestamptz:
	default:
		return time.Time{}, fmt.Errorf("DecodeTime: unsupported type: %s", c.Type())
	}

	v := c.data
	if len(v) != 8 {
		return time.Time{}, fmt.Errorf("DecodeTime: invalid length: %d", len(v))
	}

	microseconds := int64(binary.BigEndian.Uint64(v))
	switch microseconds {
	case pgTimestampInfinity, pgTimestampNegInfinity:
		return time.Time{}, fmt.Errorf("DecodeTime: infinity is not supported")
	}

	ts := pgMicrosecondsToTime(microseconds)
	if c.Type() == oids.Timestamp && c.timeZone != nil {
		return time.Date(
			ts.Year(),
			ts.Month(),
			ts.Day(),
			ts.Hour(),
			ts.Minute(),
			ts.Second(),
			ts.Nanosecond(),
			c.timeZone,
		), nil
	}
	return ts, nil
}

func (c *Conn) nextRow(ctx context.Context, fields []febe.FieldDescription) (*Row, error) {
	for {
		h, err := c.fc.ReadHeader(ctx)
		if err != nil {
			return nil, err
		}
		switch h.Type() {
		case '1', '2':
			// Header-only; no body to discard.
			continue
		case 'C':
			_, err := c.fc.ReadCommandComplete()
			if err != nil {
				return nil, err
			}
		case 'H':
			// TODO(bmizerany): maybe save NumCols for comparing against 'd' messages?
			m, err := c.fc.ReadCopyOut()
			if err != nil {
				return nil, err
			}
			if m.Format != febe.CopyFormatBinary {
				return nil, ErrCopyFormat
			}
			c.copyOutHeaderPending = true
		case 'G':
			m, err := c.fc.ReadCopyIn()
			if err != nil {
				return nil, err
			}
			if m.Format != febe.CopyFormatBinary {
				if err := c.abortCopyFrom(); err != nil {
					return nil, err
				}
				return nil, ErrCopyFormat
			}
			c.inCopyFrom = true
			c.copyHeaderWritten = false
			return &Row{err: ErrCopyFrom}, nil
		case 'D', 'd':
			discardBinaryHeader := c.copyOutHeaderPending
			c.copyOutHeaderPending = false
			m, err := c.fc.ReadDataRow(discardBinaryHeader)
			if err != nil {
				return nil, err
			}
			if m.NumCols == -1 {
				continue
			}
			if c.row == nil {
				c.row = &Row{}
			}
			columns, err := buildColumns(c.fc, c.row, fields, int(m.NumCols), c.timeZone)
			if err != nil {
				return nil, err
			}
			c.row.columns = columns
			return c.row, nil
		case 'c':
			_, err := c.fc.ReadCopyDone()
			if err != nil {
				return nil, err
			}
		case 'Z':
			if err := c.readReadyForQuery(); err != nil {
				return nil, err
			}
			// TODO(bmizerany): save the transaction status
			return nil, errDone
		default:
			return nil, unexpectedMessage(h)
		}
	}
}

func buildColumns(fc *febe.Conn, row *Row, fields []febe.FieldDescription, numCols int, loc *time.Location) ([]Column, error) {
	var columns []Column
	if row != nil {
		columns = row.columns
	}
	if cap(columns) < numCols {
		columns = make([]Column, numCols)
	} else {
		columns = columns[:numCols]
	}
	data := row.data[:0]
	for i := range numCols {
		v, err := fc.ReadDataRowValue()
		if err != nil {
			return nil, err
		}
		desc := febe.FieldDescription{}
		if i < len(fields) {
			desc = fields[i]
		}
		if v.Data == nil {
			columns[i] = Column{
				ordinal:  i,
				name:     desc.Name,
				size:     int(desc.DataTypeSize),
				typeID:   desc.DataTypeOID,
				timeZone: loc,
			}
			continue
		}
		start := len(data)
		data = append(data, v.Data...)
		columns[i] = Column{
			ordinal:  i,
			name:     desc.Name,
			size:     int(desc.DataTypeSize),
			typeID:   desc.DataTypeOID,
			data:     data[start:],
			timeZone: loc,
		}
	}
	row.data = data
	return columns, nil
}

// abortCopyFrom sends CopyFail + Sync to cancel a COPY IN that we don't
// support (e.g. non-binary format). It leaves [Conn.syncPending] set; the
// caller drains via [Conn.recover].
func (c *Conn) abortCopyFrom() error {
	c.fc.WriteCopyFail(ErrCopyFormat.Error())
	return c.flushSync()
}

func (c *Conn) finalizeCopyFrom(ctx context.Context) error {
	c.inCopyFrom = false
	c.fc.WriteCopyDataRow(nil, !c.copyHeaderWritten)
	c.fc.WriteCopyDone()
	if err := c.flushSync(); err != nil {
		return err
	}
	for {
		h, err := c.fc.ReadHeader(ctx)
		if err != nil {
			// An ErrorResponse (e.g. constraint violation) may
			// arrive instead of CommandComplete. Drain to
			// ReadyForQuery so the connection is usable, then
			// report the original error.
			if drainErr := c.drainToReady(ctx); drainErr != nil {
				return drainErr
			}
			return err
		}
		switch h.Type() {
		case 'C':
			_, err := c.fc.ReadCommandComplete()
			if err != nil {
				return err
			}
		case 'Z':
			return c.readReadyForQuery()
		default:
			return unexpectedMessage(h)
		}
	}
}

func unexpectedMessage(h febe.MessageHeader) error {
	return fmt.Errorf("polite: unexpected message type: %q", h.Type())
}

// recover returns the connection to ReadyForQuery after an error. It drains
// the backend response iff [Conn.syncPending] is set — the single signal for
// whether an unread ReadyForQuery is in flight. Fatal backend errors and
// transport failures skip the drain since no ReadyForQuery will follow.
// Context errors drain with a fresh context so the drain itself is not
// canceled.
//
// initErr is returned on success. If the drain itself errors, that error
// takes precedence.
func (c *Conn) recover(ctx context.Context, initErr error) error {
	if !c.syncPending || c.fc.Err() != nil {
		return initErr
	}
	if fe, ok := errors.AsType[*febe.Error](initErr); ok && fe.IsFatal() {
		return initErr
	}
	drainCtx := ctx
	ctxCancelled := errors.Is(initErr, context.Canceled) || errors.Is(initErr, context.DeadlineExceeded)
	if ctxCancelled {
		drainCtx = context.Background()
	}
	cancelErr, err := c.drainToReadyCapture(drainCtx)
	if err != nil {
		return err
	}
	if ctxCancelled && cancelErr != nil {
		// Replace the bare ctx error with one that also exposes the
		// backend's *febe.Error{Code:57014} via errors.As/AsType.
		return cancelledError{ctxErr: initErr, fe: cancelErr}
	}
	return initErr
}

// wrapCtxErr converts a query_canceled error from PostgreSQL into one that
// satisfies errors.Is(err, ctx.Err()) when ctx is the reason the query was
// cancelled. Other errors pass through unchanged.
func (c *Conn) wrapCtxErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	cerr := ctx.Err()
	if cerr == nil {
		return err
	}
	// If the backend reported query_canceled, wrap the febe.Error so the
	// caller sees ctx.Err() via errors.Is while still being able to extract
	// the underlying *febe.Error.
	if fe, ok := errors.AsType[*febe.Error](err); ok && fe.Code == pgQueryCanceledCode {
		return cancelledError{ctxErr: cerr, fe: err}
	}
	// If it's a context error already, just return ctx's canonical value
	// to keep errors.Is happy.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return cerr
	}
	return err
}

// cancelledError reports a query that was cancelled because ctx was done. It
// satisfies errors.Is for both the context error and the underlying
// *febe.Error.
type cancelledError struct {
	ctxErr error
	fe     error
}

func (e cancelledError) Error() string { return e.fe.Error() }
func (e cancelledError) Unwrap() []error {
	return []error{e.ctxErr, e.fe}
}

// startCancelWatcher spawns a goroutine that issues a CancelRequest on a
// fresh side connection if ctx is done before the in-flight query completes.
// The returned function must be called to stop the watcher (typically via
// defer); it blocks until the watcher exits.
func (c *Conn) startCancelWatcher(ctx context.Context) func() {
	if ctx.Done() == nil {
		return func() {}
	}
	done := make(chan struct{})
	exited := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			once.Do(func() {
				// Best-effort: a failed cancel just means the
				// query keeps running until natural completion.
				c.cancelQuery()
			})
		case <-done:
		}
	}()
	return func() {
		close(done)
		<-exited
	}
}

// cancelQuery opens a fresh connection to the same backend and writes a
// CancelRequest using the BackendKeyData captured at startup. It is
// best-effort: errors are intentionally swallowed because a failed cancel
// only means the query keeps running, which is no worse than not having
// attempted to cancel at all.
func (c *Conn) cancelQuery() {
	pid := c.fc.BackendPID()
	secret := c.fc.BackendSecret()
	if pid == 0 || c.dial == nil {
		return
	}

	// Use a short bounded timeout for the side connection; we do not want
	// the cancel attempt to hang.
	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()

	cn, err := c.dial(dctx, c.network, c.addr)
	if err != nil {
		return
	}
	defer cn.Close()
	cn.SetDeadline(time.Now().Add(5 * time.Second))

	if c.dsnOpts != nil && c.dsnOpts.SSLMode != "disable" {
		upgraded, err := maybeUpgradeToTLS(cn, c.dsnOpts, &Config{TLSConfig: c.tlsConfig})
		if err != nil {
			return
		}
		cn = upgraded
	}
	febe.WriteCancelRequest(cn, pid, secret)
}

// drainToReady reads and discards backend messages until ReadyForQuery,
// tolerating non-fatal ErrorResponses along the way. It assumes an unread
// ReadyForQuery is in flight; call [Conn.recover] for the gated variant.
func (c *Conn) drainToReady(ctx context.Context) error {
	_, err := c.drainToReadyCapture(ctx)
	return err
}

// drainToReadyCapture is like drainToReady but additionally returns the first
// non-fatal *febe.Error observed during the drain (typically the backend's
// query_canceled response). The returned error is informational; the drain
// itself succeeds when ReadyForQuery is consumed.
func (c *Conn) drainToReadyCapture(ctx context.Context) (error, error) {
	var captured error
	for {
		h, err := c.fc.ReadHeader(ctx)
		if err != nil {
			// Non-fatal ErrorResponses are expected during
			// recovery (e.g. Postgres echoing a CopyFail or
			// reporting query_canceled after a CancelRequest);
			// keep draining until ReadyForQuery. Fatal errors
			// mean the backend closed the connection and no
			// ReadyForQuery will follow.
			if fe, ok := errors.AsType[*febe.Error](err); ok && !fe.IsFatal() {
				if captured == nil {
					captured = err
				}
				continue
			}
			return captured, err
		}
		if h.Type() == 'Z' {
			return captured, c.readReadyForQuery()
		}
	}
}
