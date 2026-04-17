package polite

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math"
	"net"
	"strconv"
	"time"

	"blake.io/polite/febe"
	"blake.io/polite/oids"
)

// Errors
var (
	ErrDone                  = errors.New("polite: done")
	ErrTLSRefused            = errors.New("polite: TLS refused by backend")
	ErrUnsupportedCopyFormat = errors.New("polite: unsupported COPY format: must use `... WITH (FORMAT binary)`")
	ErrCopyFrom              = errors.New("polite: COPY FROM is active; use Exec(ctx, \"\", args...) to send rows")
)

type statement struct {
	name string
	in   febe.ParameterDescriptionResponse
	out  febe.RowDescriptionResponse
}

// Conn is a PostgreSQL connection.
//
// Conn is not safe for concurrent use by multiple goroutines.
// Use [Pool] for concurrent access from multiple goroutines.
//
// Conn prepares SQL on first use and caches prepared statements per connection
// by exact SQL text. Calls with the same SQL reuse the cached statement.
//
// This statement cache is used by [Conn.Query] and [Conn.Exec].
//
// If a context is canceled or times out during an in-flight operation, Conn
// sends a cancel request to PostgreSQL. The returned error wraps
// [context.Canceled] or [context.DeadlineExceeded]. Conn returns to
// ReadyForQuery and can be reused.
//
// Use [Conn.Status] to inspect the backend's most recently
// observed ReadyForQuery transaction state.
type Conn struct {
	cn net.Conn
	fc *febe.Conn

	now      func() time.Time
	timeZone *time.Location

	nstmt int
	cache map[string]*statement

	// current row
	row *Row

	// COPY state
	inCopyFrom           bool
	copyHeaderWritten    bool
	copyOutHeaderPending bool
}

// Close sends a Terminate message, flushes it, and then closes the underlying
// connection.
//
// Close returns the error from closing the underlying connection, if any. It
// still attempts to send Terminate and flush it before closing.
func (c *Conn) Close() error {
	c.cn.SetDeadline(time.Now().Add(100 * time.Millisecond))
	defer c.cn.Close()
	c.fc.WriteTerminate()
	c.fc.Flush()
	return c.cn.Close()
}

// idle reports whether the connection is in a clean, reusable state.
func (c *Conn) idle() bool {
	return c.fc.Err() == nil && !c.inCopyFrom && c.Status().Idle()
}

// Notifications blocks and yields each [febe.Notification] received
// on cn. It returns on the first protocol or transport error, or
// when the caller breaks out of the range loop.
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
		encoded, _, err := appendArg(scratch, arg, 0, nil)
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

// Status reports the backend transaction status from the most recent
// ReadyForQuery message.
func (c *Conn) Status() febe.ReadyStatus {
	return c.fc.Status()
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

func appendArg(dst []byte, arg any, oid oids.ID, loc *time.Location) ([]byte, int16, error) {
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
		return nil, c.maybeRecoverFrom(ctx, err)
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
		encoded, format, err := appendArg(scratch, arg, oid, c.timeZone)
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

func (c *Conn) getStmt(ctx context.Context, sql string) (*statement, error) {
	if c.cache == nil {
		c.cache = make(map[string]*statement)
	}

	stmt := c.cache[sql]
	if stmt != nil {
		return stmt, nil
	}

	stmt = &statement{name: c.newStmtName()}

	c.fc.WriteParse(stmt.name, sql)
	c.fc.WriteDescribe('S', stmt.name)
	c.fc.WriteSync()
	if err := c.fc.Flush(); err != nil {
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
			_, err := c.fc.ReadReadyForQuery()
			if err != nil {
				return nil, err
			}
			c.cache[sql] = stmt
			return stmt, nil
		default:
			return nil, fmt.Errorf("getStmt: unexpected message type: %q for: %s", h.Type(), sql)
		}
	}
}

// Query executes sql and returns the sequence of rows returned by the query.
// Each yielded [Row] carries either column data or an error. Errors are
// surfaced through [Column] methods: calling [Column.Bytes], [Column.Text],
// [Column.DecodeInt], [Column.DecodeFloat], or [Column.DecodeTime] on a
// column from an error row returns the underlying error. Use
// [Row.Column](0).[Column.Bytes]() as the canonical zero-allocation error
// check.
//
// When COPY FROM enters copy mode, Query yields a [Row] whose columns
// report [ErrCopyFrom].
func (c *Conn) Query(ctx context.Context, sql string, args ...any) iter.Seq[*Row] {
	return iter.Seq[*Row](func(yield func(*Row) bool) {
		if c.now != nil {
			if err := c.updateGoTime(ctx); err != nil {
				yield(&Row{err: err})
				return
			}
		}
		stmt, err := c.prepare(ctx, "", sql, args...)
		if err != nil {
			yield(&Row{err: err})
			return
		}
		if err := c.execAndSync(""); err != nil {
			yield(&Row{err: err})
			return
		}
		for {
			row, err := c.nextRow(ctx, stmt.out.Fields)
			if err != nil {
				if errors.Is(err, ErrDone) {
					return
				}
				err = c.maybeRecoverFrom(ctx, err)
				yield(&Row{err: err})
				return
			}
			if row != nil && row.err != nil {
				if !yield(row) {
					c.finalizeCopyFrom(ctx)
					return
				}
				if err := c.finalizeCopyFrom(ctx); err != nil {
					yield(&Row{err: err})
				}
				return
			}
			if !yield(row) {
				for {
					_, err := c.nextRow(ctx, stmt.out.Fields)
					if err != nil {
						if !errors.Is(err, ErrDone) {
							c.maybeRecoverFrom(ctx, err)
						}
						return
					}
				}
			}
		}
	})
}

func (c *Conn) execAndSync(portal string) error {
	c.fc.WriteExecute(portal, 0)
	c.fc.WriteSync()
	return c.fc.Flush()
}

// Row is a view of the current result row.
//
// It assumes the caller wrote the query and knows the number of result columns
// ahead of time. Use [Row.Column] to read a specific column by index. For more
// general row processing, use [Row.Columns] to range over all columns, for
// example to map them into a struct or another destination. Some rows carry an
// error instead of column data; the error is surfaced through [Column] methods.
// The returned columns refer to storage owned by the current row and remain
// valid only while this row is current.
type Row struct {
	data    []byte
	columns []Column
	err     error
}

// Columns returns the columns in the row. If the row carries an error,
// Columns yields a single error-bearing [Column].
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

// Column returns the column at index i.
//
// If the row carries an error, Column returns an error-bearing [Column]
// regardless of i. Otherwise it panics if i is out of range.
// The returned Column refers to storage owned by the current row and remains
// valid only while this row is current.
func (r *Row) Column(i int) Column {
	if r.err != nil {
		return Column{err: r.err}
	}
	return r.columns[i]
}

// Column describes one column returned by [Conn.Query].
//
// To obtain a Column, call [Conn.Query] and then [Row.Column].
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

// Name returns the column name from the row description.
func (c Column) Name() string { return c.name }

// Size returns the column width in bytes, or -1 for variable-width types.
func (c Column) Size() int { return c.size }

// Type returns the column type OID from the row description.
func (c Column) Type() oids.ID { return c.typeID }

// Bytes returns the raw wire bytes for the column value.
//
// The returned slice refers to storage owned by the current row and is valid
// only until the next row is determined in the sequence.
func (c Column) Bytes() ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.data, nil
}

// Text returns the column bytes as a string copy.
func (c Column) Text() (string, error) {
	if c.err != nil {
		return "", c.err
	}
	return string(c.data), nil
}

// DecodeInt decodes the column bytes as a PostgreSQL binary signed integer.
// Supported widths are 1, 2, 4, and 8 bytes.
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

// DecodeFloat decodes the column bytes as a PostgreSQL binary float.
// Supported widths are 4 and 8 bytes.
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

// DecodeTime decodes the column bytes as a PostgreSQL timestamp value and
// returns it in UTC for timestamptz or in the column's session timezone for
// timestamp.
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
				return nil, ErrUnsupportedCopyFormat
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
				return nil, ErrUnsupportedCopyFormat
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
			_, err := c.fc.ReadReadyForQuery()
			if err != nil {
				return nil, err
			}
			// TODO(bmizerany): save the transaction status
			return nil, ErrDone
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
// support (e.g. non-binary format). It does not drain the response;
// the caller's recovery path (maybeRecoverFrom) handles that.
func (c *Conn) abortCopyFrom() error {
	c.fc.WriteCopyFail(ErrUnsupportedCopyFormat.Error())
	c.fc.WriteSync()
	return c.fc.Flush()
}

func (c *Conn) finalizeCopyFrom(ctx context.Context) error {
	c.inCopyFrom = false
	c.fc.WriteCopyDataRow(nil, !c.copyHeaderWritten)
	c.fc.WriteCopyDone()
	c.fc.WriteSync()
	if err := c.fc.Flush(); err != nil {
		return err
	}
	for {
		h, err := c.fc.ReadHeader(ctx)
		if err != nil {
			// An ErrorResponse (e.g. constraint violation) may
			// arrive instead of CommandComplete. Drain to
			// ReadyForQuery so the connection is usable, then
			// report the original error.
			if drainErr := c.discardUntilReady(ctx); drainErr != nil {
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
			_, err := c.fc.ReadReadyForQuery()
			return err
		default:
			return unexpectedMessage(h)
		}
	}
}

func unexpectedMessage(h febe.MessageHeader) error {
	return fmt.Errorf("polite: unexpected message type: %q", h.Type())
}

var recoverableQueryErrors = errors.Join(
	ErrUnsupportedCopyFormat,
	febe.ErrEmptyQuery,
)

// maybeRecoverFrom drains a query to ReadyForQuery when the error is one of
// the query errors polite can recover from. It does not decide whether the
// connection itself is terminal.
func (c *Conn) maybeRecoverFrom(ctx context.Context, initErr error) error {
	if errors.Is(initErr, context.Canceled) || errors.Is(initErr, context.DeadlineExceeded) {
		return cmp.Or(c.discardUntilReady(context.Background()), initErr)
	}

	// FATAL and PANIC mean the backend closed the connection;
	// no ReadyForQuery will follow, so skip recovery.
	if fe, ok := errors.AsType[*febe.Error](initErr); ok && fe.IsFatal() {
		return initErr
	}

	// TODO(bmizerany): add "IsRecovering" to trace
	_, isFE := errors.AsType[*febe.Error](initErr)
	if isFE || errors.Is(recoverableQueryErrors, initErr) {
		if err := c.discardUntilReady(ctx); err != nil {
			return err
		}
	}
	return initErr
}

func (c *Conn) discardUntilReady(ctx context.Context) error {
	for {
		h, err := c.fc.ReadHeader(ctx)
		if err != nil {
			// Non-fatal ErrorResponses are expected during
			// recovery (e.g. Postgres echoing a CopyFail);
			// keep draining until ReadyForQuery. Fatal errors
			// mean the backend closed the connection and no
			// ReadyForQuery will follow.
			if fe, ok := errors.AsType[*febe.Error](err); ok && !fe.IsFatal() {
				continue
			}
			return err
		}
		if h.Type() == 'Z' {
			_, err := c.fc.ReadReadyForQuery()
			return err
		}
	}
}
