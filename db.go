package polite

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net"
	"strconv"
	"time"

	"blake.io/polite/febe"
)

// Errors
var (
	ErrDone                  = errors.New("polite: done")
	ErrOutOfColumns          = errors.New("polite: out of columns")
	ErrTLSRefused            = errors.New("polite: TLS refused by backend")
	ErrUnsupportedCopyFormat = errors.New("polite: unsupported COPY format: must use `... WITH (FORMAT binary)`")
)

// ConstraintViolated reports whether err represents a unique violation on the
// given constraint name.
//
// Example:
//
//	err := db.Exec(ctx, "INSERT INTO users (email) VALUES (?)", email)
//	if ConstraintViolated(err, "users_email_key") {
//	    // handle unique violation on users_email_key
//	}
func ConstraintViolated(err error, name string) bool {
	fe, ok := errors.AsType[*febe.Error](err)
	return ok && fe.Code == "23505" && fe.Constraint == name
}

type statement struct {
	name string
	in   febe.ParameterDescriptionResponse
	out  febe.RowDescriptionResponse
}

// Conn is a PostgreSQL connection.
//
// Conn is not safe for concurrent use by multiple goroutines.
//
// Conn prepares SQL on first use and caches prepared statements per connection
// by exact SQL text. Calls with the same SQL reuse the cached statement.
//
// This statement cache is used by [Conn.Query], [Conn.Exec], [Conn.CopyTo],
// and [Conn.CopyFrom].
//
// If a context is canceled or times out during an in-flight operation, Conn
// sends a cancel request to PostgreSQL. The returned error wraps
// [context.Canceled] or [context.DeadlineExceeded]. Conn returns to
// ReadyForQuery and can be reused.
type Conn struct {
	cn net.Conn
	fc *febe.Conn

	useGoTime bool

	nstmt int
	cache map[string]*statement

	rowsAffected int

	// current row
	row *Row

	closed bool
}

func (c *Conn) Close() error {
	if c.closed {
		return net.ErrClosed
	}

	c.cn.SetDeadline(time.Now().Add(100 * time.Millisecond))
	if err := c.fc.WriteTerminate(); err != nil {
		return err
	}
	if err := c.fc.Flush(); err != nil {
		return err
	}
	c.closed = true
	return c.cn.Close()
}

// Result reports metadata from the most recent completed command.
// Use [Conn.LastResult] to retrieve it.
type Result struct {
	// RowsAffected is the number of rows affected by the command.
	RowsAffected int
}

// Exec executes a query that is not intended to return rows, such as an
// INSERT, UPDATE, or DELETE. It returns only an error.
//
// After Exec returns, call [Conn.LastResult] to get the number of rows
// affected (not the number of rows matched).
//
// Exec uses [Conn.Query] internally, so row draining follows the package
// section "Row Draining".
func (c *Conn) Exec(ctx context.Context, sql string, args ...string) error {
	for row, err := range c.Query(ctx, sql, args...) {
		if err != nil {
			return err
		}
		return row.Close()
	}
	return nil
}

// LastResult returns the result of the most recent completed command on c.
// After [Conn.Exec], this includes the number of rows affected.
func (c *Conn) LastResult() Result {
	return Result{RowsAffected: int(c.rowsAffected)}
}

var allBinary = []int16{1}

func formatGoTime(t time.Time) string {
	return t.UTC().Round(time.Microsecond).Format("2006-01-02 15:04:05.999999Z07:00")
}

func (c *Conn) updateGoTime(ctx context.Context) error {
	if !c.useGoTime {
		return nil
	}
	c.useGoTime = false // prevent infinite recursion during next call to Query
	defer func() { c.useGoTime = true }()
	return c.Exec(ctx, "SELECT set_config('polite.go_time', $1, false) LIMIT 0", formatGoTime(time.Now()))
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

func (c *Conn) prepare(ctx context.Context, portal, sql string, args ...string) (err error) {
	stmt, err := c.getStmt(ctx, sql)
	if err != nil {
		err = c.maybeRecoverFrom(ctx, err)
		return err
	}
	fp := &febe.FormatParams{
		In:  allBinary,
		Out: allBinary,
	}
	return c.fc.WriteBind(portal, stmt.name, args, fp)
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

// Query executes sql and yields rows as an iterator.
//
// Query uses Conn's per-connection prepared statement cache.
//
// See the package section "Row Draining" for iterator cleanup behavior when
// columns are unread or iteration stops early.
func (c *Conn) Query(ctx context.Context, sql string, args ...string) iter.Seq2[*Row, error] {
	return iter.Seq2[*Row, error](func(yield func(*Row, error) bool) {
		if c.useGoTime {
			if err := c.updateGoTime(ctx); err != nil {
				yield(nil, err)
				return
			}
		}
		if err := c.prepare(ctx, "", sql, args...); err != nil {
			yield(nil, err)
			return
		}
		if err := c.execAndSync(""); err != nil {
			yield(nil, err)
			return
		}
		var processed int
		for {
			row, err := c.nextRow(ctx, processed)
			if err != nil {
				if errors.Is(err, ErrDone) {
					return
				}
				err = c.maybeRecoverFrom(ctx, err)
				yield(nil, err)
				return
			}
			processed++
			if !yield(row, nil) {
				for {
					err := c.maybeRecoverFrom(ctx, row.Close())
					if err != nil {
						fmt.Printf("polite: stopped early with error: %v\n", err)
						return
					}
					_, err = c.nextRow(ctx, processed)
					if errors.Is(err, ErrDone) {
						return
					}
					processed++
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

type Row struct {
	c       *Conn
	numCols int
	rem     int
}

// Columns returns the number of columns in a row.
func (r *Row) Columns() int {
	return int(r.numCols)
}

// View returns the raw bytes for the next column in the row.
// It advances the row cursor by one column.
func (r *Row) View() ([]byte, error) {
	if r.rem == 0 {
		return nil, ErrOutOfColumns
	}
	r.rem--
	v, err := r.c.fc.ReadDataRowValue()
	if err != nil {
		return nil, err
	}
	return v.Data, nil
}

// ParseInt reads the next column with [Row.View] and decodes it as a binary
// integer. Supported widths are 1, 2, 4, and 8 bytes.
func (r *Row) ParseInt() (int, error) {
	v, err := r.View()
	if err != nil {
		return 0, err
	}
	// TODO(bmizerany): test coverage for all cases
	switch len(v) {
	case 1:
		return int(v[0]), nil
	case 2:
		return int(v[0])<<8 | int(v[1]), nil
	case 4:
		return int(v[0])<<24 | int(v[1])<<16 | int(v[2])<<8 | int(v[3]), nil
	case 8:
		return int(v[0])<<56 | int(v[1])<<48 | int(v[2])<<40 | int(v[3])<<32 | int(v[4])<<24 | int(v[5])<<16 | int(v[6])<<8 | int(v[7]), nil
	default:
		return 0, fmt.Errorf("ParseInt: invalid length: %d", len(v))
	}
}

var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// ParseTime reads the next column with [Row.ParseInt] and decodes it as a
// PostgreSQL timestamp-without-timezone value.
func (r *Row) ParseTime() (time.Time, error) {
	microseconds, err := r.ParseInt()
	if err != nil {
		return time.Time{}, err
	}
	return pgEpoch.Add(time.Duration(microseconds) * time.Microsecond), nil
}

// ParseString reads the next column with [Row.View] and returns it as a string.
func (r *Row) ParseString() (string, error) {
	v, err := r.View()
	if err != nil {
		return "", err
	}
	return string(v), nil
}

// Close discards any remaining columns in the row.
//
// Query and Copy iterators drain unread row columns automatically when
// iteration advances or stops. See the package section "Row Draining".
//
// Call Close when you want to force that drain immediately and observe its
// error at the call site.
func (r *Row) Close() error {
	for {
		_, err := r.View()
		if err != nil {
			if errors.Is(err, ErrOutOfColumns) {
				return nil
			}
			return err
		}
	}
}

func (c *Conn) nextRow(ctx context.Context, processed int) (*Row, error) {
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
			m, err := c.fc.ReadCommandComplete()
			if err != nil {
				return nil, err
			}
			c.rowsAffected = int(m.RowsAffected)
		case 'H':
			// TODO(bmizerany): maybe save NumCols for comparing against 'd' messages?
			m, err := c.fc.ReadCopyOut()
			if err != nil {
				return nil, err
			}
			if m.Format != febe.CopyFormatBinary {
				return nil, ErrUnsupportedCopyFormat
			}
		case 'D', 'd':
			discardBinaryHeader := processed == 0 && h.Type() == 'd'
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
			*c.row = Row{
				c:       c,
				numCols: int(m.NumCols),
				rem:     int(m.NumCols),
			}
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

func unexpectedMessage(h febe.MessageHeader) error {
	return fmt.Errorf("polite: unexpected message type: %q", h.Type())
}

var errsWeCanRecoverFrom = errors.Join(
	ErrUnsupportedCopyFormat,
	ErrOutOfColumns,
	febe.ErrEmptyQuery,
)

func (c *Conn) maybeRecoverFrom(ctx context.Context, initErr error) error {
	if initErr == nil {
		return nil
	}
	// TODO(bmizerany): add "IsRecovering" to trace
	var fe *febe.Error
	if errors.As(initErr, &fe) || errors.Is(errsWeCanRecoverFrom, initErr) {
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
			return err
		}
		if h.Type() == 'Z' {
			return nil
		}
	}
}
