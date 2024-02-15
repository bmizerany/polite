package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"strconv"

	"blake.io/postgres/febe"
)

// Errors
var (
	ErrDone         = errors.New("pqlite: done")
	ErrOutOfColumns = errors.New("pqlite: out of columns")
	ErrTLSRefused   = errors.New("pqlite: TLS refused by backend")
)

type statement struct {
	name string
	in   febe.ParameterDescriptionResponse
	out  febe.RowDescriptionResponse
}

type Conn struct {
	cn io.Closer
	fc *febe.Conn

	nstmt int
	cache map[string]*statement

	rows *Rows
}

var errPingFailed = errors.New("pqlite: ping failed")

func Ping(ctx context.Context, cn *Conn) error {
	if err := cn.Exec(ctx, "SELECT 1"); err != nil {
		return err
	}
	if cn.LastResult().RowsAffected != 1 {
		return errPingFailed
	}
	return nil
}

func (c *Conn) Close() error {
	// TODO(bmizerany): send Terminate message
	return c.cn.Close()
}

type Result struct {
	RowsAffected int
}

// Exec executes a query that is not intended to return rows, such as an
// INSERT, UPDATE, or DELETE. It returns the number of rows affected (not the
// number of rows matched) in a [Result] or any error encountered.
func (c *Conn) Exec(ctx context.Context, sql string, args ...string) error {
	rows, err := c.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	return rows.Close()
}

func (c *Conn) LastResult() Result {
	if c.rows == nil {
		return Result{}
	}
	return c.rows.Result()
}

func (c *Conn) Query(ctx context.Context, sql string, args ...string) (*Rows, error) {
	if err := c.prepare(ctx, "", sql, args...); err != nil {
		return nil, err
	}
	if err := c.execAndSync(""); err != nil {
		return nil, err
	}
	if c.rows == nil {
		c.rows = &Rows{}
	}
	*c.rows = Rows{ctx: ctx, c: c, row: c.rows.row}
	return c.rows, nil
}

var allBinary = []int16{1}

func (c *Conn) prepare(ctx context.Context, portal, sql string, args ...string) (err error) {
	stmt, err := c.getStmt(ctx, sql)
	if err != nil {
		return err
	}
	fp := &febe.FormatParams{
		In:  allBinary,
		Out: allBinary,
	}
	return c.fc.WriteBind(portal, stmt.name, args, fp)
}

func (c *Conn) newStmtName() (string, error) {
	stmt := strconv.Itoa(c.nstmt)
	c.nstmt++
	return stmt, nil
}

func (c *Conn) getStmt(ctx context.Context, sql string) (*statement, error) {
	if c.cache == nil {
		c.cache = make(map[string]*statement)
	}

	stmt := c.cache[sql]
	if stmt != nil {
		return stmt, nil
	}

	name, err := c.newStmtName()
	if err != nil {
		return nil, err
	}

	stmt = &statement{name: name}

	c.fc.WriteParse(name, sql)
	c.fc.WriteDescribe('S', name)
	c.fc.WriteSync()
	if err := c.fc.Flush(); err != nil {
		return nil, err
	}

	for {
		h, err := c.fc.ReadHeader(ctx)
		if err != nil {
			return nil, c.maybeRecoverFrom(ctx, err)
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
			// TODO(bmizerany): track transaction status
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

// QueryAll is short for:
//
//	rs := c.Query(ctx, sql)
//	defer rs.Close()
//	for row, err := range rs.All {
//	  ...
//	}
//
// Callers must process all columns in a row. If a call to yield does not
// process all columns in the provided rows before returning, QueryAll panics.
//
// If a caller needs to discard any remaining rows, it should call rs.Close
// before returning from yield.
//
// Example:
//
//	for row, err := range c.QueryAll(ctx, "SELECT * FROM users") {
//	  ...
//	}
func (c *Conn) QueryAll(ctx context.Context, sql string, args ...string) iter.Seq2[*Row, error] {
	return func(yield func(*Row, error) bool) {
		rs, err := c.Query(ctx, sql, args...)
		if err != nil {
			yield(nil, err)
			return
		}
		defer rs.Close()
		for row, err := range rs.All {
			if !yield(row, err) {
				return
			}
		}
	}
}

func (c *Conn) execAndSync(portal string) error {
	c.fc.WriteExecute("", 0)
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

// Unprocessed returns the number of columns that have not been
// processed.
func (r *Row) Unprocessed() int {
	return r.rem
}

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

func (r *Row) ParseInt() (int, error) {
	v, err := r.View()
	if err != nil {
		return 0, err
	}
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

func (r *Row) ParseString() (string, error) {
	v, err := r.View()
	if err != nil {
		return "", err
	}
	return string(v), nil
}

// Close discards any remaining columns in the row.
//
// Most users will not need to call this method, since Rows.All will call it
// automatically after each yield.
func (r *Row) Close() error {
	if r.rem == 0 {
		// nothing to discard; carry on
		return nil
	}
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

type Rows struct {
	ctx context.Context
	c   *Conn
	err error

	rowsAffected int64
	processed    int

	row *Row // owned by next()
}

func (rs *Rows) Result() Result {
	return Result{RowsAffected: int(rs.rowsAffected)}
}

func (rs *Rows) All(yield func(*Row, error) bool) {
	if rs.err != nil {
		yield(nil, rs.err)
		return
	}
	for {
		row, err := rs.next()
		if err != nil {
			rs.err = err
			if !errors.Is(err, ErrDone) {
				yield(nil, err)
				return
			}
			return
		}

		ok := yield(row, nil)
		if row.Unprocessed() > 0 {
			// TODO(bmizerany): do not panic; consider 'SELECT *' which will panic if the table schema changes
			panic(fmt.Sprintf("pqlite: %d unprocessed columns", row.Unprocessed()))
		}
		if !ok {
			// panic("TODO")
			return
		}
	}
}

func (rs *Rows) next() (*Row, error) {
	c := rs.c.fc
	for {
		h, err := c.ReadHeader(rs.ctx)
		if err != nil {
			return nil, rs.c.maybeRecoverFrom(rs.ctx, err)
		}
		switch h.Type() {
		case '2':
			// Headers only; No discard needed
			continue
		case 'C':
			m, err := c.ReadCommandComplete()
			if err != nil {
				return nil, err
			}
			rs.rowsAffected = m.RowsAffected
		case 'H':
			// TODO(bmizerany): maybe sage NumCols for comparing against 'd' messages?
			m, err := c.ReadCopyOut()
			if err != nil {
				return nil, err
			}
			if m.Format != febe.CopyFormatBinary {
				if err := rs.c.discardUntilReady(rs.ctx); err != nil {
					return nil, err
				}
				return nil, fmt.Errorf("pqlite: unsupported COPY format (must use '... WITH (FORMAT binary)') %v", m.Format)
			}
		case 'D', 'd':
			if rs.row == nil {
				rs.row = &Row{}
			}
			discardBinaryHeader := rs.processed == 0 && h.Type() == 'd'
			m, err := c.ReadDataRow(discardBinaryHeader)
			if err != nil {
				return nil, err
			}
			if m.NumCols == -1 {
				continue
			}
			rs.processed++
			*rs.row = Row{
				c:       rs.c,
				numCols: int(m.NumCols),
				rem:     int(m.NumCols),
			}
			return rs.row, nil
		case 'c':
			_, err := c.ReadCopyDone()
			if err != nil {
				return nil, err
			}
		case 'Z':
			_, err := c.ReadReadyForQuery()
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

func (rs *Rows) Close() error {
	// TODO(bmizerany): !!!!!!!!!!!!!!!!!!!!!!!!!!!
	// This currently re executes the query!!! FIX
	for row, err := range rs.All {
		if err != nil {
			return err
		}
		if err := row.Close(); err != nil {
			return err
		}
	}
	return nil
}

func unexpectedMessage(h febe.MessageHeader) error {
	return fmt.Errorf("pqlite: unknown message type: %q", h.Type())
}
