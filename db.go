package pqlite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"strconv"

	"blake.io/pqlite/febe"
)

// Errors
var (
	ErrDone                  = errors.New("pqlite: done")
	ErrOutOfColumns          = errors.New("pqlite: out of columns")
	ErrTLSRefused            = errors.New("pqlite: TLS refused by backend")
	ErrUnsupportedCopyFormat = errors.New("pqlite: unsupported COPY format: must use `... WITH (FORMAT binary)`")
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

	rowsAffected int

	// current row
	row *Row
}

func (c *Conn) Ping(ctx context.Context) error {
	if err := c.Exec(ctx, "SELECT 1"); err != nil {
		return err
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
	for row, err := range c.Query(ctx, sql, args...) {
		if err != nil {
			return err
		}
		return row.Close()
	}
	return nil
}

func (c *Conn) LastResult() Result {
	return Result{RowsAffected: int(c.rowsAffected)}
}

var allBinary = []int16{1}

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

func errRowsSeq(err error) iter.Seq2[*Row, error] {
	return func(yield func(*Row, error) bool) {
		yield(nil, err)
	}
}

func (c *Conn) Query(ctx context.Context, sql string, args ...string) iter.Seq2[*Row, error] {
	return func(yield func(*Row, error) bool) {
		if err := c.prepare(ctx, "", sql, args...); err != nil {
			yield(nil, err)
			return
		}
		if err := c.execAndSync(""); err != nil {
			yield(nil, err)
			return
		}
		var processed int
		ok := true
		for {
			row, err := c.nextRow(ctx, processed)
			processed++
			if err != nil {
				if errors.Is(err, ErrDone) {
					return
				}
				err = c.maybeRecoverFrom(ctx, err)
				yield(nil, err)
				return
			}

			// continue to yield rows until the user says stop, and then
			// discard the remaining rows.
			if ok {
				ok = yield(row, nil)
			} else {
				if err := row.Close(); err != nil {
					yield(nil, err)
					return
				}
			}
			if row.rem != 0 {
				panic(fmt.Sprintf("pqlite: %d unprocessed columns", row.rem))
			}
		}
	}
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

func (c *Conn) nextRow(ctx context.Context, processed int) (*Row, error) {
	for {
		h, err := c.fc.ReadHeader(ctx)
		if err != nil {
			return nil, err
		}
		switch h.Type() {
		case '2':
			// Headeonly; No discard needed
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
	return fmt.Errorf("pqlite: unexpected message type: %q", h.Type())
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
