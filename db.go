package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"blake.io/postgres/febe"
	"blake.io/postgres/iter"
	"go4.org/mem"
)

// TODO: https://www.postgresql.org/docs/current/protocol-flow.html
//
// [ ] 55.2.8. Canceling Requests in Progress
// [ ] 55.2.6. COPY Operations
// [ ] 55.2.10. SSL Session Encryption
//
// TODO(bmizerany): QueryOne

// Errors
var (
	ErrProtocol     = errors.New("pqlite: protocol error")
	ErrDone         = errors.New("pqlite: done")
	ErrEmptyQuery   = errors.New("pqlite: empty query")
	ErrOutOfColumns = errors.New("pqlite: out of columns")
)

type Conn struct {
	dsnOrURL string
	cn       io.Closer
	fc       *febe.Conn

	nstmt int
	cache map[string]string // sql -> stmt name

	rowsAffected int64
}

func (c *Conn) Close() error {
	return c.cn.Close()
}

type Result struct {
	RowsAffected int
}

// Exec executes a query that is not intended to return rows, such as an
// INSERT, UPDATE, or DELETE. It returns the number of rows affected (not the
// number of rows matched) in a [Result] or any error encountered.
func (c *Conn) Exec(ctx context.Context, sql string, args ...string) error {
	return c.Query(ctx, sql, args...).Close()
}

func (c *Conn) LastResult() Result {
	return Result{RowsAffected: int(c.rowsAffected)}
}

type Rows iter.Seq2[*Row, error]

func (r Rows) Close() error {
	for row, err := range r {
		if err != nil {
			return err
		}
		if err := row.Close(); err != nil {
			return err
		}
	}
	return nil
}

func Ping(ctx context.Context, cn *Conn) error {
	return cn.Exec(ctx, "SELECT 1")
}

func (c *Conn) Query(ctx context.Context, sql string, args ...string) Rows {
	if err := c.prepare(ctx, "", sql, args...); err != nil {
		return errRows(err)
	}

	// Execute the query.
	if err := c.execAndSync(ctx, ""); err != nil {
		return errRows(err)
	}

	// Read the result.
	return func(yield func(*Row, error) bool) {
		for {
			row, err := c.next(ctx)
			if err != nil {
				if errors.Is(err, ErrDone) {
					return
				}
				yield(nil, err)
				return
			}

			ok := yield(row, nil)
			if row.Unprocessed() > 0 {
				panic(fmt.Sprintf("pqlite: %d unprocessed columns", row.Unprocessed()))
			}
			if !ok {
				return
			}
		}
	}
}

func (c *Conn) next(ctx context.Context) (*Row, error) {
	for {
		h, err := c.fc.ReadHeader(ctx)
		if err != nil {
			return nil, err
		}
		switch h.Type() {
		case '1', '2':
			// Headers only; No discard needed
			continue
		case 'C':
			m, err := c.fc.ReadCommandComplete(ctx)
			if err != nil {
				return nil, err
			}
			c.rowsAffected = m.RowsAffected
		case 'T':
			// Ignore row descriptions for now; they're pretty
			// useless if you know the query you're running,
			// especially when code can be generated from the
			// query, which would already know the types. Save
			// the energy.
			if err := c.fc.DiscardMessage(ctx); err != nil {
				return nil, err
			}
		case 'D':
			m, err := c.fc.ReadDataRow(ctx)
			if err != nil {
				return nil, err
			}
			row := &Row{
				c:       c,
				numCols: int(m.NumCols),
				rem:     int(m.NumCols),
			}
			return row, nil
		case 'I':
			return nil, ErrEmptyQuery
		case 'Z':
			_, err := c.fc.ReadReadyForQuery(ctx)
			if err != nil {
				return nil, err
			}
			// TODO(bmizerany): save the transaction status
			return nil, ErrDone
		default:
			return nil, fmt.Errorf("%w: Rows.next: unexpected message type: %q", ErrProtocol, h.Type())
		}
	}
}

func (c *Conn) prepare(ctx context.Context, portal, sql string, args ...string) (err error) {
	stmt, err := c.getStmt(ctx, sql)
	if err != nil {
		return err
	}
	return c.fc.WriteBind(ctx, portal, stmt, args)
}

func (c *Conn) newStmtName() (string, error) {
	stmt := strconv.Itoa(c.nstmt)
	c.cache[stmt] = stmt
	c.nstmt++
	return stmt, nil
}

func (c *Conn) getStmt(ctx context.Context, sql string) (string, error) {
	if c.cache == nil {
		c.cache = make(map[string]string)
	}
	stmt := c.cache[sql]
	if stmt != "" {
		return stmt, nil
	}
	stmt, err := c.newStmtName()
	if err != nil {
		return "", err
	}
	if err := c.fc.WriteParse(ctx, stmt, sql); err != nil {
		return "", err
	}
	c.cache[sql] = stmt
	return stmt, nil
}

func (c *Conn) execAndSync(ctx context.Context, portal string) error {
	c.fc.WriteExecute(ctx, "", 0)
	c.fc.WriteSync(ctx)
	if err := c.fc.Flush(); err != nil {
		return err
	}
	return nil
}

type Row struct {
	ctx     context.Context
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

func (r *Row) View() (mem.RO, error) {
	if r.rem == 0 {
		return mem.RO{}, ErrOutOfColumns
	}
	r.rem--
	v, err := r.c.fc.ReadDataRowValue(r.ctx)
	if err != nil {
		return mem.RO{}, err
	}
	return mem.B(v.Data), nil
}

func (r *Row) Atoi() (int, error) {
	n, err := r.ParseInt(10, 64)
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

func (r *Row) ParseInt(base int, bitSize int) (int64, error) {
	v, err := r.View()
	if err != nil {
		return 0, err
	}
	return mem.ParseInt(v, base, bitSize)
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

func errRows(err error) Rows {
	return func(yield func(*Row, error) bool) {
		yield(nil, err)
	}
}
