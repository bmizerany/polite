package pqlite

import (
	"fmt"
	"iter"
	"time"

	"blake.io/pqlite/febe"
)

type Value = febe.Value

// EncodeParams encodes all args based on their types and appends, in order
// seen, to buf.
//
// The below type conversions are made:
//
// - A string is converted to its []byte equivalent
// - A []byte is appended as-is
// - A time.Time is converted to its binary form
// - A bool is converted to its binary form
// - A constraint.Number is converted to its binary form
// - A float32 or float64 is converted to its binary form
//
// An error is returned if a type is unsupported.
func EncodeParams(args ...any) iter.Seq2[Value, error] {
	return func(yield func(Value, error) bool) {
		for i := range args {
			var p Value
			switch v := args[i].(type) {
			case nil:
				p.Null = true
			case []byte:
				p.Data = v
			case string:
				// TODO(bmizerany): do not allocate
				p.Data = []byte(v)
			case Value:
				p = v
			case time.Time:
				ms := v.Sub(pgEpoch) / time.Millisecond
				p.Data = []byte{
					byte(ms >> 56),
					byte(ms >> 48),
					byte(ms >> 40),
					byte(ms >> 32),
					byte(ms >> 24),
					byte(ms >> 16),
					byte(ms >> 8),
					byte(ms),
				}
			default:
				yield(Value{}, fmt.Errorf("unsupported parameter type at %d: %T", i, args[i]))
				return
			}
			if !yield(p, nil) {
				return
			}
		}
	}
}
