package pqlite

import (
	"fmt"
	"iter"
)

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
func EncodeParams(args ...any) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for i := range args {
			var b []byte
			switch v := args[i].(type) {
			case []byte:
				b = v
			case string:
				// TODO(bmizerany): do not allocate
				b = []byte(v)
			default:
				yield(nil, fmt.Errorf("unsupported parameter type at %d: %T", i, args[i]))
				return
			}
			if !yield(b, nil) {
				return
			}
		}
	}
}
