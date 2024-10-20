package pqlite

import (
	"fmt"
	"iter"

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
			var v Value
			switch a := args[i].(type) {
			case nil:
				v.Null = true
			case []byte:
				v.Data = a
			case string:
				// TODO(bmizerany): do not allocate
				v.Data = []byte(a)
			case Value:
				v = a
			default:
				yield(Value{}, fmt.Errorf("unsupported parameter type at %d: %T", i, args[i]))
				return
			}
			if !yield(v, nil) {
				return
			}
		}
	}
}
