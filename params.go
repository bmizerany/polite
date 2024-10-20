package pqlite

import (
	"fmt"
	"iter"
	"math"
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
// The each Value and its data are value only until the next call to yield.
//
// An error is returned if a type is unsupported.
func EncodeParams(args ...any) iter.Seq2[Value, error] {
	return func(yield func(Value, error) bool) {
		var n [8]byte
		for i := range args {
			var p Value
			switch v := args[i].(type) {
			case nil:
				p.Null = true
			case Value:
				p = v
			case []byte:
				p.Data = v
			case string:
				// TODO(bmizerany): do not allocate
				p.Data = []byte(v)
			case time.Time:
				ms := v.Sub(pgEpoch) / time.Millisecond
				p.Data = putUint64(n[:], uint64(ms))
			case bool:
				if v {
					p.Data = putUint8(n[:], 1)
				} else {
					p.Data = putUint8(n[:], 0)
				}
			case int8:
				p.Data = putUint8(n[:], uint8(v))
			case uint8:
				n[0] = v
			case int16:
				p.Data = putUint16(n[:], uint16(v))
			case uint16:
				p.Data = putUint16(n[:], v)
			case int32:
				p.Data = putUint32(n[:], uint32(v))
			case uint32:
				p.Data = putUint32(n[:], v)
			case int64:
				p.Data = putUint64(n[:], uint64(v))
			case uint64:
				p.Data = putUint64(n[:], v)
			case float32:
				p.Data = putUint32(n[:], math.Float32bits(v))
			case float64:
				p.Data = putUint64(n[:], math.Float64bits(v))
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

func putUint64(b []byte, v uint64) []byte {
	_ = b[7]
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
	return b[:8]
}

func putUint32(b []byte, v uint32) []byte {
	_ = b[3]
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
	return b[:4]
}

func putUint16(b []byte, v uint16) []byte {
	_ = b[1]
	b[0] = byte(v >> 8)
	b[1] = byte(v)
	return b[:2]
}

func putUint8(b []byte, v uint8) []byte {
	_ = b[0]
	b[0] = v
	return b[:1]
}
