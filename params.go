package pqlite

import (
	"fmt"
	"math"
	"time"

	"blake.io/pqlite/febe"
)

type Value = febe.Value

// EncodeParam converts a parameter to a Value, setting [Value.Null] to true
// if the parameter is nil, otherwise encoding the parameter as a byte slice.
//
// https://pkg.go.dev/golang.org/x/exp/constraints
// The each [Value] and its data are value only until the next call to yield.
//
// An error is returned if a type is unsupported.
func EncodeParam(buf []byte, arg any) (Value, error) {
	var n [8]byte
	var p Value
	switch v := arg.(type) {
	case nil:
		p.Null = true
	case Value:
		p = v
	case []byte:
		p.Data = append(buf, v...)
	case string:
		p.Data = append(buf, v...)
	case time.Time:
		ms := v.Sub(pgEpoch) / time.Microsecond
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
		p.Data = putUint8(n[:], v)
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
		return Value{}, fmt.Errorf("unsupported parameter type: %T", arg)
	}
	return p, nil
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
