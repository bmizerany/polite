package febe

import (
	"bytes"
	"context"
	"testing"
)

func BenchmarkReadErrorAllocs(b *testing.B) {
	// TODO(bmizerany):

	buf := new(bytes.Buffer)
	cn := New(buf, buf)
	cn.WriteNotice('E', testNotice)
	cn.Flush()
	ctx := context.Background()

	msg := buf.Bytes()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		*buf = *bytes.NewBuffer(msg)
		_, err := cn.ReadHeader(ctx)
		if err == nil {
			b.Fatal("expected error")
		}
	}
}
