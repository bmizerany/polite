package testutil

import "testing"

type testT struct {
	*testing.T
	n int // number of calls to Fail
}

func (t *testT) Errorf(msg string, args ...interface{}) {
	t.Helper()
	t.n++
	t.T.Logf(msg, args...) // Errors are expected, so just log, don't fail.
}

func TestFatalOnErrorLogger(t *testing.T) {
	tt := &testT{T: t}
	l := SloggerFail(tt)
	l.Error("test")
	if tt.n == 0 {
		t.Fatal("expected test to fail")
	}
}
