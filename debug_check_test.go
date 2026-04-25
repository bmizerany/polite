package polite
import (
  "testing"
  "time"
)
func TestDebugDirectCancel(t *testing.T) {
  cn := dialForCancelTest(t)
  go func() {
    time.Sleep(200 * time.Millisecond)
    cn.cancelQuery()
  }()
  start := time.Now()
  err := cn.Exec(t.Context(), "SELECT pg_sleep(10)")
  t.Logf("dur=%v err=%v", time.Since(start), err)
}
