package pqlite

import "testing"

// TestContextIsolation tests that Read/Write deadlines are isolated to single
// calls to Open/Exec/Query etc.
func TestDeadlineIsolation(t *testing.T) {
}
