package potest

import (
	"io/fs"
	"runtime"
	"testing"
)

func TestDownload(t *testing.T) {
	if testing.Short() {
		t.Skip("network: -short")
	}
	fsys, err := Download(t.Context(), runtime.GOOS, runtime.GOARCH, testVersion)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"bin/initdb", "bin/postgres"} {
		info, err := fs.Stat(fsys, name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if info.Mode()&0o111 == 0 {
			t.Errorf("%s: not executable: %v", name, info.Mode())
		}
	}
}

const testVersion = "18.3.0"
