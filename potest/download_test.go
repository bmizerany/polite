package potest

import (
	"io/fs"
	"os"
	"strconv"
	"testing"
)

// TestDownload exercises Download against every (goos, goarch) pair
// the polite tests support. A subtest must exist for each row in CI's
// download matrix; pinning the cases here keeps the matrix and the
// test in sync.
//
// The full test fetches JARs from Maven Central, so it only runs
// when CI=true. Locally, run it explicitly with `CI=1 go test`.
func TestDownload(t *testing.T) {
	if inCI, _ := strconv.ParseBool(os.Getenv("CI")); !inCI {
		t.Skip("only runs when CI is truthy")
	}
	for _, c := range []struct {
		goos, goarch string
	}{
		{"linux", "amd64"},
		{"linux", "arm64"},
		{"darwin", "amd64"},
		{"darwin", "arm64"},
	} {
		t.Run(c.goos+"_"+c.goarch, func(t *testing.T) {
			t.Parallel()
			fsys, err := Download(t.Context(), c.goos, c.goarch, testVersion)
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
		})
	}
}

const testVersion = "18.3.0"
