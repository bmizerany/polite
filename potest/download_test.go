package potest

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha512"
	"encoding/hex"
	"hash"
	"io/fs"
	"os"
	"strconv"
	"strings"
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

func TestVerifyChecksum(t *testing.T) {
	payload := []byte("embedded postgres jar bytes")
	digest := func(h hash.Hash) string {
		h.Write(payload)
		return hex.EncodeToString(h.Sum(nil))
	}
	sha512Hex := digest(sha512.New())
	sha1Hex := digest(sha1.New())

	for _, c := range []struct {
		name    string
		newHash func() hash.Hash
		data    []byte
		want    string
		wantErr bool
	}{
		{"sha512 match", func() hash.Hash { return sha512.New() }, payload, sha512Hex, false},
		{"sha512 match uppercase", func() hash.Hash { return sha512.New() }, payload, strings.ToUpper(sha512Hex), false},
		{"sha512 match with filename", func() hash.Hash { return sha512.New() }, payload, sha512Hex + "  postgres.jar\n", false},
		{"sha512 mismatch", func() hash.Hash { return sha512.New() }, payload, strings.Repeat("0", len(sha512Hex)), true},
		{"sha512 truncated", func() hash.Hash { return sha512.New() }, payload[:10], sha512Hex, true},
		{"sha1 match", func() hash.Hash { return sha1.New() }, payload, sha1Hex, false},
		{"sha1 mismatch", func() hash.Hash { return sha1.New() }, payload, sha1Hex[:len(sha1Hex)-1] + "f", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := verifyChecksum(c.newHash(), bytes.NewReader(c.data), c.want)
			if (err != nil) != c.wantErr {
				t.Fatalf("verifyChecksum err = %v, wantErr = %v", err, c.wantErr)
			}
		})
	}
}
