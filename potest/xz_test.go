package potest

import (
	"bytes"
	"encoding/base64"
	"io"
	"path/filepath"
	"testing"

	"golang.org/x/tools/txtar"
)

func TestXZ(t *testing.T) {
	matches, err := filepath.Glob("testdata/*.txtar")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Fatal("no fixtures")
	}
	for _, path := range matches {
		t.Run(filepath.Base(path), func(t *testing.T) {
			arch, err := txtar.ParseFile(path)
			if err != nil {
				t.Fatal(err)
			}
			files := map[string][]byte{}
			for _, f := range arch.Files {
				files[f.Name] = f.Data
			}
			xzBytes, err := base64.StdEncoding.DecodeString(trimNL(files["input.xz.b64"]))
			if err != nil {
				t.Fatalf("decode input: %v", err)
			}
			var want []byte
			if b, ok := files["expected"]; ok {
				want = b
			} else if b, ok := files["expected.b64"]; ok {
				want, err = base64.StdEncoding.DecodeString(trimNL(b))
				if err != nil {
					t.Fatalf("decode expected: %v", err)
				}
			} else {
				t.Fatal("fixture has no expected or expected.b64")
			}

			r, err := newXZReader(bytes.NewReader(xzBytes))
			if err != nil {
				t.Fatalf("newXZReader: %v", err)
			}
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("decompress: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("decompressed bytes mismatch: got %d bytes, want %d", len(got), len(want))
			}
		})
	}
}

func trimNL(b []byte) string { return string(bytes.TrimRight(b, "\n")) }
