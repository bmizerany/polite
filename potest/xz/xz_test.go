package xz

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"io/fs"
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
			fsys, err := txtar.FS(arch)
			if err != nil {
				t.Fatal(err)
			}
			xzBytes, err := readBase64(fsys, "input.xz.b64")
			if err != nil {
				t.Fatalf("input: %v", err)
			}
			want, err := readExpected(fsys)
			if err != nil {
				t.Fatalf("expected: %v", err)
			}

			r, err := NewReader(bytes.NewReader(xzBytes))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
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

func readBase64(fsys fs.FS, name string) ([]byte, error) {
	b, err := fs.ReadFile(fsys, name)
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(string(bytes.TrimRight(b, "\n")))
}

func readExpected(fsys fs.FS) ([]byte, error) {
	if b, err := fs.ReadFile(fsys, "expected"); err == nil {
		return b, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	return readBase64(fsys, "expected.b64")
}
