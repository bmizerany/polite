// Package potest fetches PostgreSQL binaries from the
// embedded-postgres Maven Central artifacts and exposes them as an
// fs.FS rooted at the install.
package potest

import (
	"archive/tar"
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"strings"

	"blake.io/polite/potest/xz"
)

// Download fetches the PostgreSQL binaries published by the
// embedded-postgres project for goos/goarch at the given version,
// extracts them into a fresh temp directory, and returns an fs.FS
// rooted there. The tree contains bin/, lib/, share/, and the rest
// of the embedded postgres distribution.
//
// Callers that want the binaries on disk can pass the returned FS
// (or a sub-FS via fs.Sub) to os.CopyFS.
func Download(ctx context.Context, goos, goarch, version string) (fs.FS, error) {
	dir, err := os.MkdirTemp("", "potest-")
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	jar, err := fetchJar(ctx, mavenJarURL(goos, goarch, version))
	if err != nil {
		root.Close()
		return nil, err
	}
	defer os.Remove(jar.Name())
	defer jar.Close()
	if err := extractTxz(jar, root); err != nil {
		root.Close()
		return nil, err
	}
	return root.FS(), nil
}

// mavenJarURL is the location of the embedded-postgres binary
// distribution on Maven Central.
func mavenJarURL(goos, goarch, version string) string {
	artifact := "embedded-postgres-binaries-" + goos + "-" + goarch
	return "https://repo1.maven.org/maven2/io/zonky/test/postgres/" +
		artifact + "/" + version + "/" + artifact + "-" + version + ".jar"
}

func fetchJar(ctx context.Context, url string) (*os.File, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("potest: %s: %s", url, resp.Status)
	}
	f, err := os.CreateTemp("", "potest-jar-*")
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func extractTxz(jar *os.File, dst *os.Root) error {
	st, err := jar.Stat()
	if err != nil {
		return err
	}
	zr, err := zip.NewReader(jar, st.Size())
	if err != nil {
		return err
	}
	var entry *zip.File
	for _, f := range zr.File {
		if strings.HasSuffix(f.Name, ".txz") {
			entry = f
			break
		}
	}
	if entry == nil {
		return errors.New("potest: jar has no .txz entry")
	}
	rc, err := entry.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	xr, err := xz.NewReader(rc)
	if err != nil {
		return err
	}
	return untar(tar.NewReader(xr), dst)
}

func untar(tr *tar.Reader, dst *os.Root) error {
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(h.Name, "/")
		if name == "" || !fs.ValidPath(name) || strings.Contains(name, `\`) {
			return fmt.Errorf("potest: unsafe path %q", h.Name)
		}
		switch h.Typeflag {
		case tar.TypeDir:
			if err := dst.MkdirAll(name, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if d := path.Dir(name); d != "." {
				if err := dst.MkdirAll(d, 0o755); err != nil {
					return err
				}
			}
			f, err := dst.OpenFile(name,
				os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
				fs.FileMode(h.Mode)&0o777)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if d := path.Dir(name); d != "." {
				if err := dst.MkdirAll(d, 0o755); err != nil {
					return err
				}
			}
			if err := dst.Symlink(h.Linkname, name); err != nil && !errors.Is(err, fs.ErrExist) {
				return err
			}
		}
	}
}
