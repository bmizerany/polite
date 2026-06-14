// Package potest fetches PostgreSQL binaries from the
// embedded-postgres Maven Central artifacts and exposes them as an
// fs.FS rooted at the install.
package potest

import (
	"archive/tar"
	"archive/zip"
	"context"
	"crypto/sha1"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
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
	url := mavenJarURL(goos, goarch, version)
	jar, err := fetchJar(ctx, url)
	if err != nil {
		root.Close()
		return nil, err
	}
	defer os.Remove(jar.Name())
	defer jar.Close()
	if err := verifyJar(ctx, jar, url); err != nil {
		root.Close()
		return nil, err
	}
	if err := extractTxz(jar, root); err != nil {
		root.Close()
		return nil, err
	}
	return root.FS(), nil
}

// mavenJarURL is the location of the embedded-postgres binary
// distribution on Maven Central. Zonky's artifact names use a
// slightly different vocabulary for arch than Go's GOARCH; the
// translation table is in zonkyArch.
func mavenJarURL(goos, goarch, version string) string {
	artifact := "embedded-postgres-binaries-" + goos + "-" + zonkyArch(goarch)
	return "https://repo1.maven.org/maven2/io/zonky/test/postgres/" +
		artifact + "/" + version + "/" + artifact + "-" + version + ".jar"
}

// zonkyArch translates a Go GOARCH to the suffix Zonky's
// embedded-postgres-binaries artifacts use on Maven Central.
//
// Go     Zonky
// amd64  amd64
// arm64  arm64v8
// arm    arm32v7
// 386    i386
func zonkyArch(goarch string) string {
	switch goarch {
	case "arm64":
		return "arm64v8"
	case "arm":
		return "arm32v7"
	case "386":
		return "i386"
	}
	return goarch
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

// verifyJar checks jar against the strongest checksum sidecar Maven
// publishes alongside the artifact at url. Maven Central serves both
// url+".sha512" and url+".sha1"; sha512 is preferred and sha1 is the
// fallback. A mismatch (including a truncated download) is fatal.
func verifyJar(ctx context.Context, jar *os.File, url string) error {
	for _, c := range []struct {
		ext string
		h   hash.Hash
	}{
		{".sha512", sha512.New()},
		{".sha1", sha1.New()},
	} {
		want, err := fetchSidecar(ctx, url+c.ext)
		if errors.Is(err, errNoSidecar) {
			continue
		}
		if err != nil {
			return err
		}
		if _, err := jar.Seek(0, io.SeekStart); err != nil {
			return err
		}
		return verifyChecksum(c.h, jar, want)
	}
	return fmt.Errorf("potest: %s: no checksum sidecar", url)
}

var errNoSidecar = errors.New("potest: no checksum sidecar")

// fetchSidecar returns the hex digest published at url. A 404 reports
// errNoSidecar so verifyJar can fall back to a weaker algorithm.
func fetchSidecar(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", errNoSidecar
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("potest: %s: %s", url, resp.Status)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// verifyChecksum hashes r through h and reports an error unless the
// result equals the digest in want. want is a Maven sidecar body: the
// hex digest, optionally followed by whitespace and a filename.
func verifyChecksum(h hash.Hash, r io.Reader, want string) error {
	if _, err := io.Copy(h, r); err != nil {
		return err
	}
	got := hex.EncodeToString(h.Sum(nil))
	want, _, _ = strings.Cut(strings.TrimSpace(want), " ")
	if !strings.EqualFold(got, want) {
		return fmt.Errorf("potest: checksum mismatch: got %s, want %s", got, want)
	}
	return nil
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
	entry := getTXZFile(zr)
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

func getTXZFile(zr *zip.Reader) *zip.File {
	for _, f := range zr.File {
		if strings.HasSuffix(f.Name, ".txz") {
			return f
		}
	}
	return nil
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
