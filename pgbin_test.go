package polite

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"blake.io/polite/internal/backoff"
	"blake.io/polite/potest"
)

const pgTestVersion = "18.3.0"

var pgBinDir string

func ensurePG() error {
	if os.Getenv("DATABASE_URL") != "" {
		return nil
	}
	install, err := openInstall()
	if err != nil {
		return err
	}
	defer install.Close()
	if !hasBinaries(install) {
		if err := populate(install); err != nil {
			return err
		}
	}
	if os.Getuid() == 0 {
		if err := widenForDroppedRoot(install); err != nil {
			return err
		}
	}
	pgBinDir = filepath.Join(install.Name(), "bin")
	return ensureServer(install)
}

// openInstall opens an *os.Root over the cache directory where the
// PostgreSQL install lives, creating the dir if necessary.
func openInstall() (*os.Root, error) {
	cache, err := os.UserCacheDir()
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(cache, "postgres",
		runtime.GOOS+"-"+runtime.GOARCH+"-"+pgTestVersion)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return os.OpenRoot(dir)
}

func hasBinaries(install *os.Root) bool {
	for _, name := range []string{"bin/initdb", "bin/postgres"} {
		info, err := install.Stat(name)
		if err != nil || info.Mode()&0o111 == 0 {
			return false
		}
	}
	return true
}

func populate(install *os.Root) error {
	fsys, err := potest.Download(context.Background(),
		runtime.GOOS, runtime.GOARCH, pgTestVersion)
	if err != nil {
		return fmt.Errorf("download postgres: %w", err)
	}
	if err := os.CopyFS(install.Name(), fsys); err != nil {
		return fmt.Errorf("copy postgres: %w", err)
	}
	if !hasBinaries(install) {
		return errors.New("download postgres: install missing initdb/postgres")
	}
	return nil
}

// widenForDroppedRoot makes everything under install readable +
// traversable by other users, and widens every parent directory of
// the install just enough that a dropped uid can walk down to it.
// Sticky and world-writable bits on shared dirs (e.g. /tmp) are
// preserved.
func widenForDroppedRoot(install *os.Root) error {
	err := fs.WalkDir(install.FS(), ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		mode := info.Mode().Perm() | 0o004
		if d.IsDir() || info.Mode()&0o100 != 0 {
			mode |= 0o001
		}
		if mode == info.Mode().Perm() {
			return nil
		}
		return install.Chmod(name, mode)
	})
	if err != nil {
		return err
	}
	for p := filepath.Dir(install.Name()); p != "/" && p != ""; p = filepath.Dir(p) {
		info, err := os.Stat(p)
		if err != nil {
			return err
		}
		mode := info.Mode().Perm() | 0o055
		if mode == info.Mode().Perm() {
			break
		}
		if err := os.Chmod(p, mode); err != nil {
			return err
		}
	}
	return nil
}

// dropRoot returns a syscall.Credential pointing at an unprivileged
// account if the current process is root, chowns the entire test-root
// under TMPDIR (parent of dataDir) to that account, and widens
// traversal on every parent. Returns nil when not root.
//
// We can't synthesize a uid out of thin air: initdb calls getpwuid()
// on its effective uid and aborts on lookup failure, so the uid must
// exist in /etc/passwd. We use postgres if present, otherwise nobody.
func dropRoot(dataDir string) (*syscall.Credential, error) {
	if os.Getuid() != 0 {
		return nil, nil
	}
	uid, gid, err := lookupUnprivileged()
	if err != nil {
		return nil, err
	}
	testRoot := stepUpToTemp(dataDir)
	if err := chownTree(testRoot, uid, gid); err != nil {
		return nil, err
	}
	for p := filepath.Dir(testRoot); p != "/" && p != ""; p = filepath.Dir(p) {
		info, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		mode := info.Mode().Perm() | 0o055
		if mode == info.Mode().Perm() {
			break
		}
		if err := os.Chmod(p, mode); err != nil {
			return nil, err
		}
	}
	return &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}, nil
}

func lookupUnprivileged() (uid, gid int, err error) {
	u, err := user.Lookup("postgres")
	if err != nil {
		u, err = user.Lookup("nobody")
	}
	if err != nil {
		return 0, 0, fmt.Errorf("no postgres or nobody user: %w", err)
	}
	uu, err := strconv.ParseUint(u.Uid, 10, 32)
	if err != nil {
		return 0, 0, err
	}
	gg, err := strconv.ParseUint(u.Gid, 10, 32)
	if err != nil {
		return 0, 0, err
	}
	return int(uu), int(gg), nil
}

func stepUpToTemp(dir string) string {
	tmp := os.TempDir()
	for filepath.Dir(dir) != tmp && filepath.Dir(dir) != "/" && filepath.Dir(dir) != "" {
		dir = filepath.Dir(dir)
	}
	return dir
}

func chownTree(dir string, uid, gid int) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return os.Chown(path, uid, gid)
	})
}

func currentUser() string {
	if u, err := user.Current(); err == nil {
		return u.Username
	}
	return "postgres"
}

// ensureServer makes sure a postgres server is ready on
// /tmp/.s.PGSQL.5411 — the socket the default testDSN points at. If
// one is already there it returns without doing anything; otherwise
// it spawns a fresh server backed by the install at root and lets the
// supervisor pattern in postgres_test.go shut it down on test exit.
func ensureServer(root *os.Root) error {
	const socket = "/tmp/.s.PGSQL.5411"
	if c, err := net.Dial("unix", socket); err == nil {
		c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return waitDefaultServerReady(ctx)
	}

	bin := filepath.Join(root.Name(), "bin")
	dataDir, err := os.MkdirTemp("", "polite-pg-")
	if err != nil {
		return err
	}
	cred, err := dropRoot(dataDir)
	if err != nil {
		return err
	}

	init := exec.Command(filepath.Join(bin, "initdb"),
		"-D", dataDir,
		"-U", "postgres",
		"--auth=trust",
		"--encoding=UTF8",
		"--no-locale",
	)
	if cred != nil {
		init.SysProcAttr = &syscall.SysProcAttr{Credential: cred}
	}
	if out, err := init.CombinedOutput(); err != nil {
		return fmt.Errorf("initdb: %w\n%s", err, out)
	}

	cmd := exec.Command(filepath.Join(bin, "postgres"),
		"-D", dataDir,
		"-p", "5411",
		"-c", "listen_addresses=",
		"-c", "unix_socket_directories=/tmp",
	)
	if cred != nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: cred}
	}
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	shutThisDownAfterMyDeath(cmd.Process)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := waitDefaultServerReady(ctx); err != nil {
		cmd.Process.Signal(syscall.SIGQUIT)
		return err
	}
	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		return fmt.Errorf("postgres exited early: %v", cmd.ProcessState)
	}
	return nil
}

func waitDefaultServerReady(ctx context.Context) error {
	var lastErr error
	for _, err := range backoff.Loop(ctx, 500*time.Millisecond) {
		if err != nil {
			if lastErr != nil {
				return fmt.Errorf("postgres did not start: %w", lastErr)
			}
			return errors.New("postgres did not start")
		}
		cn, err := Dial(ctx, testDSN, nil)
		if err == nil {
			cn.Close()
			return nil
		}
		lastErr = err
	}
	return nil
}
