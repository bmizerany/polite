package polite

import (
	"context"
	"errors"
	"fmt"
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

func ensurePG() (string, error) {
	bin, err := ensureBinaries()
	if err != nil {
		return "", err
	}
	if os.Getenv("DATABASE_URL") == "" {
		if err := ensureServer(bin); err != nil {
			return "", err
		}
	}
	return bin, nil
}

func ensureBinaries() (string, error) {
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(cache, "postgres",
		runtime.GOOS+"-"+runtime.GOARCH+"-"+pgTestVersion)
	if !hasBinaries(dir) {
		fsys, err := potest.Download(context.Background(),
			runtime.GOOS, runtime.GOARCH, pgTestVersion)
		if err != nil {
			return "", fmt.Errorf("download postgres: %w", err)
		}
		if err := os.CopyFS(dir, fsys); err != nil {
			return "", fmt.Errorf("copy postgres: %w", err)
		}
		if !hasBinaries(dir) {
			return "", fmt.Errorf("download postgres: %s missing initdb/postgres", dir)
		}
	}
	if os.Getuid() == 0 {
		if err := openCachePath(dir); err != nil {
			return "", err
		}
	}
	return filepath.Join(dir, "bin"), nil
}

// openCachePath widens permissions on dir and every parent up to /
// so a user the test process drops to (e.g. postgres) can traverse
// and read the install. Tests run as root in some sandboxes; this is
// a no-op everywhere else.
func openCachePath(dir string) error {
	for p := dir; p != "/" && p != ""; p = filepath.Dir(p) {
		if err := os.Chmod(p, 0o755); err != nil {
			return err
		}
	}
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		mode := info.Mode().Perm() | 0o005
		if info.IsDir() || info.Mode()&0o100 != 0 {
			mode |= 0o001
		}
		return os.Chmod(path, mode)
	})
}

// dropRoot returns a syscall.Credential pointing at an unprivileged
// account if the current process is running as root, chowns dataDir
// to that account, and widens permissions on every parent directory
// so the dropped user can traverse to it. Returns nil credential when
// not root.
func dropRoot(dataDir string) (*syscall.Credential, error) {
	if os.Getuid() != 0 {
		return nil, nil
	}
	u, err := user.Lookup("postgres")
	if err != nil {
		u, err = user.Lookup("nobody")
	}
	if err != nil {
		return nil, fmt.Errorf("no postgres or nobody user available: %w", err)
	}
	uid, err := strconv.ParseUint(u.Uid, 10, 32)
	if err != nil {
		return nil, err
	}
	gid, err := strconv.ParseUint(u.Gid, 10, 32)
	if err != nil {
		return nil, err
	}
	tmp := os.TempDir()
	root := dataDir
	for filepath.Dir(root) != tmp && filepath.Dir(root) != "/" && filepath.Dir(root) != "" {
		root = filepath.Dir(root)
	}
	if err := chownTree(root, int(uid), int(gid)); err != nil {
		return nil, err
	}
	for p := filepath.Dir(root); p != "/" && p != ""; p = filepath.Dir(p) {
		info, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		// Only widen — never strip sticky/world-writable from
		// shared dirs like /tmp.
		mode := info.Mode().Perm() | 0o055
		if mode == info.Mode().Perm() {
			continue
		}
		if err := os.Chmod(p, mode); err != nil {
			return nil, err
		}
	}
	return &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}, nil
}

// chownTree recursively assigns uid/gid to dir and every file
// underneath. Existing mode bits are preserved.
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

func hasBinaries(dir string) bool {
	for _, name := range []string{"initdb", "postgres"} {
		info, err := os.Stat(filepath.Join(dir, "bin", name))
		if err != nil || info.Mode()&0o111 == 0 {
			return false
		}
	}
	return true
}

// ensureServer makes sure a postgres server is listening on
// /tmp/.s.PGSQL.5411 — the socket the default testDSN points at. If
// one is already there it returns without doing anything; otherwise
// it spawns a fresh server backed by the binaries at bin and lets the
// supervisor pattern in postgres_test.go shut it down on test exit.
func ensureServer(bin string) error {
	const socket = "/tmp/.s.PGSQL.5411"
	if c, err := net.Dial("unix", socket); err == nil {
		c.Close()
		return nil
	}

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
	for _, err := range backoff.Loop(ctx, 500*time.Millisecond) {
		if err != nil {
			cmd.Process.Signal(syscall.SIGQUIT)
			return errors.New("postgres did not start within 10s")
		}
		c, dialErr := net.Dial("unix", socket)
		if dialErr == nil {
			c.Close()
			return nil
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			return fmt.Errorf("postgres exited early: %v", cmd.ProcessState)
		}
	}
	return nil
}
