package postgres

//go:generate go run $GOROOT/src/crypto/tls/generate_cert.go --host=localhost --rsa-bits=1024 --ca

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDialAuthNone(t *testing.T) {
	dsn := startPG(t)

	cn, err := Dial(context.Background(), dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()

	if err := cn.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDialTLS(t *testing.T) {
	// TODO: test TLS, for real

	t.Fatal("TODO: test TLS")

	dsn := startPG(t)

	cn, err := Dial(context.Background(), dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()

	if err := cn.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// startPG starts a postgres instance and returns a DSN to connect to it.
// Any settings passed in will be appended to the DSN (e.g. "sslmode=disable").
//
// Logs from the postgres instance will be printed to t.Log. The default
// debug log level is INFO, so you'll see logs like:
//
//	LOG:  database system was shut down at 2021-03-02 16:04:05 UTC
//
// The postgres instance will be stopped when the test is done.
func startPG(t *testing.T, settings ...string) string {
	t.Helper()

	port := "9999"
	dir := t.TempDir()
	socketPath := filepath.Join(dir, ".s.PGSQL."+port)

	_, err := os.Stat(socketPath)
	if err == nil {
		t.Fatal("host socket already exists: ", socketPath)
	}

	ctx, cancel := withTestDeadline(context.Background(), t)
	ctx, cancel = context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "initdb", dir)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("initdb failed:\n%s", out)
		t.Fatal(err)
	}

	// detached context to prevent timeout above
	ctx, cancel = withTestDeadline(context.Background(), t)
	t.Cleanup(cancel)

	args := append([]string{
		"-D", dir,
		"-d", "1",
		"-p", "9999",
		"-c", "listen_addresses=",
		"-c", "unix_socket_directories=" + dir,
	}, settings...)

	t.Logf("postgres %s", strings.Join(args, " "))

	cmd = exec.CommandContext(ctx, "postgres", args...)
	cmd.Dir = dir
	cmd.WaitDelay = 1 * time.Second
	cmd.Cancel = func() error {
		return cmd.Process.Signal(syscall.SIGTERM)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			t.Log("-- postgres:", sc.Text())
		}
		if err := sc.Err(); err != nil {
			t.Error(err)
		}
	}()

	t.Cleanup(func() {
		defer func() {
			// Be paranoid and prevent illegal calls to
			// t.{Log,Error,...} funcs from being called after this test
			// is done by waiting until the logging goroutine has
			// exited.
			<-done
		}()

		cancel()
		if err := cmd.Wait(); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			var e *exec.ExitError
			if errors.As(err, &e) {
				stderr := string(e.Stderr)
				if stderr == "" {
					stderr = " <no stderr>"
				} else {
					stderr = "\n" + stderr
				}
				t.Logf("postgres exited with code (%d): stderr:%s", e.ExitCode(), stderr)
				return
			}
			t.Errorf("postgres exited with error: %v", err)
			return
		}
	})

	for {
		// Avoid bootstrapping problems and use pg_isready to wait for
		// postgres to be ready, instead of our driver. Using our
		// driver can lead to confusing and hard to debug errors
		// where postgres is fine, but the driver is not, but the
		// is failing in a way to is hard to distinguish from a
		// postgres failure.

		cmd := exec.CommandContext(ctx, "pg_isready",
			"-d", "postgres",
			"-h", dir,
			"-p", "9999",
			"-t", "3",
			"-U", "postgres",
		)
		cmd.Dir = dir
		err := cmd.Run()
		if err == nil {
			break
		}
		t.Logf("pg_isready failed: %v", err)
		tt := time.NewTimer(100 * time.Millisecond)
		select {
		case <-tt.C:
		case <-ctx.Done():
			tt.Stop()
			t.Fatal(ctx.Err())
		}
	}

	user, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}

	return strings.Join([]string{
		"host=" + socketPath,
		"user=" + user.Username,
		"dbname=postgres",
		"port=" + port,
		"sslmode=disable",
	}, " ")
}

func withTestDeadline(ctx context.Context, t *testing.T) (context.Context, context.CancelFunc) {
	deadline, hasDeadline := t.Deadline()
	if !hasDeadline {
		return ctx, func() {}
	}

	// subtract 200ms to give the test time to cleanup before the actual
	// deadline
	return context.WithDeadline(ctx, deadline.Add(-200*time.Millisecond))
}
