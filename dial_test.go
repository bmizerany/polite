package pqlite

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestOpenAuthNone(t *testing.T) {
	t.Parallel()

	dsn := startPG(t)
	cn, err := Open(context.Background(), dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()
	if err := cn.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestOpenAuthMD5(t *testing.T) {
	t.Parallel()

	// hba postgres that will accept md5 passwords for md5only db, and
	// trust non-md5only database names using regexp
	mainDSN := startPG(t, withHBA(t, []string{
		"-c", "password_encryption=md5",
	}, `
		# order is paramount; first match takes precedence
		local  all  test  md5
		local  all  all   trust
	`)...)

	func() {
		// Create test user with pass
		cn, err := Open(context.Background(), mainDSN, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer cn.Close()

		const q = `CREATE USER test WITH PASSWORD 'pass'`
		if err := cn.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}()

	cases := []struct {
		user, pass string
		wantErr    bool
	}{
		{"test", "pass", false},
		{"test", "wrong", true},
		{"wrong", "pass", true},
	}

	for _, tt := range cases {
		t.Run(tt.user+"/"+tt.pass, func(t *testing.T) {
			// connect with password
			dsn := fmt.Sprintf("%s user=%s password=%s", mainDSN, tt.user, tt.pass)
			cn, err := Open(context.Background(), dsn, nil)
			if tt.wantErr && err == nil {
				t.Fatal("expected error")
			} else if !tt.wantErr && err != nil {
				t.Fatal(err)
			}
			if err != nil {
				return
			}
			defer cn.Close()

			if err := cn.Ping(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenAuthSCRAM(t *testing.T) {
	mainDSN := startPG(t, withHBA(t, []string{
		"-c", "password_encryption=scram-sha-256",
		"-c", "listen_addresses=127.0.0.1",
	}, `
		# order is paramount; first match takes precedence
		host   all  test  127.0.0.1/32 scram-sha-256
		local  all  all   trust
	`)...)

	func() {
		// Create test user with pass
		cn, err := Open(context.Background(), mainDSN, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer cn.Close()

		const q = `CREATE USER test WITH PASSWORD 'pass'`
		if err := cn.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}()

	mainDSN += " host=127.0.0.1"

	cases := []struct {
		user, pass string
		wantErr    bool
	}{
		{"test", "pass", false},
		{"test", "wrong", true},
		{"wrong", "pass", true},
	}

	for _, tt := range cases {
		t.Run(tt.user+"/"+tt.pass, func(t *testing.T) {
			// connect with password
			dsn := fmt.Sprintf("%s user=%s password=%s", mainDSN, tt.user, tt.pass)

			ctx := context.Background()
			cn, err := Open(ctx, dsn, nil)
			if cn != nil {
				defer cn.Close()
			}
			if err != nil {
				if tt.wantErr {
					return
				}
				t.Fatal(err)
			}
			if tt.wantErr {
				t.Fatal("expected error")
			}

			if err := cn.Ping(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenTLS(t *testing.T) {
	t.Parallel()

	sslrootcert, tlsargs := configureTLSOnly(t)
	dsn := startPG(t, tlsargs...)

	cases := []struct {
		sslmode string
		sslroot string
		host    string // attempts to connect to unix socket if empty
		wantErr bool
	}{
		{"disable", "", "", false}, //  no  TLS  required  for  local  unix  socket
		{"disable", "", "localhost", true},
		{"require", "", "", true},
		{"require", "", "localhost", false},

		{"verify-ca", "", "", true},
		{"verify-ca", "", "localhost", true},
		{"verify-ca", sslrootcert, "", true},
		{"verify-ca", sslrootcert, "localhost", true},

		{"verify-full", "", "", false},
		{"verify-full", "", "localhost", false},
		{"verify-full", sslrootcert, "", true},
		{"verify-full", sslrootcert, "localhost", true},
	}

	for _, tc := range cases {
		t.Run(tc.sslmode+"-"+tc.host, func(t *testing.T) {
			dsn += " sslmode=" + tc.sslmode
			if tc.host != "" {
				dsn += " host=" + tc.host
			}
			if tc.sslroot != "" {
				dsn += " sslrootcert=" + tc.sslroot
			}

			cn, err := Open(context.Background(), dsn, nil)
			if err != nil {
				if !tc.wantErr {
					t.Fatal(err)
				}
				return
			}
			defer cn.Close()

			if err := cn.Ping(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMain(m *testing.M) {
	maybeBecomeSupervisor()
	os.Exit(m.Run())
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

	port, err := getFreePort()
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()

	ctx, cancel := withTestDeadline(context.Background(), t)
	defer cancel()
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
		"-d", "2",
		"-p", port,
		"-c", "listen_addresses=",
		"-c", "unix_socket_directories=" + dir,
	}, settings...)

	t.Logf("postgres %s", strings.Join(args, " "))

	cmd = exec.CommandContext(ctx, "postgres", args...)
	cmd.Dir = dir
	cmd.WaitDelay = 1 * time.Second
	cmd.Cancel = func() error {
		return cmd.Process.Signal(syscall.SIGQUIT)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	shutThisDownAfterMyDeath(cmd.Process)

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
		t.Helper()
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

	waitForPGReady(ctx, t.Logf, dir, port)

	return strings.Join([]string{
		"host=" + dir,
		"dbname=postgres",
		"port=" + port,
		"sslmode=disable",
	}, " ")
}

// withTestDeadline returns a context that will be canceled when the test
// deadline is reached. The returned cancel func should be called when the test
// is done.
func withTestDeadline(ctx context.Context, t *testing.T) (context.Context, context.CancelFunc) {
	deadline, hasDeadline := t.Deadline()
	if !hasDeadline {
		return ctx, func() {}
	}

	// subtract 200ms to give the test time to cleanup before the actual
	// deadline
	return context.WithDeadline(ctx, deadline.Add(-200*time.Millisecond))
}

type logf func(format string, args ...interface{})

func waitForPGReady(ctx context.Context, logf logf, host, port string) error {
	for {
		// Avoid bootstrapping problems and use pg_isready to wait for
		// postgres to be ready, instead of our driver. Using our
		// driver can lead to confusing and hard to debug errors
		// where postgres is fine, but the driver is not, but the
		// is failing in a way to is hard to distinguish from a
		// postgres failure.

		cmd := exec.CommandContext(ctx, "pg_isready",
			"-d", "postgres",
			"-h", host,
			"-p", port,
			"-t", "3",
			"-U", "postgres",
		)

		cmd.Dir = host
		err := cmd.Run()
		if err == nil {
			return nil
		}
		logf("pg_isready failed: %v; trying again", err)
		tt := time.NewTimer(200 * time.Millisecond)
		select {
		case <-tt.C:
		case <-ctx.Done():
			tt.Stop()
			return err
		}
	}
}

// generated with `go run $GOPATH/src/crypto/tls/generate_cert.go --host localhost`
var certs = map[string]string{
	"server.crt": `-----BEGIN CERTIFICATE-----
MIIC+jCCAeKgAwIBAgIRAIQtkgklMXSxHaISLIzkNDkwDQYJKoZIhvcNAQELBQAw
EjEQMA4GA1UEChMHQWNtZSBDbzAeFw0yMzEyMzEyMzE1NTdaFw0yNDEyMzAyMzE1
NTdaMBIxEDAOBgNVBAoTB0FjbWUgQ28wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAw
ggEKAoIBAQDHmslDdh33tdBgPe+F+BKFlkHdVBf4U/CuqjccYz99EuXzSpFKBtpx
cNk28/yYTVbghYtkTS2iiiFy/rXi8sIMZq7Vwqhjv8LTrVomXzZxtWcaKlkNVA53
ZeDw89XbcvSI87XBJuqDwD4cCyZR7CAfZ95U+5wzc+dE90jeofo4acDULviY+eya
8ik/xy9rkkcLe5r49d5LYBeVU/Zlk/8fbJaqpdmlrWo2Z2y4+hGqKAI5jbmoHmwY
gabdbuxTZ9mjCpxwv2ENgDz68k8jECK5mJXhUBHjIvdENx8ubnJZ5df0UBhIuIva
r/kqFUTs18LGfFoqyyzlIUPInMEEu7PPAgMBAAGjSzBJMA4GA1UdDwEB/wQEAwIF
oDATBgNVHSUEDDAKBggrBgEFBQcDATAMBgNVHRMBAf8EAjAAMBQGA1UdEQQNMAuC
CWxvY2FsaG9zdDANBgkqhkiG9w0BAQsFAAOCAQEAkUiuoZNUjieBa2Hsw9MsBG0D
qfNwJHJ2THvQFe73EFEoHHXPWFxKoXRpgnoPQ0QCHTKm4IiLJOPowSeuBc/kARjt
gK280zh0M37Rmu4EmY8D7LrXoZBKWHQBp8ebfg4KbZlXfOqJ57xdrrTCu8JB+GIt
Kc0FxY0CebSCQd+C6Coc+lZlIOcctuGZQepW7fmsxPmOnZbns/danErcmA6pZ0/M
oHcD8gYc2Xh6nqO9h4P9LS2/BamBAtZXOCKcw/iWkB21thR1B22sCdPxGbUOeeUA
KfKpma7xCvcJlf3dYT2NIkUu7B9MPXTaPVKQElKJ5sOpZ5jcTBD1OBdJ2mazsg==
-----END CERTIFICATE-----
`,

	"server.key": `
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDHmslDdh33tdBg
Pe+F+BKFlkHdVBf4U/CuqjccYz99EuXzSpFKBtpxcNk28/yYTVbghYtkTS2iiiFy
/rXi8sIMZq7Vwqhjv8LTrVomXzZxtWcaKlkNVA53ZeDw89XbcvSI87XBJuqDwD4c
CyZR7CAfZ95U+5wzc+dE90jeofo4acDULviY+eya8ik/xy9rkkcLe5r49d5LYBeV
U/Zlk/8fbJaqpdmlrWo2Z2y4+hGqKAI5jbmoHmwYgabdbuxTZ9mjCpxwv2ENgDz6
8k8jECK5mJXhUBHjIvdENx8ubnJZ5df0UBhIuIvar/kqFUTs18LGfFoqyyzlIUPI
nMEEu7PPAgMBAAECggEAYuur+Nntyhh512h6bxtdGoU3yRdm9bOr4PQMce4hxbt6
6JK45EX3NicmDxzlvxj9CDJGvYOeLSJlp1biD0IdtwWgeeGJkD21sTAFMGUZV0r+
eLIqBff1X2jHDMLa5nHXDJQzLDE8IeVBIqbH6+uok8fnIJi9daZoI/6lR2hg2Fl4
JffKqxWkwxo/cF3WJk7skuATtLrGuVOi1vvf6Ru4OE+b9kyZMPSadcpzc9g/9ihA
gCxVZZuMXFTlfs/iRjYKVyQ3rItS4wbFcqwDT037UrtzOdCHLWtvLLH6PdIMymhN
3oJD2UX8PEDYbC5NwDsO/OmPaNqTpH+VFCBCvJVcmQKBgQDxPyfr+4s/L2uDBj2T
/cUzjXxE3W6Zc2gF+/wyPVa2lEDu+OBe4v7KqYxcLh6/JsD0Gy0FM+aZbJBwefrD
sUdvqCdL2UZQMczr9WNv3VcBuC9l3P8MY9okzCcnckObX8VOY5bBeBNnHfjoS6+3
vGLnJyIh4W1LoJy9cZmkQfK+FQKBgQDTz7OlB845dDQ5Cck+ErtPniahqoMimnR2
HxBA5+7Uwa8T4DU0B+FFEcdbj6S5oWhfx+fMVD28FS5OrN8qDyfz5erZ+iPXk6te
IEI8RxOaBTY5CCQmMznfh0oK/Ah0HgFamxJvQsMhbMJhD2a+A6AEogEcc7F5AvoY
o+oQe1+HUwKBgHUVgMugHrnAyv4hRjuQG2hkz54gle/1udPFs2WyrWYZV9ZjhYmn
w58aYJvQam29YkJW+iefQ9jMfuGH4fOeE5yWUNQYFG8RbHJMe0EsfHDCMobOlh4u
TA414v7bDwLWnUzpOp6VJ12Rh18+dH7I+HtVW9RzsvbUcZ3kB3xX4XbtAoGAYszd
tTtA3NfxjDGeZ5ABQ5A34O1Sog9+B6NRx5ToeOGnsxPAfXa3g6pZXcJRSi2aLCNp
YoxhB5yxrw1AQ7QyJLi4pdjiYyqEozFU9B0LbarEsjGz+wWmFzhaN2l83/0nEIaJ
QNDZiHvc571xZWz3dXHEJutoe1QCKQA8rFeAJfUCgYEA09rVXmqWt7ObIokm9zMS
rMh94ly9qR67gFrWUKrHHhBFU4glPP4ZtG/UXfsT+9wY6i7WugDpFR1JHJ9OstDZ
vfq2RQojf0vZ3MOLqQsPVXrVf7affiWDPcnUxpN7xHEz8w2ZglG+BmXiWJWDjcdX
2jlR0qPURc/JYNHXlCfWvQI=
-----END PRIVATE KEY-----
`,
}

// configureTLSOnly writes certs and postgres config to a temporary
// directory and returns the absolute path to the directory.
func configureTLSOnly(t *testing.T) (sslrootcert string, args []string) {
	dir := t.TempDir()
	for name, contents := range certs {
		certpath := filepath.Join(dir, name)
		if err := os.WriteFile(certpath, []byte(contents), 0600); err != nil {
			panic(err)
		}
	}
	sslrootcert = filepath.Join(dir, "server.crt")
	args = []string{
		"-c", "ssl=on",
		"-c", "ssl_cert_file=" + sslrootcert,
		"-c", "ssl_key_file=" + filepath.Join(dir, "server.key"),
		"-c", "listen_addresses=127.0.0.1",
	}
	return sslrootcert, withHBA(t, args, `
		hostssl  all  all  127.0.0.1/32  trust
		local    all  all                trust
	`)
}

func withHBA(t *testing.T, args []string, content string) []string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "pg_hba.conf")
	err := os.WriteFile(file, []byte(content), 0600)
	if err != nil {
		t.Fatal(err)
	}
	return append(args, "-c", "hba_file="+file)
}

func getFreePort() (string, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port), nil
}
