package polite

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

// pipeDial returns a Config.Dial that hands cli to Dial and counts calls.
// The server end must be created by the caller (e.g. via net.Pipe) so
// tests can drive it without racing Dial's assignment.
func pipeDial(cli net.Conn, calls *atomic.Int32) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if calls != nil {
			calls.Add(1)
		}
		return cli, nil
	}
}

func TestDialCancelBeforeFirstRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())

		cli, srv := net.Pipe()
		cfg := &Config{Dial: pipeDial(cli, nil)}

		type result struct {
			cn  *Conn
			err error
		}
		done := make(chan result, 1)
		go func() {
			cn, err := Dial(ctx, "sslmode=disable", cfg)
			done <- result{cn, err}
		}()

		// Drain the startup message the client writes so it can reach its
		// first ReadHeader and durably block there.
		go io.Copy(io.Discard, srv)

		synctest.Wait()
		cancel()

		r := <-done
		if r.cn != nil {
			r.cn.Close()
		}
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("Dial err = %v; want context.Canceled", r.err)
		}

		// Dial closed its end (dial.go:132-134); the server side should
		// now see io.EOF or net.ErrClosed on its next read.
		var buf [1]byte
		_, err := srv.Read(buf[:])
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("srv.Read err = %v; want closed pipe", err)
		}
	})
}

func TestDialCancelPrecanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cli, srv := net.Pipe()
	defer srv.Close()
	defer cli.Close()

	var calls atomic.Int32
	cfg := &Config{Dial: pipeDial(cli, &calls)}

	// The server drains anything the client writes and never replies.
	// Dial must not hang; the context is already dead on entry.
	drained := make(chan struct{})
	go func() {
		io.Copy(io.Discard, srv)
		close(drained)
	}()

	cn, err := Dial(ctx, "sslmode=disable", cfg)
	if cn != nil {
		cn.Close()
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Dial err = %v; want context.Canceled", err)
	}

	// Characterization: polite does NOT short-circuit on a dead ctx before
	// calling Config.Dial. The Dial hook is invoked exactly once.
	if got := calls.Load(); got != 1 {
		t.Fatalf("Config.Dial calls = %d; want 1", got)
	}

	cli.Close()
	<-drained
}

func TestDialCancelAfterStartupWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())

		cli, srv := net.Pipe()
		cfg := &Config{Dial: pipeDial(cli, nil)}

		type result struct {
			cn  *Conn
			err error
		}
		done := make(chan result, 1)
		go func() {
			cn, err := Dial(ctx, "sslmode=disable", cfg)
			done <- result{cn, err}
		}()

		// Read the startup message off the wire and throw it away. After
		// that, stop reading and never write back; the client will block
		// waiting on its first backend header.
		drained := make(chan struct{})
		go func() {
			defer close(drained)
			var buf [1024]byte
			srv.Read(buf[:])
		}()

		synctest.Wait()
		cancel()

		r := <-done
		if r.cn != nil {
			r.cn.Close()
		}
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("Dial err = %v; want context.Canceled", r.err)
		}
		<-drained
	})
}
