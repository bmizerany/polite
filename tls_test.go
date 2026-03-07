package pqlite

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"blake.io/pqlite/dsn"
)

func TestMaybeUpgradeToTLSVerifyFullLoadsRootCertAndUsesDSNHost(t *testing.T) {
	t.Parallel()

	client, sni, serverErr := startTLSUpgradeServer(t)
	o := &dsn.Options{
		Host:        "localhost",
		SSLMode:     "verify-full",
		SSLRootCert: writeTLSRootCert(t),
	}
	cfg := &Config{
		TLSConfig: &tls.Config{
			ServerName:         "example.com",
			InsecureSkipVerify: true,
		},
	}

	cn, err := maybeUpgradeToTLS(client, o, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cn.Close() })

	tlsConn, ok := cn.(*tls.Conn)
	if !ok {
		t.Fatalf("connection type = %T; want *tls.Conn", cn)
	}
	if err := tlsConn.HandshakeContext(t.Context()); err != nil {
		t.Fatal(err)
	}

	state := tlsConn.ConnectionState()
	if got, want := state.ServerName, "localhost"; got != want {
		t.Fatalf("ConnectionState.ServerName = %q; want %q", got, want)
	}
	if len(state.VerifiedChains) == 0 {
		t.Fatal("VerifiedChains empty; want certificate verification")
	}
	if got, want := <-sni, "localhost"; got != want {
		t.Fatalf("SNI = %q; want %q", got, want)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server handshake: %v", err)
	}
	if got, want := cfg.TLSConfig.ServerName, "example.com"; got != want {
		t.Fatalf("TLSConfig.ServerName = %q; want %q", got, want)
	}
	if !cfg.TLSConfig.InsecureSkipVerify {
		t.Fatal("TLSConfig.InsecureSkipVerify = false; want caller config unchanged")
	}
}

func TestMaybeUpgradeToTLSVerifyFullRejectsHostnameMismatch(t *testing.T) {
	t.Parallel()

	client, sni, serverErr := startTLSUpgradeServer(t)
	o := &dsn.Options{
		Host:    "example.com",
		SSLMode: "verify-full",
	}

	cn, err := maybeUpgradeToTLS(client, o, &Config{
		TLSConfig: &tls.Config{RootCAs: tlsRootPool(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cn.Close() })

	tlsConn, ok := cn.(*tls.Conn)
	if !ok {
		t.Fatalf("connection type = %T; want *tls.Conn", cn)
	}

	err = tlsConn.HandshakeContext(t.Context())
	if err == nil {
		t.Fatal("HandshakeContext succeeded; want hostname verification failure")
	}

	var verifyErr *tls.CertificateVerificationError
	if !errors.As(err, &verifyErr) {
		t.Fatalf("HandshakeContext error = %v; want *tls.CertificateVerificationError", err)
	}

	var hostnameErr x509.HostnameError
	if !errors.As(err, &hostnameErr) {
		t.Fatalf("HandshakeContext error = %v; want x509.HostnameError", err)
	}

	if got, want := <-sni, "example.com"; got != want {
		t.Fatalf("SNI = %q; want %q", got, want)
	}
	<-serverErr
}

func TestMaybeUpgradeToTLSVerifyFullRejectsUntrustedCertificate(t *testing.T) {
	t.Parallel()

	client, sni, serverErr := startTLSUpgradeServer(t)
	o := &dsn.Options{
		Host:    "localhost",
		SSLMode: "verify-full",
	}

	cn, err := maybeUpgradeToTLS(client, o, &Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cn.Close() })

	tlsConn, ok := cn.(*tls.Conn)
	if !ok {
		t.Fatalf("connection type = %T; want *tls.Conn", cn)
	}

	err = tlsConn.HandshakeContext(t.Context())
	if err == nil {
		t.Fatal("HandshakeContext succeeded; want certificate verification failure")
	}

	var verifyErr *tls.CertificateVerificationError
	if !errors.As(err, &verifyErr) {
		t.Fatalf("HandshakeContext error = %v; want *tls.CertificateVerificationError", err)
	}

	if got, want := <-sni, "localhost"; got != want {
		t.Fatalf("SNI = %q; want %q", got, want)
	}
	<-serverErr
}

func startTLSUpgradeServer(t *testing.T) (net.Conn, <-chan string, <-chan error) {
	t.Helper()

	cert, err := tls.X509KeyPair([]byte(certs["server.crt"]), []byte(certs["server.key"]))
	if err != nil {
		t.Fatal(err)
	}

	client, server := net.Pipe()
	deadline := time.Now().Add(5 * time.Second)
	if err := client.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := server.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}

	sni := make(chan string, 1)
	errc := make(chan error, 1)
	go func() {
		defer server.Close()

		var req [8]byte
		if _, err := io.ReadFull(server, req[:]); err != nil {
			errc <- err
			return
		}
		if req != sslRequestMessage {
			errc <- fmt.Errorf("SSL request = %v; want %v", req, sslRequestMessage)
			return
		}
		if _, err := server.Write([]byte{'S'}); err != nil {
			errc <- err
			return
		}

		serverTLS := tls.Server(server, &tls.Config{
			GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
				sni <- hello.ServerName
				return &cert, nil
			},
		})
		errc <- serverTLS.HandshakeContext(t.Context())
	}()
	return client, sni, errc
}

func writeTLSRootCert(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "server.crt")
	if err := os.WriteFile(path, []byte(certs["server.crt"]), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func tlsRootPool(t *testing.T) *x509.CertPool {
	t.Helper()

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(certs["server.crt"])) {
		t.Fatal("AppendCertsFromPEM = false")
	}
	return pool
}

var sslRequestMessage = [8]byte{0, 0, 0, 8, 4, 210, 22, 47}
