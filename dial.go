package pqlite

import (
	"cmp"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"blake.io/pqlite/dsn"
	"blake.io/pqlite/febe"
	"github.com/lib/pq/scram"
)

type Config struct {
	DialContext func(ctx context.Context, network, addr string) (net.Conn, error)
	TLSConfig   *tls.Config
	UseGoTime   bool
}

// Open opens a connection to postgres using the parameters in dsnOrURL,
// which is a DSN or URL. By default, host=/tmp (Unix socket), port=5432,
// user=$USER, and sslmode=require.
func Open(ctx context.Context, dsnOrURL string, cfg *Config) (*Conn, error) {
	o, err := dsn.ParseOptions(dsnOrURL)
	if err != nil {
		return nil, err
	}

	cfg = cmp.Or(cfg, &Config{})

	var network, addr string
	if strings.HasPrefix(o.Host, "/") {
		_, err = os.Stat(o.Host)
		if err != nil {
			return nil, err
		}
		network = "unix"
		addr = filepath.Join(o.Host, ".s.PGSQL."+o.Port)
	} else {
		network = "tcp"
		addr = o.Host + ":" + o.Port
	}

	// TODO(bmizerany): check info is a valid file and socket
	if cfg.DialContext == nil {
		cfg.DialContext = (&net.Dialer{}).DialContext
	}

	cn, err := cfg.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}

	cn, err = maybeUpgradeToTLS(cn, o, cfg)
	if err != nil {
		return nil, err
	}

	stopc := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		cn.SetReadDeadline(time.Now())
		close(stopc)
	})

	c := &Conn{
		cn:        cn,
		fc:        febe.New(cn, cn),
		created:   time.Now(),
		useGoTime: cfg.UseGoTime,
	}

	err = func() error {
		if err := c.fc.WriteStartupMessage(o.Extra...); err != nil {
			return err
		}
		if err := c.fc.Flush(); err != nil {
			return err
		}
		for {
			h, err := c.fc.ReadHeader(ctx)
			if err != nil {
				return err
			}

			switch h.Type() {
			case 'R':
				if err := c.handleAuth(ctx, o); err != nil {
					return err
				}
			case 'K':
				// TODO(bmizerany): save the key for cancellations later
			case 'Z':
				_, err := c.fc.ReadReadyForQuery()
				if err != nil {
					return err
				}
				if c.useGoTime {
					return c.initGoTime(ctx)
				}
				return nil
			default:
				return unexpectedMessage(h)
			}
		}
	}()
	if !stop() {
		<-stopc
		cn.Close()
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	return c, err
}

func (c *Conn) handleAuth(ctx context.Context, o *dsn.Options) error {
	// TODO(bmizerany): add tracing

	nextResponse := func(wantType febe.AuthenticationType) (febe.AuthenticationResponse, error) {
		var zero febe.AuthenticationResponse
		if err := c.fc.Flush(); err != nil {
			return zero, err
		}
		_, err := c.fc.ReadHeader(ctx)
		if err != nil {
			return zero, err
		}
		cr, err := c.fc.ReadAuthentication()
		if err != nil {
			return zero, err
		}
		if cr.Type != wantType {
			return zero, fmt.Errorf("unexpected authentication type %s; want %d", cr.Type, wantType)
		}
		return cr, nil
	}

	cr, err := c.fc.ReadAuthentication()
	if err != nil {
		return err
	}
	switch cr.Type {
	case febe.AuthenticationTypeOK:
		// ok
		return nil
	case febe.AuthenticationTypeSASL:
		sc := scram.NewClient(sha256.New, o.User, o.Password)
		sc.Step(nil)
		if sc.Err() != nil {
			return fmt.Errorf("SCRAM-SHA-256 error: %w", sc.Err())
		}

		// Initial response
		if err := c.fc.WriteSASLInitialResponse("SCRAM-SHA-256", sc.Out()); err != nil {
			return err
		}
		cr, err := nextResponse(febe.AuthenticationTypeSASLContinue)
		if err != nil {
			return err
		}
		sc.Step(cr.SASLData)
		if sc.Err() != nil {
			return fmt.Errorf("SCRAM-SHA-256 error: %w", sc.Err())
		}

		// Final response
		if err := c.fc.WriteSASLResponse(sc.Out()); err != nil {
			return err
		}
		cr, err = nextResponse(febe.AuthenticationTypeSASLFinal)
		if err != nil {
			return err
		}
		sc.Step(cr.SASLData)
		if sc.Err() != nil {
			return fmt.Errorf("SCRAM-SHA-256 error: %w", sc.Err())
		}

		// Success
		return nil
	case febe.AuthenticationTypeMD5:
		// See: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-START-UP
		md5 := func(s ...string) string {
			h := md5.New()
			for _, s := range s {
				io.WriteString(h, s)
			}
			return hex.EncodeToString(h.Sum(nil))
		}

		msg := "md5" + md5(md5(o.Password, o.User), string(cr.MD5Salt))
		if err := c.fc.WritePasswordMessage(msg); err != nil {
			return err
		}
		_, err := nextResponse(febe.AuthenticationTypeOK)
		return err
	default:
		return fmt.Errorf("unexpected authentication type %s", cr.Type)
	}
}

func maybeUpgradeToTLS(cn net.Conn, o *dsn.Options, cfg *Config) (net.Conn, error) {
	if o.SSLMode == "disable" {
		return cn, nil
	}

	// Ask the backend if we can TLS with it.
	ok, err := febe.RequestTLS(cn)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrTLSRefused
	}

	tcfg := cmp.Or(cfg.TLSConfig.Clone(), &tls.Config{})

	if o.SSLRootCert != "" {
		if strings.EqualFold(o.SSLRootCert, "system") {
			roots, err := x509.SystemCertPool()
			if err != nil {
				return nil, fmt.Errorf("pqlite: failed to load system root certificates: %w", err)
			}
			tcfg.RootCAs = roots
			tcfg.ClientCAs = roots
		} else {
			data, err := os.ReadFile(o.SSLRootCert)
			if err != nil {
				return nil, fmt.Errorf("postgres: failed to read root certificate: %w", err)
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(data) {
				return nil, fmt.Errorf("postgres: failed to parse root certificate")
			}
			tcfg.RootCAs = roots
			tcfg.ClientCAs = roots
		}
	}

	switch o.SSLMode {
	case "", "require":
		tcfg.InsecureSkipVerify = true
	case "verify-ca":
		tcfg.InsecureSkipVerify = true
		tcfg.VerifyPeerCertificate = func(certificates [][]byte, _ [][]*x509.Certificate) error {
			certs := make([]*x509.Certificate, len(certificates))
			for i, asn1Data := range certificates {
				cert, err := x509.ParseCertificate(asn1Data)
				if err != nil {
					return fmt.Errorf("postgres: failed to parse server certificate: %w", err)
				}
				certs[i] = cert
			}

			// Leave DNSName empty to skip hostname verification.
			opts := x509.VerifyOptions{
				Roots:         tcfg.RootCAs,
				Intermediates: x509.NewCertPool(),
			}
			// Skip the first cert because it's the leaf. All others
			// are intermediates.
			for _, cert := range certs[1:] {
				opts.Intermediates.AddCert(cert)
			}
			_, err := certs[0].Verify(opts)
			return err
		}
	case "verify-full":
		tcfg.ServerName = o.Host
	default:
		return nil, fmt.Errorf("postgres: unsupported sslmode %q", o.SSLMode)
	}
	return tls.Client(cn, tcfg), nil
}
