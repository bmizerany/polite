package polite

import (
	"cmp"
	"context"
	"crypto/md5"
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

	"blake.io/polite/dsn"
	"blake.io/polite/febe"
	"blake.io/polite/internal/scram"
)

type Config struct {
	DialContext func(ctx context.Context, network, addr string) (net.Conn, error)
	TLSConfig   *tls.Config

	// Now returns the current time for this connection. If nil, polite leaves
	// PostgreSQL time functions alone and uses normal client behavior.
	Now func() time.Time
}

// Dial opens a connection to postgres using the parameters in dsnOrURL,
// which is a DSN or URL. By default, host=/tmp (Unix socket), port=5432,
// user=$USER, and sslmode=require.
func Dial(ctx context.Context, dsnOrURL string, cfg *Config) (*Conn, error) {
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
		cn:       cn,
		fc:       febe.New(cn, cn),
		now:      cfg.Now,
		timeZone: time.UTC,
	}
	c.fc.SetParameterStatusHook(c.handleParameterStatus)

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
				_, err := c.fc.ReadBackendKeyData()
				if err != nil {
					return err
				}
			case 'Z':
				_, err := c.fc.ReadReadyForQuery()
				if err != nil {
					return err
				}
				if c.now != nil {
					if err := c.initGoTime(ctx); err != nil {
						return err
					}
					return c.updateGoTime(ctx)
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
		return scram.Handshake(c.fc, cr, o.User, o.Password)
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

// maybeUpgradeToTLS upgrades cn according to the requested sslmode.
//
// verify-full uses crypto/tls hostname verification by setting ServerName and
// leaving InsecureSkipVerify disabled.
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
	now := tcfg.Time
	if now == nil {
		now = verifyTime(cfg.Now)
		tcfg.Time = now
	}

	if o.SSLRootCert != "" {
		if strings.EqualFold(o.SSLRootCert, "system") {
			roots, err := x509.SystemCertPool()
			if err != nil {
				return nil, fmt.Errorf("polite: failed to load system root certificates: %w", err)
			}
			tcfg.RootCAs = roots
			tcfg.ClientCAs = roots
		} else {
			data, err := rootedReadFile(o.SSLRootCert)
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
	case "", "require", "verify-full":
		tcfg.InsecureSkipVerify = false
		tcfg.ServerName = o.Host
	default:
		return nil, fmt.Errorf("postgres: unsupported sslmode %q", o.SSLMode)
	}
	return tls.Client(cn, tcfg), nil
}

func rootedReadFile(name string) ([]byte, error) {
	root, err := os.OpenRoot(filepath.Dir(name))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return root.ReadFile(filepath.Base(name))
}

func verifyTime(now func() time.Time) func() time.Time {
	if now != nil {
		return now
	}
	return time.Now
}
