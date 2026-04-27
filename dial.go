package polite

import (
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"blake.io/polite/dsn"
	"blake.io/polite/febe"
	"blake.io/polite/internal/scram"
)

// Config configures a [Dial]. All fields are optional; a nil *Config is
// equivalent to a zero-value Config.
type Config struct {
	// Dial is used to open the connection to postgres. If nil, a net.Dialer is used.
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)

	// TLSConfig is cloned and used as the base TLS config for handshakes.
	// Set Time there to override the clock used by crypto/tls.
	TLSConfig *tls.Config

	// Now returns the time polite uses for unqualified calls to now(),
	// transaction_timestamp(), statement_timestamp(), and clock_timestamp()
	// on the connection.
	// If Now is nil, polite leaves PostgreSQL's default time behavior
	// unchanged.
	//
	// Now is captured at [Dial] and used for the lifetime of the returned
	// [Conn]; mutating Config.Now afterwards has no effect on existing
	// connections.
	Now func() time.Time
}

// Dial opens a connection to postgres using the parameters in dsnOrURL,
// which is a DSN or URL. The defaults are host=/tmp (Unix socket),
// port=5432, user=$(current OS user, falling back to "postgres"), and
// sslmode=verify-full.
//
// The supported sslmode values are "disable", "require", and "verify-full".
// An unset sslmode is treated as "verify-full". Any other value, including
// libpq's "allow", "prefer", and "verify-ca", is rejected.
//
// Polite's default differs from libpq, which defaults to "prefer" and falls
// back to plaintext if the server refuses TLS. Polite never falls back: if
// TLS is requested and the server refuses, Dial returns [ErrTLSRefused].
//
// Polite's "require" is stricter than libpq's. Both require TLS, but libpq
// performs no hostname verification under "require"; polite verifies the
// server name against the certificate, the same as "verify-full".
//
// The sslrootcert option selects the trust roots for verification. A path
// loads PEM-encoded certificates from that file; the special value "system"
// (case-insensitive) loads the host's system trust roots via
// [crypto/x509.SystemCertPool]. If sslrootcert is unset, the standard
// crypto/tls defaults apply.
//
// Dial honors ctx through the entire connect path: the dial itself, the
// TLS handshake, and the startup/auth round-trips. If ctx is canceled
// mid-handshake, Dial returns ctx.Err(); the partially-opened socket is
// closed before the call returns.
func Dial(ctx context.Context, dsnOrURL string, cfg *Config) (*Conn, error) {
	o, err := dsn.Parse(dsnOrURL)
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

	if cfg.Dial == nil {
		cfg.Dial = (&net.Dialer{}).DialContext
	}

	cn, err := cfg.Dial(ctx, network, addr)
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
		cn:  cn,
		fc:  febe.New(cn, cn),
		now: cfg.Now,

		dial:      cfg.Dial,
		network:   network,
		addr:      addr,
		dsnOpts:   o,
		tlsConfig: cfg.TLSConfig,
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
				bkd, err := c.fc.ReadBackendKeyData()
				if err != nil {
					return err
				}
				c.fc.SetBackendKeyData(bkd.PID, bkd.SecretKey)
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

	// Detect testing/synctest mode: inside a bubble the fake clock is
	// frozen, so two adjacent time.Now() calls return identical values
	// (monotonic component included). Outside, they always differ.
	// We avoid spawning a long-lived watcher goroutine under synctest
	// because the bubble panics on goroutines that outlive it.
	if !time.Now().Equal(time.Now()) {
		c.cancelStop = make(chan struct{}, 1)
		c.cancelReq = make(chan struct{}, 1)
		go c.cancelLoop()
	}

	return c, err
}

func (c *Conn) handleAuth(ctx context.Context, o *dsn.Options) error {
	// TODO(bmizerany): add tracing

	cr, err := c.fc.ReadAuthentication()
	if err != nil {
		return err
	}
	switch cr.Type {
	case febe.AuthenticationTypeOK:
		// ok
		return nil
	case febe.AuthenticationTypeSASL:
		return scram.Handshake(ctx, c.fc, cr, o.User, o.Password)
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
				return nil, fmt.Errorf("polite: failed to read root certificate: %w", err)
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(data) {
				return nil, fmt.Errorf("polite: failed to parse root certificate")
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
		return nil, fmt.Errorf("polite: unsupported sslmode %q", o.SSLMode)
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
