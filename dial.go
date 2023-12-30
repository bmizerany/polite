package postgres

import (
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"

	"blake.io/postgres/febe"
)

type Config struct {
	TLSConfig *tls.Config
}

// Dial connects to postgres using the parameters in dsnOrURL, which is a DSN
// or URL. The host, port, and sslmode parameters are defaulted to localhost,
// 5432, and verify, respectively. No other parameter has a default, including
// user, which must be specified by users of Dial.
//
// If sslmode is "disable", then no TLS is used. Otherwise, TLS is used.
func Dial(ctx context.Context, dsnOrURL string, cfg *Config) (*Conn, error) {
	o, err := ParseOptions(dsnOrURL)
	if err != nil {
		return nil, err
	}

	cfg = cmp.Or(cfg, &Config{})

	network := "unix"
	addr := o.Host
	_, err = os.Stat(addr)
	if err != nil {
		network = "tcp"
		addr = o.Host + ":" + o.Port
	}
	// TODO(bmizerany): check info is a valid file and socket

	var d net.Dialer
	cn, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}

	cn, err = maybeUpgradeToTLS(ctx, cn, o, cfg)
	if err != nil {
		return nil, err
	}

	c := &Conn{
		dsnOrURL: dsnOrURL,
		cn:       cn,
		fc:       febe.New(cn, cn),
	}

	if err := c.fc.WriteStartupMessage(ctx, o.Extra...); err != nil {
		return nil, err
	}

	if err := c.fc.Flush(); err != nil {
		return nil, err
	}

	for {
		h, err := c.fc.ReadHeader(ctx)
		if err != nil {
			return nil, err
		}

		switch h.Type() {
		case 'R':
			cr, err := c.fc.ReadAuthentication(ctx)
			if err != nil {
				return nil, err
			}
			if cr.Type != 0 {
				panic("TODO") // support password
			}
		case 'S':
			if err := c.fc.DiscardMessage(ctx); err != nil {
				return nil, err
			}
		case 'K':
			// TODO(bmizerany): save the key for cancellations later
			if err := c.fc.DiscardMessage(ctx); err != nil {
				return nil, err
			}
		case 'Z':
			_, err := c.fc.ReadReadyForQuery(ctx)
			if err != nil {
				return nil, err
			}
			return c, nil
		default:
			return nil, fmt.Errorf("%w: unexpected message %q", ErrProtocol, h.Type())
		}
	}
}

func maybeUpgradeToTLS(ctx context.Context, cn net.Conn, o *Options, cfg *Config) (net.Conn, error) {
	if o.SSLMode == "disable" {
		return cn, nil
	}

	// Ask the backend if we can TLS with it.
	ok, err := febe.RequestTLS(cn)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: backend does not support TLS", ErrProtocol)
	}

	tcfg := cmp.Or(cfg.TLSConfig.Clone(), &tls.Config{})
	switch o.SSLMode {
	case "", "require":
		tcfg.InsecureSkipVerify = true
	case "verify-full":
		tcfg.ServerName = o.Host
	default:
		return nil, fmt.Errorf("postgres: unsupported sslmode %q", o.SSLMode)
	}
	return tls.Client(cn, tcfg), nil
}
