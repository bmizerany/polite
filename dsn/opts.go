// Package dsn provides utilities for parsing DSN strings and URLs into a
// structured Options struct. It supports both key=value DSN strings and URL
// formats, with sensible defaults for missing values. The package also
// includes a scanner for tokenizing DSN strings, handling quoted values and
// escaping.
package dsn

import (
	"cmp"
	"fmt"
	"iter"
	"net/url"
	"os/user"
	"slices"
)

// Options is a parsed connection string. Zero-valued fields are filled in
// with defaults by [ParseOptions].
type Options struct {
	// Host is the server host (a hostname, an IP address, or an absolute
	// path for a Unix socket directory).
	Host string

	// Port is the TCP port or, for Unix sockets, the suffix appended to
	// the .s.PGSQL. socket file in Host.
	Port string

	// User is the PostgreSQL role to authenticate as. It is also placed
	// into Extra under the "user" key for the startup message.
	User string

	// Password is used for password-based authentication (cleartext or
	// SCRAM); it is never sent for trust or peer auth.
	Password string

	// Database is the database name. It is also placed into Extra under
	// the "database" key for the startup message.
	Database string

	// SSLMode selects the TLS policy: "disable", "require", or
	// "verify-full" (and "" as an alias for "verify-full"). See
	// [blake.io/polite.Dial] for the exact semantics.
	SSLMode string

	// SSLRootCert is a PEM file path or the special string "system",
	// which loads the host's system trust roots.
	SSLRootCert string

	// Extra holds the remaining startup parameters in "key=value" form,
	// passed through to PostgreSQL unchanged.
	Extra []string
}

var defaultUser = func() string {
	u, _ := user.Current()
	return u.Username
}()

// setDefaults sets the default values for the options. It also sets the
// user and database in the Extra field, overwriting them if they are already
// present.
func setDefaults(o *Options) error {
	o.User = cmp.Or(o.User, defaultUser, "postgres")
	o.Host = cmp.Or(o.Host, "/tmp")
	o.Port = cmp.Or(o.Port, "5432")
	o.Database = cmp.Or(o.Database, "postgres")
	o.SSLMode = cmp.Or(o.SSLMode, "verify-full")

	// Include username, and database in the extra options. We
	// keep them on the struct for convenience, but the backend
	// needs them too.
	o.Extra = append(o.Extra, "user="+o.User)
	o.Extra = append(o.Extra, "database="+o.Database)
	return nil
}

// ParseOptions parses a DSN or URL string into an Options struct. The defaults are:
//
//	dbname=postgres user=$USER host=/tmp port=5432 sslmode=verify-full
func ParseOptions(dsn string) (*Options, error) {
	o := &Options{}
	defer setDefaults(o)

	u, err := url.Parse(dsn)
	if err == nil && u.Scheme != "" {
		if !slices.Contains([]string{"postgres", "postgresql"}, u.Scheme) {
			return nil, fmt.Errorf("unsupported scheme: %q", u.Scheme)
		}

		o.Host = cmp.Or(u.Hostname(), o.Host)
		o.Port = cmp.Or(u.Port(), o.Port)

		// Username and password are parsed from the URL directly
		o.User = u.User.Username()

		// Password is parsed from the URL directly
		o.Password, _ = u.User.Password()

		// Database is the path of the URL without the leading slash
		if database := u.Path; len(database) > 1 {
			o.Database = database[1:]
		}

		q := u.Query()

		// SSLMode is parsed from the query string.
		o.SSLMode = q.Get("sslmode")
		q.Del("sslmode") // remove from extra options
		o.SSLRootCert = q.Get("sslrootcert")
		q.Del("sslrootcert") // remove from extra options

		for k := range q {
			// TODO(bmizerany): escape k and v?
			o.Extra = append(o.Extra, fmt.Sprintf("%s=%s", k, q.Get(k)))
		}
		return o, nil
	}

	var state int
	var key string
	var val string
	for tok, err := range ScanDSN(dsn) {
		if err != nil {
			return o, err
		}
		switch state {
		case 0:
			key = tok
			state = 1
		case 1:
			if tok != "=" {
				return o, fmt.Errorf("expected =, got %q", tok)
			}
			state = 2
		case 2:
			val = tok
			switch key {
			case "host":
				o.Host = val
			case "port":
				o.Port = val
			case "sslmode":
				o.SSLMode = val
			case "sslrootcert":
				o.SSLRootCert = val
			case "dbname", "database":
				// for backwards compat when switching from drivers
				// that support dbname
				o.Database = val
			case "user":
				o.User = val
			case "password":
				o.Password = val
			default:
				o.Extra = append(o.Extra, key+"="+val)
			}

			key = ""
			val = ""
			state = 0
		default:
			panic(fmt.Sprintf("invalid state %d; please file a bug", state))
		}
	}
	return o, nil
}

var isWhitespace = [256]bool{
	' ':  true,
	'\t': true,
	'\n': true,
	'\r': true,
}

// ScanDSN scans a DSN string and yields tokens. It is used to implement
// ParseOptions.
//
// A valid DSN looks like this:
//
//	host=localhost port=5432 sslmode=verify-full somestring='some \'quoted\' string' k = v
//
// The following tokens are yielded:
//
//	"host"
//	"="
//	"localhost"
//	"port"
//	"="
//	"5432"
//	"sslmode"
//	"="
//	"verify-full"
//	"somestring"
//	"="
//	"some 'quoted' string"
//	"k"
//	"="
//	"v"
func ScanDSN(dsn string) iter.Seq2[string, error] {
	return func(yield func(tok string, err error) bool) {
		if len(dsn) == 0 {
			return
		}
		for len(dsn) > 0 {
			c := dsn[0]
			if isWhitespace[c] {
				dsn = dsn[1:]
				continue
			}
			switch c {
			case '=':
				if !yield("=", nil) {
					return
				}
				dsn = dsn[1:]
			default:
				tok, rest, ok := parseString(dsn)
				if !ok {
					yield("", fmt.Errorf("unclosed quoted string"))
					return
				}
				if !yield(tok, nil) {
					return
				}
				dsn = rest
			}
		}
	}
}

func parseString(dsn string) (tok, rest string, ok bool) {
	if len(dsn) == 0 {
		return "", "", true
	}
	var quoted bool
	var escaped bool
	var offset int

	dsn0 := dsn
	if dsn[0] == '\'' {
		quoted = true
		dsn = dsn[1:]
		offset++
	}
	for i := range dsn {
		c := dsn[i]
		offset++
		switch {
		case escaped:
			escaped = false
		case quoted && c == '\\':
			escaped = true
		case quoted && c == '\'':
			return dsn0[:offset], dsn0[offset:], true
		case !quoted && (isWhitespace[c] || c == '='):
			return dsn0[:offset-1], dsn0[offset-1:], true
		}
	}
	if quoted {
		// Unclosed quoted string
		return "", "", false
	}

	// The string is unquoted, with no whitespace. Return the whole thing.
	return dsn, "", true
}
