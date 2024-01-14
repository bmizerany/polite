package dsn

import (
	"cmp"
	"fmt"
	"net/url"
	"strings"

	"blake.io/postgres/iter"
)

type Options struct {
	Host string
	Port string

	User     string // also included in Extra
	Password string

	Database string // also included in Extra

	SSLMode     string
	SSLRootCert string

	// Extra holds extra options that are not parsed or interpreted by this
	// package. They are passed to postgres as-is and so should be in
	// key=value form.
	Extra []string
}

// ParseOptions parses a DSN or URL string into an Options struct. The defaults are:
//
//	dbname=postgres user=postgres host=localhost port=5432 sslmode=verify
func ParseOptions(dsn string) (*Options, error) {
	o := &Options{
		SSLMode: "require", // match lib/pq default
		Host:    "localhost",
		Port:    "5432",
	}

	if strings.HasPrefix(dsn, "postgres://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return o, err
		}

		o.Host = cmp.Or(u.Hostname(), o.Host)
		o.Port = cmp.Or(u.Port(), o.Port)

		// Username and password are parsed from the URL directly
		o.User = u.User.Username()

		// Password is parsed from the URL directly
		o.Password, _ = u.User.Password()

		// Database is the path of the URL
		if database := u.Path; len(database) > 1 {
			o.Database = database[1:]
		}

		q := u.Query()

		// SSLMode is parsed from the query string.
		o.SSLMode = cmp.Or(q.Get("sslmode"), o.SSLMode)
		q.Del("sslmode") // remove from extra options
		o.SSLRootCert = q.Get("sslrootcert")
		q.Del("sslrootcert") // remove from extra options

		// Include username, and database in the extra options. We
		// keep them on the struct for convenience, but the backend
		// needs them too.
		o.Extra = append(o.Extra, "user="+o.User)
		o.Extra = append(o.Extra, "database="+o.Database)

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
				o.Extra = append(o.Extra, "database="+val)
			case "user":
				o.User = val
				o.Extra = append(o.Extra, "user="+val)
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
//	host=localhost port=5432 sslmode=verify somestring='some \'quoted\' string' k = v
//
// The following tokens are yielded:
//
//	"localhost"
//	"="
//	"5432"
//	"="
//	"verify"
//	"some quoted string"
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
	for _, c := range dsn {
		offset++
		switch {
		case escaped:
			escaped = false
		case c == '\\':
			escaped = true
		case quoted && c == '\'':
			return dsn0[:offset], dsn0[offset:], true
		case !quoted && (isWhitespace[c] || c == '='):
			return dsn0[:offset-1], dsn0[offset-1:], true
		case !quoted && c == '=':
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
