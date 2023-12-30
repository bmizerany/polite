package postgres

import (
	"cmp"
	"fmt"
	"net/url"
	"strings"
)

type Options struct {
	Host string
	Port string

	User     string // also included in Extra
	Password string

	Database string // also included in Extra

	SSLMode string

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

		// Do not include sslmode in the extra options
		q.Del("sslmode")

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

	for _, kv := range strings.Fields(dsn) {
		k, v, _ := strings.Cut(kv, "=")
		switch k {
		case "host":
			o.Host = v
		case "port":
			o.Port = v
		case "sslmode":
			o.SSLMode = v
		case "dbname":
			// for backwards compat when switching from drivers
			// that support dbname
			o.Extra = append(o.Extra, "database="+v)
		default:
			o.Extra = append(o.Extra, kv)
		}
	}
	return o, nil
}
