package pqlite

import (
	"fmt"
	"net/url"
	"strings"
)

type Options struct {
	DBName   string
	User     string
	Password string
	Host     string
	Port     string

	SSLMode string

	// Extra holds extra options that are not parsed or interpreted by this
	// package. They are passed to postgres as-is.
	Extra []string

	// TODO: TLS
}

// ParseOptions parses a DSN or URL string into an Options struct. The defaults are:
//
//	dbname=postgres user=postgres host=localhost port=5432 sslmode=verify
func ParseOptions(dsn string) (Options, error) {
	o := Options{
		DBName:  "postgres",
		User:    "postgres",
		Host:    "localhost",
		Port:    "5432",
		SSLMode: "verify",
	}

	if strings.HasPrefix(dsn, "postgres://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return o, err
		}

		q := u.Query()
		yank := func(s string) string {
			v := q.Get(s)
			q.Del(s)
			return v
		}
		o.DBName = strings.TrimPrefix(u.Path, "/")
		o.User = coallesce(u.User.Username(), o.User)
		o.Password, _ = u.User.Password()
		o.Password = coallesce(o.Password, o.Password)
		o.Host = coallesce(u.Hostname(), o.Host)
		o.Port = coallesce(u.Port(), o.Port)
		o.SSLMode = coallesce(yank("sslmode"), o.SSLMode)
		for k := range q {
			// TODO(bmizerany): escape k and v?
			o.Extra = append(o.Extra, fmt.Sprintf("%s=%s", k, q.Get(k)))
		}
		return o, nil
	}

	for _, kv := range strings.Fields(dsn) {
		k, v, _ := strings.Cut(kv, "=")
		switch k {
		case "dbname":
			o.DBName = v
		case "user":
			o.User = v
		case "password":
			o.Password = v
		case "host":
			o.Host = v
		case "port":
			o.Port = v
		case "sslmode":
			o.SSLMode = v
		default:
			// TODO(bmizerany): escape k and v?
			o.Extra = append(o.Extra, fmt.Sprintf("%s=%s", k, v))
		}
	}
	return o, nil
}

func coallesce(a, b string) string {
	if a == "" {
		return b
	}
	return a
}
