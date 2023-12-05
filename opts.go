package postgres

import (
	"fmt"
	"net/url"
	"strings"
)

type Options struct {
	Host string
	Port string

	SSLMode string

	// Extra holds extra options that are not parsed or interpreted by this
	// package. They are passed to postgres as-is and so should be in
	// key=value form.
	Extra []string
}

// ParseOptions parses a DSN or URL string into an Options struct. The defaults are:
//
//	dbname=postgres user=postgres host=localhost port=5432 sslmode=verify
func ParseOptions(dsn string) (Options, error) {
	o := Options{
		SSLMode: "verify",
		Host:    "localhost",
		Port:    "5432",
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

func coallesce(a, b string) string {
	if a == "" {
		return b
	}
	return a
}
