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
		o.DBName = strings.TrimPrefix(u.Path, "/")
		o.User = coallesce(u.User.Username(), o.User)
		o.Password, _ = u.User.Password()
		o.Password = coallesce(o.Password, o.Password)
		o.Host = coallesce(u.Hostname(), o.Host)
		o.Port = coallesce(u.Port(), o.Port)

		q := u.Query()
		o.SSLMode = coallesce(q.Get("sslmode"), o.SSLMode)
		return o, nil
	}

	for _, kv := range strings.Fields(dsn) {
		kv := strings.SplitN(kv, "=", 2)
		if len(kv) != 2 {
			return o, fmt.Errorf("invalid key-value pair: %q", kv)
		}
		switch kv[0] {
		case "dbname":
			o.DBName = kv[1]
		case "user":
			o.User = kv[1]
		case "password":
			o.Password = kv[1]
		case "host":
			o.Host = kv[1]
		case "port":
			o.Port = kv[1]
		case "sslmode":
			o.SSLMode = kv[1]
		default:
			return o, fmt.Errorf("unknown key: %q", kv[0])
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
