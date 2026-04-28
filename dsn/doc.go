// Package dsn provides utilities for parsing DSN strings and URLs into a
// structured [Options] struct. It supports both key=value DSN strings and
// URL formats, with sensible defaults for missing values. The package also
// includes a scanner for tokenizing DSN strings, handling quoted values and
// escaping.
//
// # Connection parameters
//
// [Parse] accepts either a libpq-style DSN ("host=… port=… …") or a URL
// with the "postgres" or "postgresql" scheme. Polite recognizes the
// parameters listed below; any other key=value is forwarded to
// PostgreSQL unchanged in the startup message.
//
//	General:
//		- host        server host: a hostname, an IP address, or
//		              an absolute path to a Unix socket directory.
//		              default: /tmp
//		- port        TCP port; for Unix sockets, the suffix
//		              appended to ".s.PGSQL." inside host.
//		              default: 5432
//		- user        PostgreSQL role to authenticate as.
//		              default: current OS user, or "postgres" if
//		              that lookup fails
//		- password    secret for cleartext or SCRAM auth; not
//		              sent under trust or peer auth.
//		              default: empty
//		- database    database name; "dbname" is accepted as an
//		              alias for compatibility with other drivers.
//		              default: postgres
//
//	TLS:
//		- sslmode     TLS policy:
//		                disable      no TLS; plaintext only.
//		                require      TLS required, with hostname
//		                             verification. Stricter than
//		                             libpq, which skips verification
//		                             under "require".
//		                verify-full  TLS required, with hostname
//		                             verification.
//		              An unset or empty value is treated as
//		              verify-full. "allow", "prefer", and
//		              "verify-ca" are rejected; polite never
//		              falls back from TLS to plaintext.
//		              default: verify-full
//		- sslrootcert trust roots for certificate verification:
//		                <abs-path>   PEM-encoded certificates
//		                             loaded from that file. Must
//		                             be an absolute path; relative
//		                             paths are rejected.
//		                system       case-insensitive; loads the
//		                             host's system trust roots
//		                             via [crypto/x509.SystemCertPool].
//		              The file is read through [os.OpenRoot] rooted
//		              at its containing directory, refusing symlinks
//		              that escape it.
//		              If unset, the [crypto/tls] defaults apply.
//		              default: empty
//
//	Pass-through:
//		Any other key=value pair — application_name, search_path,
//		options, and so on — is sent to the backend in the startup
//		message and applied as a session GUC.
//
// URL form mirrors DSN form: the userinfo supplies user and password,
// the host and port supply host and port, the path supplies database,
// and unrecognized query parameters are forwarded the same way as
// DSN extras.
package dsn
