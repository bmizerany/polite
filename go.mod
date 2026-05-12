module blake.io/polite

go 1.26.0

require (
	blake.io/wait v0.0.0-20260313223221-354793b3b0e0
	golang.org/x/tools v0.45.0
	kr.dev/diff v0.3.0
)

require (
	blake.io/unreturned v1.0.8 // indirect
	github.com/BurntSushi/toml v1.4.1-0.20240526193622-a339e1f7089c // indirect
	github.com/pkg/diff v0.0.0-20210226163009-20ebb0f2a09e // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	golang.org/x/exp v0.0.0-20231110203233-9a3e6036ecaa // indirect
	golang.org/x/exp/typeparams v0.0.0-20231108232855-2478ac86f678 // indirect
	golang.org/x/mod v0.36.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	honnef.co/go/tools v0.7.0 // indirect
	rsc.io/uncover v0.0.2 // indirect
)

tool (
	blake.io/unreturned
	honnef.co/go/tools/cmd/staticcheck
	rsc.io/uncover
)
