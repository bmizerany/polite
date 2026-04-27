package scram

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"blake.io/polite/febe"
)

// TestHandshakeCtxCancel verifies that a canceled caller context aborts
// the SCRAM handshake on the first read instead of blocking on the
// underlying reader.
func TestHandshakeCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// A reader that would block forever if read; the canceled ctx must
	// short-circuit the read before it reaches the wire.
	cn := febe.New(blockingReader{}, &bytes.Buffer{})
	err := Handshake(ctx, cn, febe.AuthenticationResponse{
		Type:           febe.AuthenticationTypeSASL,
		SASLMechanisms: []string{"SCRAM-SHA-256"},
	}, "user", "pencil")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Handshake() err = %v; want context.Canceled", err)
	}
}

// blockingReader blocks on Read until the test fails or returns; it
// exists so a test can prove no read happened. Read is never expected
// to be called.
type blockingReader struct{}

func (blockingReader) Read([]byte) (int, error) {
	select {}
}

func TestParseChallenge(t *testing.T) {
	tests := []struct {
		name      string
		parts     []string
		wantNonce string
		wantSalt  string
		wantIter  int
		wantErr   string
	}{
		{
			name: "ok",
			parts: []string{
				"r=client-and-server-nonce",
				"s=" + base64.StdEncoding.EncodeToString([]byte("salt")),
				"i=4096",
			},
			wantNonce: "client-and-server-nonce",
			wantSalt:  "salt",
			wantIter:  4096,
		},
		{
			name: "duplicate nonce",
			parts: []string{
				"r=first",
				"r=second",
				"s=" + base64.StdEncoding.EncodeToString([]byte("salt")),
				"i=1",
			},
			wantErr: `polite: duplicate SASL attribute: "r"`,
		},
		{
			name: "missing nonce",
			parts: []string{
				"s=" + base64.StdEncoding.EncodeToString([]byte("salt")),
				"i=4096",
			},
			wantErr: "polite: missing SCRAM server nonce",
		},
		{
			name: "malformed salt attribute",
			parts: []string{
				"r=nonce",
				"bad",
				"s=" + base64.StdEncoding.EncodeToString([]byte("salt")),
				"i=4096",
			},
			wantErr: `polite: malformed SASL attribute (missing '='): "bad"`,
		},
		{
			name: "invalid salt encoding",
			parts: []string{
				"r=nonce",
				"s=%%%",
				"i=4096",
			},
			wantErr: "polite: error decoding SCRAM salt: illegal base64 data at input byte 0",
		},
		{
			name: "empty salt",
			parts: []string{
				"r=nonce",
				"s=",
				"i=4096",
			},
			wantErr: "polite: empty SCRAM salt",
		},
		{
			name: "malformed iteration attribute",
			parts: []string{
				"r=nonce",
				"s=" + base64.StdEncoding.EncodeToString([]byte("salt")),
				"bad",
				"i=4096",
			},
			wantErr: `polite: malformed SASL attribute (missing '='): "bad"`,
		},
		{
			name: "invalid iteration encoding",
			parts: []string{
				"r=nonce",
				"s=" + base64.StdEncoding.EncodeToString([]byte("salt")),
				"i=abc",
			},
			wantErr: `polite: error decoding SCRAM iteration count: strconv.Atoi: parsing "abc": invalid syntax`,
		},
		{
			name: "zero iterations",
			parts: []string{
				"r=nonce",
				"s=" + base64.StdEncoding.EncodeToString([]byte("salt")),
				"i=0",
			},
			wantErr: "polite: invalid SCRAM iteration count: 0",
		},
		{
			name: "negative iterations",
			parts: []string{
				"r=nonce",
				"s=" + base64.StdEncoding.EncodeToString([]byte("salt")),
				"i=-1",
			},
			wantErr: "polite: invalid SCRAM iteration count: -1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nonce, salt, iter, err := parseChallenge([]byte(strings.Join(tt.parts, ",")))
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("parseChallenge() error = nil; want %q", tt.wantErr)
				}
				if err.Error() != tt.wantErr {
					t.Fatalf("parseChallenge() error = %q; want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseChallenge() error = %v", err)
			}
			if nonce != tt.wantNonce {
				t.Fatalf("nonce = %q; want %q", nonce, tt.wantNonce)
			}
			if string(salt) != tt.wantSalt {
				t.Fatalf("salt = %q; want %q", salt, tt.wantSalt)
			}
			if iter != tt.wantIter {
				t.Fatalf("iter = %d; want %d", iter, tt.wantIter)
			}
		})
	}
}

func TestVerifyServerFinal(t *testing.T) {
	want := []byte("server-signature")

	tests := []struct {
		name    string
		parts   []string
		wantErr string
	}{
		{
			name: "ok",
			parts: []string{
				"v=" + base64.StdEncoding.EncodeToString(want),
			},
		},
		{
			name: "duplicate signature",
			parts: []string{
				"v=" + base64.StdEncoding.EncodeToString(want),
				"v=" + base64.StdEncoding.EncodeToString([]byte("other-signature")),
			},
			wantErr: `polite: duplicate SASL attribute: "v"`,
		},
		{
			name: "server error wins",
			parts: []string{
				"e=invalid-proof",
				"v=" + base64.StdEncoding.EncodeToString(want),
			},
			wantErr: "polite: server SASL error: invalid-proof",
		},
		{
			name: "malformed error attribute",
			parts: []string{
				"bad",
				"v=" + base64.StdEncoding.EncodeToString(want),
			},
			wantErr: `polite: malformed SASL attribute (missing '='): "bad"`,
		},
		{
			name: "malformed signature attribute",
			parts: []string{
				"v:bad",
			},
			wantErr: `polite: malformed SASL attribute (missing '='): "v:bad"`,
		},
		{
			name: "missing signature",
			parts: []string{
				"",
			},
			wantErr: "polite: missing SCRAM server signature",
		},
		{
			name: "invalid signature encoding",
			parts: []string{
				"v=%%%",
			},
			wantErr: "polite: invalid SCRAM server signature: illegal base64 data at input byte 0",
		},
		{
			name: "signature mismatch",
			parts: []string{
				"v=" + base64.StdEncoding.EncodeToString([]byte("other-signature")),
			},
			wantErr: "polite: scram server signature mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := verifyServerFinal([]byte(strings.Join(tt.parts, ",")), want)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("verifyServerFinal() error = %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("verifyServerFinal() error = nil; want %q", tt.wantErr)
			}
			if err.Error() != tt.wantErr {
				t.Fatalf("verifyServerFinal() error = %q; want %q", err, tt.wantErr)
			}
		})
	}
}
