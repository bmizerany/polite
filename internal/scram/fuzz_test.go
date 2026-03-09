package scram

import (
	"bytes"
	"crypto/hmac"
	"encoding/base64"
	"strconv"
	"strings"
	"testing"
)

func FuzzParseChallenge(f *testing.F) {
	f.Add("r=fyko+d2lbbFgONRv9qkxdawL,s=QSXCR+Q6sek8bf92,i=4096")
	f.Add("s=QSXCR+Q6sek8bf92,i=4096")
	f.Add("r=fyko+d2lbbFgONRv9qkxdawL,i=4096")
	f.Add("r=fyko+d2lbbFgONRv9qkxdawL,s=,i=4096")
	f.Add("r=fyko+d2lbbFgONRv9qkxdawL,s=%%% ,i=4096")
	f.Add("r=fyko+d2lbbFgONRv9qkxdawL,s=QSXCR+Q6sek8bf92")
	f.Add("r=fyko+d2lbbFgONRv9qkxdawL,s=QSXCR+Q6sek8bf92,i=0")
	f.Add("r=fyko+d2lbbFgONRv9qkxdawL,s=QSXCR+Q6sek8bf92,i=-1")
	f.Add("r=fyko+d2lbbFgONRv9qkxdawL,s=QSXCR+Q6sek8bf92,i=abc")
	f.Add("r=one,r=one,s=c2FsdA==,i=4096")
	f.Add("r=one,r=two,s=c2FsdA==,i=4096")
	f.Add("r=one,s=c2FsdA==,s=c2FsdA==,i=4096")
	f.Add("r=one,s=c2FsdA==,s=bW9yZQ==,i=4096")
	f.Add("r=one,s=c2FsdA==,i=4096,i=4096")
	f.Add("r=one,s=c2FsdA==,i=4096,i=8192")
	f.Add("r=0,s=0000,i=1,0")
	f.Add("x")
	f.Add("r")
	f.Add("r:foo")
	f.Add("=foo")
	f.Add("rr=foo")
	f.Add(",,r=one,,s=c2FsdA==,,i=4096,,")

	f.Fuzz(func(t *testing.T, seed string) {
		data := []byte(seed)
		oracle := parseChallengeOracle(data)
		nonce, salt, iter, err := parseChallenge(data)
		if (err == nil) != oracle.ok {
			t.Fatalf("parseChallenge(%q) success = %v; want %v", data, err == nil, oracle.ok)
		}
		if err != nil {
			return
		}
		if nonce != oracle.nonce {
			t.Fatalf("nonce = %q; want %q", nonce, oracle.nonce)
		}
		if !bytes.Equal(salt, oracle.salt) {
			t.Fatalf("salt = %q; want %q", salt, oracle.salt)
		}
		if iter != oracle.iter {
			t.Fatalf("iter = %d; want %d", iter, oracle.iter)
		}

		canonical := serializeChallenge(oracle)
		nonce2, salt2, iter2, err := parseChallenge([]byte(canonical))
		if err != nil {
			t.Fatalf("parseChallenge(%q) = %v", canonical, err)
		}
		if nonce2 != nonce {
			t.Fatalf("round-trip nonce = %q; want %q", nonce2, nonce)
		}
		if !bytes.Equal(salt2, salt) {
			t.Fatalf("round-trip salt = %q; want %q", salt2, salt)
		}
		if iter2 != iter {
			t.Fatalf("round-trip iter = %d; want %d", iter2, iter)
		}

		oracle2 := parseChallengeOracle([]byte(canonical))
		if !oracle2.ok {
			t.Fatalf("challenge oracle rejected canonical form %q", canonical)
		}
		if oracle2.nonce != nonce || !bytes.Equal(oracle2.salt, salt) || oracle2.iter != iter {
			t.Fatalf("oracle round-trip mismatch for %q", canonical)
		}
	})
}

func FuzzParseServerFinal(f *testing.F) {
	want := []byte("expected-server-signature")
	wantText := base64.StdEncoding.EncodeToString(want)

	for _, seed := range []string{
		"v=" + wantText,
		"v=" + base64.StdEncoding.EncodeToString([]byte("wrong-server-signature")),
		"e=invalid-proof",
		"e=invalid-proof,v=" + wantText,
		"v=" + wantText + ",e=invalid-proof",
		"v=" + wantText + ",v=" + wantText,
		"v=" + wantText + ",v=" + base64.StdEncoding.EncodeToString([]byte("other")),
		"e=invalid-proof,e=invalid-proof",
		"e=invalid-proof,e=other",
		"e=0,0",
		"v=",
		"v=%%%",
		"v",
		"v:foo",
		"=foo",
		"vv=foo",
		"",
		",,v=" + wantText + ",,",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, seed string) {
		data := []byte(seed)
		oracle := parseServerFinalOracle(data, want)
		got := classifyServerFinal(verifyServerFinal(data, want))
		if got != oracle.class {
			t.Fatalf("verifyServerFinal(%q) = %v; want %v", data, got, oracle.class)
		}

		canonical := serializeServerFinal(oracle)
		got2 := classifyServerFinal(verifyServerFinal([]byte(canonical), want))
		if got2 != oracle.class {
			t.Fatalf("verifyServerFinal(%q) = %v; want %v", canonical, got2, oracle.class)
		}

		oracle2 := parseServerFinalOracle([]byte(canonical), want)
		if oracle2.class != oracle.class {
			t.Fatalf("server-final oracle round-trip = %v; want %v", oracle2.class, oracle.class)
		}
	})
}

type challengeResult struct {
	ok    bool
	nonce string
	salt  []byte
	iter  int
}

func parseChallengeOracle(data []byte) challengeResult {
	nonce, ok := firstAttrOracle(data, 'r')
	if !ok {
		return challengeResult{}
	}
	if nonce == "" {
		return challengeResult{}
	}

	saltText, ok := firstAttrOracle(data, 's')
	if !ok {
		return challengeResult{}
	}
	salt, err := base64.StdEncoding.DecodeString(saltText)
	if err != nil || len(salt) == 0 {
		return challengeResult{}
	}

	iterText, ok := firstAttrOracle(data, 'i')
	if !ok {
		return challengeResult{}
	}
	iter, err := strconv.Atoi(iterText)
	if err != nil || iter <= 0 {
		return challengeResult{}
	}

	return challengeResult{
		ok:    true,
		nonce: nonce,
		salt:  salt,
		iter:  iter,
	}
}

func serializeChallenge(r challengeResult) string {
	return "r=" + r.nonce + ",s=" + base64.StdEncoding.EncodeToString(r.salt) + ",i=" + strconv.Itoa(r.iter)
}

type serverFinalClass int

const (
	serverFinalOK serverFinalClass = iota
	serverFinalMalformed
	serverFinalServerError
	serverFinalMissingSignature
	serverFinalInvalidSignature
	serverFinalMismatch
)

type serverFinalResult struct {
	class   serverFinalClass
	errText string
	sigText string
}

func parseServerFinalOracle(data, want []byte) serverFinalResult {
	errText, ok := firstAttrOracle(data, 'e')
	if !ok {
		return serverFinalResult{class: serverFinalMalformed}
	}
	if errText != "" {
		return serverFinalResult{
			class:   serverFinalServerError,
			errText: errText,
		}
	}

	sigText, ok := firstAttrOracle(data, 'v')
	if !ok {
		return serverFinalResult{class: serverFinalMalformed}
	}
	if sigText == "" {
		return serverFinalResult{class: serverFinalMissingSignature}
	}

	have, err := base64.StdEncoding.DecodeString(sigText)
	if err != nil {
		return serverFinalResult{
			class:   serverFinalInvalidSignature,
			sigText: sigText,
		}
	}
	if !hmac.Equal(have, want) {
		return serverFinalResult{
			class:   serverFinalMismatch,
			sigText: sigText,
		}
	}
	return serverFinalResult{
		class:   serverFinalOK,
		sigText: base64.StdEncoding.EncodeToString(have),
	}
}

func serializeServerFinal(r serverFinalResult) string {
	if r.class == serverFinalMalformed {
		return "x"
	}
	if r.errText != "" {
		return "e=" + r.errText
	}
	if r.sigText != "" {
		return "v=" + r.sigText
	}
	return "v="
}

func classifyServerFinal(err error) serverFinalClass {
	switch {
	case err == nil:
		return serverFinalOK
	case strings.Contains(err.Error(), "malformed SASL attribute"):
		return serverFinalMalformed
	case strings.Contains(err.Error(), "server SASL error"):
		return serverFinalServerError
	case strings.Contains(err.Error(), "missing SCRAM server signature"):
		return serverFinalMissingSignature
	case strings.Contains(err.Error(), "invalid SCRAM server signature"):
		return serverFinalInvalidSignature
	case strings.Contains(err.Error(), "scram server signature mismatch"):
		return serverFinalMismatch
	default:
		return -1
	}
}

func firstAttrOracle(data []byte, want byte) (string, bool) {
	for part := range strings.SplitSeq(string(data), ",") {
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok || len(key) != 1 {
			return "", false
		}
		if key[0] == want {
			return value, true
		}
	}
	return "", true
}
