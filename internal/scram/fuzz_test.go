package scram

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"hash"
	"io"
	"strconv"
	"strings"
	"testing"

	"blake.io/polite/febe"
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

func truncate[S ~string | ~[]byte](s S, n int) S {
	return s[:min(n, len(s))]
}

func FuzzSCRAMComputation(f *testing.F) {
	f.Add("user", "pencil", "client", "server", []byte("salt"), uint16(4096), byte(0))
	f.Add("user,=name", "secret", "nonce", "suffix", []byte("salt"), uint16(1), byte(1))
	f.Add("user", "password", "client", "server", []byte("more-salt"), uint16(8192), byte(2))
	f.Add("user", "password", "", "server", []byte("salt"), uint16(4096), byte(3))
	f.Add("user", "password", "client", "", []byte("salt"), uint16(4096), byte(4))
	f.Add("user", "password", "client", "server", []byte(nil), uint16(4096), byte(5))
	f.Add("user", "password", "client", "server", []byte("salt"), uint16(0), byte(6))

	f.Fuzz(func(t *testing.T, user, password, clientNonce, serverSuffix string, salt []byte, iterSeed uint16, mutation byte) {
		var s state
		s.user = truncate(user, 32)
		s.password = truncate(password, 32)
		s.salt = truncate(salt, 32)
		s.clientNonce = truncate(s.clientNonce, 32)
		s.serverNonce = s.clientNonce + truncate(serverSuffix, 24)
		s.iter = int(iterSeed % 8193)

		got0, ok := referenceSCRAM(sha256.New, s)
		if !ok {
			return
		}
		got1, ok := referenceSCRAM(sha256.New, s)
		if !ok {
			t.Fatalf("referenceSCRAM lost validity on identical input")
		}
		if got0.clientFinal != got1.clientFinal {
			t.Fatalf("client final is not deterministic")
		}
		if !bytes.Equal(got0.serverSig, got1.serverSig) {
			t.Fatalf("server signature is not deterministic")
		}
		if err := verifyServerFinal(serverFinalMessage(got0.serverSig), got0.serverSig); err != nil {
			t.Fatalf("verifyServerFinal(valid signature) = %v", err)
		}

		s = mutateStructuredSCRAM(s, mutation)
		got3, ok := referenceSCRAM(sha256.New, s)
		if !ok {
			return
		}
		if got0.clientFinal == got3.clientFinal && bytes.Equal(got0.serverSig, got3.serverSig) {
			t.Fatalf("mutation did not change proof or server signature")
		}
		if classifyServerFinal(verifyServerFinal(serverFinalMessage(got0.serverSig), got3.serverSig)) != serverFinalMismatch {
			t.Fatalf("original signature verified against mutated transcript")
		}
	})
}

func FuzzSCRAMHandshake(f *testing.F) {
	f.Add(byte(0), byte(3), "user", "pencil", "server", []byte("salt"), uint16(4096))
	f.Add(byte(1), byte(3), "user", "pencil", "server", []byte("salt"), uint16(4096))
	f.Add(byte(2), byte(3), "user", "pencil", "server", []byte("salt"), uint16(4096))
	f.Add(byte(3), byte(4), "user", "pencil", "server", []byte("salt"), uint16(4096))
	f.Add(byte(4), byte(3), "user", "", "server", []byte("salt"), uint16(4096))
	f.Add(byte(5), byte(3), "user", "pencil", "", []byte("salt"), uint16(4096))
	f.Add(byte(6), byte(3), "user", "pencil", "server", []byte("salt"), uint16(0))

	f.Fuzz(func(t *testing.T, scenario, mechanismBits byte, user, password, serverSuffix string, salt []byte, iterSeed uint16) {
		var s state
		s.user = truncate(user, 32)
		s.password = truncate(password, 32)
		s.salt = truncate(salt, 32)
		s.clientNonce = truncate(fixedNonce(), 32)
		s.serverNonce = s.clientNonce + truncate(serverSuffix, 24)
		s.iter = int(iterSeed % 8193)

		clientFirst := "n=" + escapeUsernameRE.Replace(s.user) + ",r=" + s.clientNonce

		mechanisms := handshakeMechanisms(mechanismBits)
		selected, newHash, hashErr := getHasher(mechanisms)

		continueType := febe.AuthenticationTypeSASLContinue
		okType := febe.AuthenticationTypeOK
		switch scenario % 7 {
		case 1:
			s.serverNonce = "wrong" + truncate(serverSuffix, 24)
		case 2:
			continueType = febe.AuthenticationTypeSASLFinal
		case 3:
			okType = febe.AuthenticationTypeCleartext
		case 4:
			mechanisms = []string{"PLAIN"}
			selected, newHash, hashErr = getHasher(mechanisms)
		}

		serverFirst := challengeMessage(s.serverNonce, s.salt, s.iter)
		want, ok := referenceSCRAM(newHash, s)
		if hashErr != nil || s.password == "" {
			ok = false
		}

		finalData := []byte("e=invalid-proof")
		if ok {
			finalData = serverFinalMessage(want.serverSig)
		}
		switch scenario % 7 {
		case 5:
			finalData = serverFinalMessage(flipBytes(want.serverSig))
		case 6:
			serverFirst = []byte("r=" + s.serverNonce + ",s=%%%,i=4096")
			ok = false
		}

		got, err := runHandshake(t, mechanisms, continueType, serverFirst, finalData, okType, s.user, s.password)

		wantInitial := hashErr == nil && s.password != ""
		if (got.initial != nil) != wantInitial {
			t.Fatalf("sent initial = %v; want %v", got.initial != nil, wantInitial)
		}
		if !wantInitial {
			if err == nil {
				t.Fatalf("Handshake() = nil; want error")
			}
			return
		}

		if got.initial.mechanism != selected {
			t.Fatalf("mechanism = %q; want %q", got.initial.mechanism, selected)
		}
		if string(got.initial.data) != "n,,"+clientFirst {
			t.Fatalf("client first = %q; want %q", got.initial.data, "n,,"+clientFirst)
		}

		wantSuccess := ok &&
			continueType == febe.AuthenticationTypeSASLContinue &&
			okType == febe.AuthenticationTypeOK &&
			string(finalData) == string(serverFinalMessage(want.serverSig))

		if got.final != nil && ok && string(got.final) != want.clientFinal {
			t.Fatalf("client final = %q; want %q", got.final, want.clientFinal)
		}
		if (err == nil) != wantSuccess {
			t.Fatalf("Handshake() success = %v; want %v; err=%v", err == nil, wantSuccess, err)
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
	case strings.Contains(err.Error(), "malformed SASL attribute"), strings.Contains(err.Error(), "duplicate SASL attribute"):
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
	seen := false
	var have string
	for part := range strings.SplitSeq(string(data), ",") {
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok || len(key) != 1 {
			return "", false
		}
		if key[0] == want {
			if seen {
				return "", false
			}
			have = value
			seen = true
		}
	}
	return have, true
}

type referenceResult struct {
	clientFirst string
	clientFinal string
	serverSig   []byte
}

func referenceSCRAM(newHash func() hash.Hash, state state) (referenceResult, bool) {
	if newHash == nil || state.clientNonce == "" || state.serverNonce == "" || len(state.salt) == 0 || state.iter <= 0 {
		return referenceResult{}, false
	}
	if strings.Contains(state.clientNonce, ",") || strings.Contains(state.serverNonce, ",") {
		return referenceResult{}, false
	}
	if !strings.HasPrefix(state.serverNonce, state.clientNonce) || len(state.serverNonce) <= len(state.clientNonce) {
		return referenceResult{}, false
	}

	clientFirst := "n=" + escapeUsernameRE.Replace(state.user) + ",r=" + state.clientNonce
	serverFirst := string(challengeMessage(state.serverNonce, state.salt, state.iter))
	withoutProof := "c=" + channelBinding + ",r=" + state.serverNonce
	authText := clientFirst + "," + serverFirst + "," + withoutProof
	hmacSum := func(key []byte, msg string) []byte {
		mac := hmac.New(newHash, key)
		io.WriteString(mac, msg)
		return mac.Sum(nil)
	}

	salted, err := pbkdf2.Key(newHash, state.password, state.salt, state.iter, newHash().Size())
	if err != nil {
		return referenceResult{}, false
	}
	clientKey := hmacSum(salted, "Client Key")
	storedKey := newHash()
	storedKey.Write(clientKey)
	clientSig := hmacSum(storedKey.Sum(nil), authText)
	clientProof := bytes.Clone(clientKey)
	for i := range clientProof {
		clientProof[i] ^= clientSig[i]
	}

	return referenceResult{
		clientFirst: clientFirst,
		clientFinal: withoutProof + ",p=" + base64.StdEncoding.EncodeToString(clientProof),
		serverSig:   hmacSum(hmacSum(salted, "Server Key"), authText),
	}, true
}

type state struct {
	user        string
	password    string
	clientNonce string
	serverNonce string
	salt        []byte
	iter        int
}

func mutateStructuredSCRAM(s state, mutation byte) state {
	switch mutation % 5 {
	case 0:
		s.password = flipString(s.password)
		return s
	case 1:
		s.salt = flipBytes(s.salt)
		return s
	case 2:
		s.clientNonce = flipString(s.clientNonce)
		suffix := strings.TrimPrefix(s.serverNonce, s.clientNonce)
		s.serverNonce = s.clientNonce + suffix
		return s
	case 3:
		suffix := strings.TrimPrefix(s.serverNonce, s.clientNonce)
		s.serverNonce = s.clientNonce + flipString(suffix)
		return s
	default:
		s.iter++
		return s
	}
}

func handshakeMechanisms(bits byte) []string {
	var mechanisms []string
	if bits&1 != 0 {
		mechanisms = append(mechanisms, "SCRAM-SHA-256")
	}
	if bits&2 != 0 {
		mechanisms = append(mechanisms, "SCRAM-SHA-512")
	}
	if bits&4 != 0 {
		mechanisms = append(mechanisms, "PLAIN")
	}
	if len(mechanisms) == 0 {
		mechanisms = []string{"SCRAM-SHA-256", "SCRAM-SHA-512"}
	}
	return mechanisms
}

type clientMessages struct {
	initial *clientInitial
	final   []byte
}

type clientInitial struct {
	mechanism string
	data      []byte
}

func runHandshake(t *testing.T, mechanisms []string, continueType febe.AuthenticationType, continueData, finalData []byte, okType febe.AuthenticationType, user, password string) (clientMessages, error) {
	t.Helper()

	var server bytes.Buffer
	writeAuthentication(&server, continueType, continueData)
	writeAuthentication(&server, febe.AuthenticationTypeSASLFinal, finalData)
	writeAuthentication(&server, okType, nil)

	var client bytes.Buffer
	err := withFixedRand(func() error {
		return Handshake(context.Background(), febe.New(bytes.NewReader(server.Bytes()), &client), febe.AuthenticationResponse{
			Type:           febe.AuthenticationTypeSASL,
			SASLMechanisms: mechanisms,
		}, user, password)
	})

	got, parseErr := readClientMessages(client.Bytes())
	if parseErr != nil {
		t.Fatalf("read client messages: %v", parseErr)
	}
	return got, err
}

func writeAuthentication(w io.Writer, typ febe.AuthenticationType, data []byte) {
	var header [9]byte
	header[0] = 'R'
	binary.BigEndian.PutUint32(header[1:5], uint32(8+len(data)))
	binary.BigEndian.PutUint32(header[5:9], uint32(typ))
	if _, err := w.Write(header[:]); err != nil {
		panic(err)
	}
	if len(data) == 0 {
		return
	}
	if _, err := w.Write(data); err != nil {
		panic(err)
	}
}

func readClientMessages(data []byte) (clientMessages, error) {
	var got clientMessages

	msg, err := readMessage(data)
	if err != nil || !msg.ok {
		return got, err
	}
	initial, err := parseClientInitial(msg.body)
	if err != nil {
		return got, err
	}
	got.initial = &initial

	msg, err = readMessage(msg.rest)
	if err != nil {
		return got, err
	}
	if msg.ok {
		got.final = msg.body
	}
	if len(msg.rest) != 0 {
		return got, io.ErrUnexpectedEOF
	}
	return got, nil
}

type clientMessage struct {
	body []byte
	rest []byte
	ok   bool
}

func readMessage(data []byte) (clientMessage, error) {
	if len(data) == 0 {
		return clientMessage{}, nil
	}
	if len(data) < 5 {
		return clientMessage{}, io.ErrUnexpectedEOF
	}
	if data[0] != 'p' {
		return clientMessage{}, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint32(data[1:5]))
	if n < 4 {
		return clientMessage{}, io.ErrUnexpectedEOF
	}
	end := 1 + n
	if len(data) < end {
		return clientMessage{}, io.ErrUnexpectedEOF
	}
	return clientMessage{
		body: bytes.Clone(data[5:end]),
		rest: data[end:],
		ok:   true,
	}, nil
}

func parseClientInitial(body []byte) (clientInitial, error) {
	mechEnd := bytes.IndexByte(body, 0)
	if mechEnd < 0 || len(body) < mechEnd+5 {
		return clientInitial{}, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint32(body[mechEnd+1 : mechEnd+5]))
	data := body[mechEnd+5:]
	if n != len(data) {
		return clientInitial{}, io.ErrUnexpectedEOF
	}
	return clientInitial{
		mechanism: string(body[:mechEnd]),
		data:      bytes.Clone(data),
	}, nil
}

func challengeMessage(serverNonce string, salt []byte, iter int) []byte {
	return []byte("r=" + serverNonce + ",s=" + base64.StdEncoding.EncodeToString(salt) + ",i=" + strconv.Itoa(iter))
}

func serverFinalMessage(sig []byte) []byte {
	return []byte("v=" + base64.StdEncoding.EncodeToString(sig))
}

func withFixedRand(fn func() error) error {
	old := crand.Reader
	crand.Reader = bytes.NewReader(bytes.Repeat([]byte{0x42}, 256))
	defer func() { crand.Reader = old }()
	return fn()
}

func fixedNonce() string {
	old := crand.Reader
	crand.Reader = bytes.NewReader(bytes.Repeat([]byte{0x42}, 256))
	defer func() { crand.Reader = old }()
	return crand.Text()[:16]
}

func flipBytes(data []byte) []byte {
	if len(data) == 0 {
		return []byte{1}
	}
	out := bytes.Clone(data)
	out[0] ^= 1
	return out
}

func flipString(s string) string {
	if s == "" {
		return "\x01"
	}
	b := []byte(s)
	b[0] ^= 1
	return string(b)
}
