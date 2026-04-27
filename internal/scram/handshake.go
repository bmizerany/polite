// Package scram implements the client side of PostgreSQL SCRAM SASL
// authentication over febe connections.
//
// Note: this package has been fuzz tested.
package scram

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
	"slices"
	"strconv"
	"strings"

	"blake.io/polite/febe"
)

const (
	channelBinding     = "biws"
	maxSCRAMIterations = 1_000_000
)

var escapeUsernameRE = strings.NewReplacer("=", "=3D", ",", "=2C")

func Handshake(ctx context.Context, cn *febe.Conn, cr febe.AuthenticationResponse, user, password string) error {
	next := func(want febe.AuthenticationType) (febe.AuthenticationResponse, error) {
		var zero febe.AuthenticationResponse
		if err := cn.Flush(); err != nil {
			return zero, err
		}
		if _, err := cn.ReadHeader(ctx); err != nil {
			return zero, err
		}
		cr, err := cn.ReadAuthentication()
		if err != nil {
			return zero, err
		}
		if cr.Type != want {
			return zero, fmt.Errorf("unexpected authentication type %s; want %d", cr.Type, want)
		}
		return cr, nil
	}

	usingMechanism, newHash, err := getHasher(cr.SASLMechanisms)
	if err != nil {
		return err
	}
	if password == "" {
		return fmt.Errorf("polite: password is required for %q authentication", usingMechanism)
	}

	nonce := rand.Text()[:16]

	// Send the client-first message with the username and client nonce.
	clientFirst := "n=" + escapeUsernameRE.Replace(user) + ",r=" + nonce
	var b bytes.Buffer
	b.WriteString("n,,")
	b.WriteString(clientFirst)
	if err := cn.WriteSASLInitialResponse(usingMechanism, b.Bytes()); err != nil {
		return err
	}

	serverFirst, err := next(febe.AuthenticationTypeSASLContinue)
	if err != nil {
		return err
	}
	serverNonce, salt, iter, err := parseChallenge(serverFirst.SASLData)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(serverNonce, nonce) || len(serverNonce) <= len(nonce) {
		return fmt.Errorf("polite: invalid SCRAM server nonce: %q", serverNonce)
	}

	withoutProof := "c=" + channelBinding + ",r=" + serverNonce

	// Compute the client proof and the auth message used later to verify the
	// server final message.
	//
	//   saltedPassword  := Hi(password, salt, iterations)
	//   clientKey       := HMAC(saltedPassword, "Client Key")
	//   storedKey       := H(clientKey)
	//   authMessage     := clientFirst + "," + serverFirst + "," + withoutProof
	//   clientSignature := HMAC(storedKey, authMessage)
	//   clientProof     := clientKey XOR clientSignature
	//
	// See: https://datatracker.ietf.org/doc/html/rfc5802#section-3
	b.Reset()
	b.WriteString(clientFirst)
	b.WriteByte(',')
	b.Write(serverFirst.SASLData)
	b.WriteByte(',')
	b.WriteString(withoutProof)
	authText := b.String()
	hmacSum := func(key []byte, msg string) []byte {
		mac := hmac.New(newHash, key)
		io.WriteString(mac, msg)
		return mac.Sum(nil)
	}

	// Hi is PBKDF2 with the negotiated hash and an output length equal to the
	// hash size.
	salted, err := pbkdf2.Key(newHash, password, salt, iter, newHash().Size())
	if err != nil {
		return err
	}
	clientKey := hmacSum(salted, "Client Key")
	storedKey := newHash()
	storedKey.Write(clientKey)
	clientSig := hmacSum(storedKey.Sum(nil), authText)
	for i := range clientKey {
		clientKey[i] ^= clientSig[i]
	}

	// Send the client proof.
	b.Reset()
	b.WriteString(withoutProof)
	b.WriteString(",p=")
	b.WriteString(base64.StdEncoding.EncodeToString(clientKey))
	if err := cn.WriteSASLResponse(b.Bytes()); err != nil {
		return err
	}

	// Verify the server final message. On success it carries the server
	// signature:
	//
	//   serverKey       := HMAC(saltedPassword, "Server Key")
	//   serverSignature := HMAC(serverKey, authMessage)
	//
	// See: https://datatracker.ietf.org/doc/html/rfc5802#section-3
	//
	// If the server signature doesn't match what we expect, then the
	// authentication has failed.
	serverFinal, err := next(febe.AuthenticationTypeSASLFinal)
	if err != nil {
		return err
	}
	if err := verifyServerFinal(serverFinal.SASLData, hmacSum(hmacSum(salted, "Server Key"), authText)); err != nil {
		return err
	}
	_, err = next(febe.AuthenticationTypeOK)
	return err
}

// supportedHashers lists supported SCRAM hash functions in client preference
// order.
var supportedHashers = []struct {
	name    string
	newHash func() hash.Hash
}{
	{"SCRAM-SHA-512", sha512.New},
	{"SCRAM-SHA-256", sha256.New},
}

func getHasher(mechanisms []string) (using string, _ func() hash.Hash, _ error) {
	for _, h := range supportedHashers {
		if slices.Contains(mechanisms, h.name) {
			return h.name, h.newHash, nil
		}
	}
	return "", nil, fmt.Errorf("polite: unsupported SASL mechanism(s): %s", strings.Join(mechanisms, ", "))
}

func parseChallenge(data []byte) (nonce string, salt []byte, iter int, err error) {
	nonce, err = attr(data, 'r')
	if err != nil {
		return "", nil, 0, err
	}
	if nonce == "" {
		return "", nil, 0, fmt.Errorf("polite: missing SCRAM server nonce")
	}

	saltText, err := attr(data, 's')
	if err != nil {
		return "", nil, 0, err
	}
	salt, err = base64.StdEncoding.DecodeString(saltText)
	if err != nil {
		return "", nil, 0, fmt.Errorf("polite: error decoding SCRAM salt: %w", err)
	}
	if len(salt) == 0 {
		return "", nil, 0, fmt.Errorf("polite: empty SCRAM salt")
	}

	iterText, err := attr(data, 'i')
	if err != nil {
		return "", nil, 0, err
	}
	iter, err = strconv.Atoi(iterText)
	if err != nil {
		return "", nil, 0, fmt.Errorf("polite: error decoding SCRAM iteration count: %w", err)
	}
	if iter <= 0 {
		return "", nil, 0, fmt.Errorf("polite: invalid SCRAM iteration count: %d", iter)
	}
	if iter > maxSCRAMIterations {
		return "", nil, 0, fmt.Errorf("polite: invalid SCRAM iteration count: %d", iter)
	}
	return nonce, salt, iter, nil
}

func verifyServerFinal(data, want []byte) error {
	errText, err := attr(data, 'e')
	if err != nil {
		return err
	}
	if errText != "" {
		return fmt.Errorf("polite: server SASL error: %s", errText)
	}
	sig, err := attr(data, 'v')
	if err != nil {
		return err
	}
	if sig == "" {
		return fmt.Errorf("polite: missing SCRAM server signature")
	}
	have, err := base64.StdEncoding.DecodeString(sig)
	if err != nil {
		return fmt.Errorf("polite: invalid SCRAM server signature: %w", err)
	}
	if !hmac.Equal(have, want) {
		return fmt.Errorf("polite: scram server signature mismatch")
	}
	return nil
}

func attr(data []byte, want byte) (string, error) {
	var (
		have string
		seen bool
	)
	for part := range strings.SplitSeq(string(data), ",") {
		if part == "" {
			continue
		}
		key, attr, ok := strings.Cut(part, "=")
		if !ok || len(key) != 1 {
			return "", fmt.Errorf("polite: malformed SASL attribute (missing '='): %q", part)
		}
		if key[0] == want {
			if seen {
				return "", fmt.Errorf("polite: duplicate SASL attribute: %q", key)
			}
			have = attr
			seen = true
		}
	}
	return have, nil
}
