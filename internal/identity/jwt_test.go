package identity

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTestKey generates a 2048-bit RSA key, writes it as a PKCS#1 PEM to a
// temp file, and returns the path + the key for verification.
func writeTestKey(t *testing.T) (string, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	der := x509.MarshalPKCS1PrivateKey(key)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: der})
	path := t.TempDir() + "/key.pem"
	require.NoError(t, os.WriteFile(path, pemBytes, 0o600))
	return path, key
}

func TestIssuer_IssueAndVerify(t *testing.T) {
	path, key := writeTestKey(t)

	iss, err := NewIssuer(path, "kid-1", "control-plane", "inference-gateway", "15m")
	require.NoError(t, err)
	assert.Equal(t, "kid-1", iss.KeyID())

	token, err := iss.Issue("11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)

	parts := strings.Split(token, ".")
	require.Len(t, parts, 3, "JWT must have 3 parts")

	// Header.
	header, err := decodeJSON(parts[0])
	require.NoError(t, err)
	assert.Equal(t, "RS256", header["alg"])
	assert.Equal(t, "JWT", header["typ"])
	assert.Equal(t, "kid-1", header["kid"])

	// Claims.
	claims, err := decodeJSON(parts[1])
	require.NoError(t, err)
	assert.Equal(t, "control-plane", claims["iss"])
	assert.Equal(t, "11111111-1111-1111-1111-111111111111", claims["sub"])
	assert.Equal(t, "inference-gateway", claims["aud"])
	assert.Greater(t, claims["exp"], claims["iat"])

	// Signature verifies with the public key.
	signingInput := parts[0] + "." + parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	require.NoError(t, err)
	sum := sha256.Sum256([]byte(signingInput))
	assert.NoError(t, rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, sum[:], sig),
		"signature must verify against the issuer public key")
}

func TestIssuer_JWKS(t *testing.T) {
	path, key := writeTestKey(t)

	iss, err := NewIssuer(path, "kid-1", "cp", "gw", "5m")
	require.NoError(t, err)

	out, err := iss.JWKS()
	require.NoError(t, err)

	var jwks struct {
		Keys []map[string]string `json:"keys"`
	}
	require.NoError(t, json.Unmarshal(out, &jwks))
	require.Len(t, jwks.Keys, 1)
	jwk := jwks.Keys[0]
	assert.Equal(t, "RSA", jwk["kty"])
	assert.Equal(t, "sig", jwk["use"])
	assert.Equal(t, "RS256", jwk["alg"])
	assert.Equal(t, "kid-1", jwk["kid"])

	// The JWKS n/e must reconstruct the issuer public key.
	nBytes, err := base64.RawURLEncoding.DecodeString(jwk["n"])
	require.NoError(t, err)
	eBytes, err := base64.RawURLEncoding.DecodeString(jwk["e"])
	require.NoError(t, err)
	e := new(big.Int).SetBytes(eBytes).Int64()

	reconstructed := &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: int(e)}
	assert.Equal(t, key.N.String(), reconstructed.N.String())
	assert.Equal(t, key.E, reconstructed.E)
}

func TestNewIssuer_Errors(t *testing.T) {
	_, err := NewIssuer("", "k", "i", "a", "15m")
	require.Error(t, err)

	_, err = NewIssuer("/nonexistent", "k", "i", "a", "15m")
	require.Error(t, err)

	path, _ := writeTestKey(t)
	_, err = NewIssuer(path, "k", "i", "a", "nope")
	require.Error(t, err)
}

func decodeJSON(part string) (map[string]any, error) {
	b, err := base64.RawURLEncoding.DecodeString(part)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	return m, json.Unmarshal(b, &m)
}
