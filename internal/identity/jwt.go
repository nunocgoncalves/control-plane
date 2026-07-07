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
	"errors"
	"fmt"
	"math/big"
	"os"
	"time"
)

// Issuer signs RS256 JWTs and publishes the corresponding JWKS. The RSA private
// key is loaded from a file path (a Kubernetes Secret mounted as a volume); the
// api never calls the Kubernetes API for it.
type Issuer struct {
	key      *rsa.PrivateKey
	keyID    string
	issuer   string
	audience string
	ttl      time.Duration
}

// NewIssuer loads the RSA private key PEM from path and returns an Issuer ready
// to mint tokens and serve JWKS.
func NewIssuer(path, keyID, issuer, audience, ttl string) (*Issuer, error) {
	if path == "" {
		return nil, errors.New("identity: jwt signing key path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("identity: reading jwt signing key: %w", err)
	}
	key, err := parseRSAPrivateKey(data)
	if err != nil {
		return nil, fmt.Errorf("identity: parsing jwt signing key: %w", err)
	}
	d, err := time.ParseDuration(ttl)
	if err != nil {
		return nil, fmt.Errorf("identity: invalid jwt ttl %q: %w", ttl, err)
	}
	if d <= 0 {
		return nil, errors.New("identity: jwt ttl must be positive")
	}
	kid := keyID
	if kid == "" {
		kid = "default"
	}
	return &Issuer{key: key, keyID: kid, issuer: issuer, audience: audience, ttl: d}, nil
}

// KeyID returns the key identifier published in the JWT header and JWKS.
func (i *Issuer) KeyID() string { return i.keyID }

// Issue signs a JWT for the given identity ID. The token carries iss/sub/aud/
// iat/exp (and kid in the header); capabilities are intentionally omitted — the
// gateway enforces them from its permission snapshot (HOR-243/247).
func (i *Issuer) Issue(identityID string) (string, error) {
	now := time.Now().UTC()
	claims := jwtClaims{
		Issuer:    i.issuer,
		Subject:   identityID,
		Audience:  i.audience,
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(i.ttl).Unix(),
	}
	header := map[string]string{"alg": "RS256", "typ": "JWT", "kid": i.keyID}

	hb, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("marshal header: %w", err)
	}
	cb, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshal claims: %w", err)
	}

	signingInput := b64url(hb) + "." + b64url(cb)
	sum := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, i.key, crypto.SHA256, sum[:])
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	return signingInput + "." + b64url(sig), nil
}

// JWKS returns the JSON Web Key Set (the RSA public key) for the
// /.well-known/jwks.json endpoint.
func (i *Issuer) JWKS() ([]byte, error) {
	jwk, err := i.publicJWK()
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"keys": []any{jwk}})
}

func (i *Issuer) publicJWK() (map[string]string, error) {
	pub := &i.key.PublicKey
	// n and e are the big-endian unsigned representations required by RFC 7518.
	// Encoding the exponent via big.Int avoids any int->uint narrowing.
	e := new(big.Int).SetInt64(int64(pub.E)).Bytes()
	return map[string]string{
		"kty": "RSA",
		"use": "sig",
		"alg": "RS256",
		"kid": i.keyID,
		"n":   b64url(pub.N.Bytes()),
		"e":   b64url(e),
	}, nil
}

type jwtClaims struct {
	Issuer    string `json:"iss"`
	Subject   string `json:"sub"`
	Audience  string `json:"aud"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
}

// parseRSAPrivateKey parses a PEM-encoded RSA private key (PKCS#1 or PKCS#8).
func parseRSAPrivateKey(data []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("no PEM block found")
	}
	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return k, nil
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	rk, ok := k.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not RSA")
	}
	return rk, nil
}

// b64url is base64 raw URL encoding without padding (JWT/JWK standard).
func b64url(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}
