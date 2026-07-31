// Package testca generates an in-memory self-signed CA + leaf certificates
// with SPIFFE URI SANs for hermetic mTLS integration tests (HOR-392). It
// mirrors the harness testserver's cert generation but uses URI SANs (the real
// SPIFFE identity carrier) instead of CommonName.
package testca

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/url"
	"time"
)

// CA is an in-memory self-signed CA for tests.
type CA struct {
	Cert    *x509.Certificate
	Key     *ecdsa.PrivateKey
	CertPEM []byte
	Pool    *x509.CertPool
}

// New generates a fresh self-signed CA.
func New() (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		Subject:               pkix.Name{CommonName: "gateway-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &CA{Cert: cert, Key: key, CertPEM: certPEM, Pool: pool}, nil
}

// LeafOpts configures a leaf certificate.
type LeafOpts struct {
	// SPIFFEID is the URI SAN carried by the leaf (the workload identity).
	SPIFFEID string
	// DNSNames are additional DNS SANs (e.g. ["localhost"] for a server cert).
	DNSNames []string
	// IsServer marks the leaf for server auth (adds ExtKeyUsageServerAuth).
	IsServer bool
}

// Leaf issues a leaf certificate signed by the CA.
func (c *CA) Leaf(opts LeafOpts) (tls.Certificate, error) {
	if opts.SPIFFEID == "" {
		return tls.Certificate{}, fmt.Errorf("testca: SPIFFEID required")
	}
	uri, err := url.Parse(opts.SPIFFEID)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("testca: parse spiffe id: %w", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	eku := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	if opts.IsServer {
		eku = append(eku, x509.ExtKeyUsageServerAuth)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: opts.SPIFFEID},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  eku,
		DNSNames:     opts.DNSNames,
		URIs:         []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, c.Cert, &key.PublicKey, c.Key)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
}
