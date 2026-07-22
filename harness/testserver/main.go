// Command harness-testserver is a minimal in-memory connect-go gRPC+mTLS server
// for the HOR-381 transport integration test (tier 2). It has no Postgres and
// no real dispatcher: it drives a fixed Work-stream scenario against the REAL
// TypeScript client (createGrpcTransport — native gRPC over HTTP/2 + mTLS),
// exercising Hello/Welcome/fencing, AssignTurn, EventAck, Heartbeat,
// TokenDelta receipt, AbortTurn, stream-loss + reconnect, unacked-tail replay
// + ACK, and Ready-after-replay. It refuses HTTP/1.1 (HTTP/2-only TLS) and
// verifies the client cert (mTLS identity).
//
// On startup it prints a JSON "ready" line with the listen address + the paths
// of the in-memory-generated CA/server/client PEM files. When the scenario
// completes it prints a JSON "report" line and exits 0/1.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	connect "connectrpc.com/connect"
	v1 "github.com/nunocgoncalves/control-plane/internal/harnessrpc/iterabase/harness/v1"
	"github.com/nunocgoncalves/control-plane/internal/harnessrpc/iterabase/harness/v1/harnessv1connect"
)

const (
	wantWorker = "pod-1"
	wantPool   = "pool-1"
	turnID     = "turn-1"
)

type report struct {
	Handshake          bool     `json:"handshake"`
	IdentityVerified   bool     `json:"identityVerified"`
	AssignTurnSent     bool     `json:"assignTurnSent"`
	AckMatched         bool     `json:"ackMatched"`
	TokenDeltaReceived bool     `json:"tokenDeltaReceived"`
	HeartbeatReceived  bool     `json:"heartbeatReceived"`
	AbortTurnSent      bool     `json:"abortTurnSent"`
	ReplayAcked        bool     `json:"replayAcked"`
	ReadyAfterReplay   bool     `json:"readyAfterReplay"`
	HTTP2Only          bool     `json:"http2Only"`
	FencingGenerations []uint64 `json:"fencingGenerations"`
	Error              string   `json:"error,omitempty"`
}

type server struct {
	mu       sync.Mutex
	rep      report
	conns    int64
	clientCN string
	done     bool
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "testserver: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	tmp, err := os.MkdirTemp("", "harness-testserver-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	ca, serverCert, clientCert, err := genCerts()
	if err != nil {
		return err
	}
	caPath, srvCrt, srvKey, cliCrt, cliKey := tmp+"/ca.pem", tmp+"/server.crt", tmp+"/server.key", tmp+"/client.crt", tmp+"/client.key"
	if err := writePEM(caPath, "CERTIFICATE", ca.CertPEM); err != nil {
		return err
	}
	if err := writePEM(srvCrt, "CERTIFICATE", serverCert.CertPEM); err != nil {
		return err
	}
	if err := writePEM(srvKey, "EC PRIVATE KEY", serverCert.KeyPEM); err != nil {
		return err
	}
	if err := writePEM(cliCrt, "CERTIFICATE", clientCert.CertPEM); err != nil {
		return err
	}
	if err := writePEM(cliKey, "EC PRIVATE KEY", clientCert.KeyPEM); err != nil {
		return err
	}

	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(ca.CertPEM)

	s := &server{}

	mux := http.NewServeMux()
	path, handler := harnessv1connect.NewHarnessHandler(s)
	wrap := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		if r.Proto != "HTTP/2.0" {
			s.rep.HTTP2Only = false
		} else {
			// first observation defaults to true; any non-h2 flips it false.
			if !s.rep.HTTP2Only && len(s.rep.FencingGenerations) == 0 {
				s.rep.HTTP2Only = true
			}
		}
		if len(r.TLS.PeerCertificates) > 0 {
			s.clientCN = r.TLS.PeerCertificates[0].Subject.CommonName
		}
		s.mu.Unlock()
		handler.ServeHTTP(w, r)
	})
	mux.Handle(path, wrap)

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCert.TLS},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"}, // HTTP/2 only — no HTTP/1.1 fallback
	}
	srv := &http.Server{Handler: mux, TLSConfig: tlsCfg}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	addr := ln.Addr().String()

	ready, _ := json.Marshal(map[string]string{
		"ready":      "true",
		"addr":       "https://" + addr,
		"ca":         caPath,
		"cert":       cliCrt,
		"key":        cliKey,
		"serverName": "localhost",
	})
	fmt.Println(string(ready))

	go func() { _ = srv.ServeTLS(ln, "", "") }()

	// Wait for the scenario to finish (the handler calls finish() after the
	// reconnect/replay completes), then shut down.
	<-s.finished()
	_ = srv.Shutdown(context.Background())

	s.mu.Lock()
	rep := s.rep
	s.mu.Unlock()
	out, _ := json.Marshal(rep)
	fmt.Println("REPORT " + string(out))
	if rep.Error != "" || !scenarioComplete(rep) {
		return errors.New("scenario incomplete")
	}
	return nil
}

func scenarioComplete(r report) bool {
	return r.Handshake && r.IdentityVerified && r.AssignTurnSent && r.AckMatched &&
		r.TokenDeltaReceived && r.HeartbeatReceived && r.AbortTurnSent &&
		r.ReplayAcked && r.ReadyAfterReplay && r.HTTP2Only &&
		len(r.FencingGenerations) == 2
}

var finishOnce sync.Once
var finishCh = make(chan struct{})

func (s *server) finished() <-chan struct{} { return finishCh }
func (s *server) finish()                   { finishOnce.Do(func() { close(finishCh) }) }

// Work implements harnessv1connect.HarnessHandler.
func (s *server) Work(ctx context.Context, st *connect.BidiStream[v1.WorkerMessage, v1.ControlMessage]) error {
	conn := atomic.AddInt64(&s.conns, 1)
	gen := uint64(conn) // a new connection fences the prior generation

	hello, err := st.Receive()
	if err != nil {
		return s.failErr("no hello: " + err.Error())
	}
	if h := hello.GetHello(); h != nil {
		s.mu.Lock()
		s.rep.Handshake = h.WorkerId == wantWorker && h.PoolId == wantPool
		s.rep.IdentityVerified = s.clientCN == "harness-worker"
		s.rep.FencingGenerations = append(s.rep.FencingGenerations, gen)
		s.mu.Unlock()
	} else {
		return s.failErr("expected Hello first")
	}

	if err := st.Send(welcome(gen)); err != nil {
		return s.failErr("welcome send: " + err.Error())
	}

	if conn == 1 {
		return s.scenario1(st)
	}
	return s.scenario2(st)
}

// scenario1: Ready -> AssignTurn -> assistantMessage(seq1) -> ACK -> TokenDelta
// -> Heartbeat -> workerOutcome(seq2) -> ACK + AbortTurn -> close (stream loss).
func (s *server) scenario1(st *connect.BidiStream[v1.WorkerMessage, v1.ControlMessage]) error {
	// Ready
	if _, err := st.Receive(); err != nil {
		return s.failErr("no ready: " + err.Error())
	}
	if err := st.Send(assignTurn()); err != nil {
		return s.failErr("assign send: " + err.Error())
	}
	s.set(func(r *report) { r.AssignTurnSent = true })

	// assistantMessage (seq 1) -> ACK through 1
	msg, err := st.Receive()
	if err != nil {
		return s.failErr("no assistant event: " + err.Error())
	}
	te := msg.GetTurnEvent()
	if te == nil || te.GetAssistantMessage() == nil || te.GetSequence() != 1 {
		return s.failErr("expected assistantMessage seq 1")
	}
	if err := st.Send(eventAck(te.TurnId, 1)); err != nil {
		return s.failErr("ack1 send: " + err.Error())
	}
	s.set(func(r *report) { r.AckMatched = true })

	// TokenDelta (ephemeral)
	if msg, err = st.Receive(); err != nil {
		return s.failErr("no tokenDelta: " + err.Error())
	}
	if msg.GetTokenDelta() == nil {
		return s.failErr("expected tokenDelta")
	}
	s.set(func(r *report) { r.TokenDeltaReceived = true })

	// Heartbeat
	if msg, err = st.Receive(); err != nil {
		return s.failErr("no heartbeat: " + err.Error())
	}
	if msg.GetHeartbeat() == nil {
		return s.failErr("expected heartbeat")
	}
	s.set(func(r *report) { r.HeartbeatReceived = true })

	// workerOutcome COMPLETED (seq 2) -> ACK through 2 + AbortTurn, then close.
	if msg, err = st.Receive(); err != nil {
		return s.failErr("no outcome: " + err.Error())
	}
	te = msg.GetTurnEvent()
	if te == nil || te.GetWorkerOutcome() == nil || te.GetSequence() != 2 {
		return s.failErr("expected workerOutcome seq 2")
	}
	if err := st.Send(eventAck(te.TurnId, 2)); err != nil {
		return s.failErr("ack2 send: " + err.Error())
	}
	if err := st.Send(abortTurn(te.TurnId)); err != nil {
		return s.failErr("abort send: " + err.Error())
	}
	s.set(func(r *report) { r.AbortTurnSent = true })
	return nil // close the stream (simulated stream loss)
}

// scenario2 (reconnect): replayed unacked tail (workerOutcome seq 2) -> ACK ->
// Ready -> drain the client's request side -> clean close. The replay proves the
// audit tail survived the stream loss; the clean end lets the client observe
// an OK end-of-stream (not another stream loss).
func (s *server) scenario2(st *connect.BidiStream[v1.WorkerMessage, v1.ControlMessage]) error {
	msg, err := st.Receive()
	if err != nil {
		return s.failErr("no replay event: " + err.Error())
	}
	te := msg.GetTurnEvent()
	if te == nil || te.GetWorkerOutcome() == nil || te.GetSequence() != 2 {
		return s.failErr("expected replayed workerOutcome seq 2")
	}
	if err := st.Send(eventAck(te.TurnId, 2)); err != nil {
		return s.failErr("replay ack send: " + err.Error())
	}
	s.set(func(r *report) { r.ReplayAcked = true })

	if _, err := st.Receive(); err != nil { // Ready after replay
		return s.failErr("no ready after replay: " + err.Error())
	}
	s.set(func(r *report) { r.ReadyAfterReplay = true })

	// Drain the client's half-close before returning so the stream ends OK.
	for {
		if _, err := st.Receive(); err != nil {
			break // client closed its request side (EOF)
		}
	}
	s.finish()
	return nil
}

func (s *server) failErr(msg string) error {
	s.set(func(r *report) { r.Error = msg })
	s.finish()
	return errors.New(msg)
}

func (s *server) set(f func(*report)) {
	s.mu.Lock()
	f(&s.rep)
	s.mu.Unlock()
}

// ---- message builders ----

func welcome(gen uint64) *v1.ControlMessage {
	return &v1.ControlMessage{Kind: &v1.ControlMessage_Welcome{Welcome: &v1.Welcome{
		ProtocolVersion: "1", FencingGeneration: gen,
		HeartbeatIntervalMs: 60000, LeaseTimeoutMs: 120000,
	}}}
}

func assignTurn() *v1.ControlMessage {
	uid, gid := currentUIDGID()
	return &v1.ControlMessage{Kind: &v1.ControlMessage_AssignTurn{AssignTurn: &v1.AssignTurn{
		TurnId:    turnID,
		SessionId: "sess-1",
		Sandbox: &v1.SandboxRef{
			SandboxId: "sandbox-1",
			Uid:       uid, Gid: gid,
			WorkingDir: "home",
		},
		Persona:       "you are an agent",
		Model:         &v1.ModelConfig{Id: "m", Api: "openai-completions", ContextWindow: 131072},
		ToolAllowList: &v1.ToolAllowList{All: true},
		Message:       "classify this email",
	}}}
}

func eventAck(turnID string, through uint64) *v1.ControlMessage {
	return &v1.ControlMessage{Kind: &v1.ControlMessage_EventAck{EventAck: &v1.EventAck{
		TurnId: turnID, ThroughSequence: through,
	}}}
}

func abortTurn(turnID string) *v1.ControlMessage {
	return &v1.ControlMessage{Kind: &v1.ControlMessage_AbortTurn{AbortTurn: &v1.AbortTurn{
		TurnId: turnID, Reason: v1.AbortReason_ABORT_REASON_LEASE_EXPIRED, Message: "test abort",
	}}}
}

// ---- cert generation (in-memory, self-signed CA + server/client leaf) ----

type certPair struct {
	CertPEM, KeyPEM []byte
	TLS             tls.Certificate
}

func genCerts() (*certPair, *certPair, *certPair, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	caTpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), IsCA: true, KeyUsage: x509.KeyUsageCertSign,
		Subject:   pkix.Name{CommonName: "harness-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, nil, err
	}
	ca := &certPair{CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})}

	server, err := leaf(caCert, caKey, "localhost", []string{"localhost", "127.0.0.1"})
	if err != nil {
		return nil, nil, nil, err
	}
	client, err := leaf(caCert, caKey, "harness-worker", nil)
	if err != nil {
		return nil, nil, nil, err
	}
	return ca, server, client, nil
}

func leaf(parent *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, dns []string) (*certPair, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    dns,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, parent, &key.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	tlsCert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		return nil, err
	}
	return &certPair{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		TLS:     tlsCert,
	}, nil
}

func writePEM(path, typ string, b []byte) error {
	return os.WriteFile(path, b, 0o600)
}

func currentUIDGID() (uint32, uint32) {
	return uint32(os.Getuid()), uint32(os.Getgid())
}
