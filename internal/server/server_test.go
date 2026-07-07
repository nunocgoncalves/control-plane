package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nunocgoncalves/control-plane/internal/server"
)

func TestHealthz(t *testing.T) {
	router := server.Router(nil)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestReadyz_NoDatabase(t *testing.T) {
	router := server.Router(nil)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
}
