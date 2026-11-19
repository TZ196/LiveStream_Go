package httpserver_test

import (
	"encoding/json"
	"mime"
	"net/http"
	"testing"

	"github.com/TZ196/LiveStream_Go/backend/internal/httpserver"
	"github.com/cloudwego/hertz/pkg/common/ut"
)

func TestHealthEndpoint(t *testing.T) {
	h := httpserver.New("127.0.0.1:0")
	response := ut.PerformRequest(h.Engine, http.MethodGet, "/healthz", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header().Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		t.Fatalf("Content-Type = %q, error = %v", response.Header().Get("Content-Type"), err)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	var body map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if len(body) != 1 || body["status"] != "ok" {
		t.Fatalf("unexpected health response: %v", body)
	}
}

func TestRouteBoundaries(t *testing.T) {
	h := httpserver.New("127.0.0.1:0")
	for _, test := range []struct {
		name   string
		method string
		path   string
		status int
	}{
		{"unsupported method", http.MethodPost, "/healthz", http.StatusMethodNotAllowed},
		{"unknown route", http.MethodGet, "/does-not-exist", http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := ut.PerformRequest(h.Engine, test.method, test.path, nil)
			if response.Code != test.status {
				t.Fatalf("%s %s status = %d, want %d", test.method, test.path, response.Code, test.status)
			}
		})
	}
}
