package hoglite

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

func TestMetricsListenerIsolation(t *testing.T) {
	f := newAuthFixture(t)
	s := NewServer(&Manager{metrics: &Metrics{}}, nil).WithAuth(f.auth)
	metrics := s.MetricsHandler()
	r := httptest.NewRecorder()
	metrics.ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	if r.Code != 200 || !strings.Contains(r.Body.String(), "hoglite_saturation ") ||
		r.Header().Get("Content-Type") != "text/plain; version=0.0.4" {
		t.Fatalf("metrics: %d %s", r.Code, r.Body.String())
	}
	operator := f.token(t, func(c jwt.MapClaims) {
		c["ten"] = "operator"
		delete(c, "cat")
		delete(c, "org")
		delete(c, "rtg")
	})
	for _, route := range []struct{ method, path string }{
		{"GET", "/v1/healthz"}, {"GET", "/v1/info"},
		{"GET", "/v1/shutdown"}, {"POST", "/v1/shutdown"},
		{"PUT", "/v1/tenants/opaque_1"}, {"DELETE", "/v1/tenants/opaque_1"},
		{"POST", "/v1/t/opaque_1/query"}, {"POST", "/v1/t/opaque_1/tx"},
		{"POST", "/v1/query-batch"}, {"GET", "/v1/t/opaque_1/status"},
		{"POST", "/v1/t/opaque_1/release"}, {"POST", "/v1/debug/crash/opaque_1"},
	} {
		for _, token := range []string{"", operator} {
			req := httptest.NewRequest(route.method, route.path, strings.NewReader("{}"))
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			rec := httptest.NewRecorder()
			metrics.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Errorf("%s %s: %d", route.method, route.path, rec.Code)
			}
		}
	}
	for _, method := range []string{"HEAD", "POST", "PUT", "DELETE", "OPTIONS"} {
		rec := httptest.NewRecorder()
		metrics.ServeHTTP(rec, httptest.NewRequest(method, "/metrics", nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /metrics: %d", method, rec.Code)
		}
	}
	for _, tc := range []struct {
		token string
		want  int
	}{{"", 401}, {f.token(t, nil), 403}, {operator, 200}} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/metrics", nil)
		if tc.token != "" {
			req.Header.Set("Authorization", "Bearer "+tc.token)
		}
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("API metrics: %d; want %d", rec.Code, tc.want)
		}
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/v1/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("health probe changed its lease check: %d", rec.Code)
	}
	legacy := NewServer(&Manager{metrics: &Metrics{}}, nil)
	rec = httptest.NewRecorder()
	legacy.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("disabled authentication changed: %d", rec.Code)
	}
}
