package main

import (
	"strings"
	"testing"
)

func TestMetricsAddress(t *testing.T) {
	for _, tc := range []struct {
		api, port, want string
	}{
		{":8080", "0", ""},
		{"legacy-address", "0", ""},
		{":8080", "9090", ":9090"},
		{"127.0.0.1:8080", "9090", "127.0.0.1:9090"},
		{"[::1]:8080", "9090", "[::1]:9090"},
		{":8080", "65535", ":65535"},
	} {
		got, err := metricsAddress(tc.api, tc.port)
		if err != nil || got != tc.want {
			t.Errorf("%q / %q: %q, %v; want %q", tc.api, tc.port, got, err, tc.want)
		}
	}
	for _, tc := range []struct{ api, port string }{
		{":8080", "-1"}, {":8080", "65536"}, {":8080", "invalid"},
		{":8080", "8080"}, {"127.0.0.1:8080", "8080"},
		{":http", "80"}, {"invalid", "9090"}, {":invalid", "9090"},
	} {
		if got, err := metricsAddress(tc.api, tc.port); err == nil || got != "" {
			t.Errorf("accepted %q / %q: %q, %v", tc.api, tc.port, got, err)
		}
	}
}

func TestHoglitedMetricsStartup(t *testing.T) {
	binary := buildHoglited(t, "")
	for _, tc := range []struct {
		name, env string
		args      []string
		want      string
	}{
		{"default disabled", "", nil, "-bucket / HL_BUCKET required"},
		{"enabled env", "9090", nil, "-bucket / HL_BUCKET required"},
		{"flag overrides env", "8080", []string{"-metrics-port=9090"}, "-bucket / HL_BUCKET required"},
		{"disabled flag", "9090", []string{"-metrics-port=0"}, "-bucket / HL_BUCKET required"},
		{"negative", "-1", nil, "metrics port must be an integer"},
		{"range", "65536", nil, "metrics port must be an integer"},
		{"malformed", "invalid", nil, "metrics port must be an integer"},
		{"collision", "8080", nil, "metrics port must differ"},
		{"malformed API", "9090", []string{"-addr=invalid"}, "requires an API host:port"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"-addr=:8080", "-sqlite-heap-limit=-1"}, tc.args...)
			stdout, stderr, code := runHoglited(t, binary, map[string]string{"HL_METRICS_PORT": tc.env}, args...)
			if code != 1 || stdout != "" || !strings.Contains(stderr, tc.want) {
				t.Fatalf("exit %d; stdout %q; stderr %q; want %q", code, stdout, stderr, tc.want)
			}
			if !strings.Contains(tc.want, "-bucket") && strings.Contains(stderr, "-compress=") {
				t.Fatal("invalid metrics configuration reached storage initialization")
			}
		})
	}
}
