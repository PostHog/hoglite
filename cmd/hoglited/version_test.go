package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func buildHoglited(t *testing.T, ldflags string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "hoglited")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	args := []string{"build", "-o", binary}
	if ldflags != "" {
		args = append(args, "-ldflags", ldflags)
	}
	args = append(args, ".")
	cmd := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build hoglited: %v\n%s", err, out)
	}
	return binary
}

func runHoglited(t *testing.T, binary string, settings map[string]string, args ...string) (string, string, int) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "must-not-be-created")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, "HL_") && !strings.HasPrefix(env, "AWS_") {
			cmd.Env = append(cmd.Env, env)
		}
	}
	cmd.Env = append(cmd.Env,
		"HL_DIR="+dir,
		"AWS_CONFIG_FILE="+filepath.Join(dir, "invalid-config"),
		"AWS_SHARED_CREDENTIALS_FILE="+filepath.Join(dir, "invalid-credentials"))
	for name, value := range settings {
		cmd.Env = append(cmd.Env, name+"="+value)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("hoglited did not exit: %v", ctx.Err())
	}
	code := 0
	if err != nil {
		exit, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("run hoglited: %v", err)
		}
		code = exit.ExitCode()
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("startup created its cache directory, or stat failed: %v", err)
	}
	return stdout.String(), stderr.String(), code
}

func TestHoglitedVersionCLI(t *testing.T) {
	binary := buildHoglited(t, "")
	for _, arg := range []string{"-version", "--version", "-version=true", "--version=true"} {
		t.Run(arg, func(t *testing.T) {
			stdout, stderr, code := runHoglited(t, binary, map[string]string{"HL_INTERIOR_READS": "invalid-deployment-value"}, arg)
			if code != 0 || stdout != "hoglited dev (commit unknown)\n" || stderr != "" {
				t.Fatalf("exit %d; stdout %q; stderr %q", code, stdout, stderr)
			}
		})
	}

	t.Run("rejected version arguments", func(t *testing.T) {
		for _, args := range [][]string{
			{"-version=invalid"}, {"--version=invalid"},
			{"-version=false"}, {"--version=false"},
			{"-version", "-unknown"}, {"-unknown", "--version"},
			{"-version", "-bucket=example"}, {"-bucket=example", "-version"},
			{"-version", "extra"}, {"extra", "-version"},
			{"-version", "--version"}, {"--", "-version"},
		} {
			stdout, stderr, code := runHoglited(t, binary, map[string]string{"HL_INTERIOR_READS": "invalid-deployment-value"}, args...)
			if code != 2 || stdout != "" || !strings.Contains(stderr, "-version") {
				t.Errorf("%q: exit %d; stdout %q; stderr %q", args, code, stdout, stderr)
			}
			if strings.Contains(stderr, "HL_INTERIOR_READS") || strings.Contains(stderr, "-interior-reads=") {
				t.Errorf("%q inspected runtime configuration: %s", args, stderr)
			}
		}
	})

	t.Run("help", func(t *testing.T) {
		stdout, stderr, code := runHoglited(t, binary, nil, "-help")
		if code != 0 || stdout != "" {
			t.Fatalf("exit %d; stdout %q; stderr %q", code, stdout, stderr)
		}
		for _, want := range []string{"-version", "-bucket", "-interior-reads", "alone"} {
			if !strings.Contains(stderr, want) {
				t.Errorf("help lacks %q:\n%s", want, stderr)
			}
		}
	})

	t.Run("normal startup still validates configuration", func(t *testing.T) {
		for _, tc := range []struct {
			interior string
			args     []string
			code     int
			want     string
		}{
			{"invalid-deployment-value", nil, 2, "HL_INTERIOR_READS: strconv.ParseBool:"},
			{"", []string{"-sqlite-heap-limit=-1"}, 1, "-bucket / HL_BUCKET required"},
			{"", []string{"-unknown"}, 2, "flag provided but not defined"},
		} {
			stdout, stderr, code := runHoglited(t, binary, map[string]string{"HL_INTERIOR_READS": tc.interior}, tc.args...)
			if code != tc.code || stdout != "" || !strings.Contains(stderr, tc.want) {
				t.Errorf("%q: exit %d; stdout %q; stderr %q", tc.args, code, stdout, stderr)
			}
		}
	})
}

func TestHoglitedVersionLinkerMetadata(t *testing.T) {
	const revision = "0123456789abcdef0123456789abcdef01234567"
	binary := buildHoglited(t, "-X main.version=v1.2.3 -X main.commit="+revision)
	stdout, stderr, code := runHoglited(t, binary, map[string]string{"HL_INTERIOR_READS": "invalid-deployment-value"}, "-version")
	want := "hoglited v1.2.3 (commit " + revision + ")\n"
	if code != 0 || stdout != want || stderr != "" {
		t.Fatalf("exit %d; stdout %q; stderr %q; want %q", code, stdout, stderr, want)
	}
}
