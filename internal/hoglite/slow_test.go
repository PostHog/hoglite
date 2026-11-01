package hoglite

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The slow tests live in *_slow_test.go files behind the "slow" build tag.
// Under -race they take about as long as the rest of the package together
// (TestDeltaByteIdentity alone is a third of it), and one package run of
// both sat right at go test's default 10 minute timeout. CI runs the two
// groups as separate jobs, each with its own explicit -timeout:
//
//	go test -race ./...                                     (go-checks)
//	go test -race -tags slow -run '^(<slow tests>)$' ./internal/hoglite (go-race-slow)
//
// The slow job builds its -run pattern from the func Test lines of the
// *_slow_test.go files. Run everything locally with -tags slow.
//
// TestSlowFilesTagged keeps the name and the tag together: a slow file
// without the tag would run in both jobs, and a tagged file with another
// name would run in neither.
func TestSlowFilesTagged(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	slow := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		first, _, _ := strings.Cut(string(b), "\n")
		tagged := first == "//go:build slow"
		named := strings.HasSuffix(f, "_slow_test.go")
		switch {
		case named && !tagged:
			t.Errorf("%s: first line %q, want //go:build slow", f, first)
		case tagged && !named:
			t.Errorf("%s has //go:build slow but is not named *_slow_test.go: CI would run it in neither job", f)
		}
		if named {
			slow++
		}
	}
	if slow == 0 {
		t.Error("no *_slow_test.go files: the go-race-slow job would run nothing")
	}
}
