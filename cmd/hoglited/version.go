package main

import (
	"flag"
	"fmt"
	"io"
	"strings"
)

// Release builds set these with -ldflags "-X main.version=... -X main.commit=...".
// Keep the full commit for release verification.
var (
	version = "dev"
	commit  = "unknown"
)

const versionUsage = "print version and full commit, then exit; use alone (or -version=true)"

// runVersion runs before runtime flag defaults or deployment configuration.
// Version inspection is standalone, so no other argument is silently ignored.
// Explicit false is an error, rather than an accidental server start.
func runVersion(args []string, stdout, stderr io.Writer) (handled bool, code int) {
	for _, arg := range args {
		name, _, _ := strings.Cut(arg, "=")
		if name != "-version" && name != "--version" {
			continue
		}
		if len(args) != 1 {
			fmt.Fprintln(stderr, "hoglited: -version must be used alone")
			return true, 2
		}
		fs := flag.NewFlagSet("hoglited", flag.ContinueOnError)
		fs.SetOutput(stderr)
		show := fs.Bool("version", false, versionUsage)
		if err := fs.Parse(args); err != nil {
			return true, 2
		}
		if !*show {
			fmt.Fprintln(stderr, "hoglited: -version requires true; omit it for normal startup")
			return true, 2
		}
		fmt.Fprintf(stdout, "hoglited %s (commit %s)\n", version, commit)
		return true, 0
	}
	return false, 0
}
