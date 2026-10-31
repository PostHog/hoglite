package main

import (
	"flag"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// The cases are hoglake's BoolSettingConfigTest table, row for row, so the
// two sides cannot drift without one table being edited on its own.
var boolSettingCases = []struct {
	raw  string
	want bool
}{
	{"true", true}, {"1", true}, {"TRUE", true}, {"yes", true}, {"on", true},
	{"True", true}, {" on ", true}, {"YES", true},
	{"false", false}, {"0", false}, {"no", false}, {"off", false},
	{"FALSE", false}, {"Off", false}, {" 0 ", false},
}

var boolSettingRejected = []string{"enabled", "2", "-1", "t", "y", "truee", "on1", "nope"}

// MUTATION: revert to `raw == "true"` and the 1/yes/on/TRUE rows go red;
// map unknown spellings to false and every rejected row goes red.
func TestParseBoolSetting(t *testing.T) {
	for _, c := range boolSettingCases {
		got, err := parseBoolSetting("HL_X", c.raw)
		if err != nil || got != c.want {
			t.Errorf("%q: got %v, %v; want %v", c.raw, got, err, c.want)
		}
	}
	for _, raw := range boolSettingRejected {
		_, err := parseBoolSetting("HL_X", raw)
		if err == nil {
			t.Errorf("%q accepted", raw)
			continue
		}
		for _, want := range []string{"HL_X", boolSettingSpellings, `"` + raw + `"`} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%q: error %q does not name %q", raw, err, want)
			}
		}
	}
}

func lookupOf(env map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := env[k]; return v, ok }
}

func TestBoolSettingVar(t *testing.T) {
	parse := func(env map[string]string, def bool, args ...string) (bool, error) {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		p, err := boolSettingVar(fs, lookupOf(env), "x", "HL_X", def, "x")
		if err != nil {
			return false, err
		}
		if err := fs.Parse(args); err != nil {
			return false, err
		}
		return *p, nil
	}
	for _, c := range boolSettingCases {
		if got, err := parse(map[string]string{"HL_X": c.raw}, !c.want); err != nil || got != c.want {
			t.Errorf("env %q: got %v, %v; want %v", c.raw, got, err, c.want)
		}
		if got, err := parse(nil, !c.want, "-x="+c.raw); err != nil || got != c.want {
			t.Errorf("flag %q: got %v, %v; want %v", c.raw, got, err, c.want)
		}
	}
	for _, raw := range boolSettingRejected {
		if got, err := parse(map[string]string{"HL_X": raw}, false); err == nil {
			t.Errorf("env %q accepted as %v", raw, got)
		}
		if got, err := parse(nil, false, "-x="+raw); err == nil {
			t.Errorf("flag %q accepted as %v", raw, got)
		}
	}
	for _, c := range []struct {
		env  map[string]string
		def  bool
		args []string
		want bool
	}{
		{nil, false, nil, false},
		{nil, true, nil, true},
		{map[string]string{"HL_X": " "}, true, nil, true}, // blank is unset
		{nil, false, []string{"-x"}, true},                // bare flag
		{map[string]string{"HL_X": "1"}, false, []string{"-x=off"}, false},
	} {
		if got, err := parse(c.env, c.def, c.args...); err != nil || got != c.want {
			t.Errorf("%v %v def %v: got %v, %v; want %v", c.env, c.args, c.def, got, err, c.want)
		}
	}
}

// The wiring, in a real process: hoglited itself, with the variable
// really set. It logs -interior-reads=<value> right after parsing, then
// stops at the missing -bucket.
func TestHoglitedInteriorReadsEnv(t *testing.T) {
	if os.Getenv("HOGLITED_TEST_RUN_MAIN") == "1" {
		os.Args = []string{"hoglited", "-sqlite-heap-limit=-1"}
		main()
		return
	}
	run := func(val string) (string, int) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestHoglitedInteriorReadsEnv$")
		cmd.Env = append(os.Environ(), "HOGLITED_TEST_RUN_MAIN=1", "HL_BUCKET=", "HL_INTERIOR_READS="+val)
		out, _ := cmd.CombinedOutput()
		return string(out), cmd.ProcessState.ExitCode()
	}
	for _, c := range []struct{ val, want string }{
		{"1", "-interior-reads=true"}, {"on", "-interior-reads=true"}, {"TRUE", "-interior-reads=true"},
		{"0", "-interior-reads=false"}, {"", "-interior-reads=false"},
	} {
		out, _ := run(c.val)
		if !strings.Contains(out, c.want) || !strings.Contains(out, "-bucket / HL_BUCKET required") {
			t.Errorf("HL_INTERIOR_READS=%q: want %q then the -bucket refusal, got:\n%s", c.val, c.want, out)
		}
	}
	out, code := run("enabled")
	if code == 0 || !strings.Contains(out, `HL_INTERIOR_READS must be one of true/1/yes/on or false/0/no/off (case-insensitive), got "enabled"`) ||
		strings.Contains(out, "-interior-reads=") {
		t.Errorf("HL_INTERIOR_READS=enabled: want a refusal before any start, exit %d:\n%s", code, out)
	}
}
