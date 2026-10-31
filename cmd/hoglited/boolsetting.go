package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// boolSettingSpellings is the one spelling rule for boolean settings,
// shared with hoglake's Config.parseBoolSetting: the same words, the same
// case-insensitivity, the same refusal of everything else.
const boolSettingSpellings = "true/1/yes/on or false/0/no/off"

// parseBoolSetting reads a boolean setting: true, 1, yes, on and false, 0,
// no, off, case-insensitive, surrounding whitespace ignored. Anything else
// is an error naming the setting, the accepted values and what it got.
//
// Never a silent fallback. `v == "true"` turns HL_X=1 into false and
// `v != "false"` turns HL_X=0 into true, so a manifest says one thing and
// the process does the other while its operator believes the flag is set.
// And not strconv.ParseBool, which accepts "t" and "F" but refuses "yes"
// and "on", a different set from hoglake's.
func parseBoolSetting(name, raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	}
	return false, fmt.Errorf("%s must be one of %s (case-insensitive), got %q", name, boolSettingSpellings, raw)
}

// envBoolSetting is a boolean setting from the environment: unset or blank
// is def, otherwise parseBoolSetting.
func envBoolSetting(lookup func(string) (string, bool), name string, def bool) (bool, error) {
	raw, ok := lookup(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return def, nil
	}
	return parseBoolSetting(name, raw)
}

// boolSettingFlag is a command-line boolean under the same rule: a bare
// -name is true, -name=<spelling> is parseBoolSetting.
type boolSettingFlag struct {
	name string
	v    *bool
}

func (f boolSettingFlag) String() string {
	if f.v == nil {
		return "false"
	}
	return fmt.Sprint(*f.v)
}

func (f boolSettingFlag) Set(s string) error {
	b, err := parseBoolSetting("-"+f.name, s)
	if err != nil {
		return err
	}
	*f.v = b
	return nil
}

func (f boolSettingFlag) IsBoolFlag() bool { return true }

// boolSettingVar defines -name on fs, defaulting to env (parseBoolSetting)
// and then to def. A value of env no rule accepts is returned as an error
// for the caller to refuse the start with; it never falls back to def.
func boolSettingVar(fs *flag.FlagSet, lookup func(string) (string, bool),
	name, env string, def bool, usage string) (*bool, error) {
	v, err := envBoolSetting(lookup, env, def)
	p := new(bool)
	*p = v
	fs.Var(boolSettingFlag{name: name, v: p}, name,
		fmt.Sprintf("%s (env %s; %s)", usage, env, boolSettingSpellings))
	return p, err
}

// mustBoolSettingVar is boolSettingVar on the process flags and
// environment, refusing the start on a bad environment value.
func mustBoolSettingVar(name, env string, def bool, usage string) *bool {
	p, err := boolSettingVar(flag.CommandLine, os.LookupEnv, name, env, def, usage)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hoglited:", err)
		os.Exit(2)
	}
	return p
}
