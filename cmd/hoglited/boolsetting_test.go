package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestHoglitedBoolSettings(t *testing.T) {
	binary := buildHoglited(t, "")
	settings := []struct {
		flag string
		env  string
		def  bool
	}{
		{"compress", "HL_COMPRESS", true},
		{"delta-checkpoints", "HL_DELTA_CHECKPOINTS", true},
		{"interior-reads", "HL_INTERIOR_READS", false},
	}
	accepted := []struct {
		raw  string
		want bool
	}{
		{"1", true}, {"t", true}, {"T", true},
		{"true", true}, {"TRUE", true}, {"True", true},
		{"0", false}, {"f", false}, {"F", false},
		{"false", false}, {"FALSE", false}, {"False", false},
	}
	rejected := []string{"yes", "no", "on", "off", "enabled", "2", "-1", "TrUe", " 0 ", " "}
	checkStartup := func(t *testing.T, env map[string]string, args []string, want string) {
		t.Helper()
		args = append([]string{"-sqlite-heap-limit=-1"}, args...)
		stdout, stderr, code := runHoglited(t, binary, env, args...)
		if code != 1 || stdout != "" || !strings.Contains(stderr, "-bucket / HL_BUCKET required") || !strings.Contains(stderr, want) {
			t.Fatalf("exit %d; stdout %q; stderr %q; want %q before the bucket error", code, stdout, stderr, want)
		}
	}
	for _, setting := range settings {
		t.Run(setting.flag, func(t *testing.T) {
			wantDefault := fmt.Sprintf("-%s=%v", setting.flag, setting.def)
			t.Run("unset", func(t *testing.T) {
				checkStartup(t, nil, nil, wantDefault)
			})
			t.Run("empty", func(t *testing.T) {
				checkStartup(t, map[string]string{setting.env: ""}, nil, wantDefault)
			})
			t.Run("bare flag", func(t *testing.T) {
				checkStartup(t, map[string]string{setting.env: "false"}, []string{"-" + setting.flag}, "-"+setting.flag+"=true")
			})
			for _, tc := range accepted {
				t.Run("env/"+tc.raw, func(t *testing.T) {
					checkStartup(t, map[string]string{setting.env: tc.raw}, nil, fmt.Sprintf("-%s=%v", setting.flag, tc.want))
				})
				t.Run("flag/"+tc.raw, func(t *testing.T) {
					env := map[string]string{setting.env: fmt.Sprint(!tc.want)}
					checkStartup(t, env, []string{"-" + setting.flag + "=" + tc.raw}, fmt.Sprintf("-%s=%v", setting.flag, tc.want))
				})
			}
			for _, raw := range rejected {
				t.Run("reject env/"+raw, func(t *testing.T) {
					// An explicit flag does not hide an invalid environment value.
					stdout, stderr, code := runHoglited(t, binary, map[string]string{setting.env: raw}, "-"+setting.flag+"=false")
					want := setting.env + ": strconv.ParseBool: parsing " + fmt.Sprintf("%q", raw)
					if code != 2 || stdout != "" || !strings.Contains(stderr, want) || strings.Contains(stderr, "-compress=") {
						t.Fatalf("exit %d; stdout %q; stderr %q; want %q before startup", code, stdout, stderr, want)
					}
				})
				t.Run("reject flag/"+raw, func(t *testing.T) {
					stdout, stderr, code := runHoglited(t, binary, nil, "-"+setting.flag+"="+raw)
					if code != 2 || stdout != "" || !strings.Contains(stderr, "invalid boolean value") || strings.Contains(stderr, "-compress=") {
						t.Fatalf("exit %d; stdout %q; stderr %q; want a flag error before startup", code, stdout, stderr)
					}
				})
			}
		})
	}

	t.Run("other Boolean flags", func(t *testing.T) {
		for _, name := range []string{"sweep", "force-format"} {
			for _, tc := range accepted {
				t.Run(name+"/"+tc.raw, func(t *testing.T) {
					checkStartup(t, nil, []string{"-" + name + "=" + tc.raw}, "-compress=true -delta-checkpoints=true -interior-reads=false")
				})
			}
		}
	})

	t.Run("version ignores invalid environment", func(t *testing.T) {
		env := make(map[string]string)
		for _, setting := range settings {
			env[setting.env] = "invalid"
		}
		stdout, stderr, code := runHoglited(t, binary, env, "-version")
		if code != 0 || stdout != "hoglited dev (commit unknown)\n" || stderr != "" {
			t.Fatalf("exit %d; stdout %q; stderr %q", code, stdout, stderr)
		}
	})
}
