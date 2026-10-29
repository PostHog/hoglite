#!/usr/bin/env python3
"""Tests for ecr-release.sh against a mocked crane/gh/date.

The mocks are small scripts in a temporary directory on PATH:

  crane  reads a JSON "registry" from the file named by $MOCK_DB:
           {"tags":   {"r<minute>-<sha6>": "<40-hex revision it was built from>"},
            "fail_index":    [tags whose index read fails with a 503],
            "fail_manifest": [tags whose per-arch manifest read fails],
            "fail_config":   [tags whose per-arch config read fails],
            "bad_json":      [tags whose index read returns garbage]}
         $MOCK_LS_FAIL=1 makes crane ls fail (registry unreachable).
  gh     answers `gh api repos/<repo>/actions/runs?...` with $MOCK_RUNS
         and `gh api repos/<repo>/commits/main --jq .sha` with
         $MOCK_MAIN_TIP. $MOCK_GH_FAIL=1 makes the tip read fail.
  date   prints $MOCK_NOW for `date -u +%Y%m%d%H%M`.

Run: python3 .github/scripts/ecr-release-test.py
"""

import json
import os
import pathlib
import subprocess
import sys
import tempfile

SCRIPT = pathlib.Path(__file__).resolve().parent / "ecr-release.sh"
SOURCE_URL = "https://github.com/PostHog/hoglite"

A = "abcdef" + "1" * 34  # a commit
B = "abcdef" + "2" * 34  # another commit with the same sha6 prefix
C = "c0ffee" + "3" * 34  # an unrelated commit
T_A = "r202610271200-abcdef"  # release tag of A
T_B = "r202610281200-abcdef"  # later release tag of B, same sha6
NOW = "202610291000"

MOCK_CRANE = r"""#!/usr/bin/env python3
import json, os, sys
cmd, args = sys.argv[1], sys.argv[2:]
db = json.load(open(os.environ["MOCK_DB"]))
if cmd == "ls":
    if os.environ.get("MOCK_LS_FAIL"):
        print("connection refused", file=sys.stderr)
        sys.exit(1)
    for tag in sorted(db["tags"]):
        print(tag)
    sys.exit(0)
ref = args[-1]
tag = ref.rsplit(":", 1)[1]
if tag not in db["tags"]:
    print("MANIFEST_UNKNOWN", file=sys.stderr)
    sys.exit(1)
def fail(kind):
    return tag in db.get(kind, [])
src = "https://github.com/PostHog/hoglite"
ann = {"org.opencontainers.image.source": src,
       "org.opencontainers.image.revision": db["tags"][tag]}
if cmd == "manifest" and "--platform" not in args:
    if fail("fail_index"):
        print("transient 503", file=sys.stderr)
        sys.exit(1)
    if fail("bad_json"):
        print("<html>503 from a proxy</html>")
        sys.exit(0)
    print(json.dumps({
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "annotations": ann,
        "manifests": [{"platform": {"os": "linux", "architecture": a}}
                      for a in ["amd64", "arm64"]]}))
elif cmd == "manifest":
    if fail("fail_manifest"):
        print("transient 503", file=sys.stderr)
        sys.exit(1)
    print(json.dumps({"annotations": ann}))
elif cmd == "config":
    if fail("fail_config"):
        print("transient 503", file=sys.stderr)
        sys.exit(1)
    print(json.dumps({"config": {"Labels": ann}}))
else:
    sys.exit(2)
"""

MOCK_GH = r"""#!/bin/sh
if [ -n "$MOCK_GH_FAIL" ]; then
  echo "gh: api error" >&2
  exit 1
fi
case " $* " in
  *"commits/main "*) printf '%s\n' "$MOCK_MAIN_TIP" ;;
  *"actions/runs"*) printf '%s\n' "$MOCK_RUNS" ;;
  *) echo "unexpected gh call: $*" >&2; exit 1 ;;
esac
"""

MOCK_DATE = '#!/bin/sh\nprintf "%s\\n" "$MOCK_NOW"\n'


def ci_run(run_id, status, conclusion, path=".github/workflows/ci.yml"):
    return {"id": run_id, "path": path, "status": status, "conclusion": conclusion}


class Failure(Exception):
    pass


class Harness:
    def __init__(self, root):
        self.dir = root
        for name, body in [("crane", MOCK_CRANE), ("gh", MOCK_GH), ("date", MOCK_DATE)]:
            p = root / name
            p.write_text(body)
            p.chmod(0o755)
        self.passed = 0

    def run(self, args, db=None, env_extra=None):
        env = dict(os.environ)
        env["PATH"] = f"{self.dir}:{env['PATH']}"
        env["MOCK_NOW"] = NOW
        if db is not None:
            db_file = self.dir / "db.json"
            db_file.write_text(json.dumps(db))
            env["MOCK_DB"] = str(db_file)
        env.update(env_extra or {})
        return subprocess.run([str(SCRIPT)] + args, env=env,
                              text=True, capture_output=True, timeout=60)

    def check(self, label, cond, detail=""):
        if cond:
            self.passed += 1
            print(f"ok   {label}")
        else:
            print(f"FAIL {label}\n{detail}")
            raise Failure(label)

    def expect_reuse(self, label, r, tag):
        self.check(label,
                   r.returncode == 0 and r.stdout == f"tag={tag}\npush=false\n",
                   f"exit={r.returncode}\nstdout={r.stdout}\nstderr={r.stderr}")

    def expect_new_tag(self, label, r, sha6):
        want = f"tag=r{NOW}-{sha6}\npush=true\n"
        self.check(label, r.returncode == 0 and r.stdout == want,
                   f"exit={r.returncode}\nstdout={r.stdout}\nstderr={r.stderr}")

    def expect_refuse(self, label, r):
        self.check(label, r.returncode != 0 and r.stdout == "",
                   f"exit={r.returncode}\nstdout={r.stdout}\nstderr={r.stderr}")

    def expect_publish(self, label, r, publish):
        self.check(label, r.returncode == 0 and f"publish={'true' if publish else 'false'}" in r.stdout.split(),
                   f"exit={r.returncode}\nstdout={r.stdout}\nstderr={r.stderr}")


def main():
    with tempfile.TemporaryDirectory() as d:
        h = Harness(pathlib.Path(d))

        # --- select: rerun rules -----------------------------------
        r = h.run(["select", "registry.example", A, "hoglite"], db={"tags": {T_A: A}})
        h.expect_reuse("select reuses a good release of the same commit", r, T_A)

        r = h.run(["select", "registry.example", A, "hoglite"], db={"tags": {}})
        h.expect_new_tag("select mints a tag when the repository is empty", r, "abcdef")

        # Bug 1: a transient read failure must abort with no output,
        # never mint a second tag for a released commit.
        for kind in ["fail_index", "fail_manifest", "fail_config", "bad_json"]:
            r = h.run(["select", "registry.example", A, "hoglite"],
                      db={"tags": {T_A: A}, kind: [T_A]})
            h.expect_refuse(f"select refuses to decide on {kind}", r)

        r = h.run(["select", "registry.example", A, "hoglite"],
                  db={"tags": {T_A: A}}, env_extra={"MOCK_LS_FAIL": "1"})
        h.expect_refuse("select fails closed when crane ls fails", r)

        # A positively broken release (all reads succeed, annotations
        # name another commit) is repaired with a new tag.
        r = h.run(["select", "registry.example", A, "hoglite"], db={"tags": {T_A: B}})
        h.expect_new_tag("select re-releases a positively broken tag", r, "abcdef")

        # Bug 2: two commits share a sha6 prefix. A rerun of the old
        # commit must find its own tag by full revision, not stop at
        # the newest prefix match.
        db = {"tags": {T_A: A, T_B: B}}
        r = h.run(["select", "registry.example", A, "hoglite"], db=db)
        h.expect_reuse("same-sha6: rerun of old commit reuses its own tag", r, T_A)
        r = h.run(["select", "registry.example", B, "hoglite"], db=db)
        h.expect_reuse("same-sha6: rerun of new commit reuses its own tag", r, T_B)

        # Same-sha6, but the old commit's tag cannot be read: refuse.
        db = {"tags": {T_A: A, T_B: B}, "fail_config": [T_A]}
        r = h.run(["select", "registry.example", A, "hoglite"], db=db)
        h.expect_refuse("same-sha6: unreadable old tag aborts select", r)

        # Same-sha6, old commit was never released: mint a new tag.
        r = h.run(["select", "registry.example", A, "hoglite"], db={"tags": {T_B: B}})
        h.expect_new_tag("same-sha6: unreleased old commit gets a new tag", r, "abcdef")

        # --- tip and gate: publish order ----------------------------
        r = h.run(["tip", "PostHog/hoglite", A], env_extra={"MOCK_MAIN_TIP": A})
        h.expect_publish("tip: the tip of main publishes", r, True)

        r = h.run(["tip", "PostHog/hoglite", A], env_extra={"MOCK_MAIN_TIP": C})
        h.expect_publish("tip: an old commit does not publish", r, False)

        r = h.run(["tip", "PostHog/hoglite", A], env_extra={"MOCK_GH_FAIL": "1"})
        h.expect_refuse("tip fails closed when the tip read fails", r)

        # Bug 3: CI green but the commit is not the tip of main.
        runs = json.dumps({"workflow_runs": [ci_run(1, "completed", "success")]})
        r = h.run(["gate", "PostHog/hoglite", A],
                  env_extra={"MOCK_RUNS": runs, "MOCK_MAIN_TIP": C})
        h.expect_publish("gate: green CI on an old commit does not publish", r, False)

        r = h.run(["gate", "PostHog/hoglite", A],
                  env_extra={"MOCK_RUNS": runs, "MOCK_MAIN_TIP": A})
        h.expect_publish("gate: green CI on the tip publishes", r, True)

        runs = json.dumps({"workflow_runs": [ci_run(1, "completed", "success"),
                                             ci_run(3, "completed", "failure")]})
        r = h.run(["gate", "PostHog/hoglite", A],
                  env_extra={"MOCK_RUNS": runs, "MOCK_MAIN_TIP": A})
        h.expect_publish("gate: newest CI run failed on the tip", r, False)

        runs = json.dumps({"workflow_runs": []})
        r = h.run(["gate", "PostHog/hoglite", A],
                  env_extra={"MOCK_RUNS": runs, "MOCK_MAIN_TIP": A})
        h.expect_publish("gate: no CI run yet", r, False)

        runs = json.dumps({"workflow_runs": [ci_run(1, "in_progress", None)]})
        r = h.run(["gate", "PostHog/hoglite", A],
                  env_extra={"MOCK_RUNS": runs, "MOCK_MAIN_TIP": A})
        h.expect_publish("gate: CI still running", r, False)

        # --- verify exit codes --------------------------------------
        r = h.run(["verify", f"registry.example/hoglite:{T_A}", A], db={"tags": {T_A: A}})
        h.check("verify accepts a good release", r.returncode == 0,
                f"exit={r.returncode}\nstderr={r.stderr}")
        r = h.run(["verify", f"registry.example/hoglite:{T_A}", B], db={"tags": {T_A: A}})
        h.check("verify rejects another commit's release (exit 1)", r.returncode == 1,
                f"exit={r.returncode}\nstderr={r.stderr}")
        r = h.run(["verify", f"registry.example/hoglite:{T_A}", A],
                  db={"tags": {T_A: A}, "fail_index": [T_A]})
        h.check("verify reports an inconclusive read (exit 2)", r.returncode == 2,
                f"exit={r.returncode}\nstderr={r.stderr}")

        # --- check-release ------------------------------------------
        r = h.run(["check-release", "registry.example", T_A, A, "hoglite"],
                  db={"tags": {T_A: A}})
        h.check("check-release accepts a complete release", r.returncode == 0,
                f"exit={r.returncode}\nstderr={r.stderr}")
        r = h.run(["check-release", "registry.example", T_A, A, "hoglite"],
                  db={"tags": {T_A: A}, "fail_manifest": [T_A]})
        h.expect_refuse("check-release fails when a read fails", r)

        print(f"\n{h.passed} checks passed")


if __name__ == "__main__":
    try:
        main()
    except Failure:
        sys.exit(1)
