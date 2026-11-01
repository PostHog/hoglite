#!/usr/bin/env python3
"""Report on a `go test -json` run against the job's time budget.

usage: go-test-report.py RESULT.json BUDGET_SECONDS [TOP]

Prints the output of every failed test and package (the goroutine dump of
a -timeout panic included), the elapsed time of each package, and the TOP
slowest top-level tests. The same table goes to $GITHUB_STEP_SUMMARY.

A package past half of BUDGET_SECONDS gets a ::warning:: naming its
slowest tests, so a suite that creeps toward its -timeout is visible on a
green run, long before it fails at the limit. The exit status is always
0: the caller exits with go test's own status.
"""
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

path, budget = sys.argv[1], float(sys.argv[2])
top = int(sys.argv[3]) if len(sys.argv) > 3 else 15



def stamp(e):
    # Go writes nanoseconds; datetime takes microseconds
    t = e.get("Time", "")
    if not t:
        return None
    head, _, frac = t.rstrip("Z").partition(".")
    return datetime.fromisoformat(head + "." + (frac + "000000")[:6] + "+00:00")


output = defaultdict(list)  # (package, test or "") -> output lines
result = {}  # (package, test or "") -> (action, elapsed)
started = {}  # (package, test) -> time of its run event
with open(path, encoding="utf-8", errors="replace") as f:
    for line in f:
        if not line.startswith("{"):
            print(line, end="")  # build errors and the like
            continue
        e = json.loads(line)
        key = (e.get("Package", ""), e.get("Test", ""))
        act = e.get("Action")
        if act == "build-output":  # compile errors (Go 1.24+ -json)
            print(e.get("Output", ""), end="")
        elif act == "output":
            output[key].append(e.get("Output", ""))
        elif act == "run":
            started[key] = stamp(e)
        elif act in ("pass", "fail", "skip"):
            el = e.get("Elapsed", 0.0)
            # a test whose subtests are parallel reports Elapsed 0: use
            # the span from its run event instead
            t0, t1 = started.get(key), stamp(e)
            if t0 and t1:
                el = max(el, (t1 - t0).total_seconds())
            result[key] = (act, el)

failed = sorted(k for k, (a, _) in result.items() if a == "fail")
# a test still running at a -timeout panic gets the panic and the
# goroutine dump as its output but never a result; same for a package
# that never reported
unfinished = sorted(k for k in set(started) | set(output) if k not in result)
failed += unfinished
for key in failed:
    pkg, test = key
    print(f"::group::FAIL {pkg} {test}".rstrip())
    print("".join(output[key]), end="")
    print("::endgroup::")

pkgs = sorted((k[0], a, el) for k, (a, el) in result.items() if not k[1])
tests = sorted(((el, a, k[0], k[1]) for k, (a, el) in result.items()
                if k[1] and "/" not in k[1]), reverse=True)
rows = [f"budget {budget:.0f}s (go test -timeout)", ""]
rows += [f"DID NOT FINISH {t}  ({pkg})" for pkg, t in unfinished if t]
rows += [f"{a:4} {el:8.1f}s {el / budget:4.0%}  {pkg}" for pkg, a, el in pkgs]
rows += ["", f"slowest {top} top-level tests:"]
rows += [f"{el:8.1f}s {a:4} {t}  ({pkg.rsplit('/', 1)[-1]})" for el, a, pkg, t in tests[:top]]
print("\n".join(rows))

summary = os.environ.get("GITHUB_STEP_SUMMARY")
if summary:
    with open(summary, "a", encoding="utf-8") as f:
        f.write("```\n" + "\n".join(rows) + "\n```\n")

for pkg, _, el in pkgs:
    if el > budget / 2:
        slow = ", ".join([f"{t} {e:.0f}s" for e, _, p, t in tests if p == pkg][:5])
        print(f"::warning::{pkg} took {el:.0f}s, over half of its {budget:.0f}s budget. "
              f"Slowest: {slow}. Split it further before it reaches the -timeout (see internal/hoglite/slow_test.go).")

if [k for k in unfinished if k[1]]:
    names = ", ".join(f"{t} ({p.rsplit('/', 1)[-1]})" for p, t in unfinished if t)
    print(f"::error::go test stopped before these tests finished: {names}. "
          f"If it was the {budget:.0f}s -timeout, compare the package times above with "
          f"the budget: a suite near its limit needs splitting, a single stuck test does not.")
