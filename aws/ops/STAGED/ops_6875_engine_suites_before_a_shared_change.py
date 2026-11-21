"""ops 6875 - READ-ONLY: would a change to aws/shared/fmp_bank.py deploy?  Every importing engine's own gate, run now.

Why.  The history book 1.3.0 (branch history-book-one-snapshot-doc) needs a small change to the shared FMP-bank reader.
That module is bundled into 225 engines, so merging it makes deploy-lambdas redeploy all of them - and the deploy gate
runs every one of those engines' own test suites BEFORE it touches AWS: one red suite stops the whole roll-out.  The
history book's own suite had been red since 2026-10-28 without anyone seeing it (a fixed date in a fixture; it only
runs when that engine deploys).  This runs the same gate for the 225 engines on the runner, changes nothing, and names
every suite that is red today - each of which also blocks that engine's own next deploy, whatever the reason for it.

What runs (as deploy-lambdas does): scripts/validate_lambda_sources.py and validate_lambda_configs.py for the targets,
py_compile of each engine's sources, then each engine's tests/run_tests.py (or pytest where it ships test_*.py only).
The suites run with the AWS credentials removed from their environment: they are offline suites by rule, and a suite
that tries to reach the account fails here instead of touching it.

Nothing is written but this report.
"""
from pathlib import Path
import concurrent.futures as cf
import os
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
SHARED = "aws/shared/fmp_bank.py"
PER_SUITE_S = 420
WALL_S = 46 * 60
WORKERS = 4
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")


def clean(text, n=220):
    return EMAIL.sub("[address]", (text or "").strip().replace("\n", " | "))[:n]


def verdict(rc, out):
    """(status, where, why) from a suite's exit code and output."""
    if rc == 0:
        return "green", "", ""
    lines = [l for l in (out or "").splitlines() if l.strip()]
    where = ""
    for l in lines:
        m = re.search(r'File "[^"]*?/(tests/[^"]+)", line (\d+), in (test_\w+)', l)
        if m:
            where = "%s:%s %s" % (m.group(1), m.group(2), m.group(3))
    why = clean(lines[-1] if lines else "", 200)
    return ("timeout" if rc == 124 else "red"), where, why


def selftest():
    out = 'Traceback (most recent call last):\n  File "/x/aws/lambdas/e/tests/run_tests.py", line 188, in test_a_thing\n    assert f() is True, "as fresh as the bank"\nAssertionError: as fresh as the bank (write to a.b@c.org)\n'
    assert verdict(1, out) == ("red", "tests/run_tests.py:188 test_a_thing", "AssertionError: as fresh as the bank (write to [address])"), verdict(1, out)
    assert verdict(0, "3 tests passed") == ("green", "", "") and verdict(124, "")[0] == "timeout"
    assert clean("x@y.zz and\nmore") == "[address] and | more"


def main():
    from ops_report import report

    selftest()
    t0 = time.time()
    env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_") and "TOKEN" not in k and "SECRET" not in k and k not in ("GH_PAT",)}
    env.update(AWS_EC2_METADATA_DISABLED="true", AWS_DEFAULT_REGION="us-east-1", AWS_ACCESS_KEY_ID="offline", AWS_SECRET_ACCESS_KEY="offline", PYTHONDONTWRITEBYTECODE="1")

    def run(cmd, timeout):
        try:
            p = subprocess.run(cmd, cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=timeout)
            return p.returncode, (p.stdout or "") + (p.stderr or "")
        except subprocess.TimeoutExpired as e:
            return 124, ((e.stdout or b"").decode("utf-8", "replace") if isinstance(e.stdout, bytes) else (e.stdout or ""))

    with report("ops_6875_engine_suites_before_a_shared_change") as r:
        r.heading("ops 6875 - the deploy gate of every engine that bundles the FMP-bank reader, run now (read-only)")
        rc, out = run([sys.executable, "scripts/shared_dependents.py", SHARED], 120)
        targets = sorted({t for t in out.split() if re.fullmatch(r"[A-Za-z0-9_.-]+", t) and (ROOT / "aws" / "lambdas" / t / "source").is_dir()})
        r.log("%s is bundled into %d engines" % (SHARED, len(targets)))
        if len(targets) < 50:
            r.fail("the importer list looks wrong (%d): %s" % (len(targets), clean(out, 300)))
            sys.exit(0)
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "boto3", "pytest", "jsonschema", "numpy==2.3.5"], cwd=str(ROOT), capture_output=True, text=True)

        r.section("1. The static checks for the whole set")
        for script in ("scripts/validate_lambda_sources.py", "scripts/validate_lambda_configs.py"):
            rc, out = run([sys.executable, script] + targets, 900)
            (r.ok if rc == 0 else r.fail)("%s for %d engines: %s%s" % (script, len(targets), "passes" if rc == 0 else "FAILS", "" if rc == 0 else " - " + clean(out[-600:], 400)))
        bad_compile = []
        for t in targets:
            srcs = sorted(str(p) for p in (ROOT / "aws" / "lambdas" / t / "source").glob("*.py"))
            if srcs:
                rc, out = run([sys.executable, "-m", "py_compile"] + srcs, 120)
                if rc != 0:
                    bad_compile.append((t, clean(out[-300:], 160)))
        (r.fail if bad_compile else r.ok)("py_compile: %d engines do not compile %s" % (len(bad_compile), bad_compile[:6]) if bad_compile else "py_compile: every engine's sources compile")

        r.section("2. Each engine's own suite")
        jobs = []
        for t in targets:
            d = ROOT / "aws" / "lambdas" / t
            if (d / "tests" / "run_tests.py").is_file():
                jobs.append((t, [sys.executable, str(d / "tests" / "run_tests.py")], None))
            elif list((d / "tests").glob("test_*.py")) if (d / "tests").is_dir() else False:
                jobs.append((t, [sys.executable, "-m", "pytest", "-q", str(d / "tests"), "-x"], "aws/shared:%s/source:%s/tests" % (d, d)))
        r.log("%d engines ship a suite; %d ship none (their gate is the static checks)" % (len(jobs), len(targets) - len(jobs)))

        def one(job):
            t, cmd, pp = job
            if time.time() - t0 > WALL_S:
                return t, "not reached", "", "", 0.0
            ts = time.time()
            if pp:
                env2 = dict(env, PYTHONPATH=pp)
                try:
                    p = subprocess.run(cmd, cwd=str(ROOT), env=env2, capture_output=True, text=True, timeout=PER_SUITE_S)
                    rc, out = p.returncode, (p.stdout or "") + (p.stderr or "")
                except subprocess.TimeoutExpired:
                    rc, out = 124, ""
            else:
                rc, out = run(cmd, PER_SUITE_S)
            st, where, why = verdict(rc, out)
            return t, st, where, why, time.time() - ts
        res = []
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for t, st, where, why, secs in ex.map(one, jobs):
                res.append((t, st, where, why, secs))
                if st != "green":
                    r.log("%-44s %-11s %5.0f s | %s | %s" % (t, st.upper(), secs, where, why))
        green = [x for x in res if x[1] == "green"]
        red = [x for x in res if x[1] == "red"]
        slow = [x for x in res if x[1] == "timeout"]
        skipped = [x for x in res if x[1] == "not reached"]
        r.log("green %d, red %d, over %d s %d, not reached %d; the slowest green suites: %s" % (
            len(green), len(red), PER_SUITE_S, len(slow), len(skipped), ", ".join("%s %.0f s" % (t, s) for t, _, _, _, s in sorted(green, key=lambda x: -x[4])[:5])))
        r.log("all suites one after another would take about %.0f minutes on this runner" % (sum(x[4] for x in res) / 60.0))
        if red or slow:
            r.warn("a roll-out of the shared reader would stop at the first of these %d suites; each also blocks that engine's own next deploy" % (len(red) + len(slow)))
        elif not skipped:
            r.ok("every importing engine's gate is green today: the shared reader could roll out")
        r.kv(engines=len(targets), with_suite=len(jobs), green=len(green), red=len(red), timed_out=len(slow), not_reached=len(skipped), does_not_compile=len(bad_compile),
             wall_s=round(time.time() - t0))
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
