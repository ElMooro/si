"""ops 6683 - read-only: justhodl-equity-research (the why.html desk) shows 16 out-of-memory errors in 129 invocations
over 14:46-20:48 UTC (census 6664) after the morning's fix had it at 227 MB cold.  Timeline of every REPORT line in the
window (duration, memory, status, init), split before / after the fleet-lane redeploy (17:23 UTC), the shim / bank lines
around each error (table cache thrashing, bank stepped aside, misses), and which tickers the failing requests asked for
(the engine's own "[research]" lines).  Nothing is written.  No key involved.
"""
from pathlib import Path
import os
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-equity-research"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120)
HOURS = float(os.environ.get("HOURS") or "7")


def events(logs, pattern, since_ms, limit=1500):
    out, token = [], None
    while True:
        kw = dict(logGroupName="/aws/lambda/" + FN, startTime=since_ms, filterPattern=pattern, limit=400)
        if token:
            kw["nextToken"] = token
        resp = logs.filter_log_events(**kw)
        out += resp.get("events", [])
        token = resp.get("nextToken")
        if not token or len(out) >= limit:
            break
    return sorted(out, key=lambda e: e["timestamp"])


def fields(m):
    d = {}
    for p in m.split("\t"):
        p = p.strip()
        for k in ("Duration:", "Max Memory Used:", "Status:", "Init Duration:", "Memory Size:"):
            if p.startswith(k):
                d[k[:-1]] = p[len(k):].strip()
    return d


def main():
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    since_ms = int((time.time() - HOURS * 3600) * 1000)
    with report("ops_6683_desk_oom_timeline") as r:
        r.heading("ops 6683 - equity-research out-of-memory timeline, last %.0f h (read-only)" % HOURS)
        c = lam.get_function_configuration(FunctionName=FN)
        r.log("deployed: %s MB / %s s, modified %s, reserved concurrency %s" % (c.get("MemorySize"), c.get("Timeout"), (c.get("LastModified") or "")[:19],
                                                                           lam.get_function_concurrency(FunctionName=FN).get("ReservedConcurrentExecutions", "(none)")))
        reps = events(logs, '"REPORT RequestId"', since_ms)
        r.section("1. REPORT lines: %d invocations" % len(reps))
        bad, ok, mem_ok = [], [], []
        for e in reps:
            f = fields(e.get("message", ""))
            (bad if f.get("Status") else ok).append((e["timestamp"], f))
            if not f.get("Status"):
                try:
                    mem_ok.append(int(f.get("Max Memory Used", "0 MB").split()[0]))
                except Exception:  # noqa: BLE001
                    pass
        r.log("ok %d (memory p50 %s MB, max %s MB), errors/timeouts %d" % (len(ok), sorted(mem_ok)[len(mem_ok) // 2] if mem_ok else None, max(mem_ok) if mem_ok else None, len(bad)))
        redeploy = time.mktime(time.strptime((c.get("LastModified") or "2026-10-26T17:23:07")[:19], "%Y-%m-%dT%H:%M:%S")) * 1000
        r.log("errors before the redeploy (%s): %d; after: %d" % ((c.get("LastModified") or "")[:19], sum(1 for t, _ in bad if t < redeploy), sum(1 for t, _ in bad if t >= redeploy)))
        for t, f in bad[-20:]:
            r.log("   %s | %s" % (time.strftime("%H:%M:%S", time.gmtime(t / 1000)), " ".join("%s %s" % (k, v) for k, v in f.items())))
        r.section("2. the engine's and the shim's lines around the errors")
        lines = events(logs, '?"[research]" ?"[fmp-bankfirst]" ?"[fmp-bank]" ?"Runtime.OutOfMemory" ?"Task timed out" ?Traceback ?"[ERROR]"', since_ms, limit=3000)
        r.log("%d matching lines; showing the 25 lines before each of the last 6 errors" % len(lines))
        for t, f in bad[-6:]:
            r.log("--- error at %s (%s)" % (time.strftime("%H:%M:%S", time.gmtime(t / 1000)), f.get("Status")))
            window = [x for x in lines if t - 180_000 <= x["timestamp"] <= t + 2_000]
            for x in window[-25:]:
                r.log("   %s | %s" % (time.strftime("%H:%M:%S", time.gmtime(x["timestamp"] / 1000)), x.get("message", "").strip().replace("\n", " ⏎ ")[:260]))
        r.section("3. tickers asked (from '[research]' lines) and shim bypass reasons in the window")
        asked = {}
        reasons = {}
        for x in lines:
            m = x.get("message", "")
            if "[research]" in m and "ticker" in m.lower():
                asked[m.strip()[:120]] = asked.get(m.strip()[:120], 0) + 1
            if "stepped aside" in m:
                key = m.split("stepped aside for the rest of this invocation:")[-1].split("(")[0].strip()[:80]
                reasons[key] = reasons.get(key, 0) + 1
        r.log("bypass reasons: %s" % reasons)
        for k, v in sorted(asked.items(), key=lambda kv: -kv[1])[:15]:
            r.log("   %d x %s" % (v, k))
        r.ok("ops 6683 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
