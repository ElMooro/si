"""ops 6664 - read-only fleet error census (default: the last 6 hours; CENSUS_HOURS / CENSUS_SINCE env): which engines are
failing NOW, how (errors / timeouts / out-of-memory), and whether they are FMP consumers - the data-driven queue for bank
fusion wave 3 instead of the keyword map.

Method: CloudWatch GetMetricData (Errors, Invocations, Duration max, Throttles) for every Lambda in the account over the
window, 500 metric queries per call; for every function with >= 1 error the last 3 error lines from its log group
(Task timed out / Runtime.OutOfMemory / Traceback / 429 / Bandwidth).  Cross-referenced with the repo's FMP consumer
census (scripts/shared_dependents.py fmp_bank importers) and the fused sets.  The report (committed by the lane) is the
only output; nothing in AWS is touched.  No key involved.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import os
import subprocess
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)
SINCE = datetime.fromisoformat(os.environ["CENSUS_SINCE"]) if os.environ.get("CENSUS_SINCE") else (datetime.now(timezone.utc) - timedelta(hours=int(os.environ.get("CENSUS_HOURS") or "6"))).replace(microsecond=0)
FUSED = set(("justhodl-bank-factors justhodl-magic-formula justhodl-divcut-warning justhodl-deep-value-screener justhodl-equity-ftd "
             "justhodl-stock-valuations justhodl-analyst-consensus justhodl-quality-on-sale justhodl-beneish justhodl-insider-radar "
             "justhodl-pre-pump-detector justhodl-pump-mechanics justhodl-ignition justhodl-coffee-can justhodl-buyback-yield-ranking "
             "justhodl-eva-spread justhodl-smart-beta justhodl-dividend-growth justhodl-pead-detector justhodl-post-earnings-mean-rev "
             "justhodl-rating-change-cluster justhodl-starmine justhodl-insider-cluster-scanner justhodl-insider-sell-cluster "
             "justhodl-insider-buyback-confluence justhodl-smart-money-holdings justhodl-smart-money-tracker justhodl-political-trades").split())


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def all_functions(lam):
    out, marker = [], None
    while True:
        kw = {"MaxItems": 50}
        if marker:
            kw["Marker"] = marker
        r = lam.list_functions(**kw)
        out += [(f["FunctionName"], f.get("MemorySize"), f.get("Timeout")) for f in r.get("Functions", [])]
        marker = r.get("NextMarker")
        if not marker:
            break
    return out


def metrics(cw, names, start, end):
    """{fn: {errors, invocations, duration_max_ms, throttles}} via GetMetricData, 500 queries per call."""
    out = {n: {"errors": 0, "invocations": 0, "duration_max_ms": 0, "throttles": 0} for n in names}
    specs = [("errors", "Errors", "Sum"), ("invocations", "Invocations", "Sum"), ("duration_max_ms", "Duration", "Maximum"), ("throttles", "Throttles", "Sum")]
    queries, keymap = [], {}
    for i, n in enumerate(names):
        for j, (field, metric, stat) in enumerate(specs):
            qid = "q%d_%d" % (i, j)
            keymap[qid] = (n, field)
            queries.append({"Id": qid, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": metric, "Dimensions": [{"Name": "FunctionName", "Value": n}]},
                                                      "Period": 86400 * 2, "Stat": stat}, "ReturnData": True})
    for k in range(0, len(queries), 500):
        batch = queries[k:k + 500]
        token = None
        while True:
            kw = {"MetricDataQueries": batch, "StartTime": start, "EndTime": end, "ScanBy": "TimestampDescending"}
            if token:
                kw["NextToken"] = token
            r = cw.get_metric_data(**kw)
            for res in r.get("MetricDataResults", []):
                n, field = keymap[res["Id"]]
                vals = res.get("Values") or []
                if vals:
                    out[n][field] = max(vals) if field == "duration_max_ms" else sum(vals)
            token = r.get("NextToken")
            if not token:
                break
    return out


def last_errors(logs, fn, since_ms, limit=3):
    try:
        r = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=since_ms, limit=40,
                                   filterPattern='?"Task timed out" ?"Runtime.OutOfMemory" ?Traceback ?"[ERROR]" ?"Bandwidth Limit" ?"HTTP Error 429" ?"errorMessage"')
        ev = [e.get("message", "").strip().replace("\n", " | ")[:220] for e in r.get("events", [])]
        return ev[-limit:]
    except Exception as e:  # noqa: BLE001
        return ["(logs: %s)" % type(e).__name__]


def classify(lines):
    s = " ".join(lines)
    if "Runtime.OutOfMemory" in s or "OutOfMemory" in s:
        return "out_of_memory"
    if "Task timed out" in s:
        return "timeout"
    if "Bandwidth Limit" in s or "429" in s:
        return "fmp_429"
    if "Traceback" in s or "[ERROR]" in s or "errorMessage" in s:
        return "exception"
    return "unknown"


def main():
    lam, cw, logs = c("lambda"), c("cloudwatch"), c("logs")
    try:
        fmp_consumers = set(subprocess.run([sys.executable, str(ROOT / "scripts" / "shared_dependents.py"), "aws/shared/fmp_bank.py"], capture_output=True, text=True, cwd=str(ROOT)).stdout.split())
    except Exception:  # noqa: BLE001
        fmp_consumers = set()
    with report("ops_6664_fleet_error_census") as r:
        r.heading("ops 6664 - fleet error census since the FMP block (%s UTC) - read-only" % SINCE.strftime("%Y-%m-%d %H:%M"))
        fns = all_functions(lam)
        names = [n for n, _, _ in fns]
        meta = {n: (m, t) for n, m, t in fns}
        r.log("functions: %d; fmp_bank importers in the repo: %d; fused so far: %d" % (len(fns), len(fmp_consumers), len(FUSED)))
        end = datetime.now(timezone.utc)
        m = metrics(cw, names, SINCE, end)
        failing = sorted([n for n in names if m[n]["errors"] >= 1], key=lambda n: -m[n]["errors"])
        invoked = sum(1 for n in names if m[n]["invocations"] >= 1)
        r.section("1. totals")
        r.log("invoked since the block: %d functions; with errors: %d; total errors %d; total invocations %d" % (invoked, len(failing), sum(m[n]["errors"] for n in names), sum(m[n]["invocations"] for n in names)))
        r.log("failing FMP consumers: %d (fused: %d, not yet fused: %d); failing non-FMP engines: %d" % (
            sum(1 for n in failing if n in fmp_consumers), sum(1 for n in failing if n in fmp_consumers and n in FUSED),
            sum(1 for n in failing if n in fmp_consumers and n not in FUSED), sum(1 for n in failing if n not in fmp_consumers)))
        r.section("2. failing functions (errors desc) - class, memory, timeout, duration max, last error lines")
        since_ms = int(SINCE.timestamp() * 1000)
        rows = []
        for n in failing[:120]:
            lines = last_errors(logs, n, since_ms)
            cls = classify(lines)
            mem, to = meta.get(n, (None, None))
            rows.append({"function": n, "errors": m[n]["errors"], "invocations": m[n]["invocations"], "class": cls, "memory_mb": mem, "timeout_s": to,
                         "duration_max_s": round((m[n]["duration_max_ms"] or 0) / 1000, 1), "fmp_consumer": n in fmp_consumers, "fused": n in FUSED, "last_errors": lines})
            r.log("%s: errors %d / inv %d | %s | %s MB / %s s | max %.0f s | fmp %s fused %s | %s" % (
                n, m[n]["errors"], m[n]["invocations"], cls, mem, to, (m[n]["duration_max_ms"] or 0) / 1000, "Y" if n in fmp_consumers else "n", "Y" if n in FUSED else "n", (lines[-1][:160] if lines else "")))
        r.section("3. by class")
        by = {}
        for x in rows:
            by.setdefault(x["class"], []).append(x["function"])
        for k, v in sorted(by.items(), key=lambda kv: -len(kv[1])):
            r.log("%s: %d -> %s" % (k, len(v), " ".join(v[:40])))
        r.section("4. wave-3 queue = failing FMP consumers not yet fused, by errors")
        q = [x for x in rows if x["fmp_consumer"] and not x["fused"]]
        r.log(" ".join(x["function"] for x in q))
        r.log("queue json: " + json.dumps({"since": SINCE.isoformat(), "wave3_queue": [x["function"] for x in q], "classes": {k: len(v) for k, v in by.items()}}))
        r.ok("ops 6664 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
