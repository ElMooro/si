"""
Ops 1097: audit Lambda performance configs for key engines.
Checks memory, timeout, and recent duration/error metrics.
Writes: aws/ops/reports/1097_perf_audit.json. Never raises.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone, timedelta
OUT_PATH = os.path.join("aws", "ops", "reports", "1097_perf_audit.json")
REGION = "us-east-1"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
from botocore.exceptions import ClientError
lam = boto3.client("lambda", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)

ENGINES = [
    "justhodl-ticker-360",
    "justhodl-master-ranker",
    "justhodl-conviction-engine",
    "justhodl-flow-confluence",
    "justhodl-best-ideas",
    "justhodl-options-confluence",
    "justhodl-short-interest",
    "justhodl-sec-8k-enrich",
    "justhodl-xbrl-fundamentals",
    "justhodl-corporate-actions",
    "justhodl-etf-issuer-holdings",
]

def get_config(fn):
    try:
        r = lam.get_function(FunctionName=fn)
        c = r["Configuration"]
        return {"memory": c.get("MemorySize"), "timeout": c.get("Timeout"),
                "runtime": c.get("Runtime")}
    except Exception as e:
        return {"error": str(e)[:60]}

def get_metrics(fn):
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=24)
        # Max duration
        dur = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Duration",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=start, EndTime=end, Period=86400,
            Statistics=["Maximum", "Average"])["Datapoints"]
        # Errors
        err = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=start, EndTime=end, Period=86400,
            Statistics=["Sum"])["Datapoints"]
        # Throttles
        thr = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Throttles",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=start, EndTime=end, Period=86400,
            Statistics=["Sum"])["Datapoints"]
        max_dur = max([d["Maximum"] for d in dur], default=0) / 1000  # ms to s
        avg_dur = sum(d["Average"] for d in dur) / len(dur) / 1000 if dur else 0
        errors = sum(d["Sum"] for d in err)
        throttles = sum(d["Sum"] for d in thr)
        return {"max_duration_s": round(max_dur, 1),
                "avg_duration_s": round(avg_dur, 1),
                "errors_24h": int(errors),
                "throttles_24h": int(throttles)}
    except Exception as e:
        return {"error": str(e)[:60]}

def main():
    rep = {"script": "1097_perf_audit",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "engines": []}
    for fn in ENGINES:
        entry = {"lambda": fn}
        entry["config"] = get_config(fn)
        entry["metrics"] = get_metrics(fn)
        # Flag issues
        cfg = entry["config"]
        met = entry["metrics"]
        issues = []
        if isinstance(cfg.get("timeout"), int) and isinstance(met.get("max_duration_s"), (int, float)):
            if met["max_duration_s"] > cfg["timeout"] * 0.8:
                issues.append(f"duration {met['max_duration_s']}s near timeout {cfg['timeout']}s")
        if met.get("throttles_24h", 0) > 0:
            issues.append(f"throttled {met['throttles_24h']}x")
        if met.get("errors_24h", 0) > 0:
            issues.append(f"{met['errors_24h']} errors")
        entry["issues"] = issues
        rep["engines"].append(entry)
    rep["status"] = "COMPLETE"
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    n_issues = sum(1 for e in rep["engines"] if e["issues"])
    print(json.dumps({"engines": len(rep["engines"]), "with_issues": n_issues}))

if __name__ == "__main__":
    main()
