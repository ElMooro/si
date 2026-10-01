"""
Ops 1068: final production validation sweep — classify all 10 upgrades (read-only).

Re-checks each Bloomberg-parity upgrade after today's fixes:
  1. FRED macro regime        2. SEC 8-K enrichment      3. FINRA short-interest
  4. SEC XBRL                 5. CBOE options            6. FINRA TRACE (bond)
  7. Corporate actions         8. Price redundancy       9. Symbology/OpenFIGI
 10. ETF issuer holdings

For each: trigger/schedule state, code freshness (LastModified), recent
invocations + errors (CloudWatch, 24h), and key S3 artifact presence.
Also re-checks the 8 canonical R-engine Scheduler schedules + transcript.

Writes: aws/ops/reports/1068_final_validation.json (committed back by run-ops).
Read-only. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1068_final_validation.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))

import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

lam = boto3.client("lambda", region_name=REGION)
ev = boto3.client("events", region_name=REGION)
sched = boto3.client("scheduler", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

NOW = datetime.now(timezone.utc)
DAY = timedelta(days=1)


def _safe(fn, *a, **k):
    try:
        return fn(*a, **k)
    except Exception:  # noqa: BLE001 - best effort
        return None


def lambda_state(name):
    """Code freshness for a lambda. Never raises."""
    r = _safe(lam.get_function, FunctionName=name)
    if not r:
        return {"exists": False}
    cfg = r.get("Configuration", {})
    return {"exists": True,
            "last_modified": cfg.get("LastModified"),
            "timeout": cfg.get("Timeout")}


def invocations(name, hours=24):
    """Invocation + error counts from CloudWatch. Never raises."""
    out = {"invocations": None, "errors": None}
    for metric, key in (("Invocations", "invocations"), ("Errors", "errors")):
        r = _safe(cw.get_metric_statistics,
                  Namespace="AWS/Lambda", MetricName=metric,
                  Dimensions=[{"Name": "FunctionName", "Value": name}],
                  StartTime=NOW - timedelta(hours=hours), EndTime=NOW,
                  Period=hours * 3600, Statistics=["Sum"])
        if r and r.get("Datapoints"):
            out[key] = int(sum(d.get("Sum", 0) for d in r["Datapoints"]))
    return out


def artifact(key):
    """Head-object check. Never raises."""
    r = _safe(s3.head_object, Bucket=BUCKET, Key=key)
    if not r:
        return {"exists": False}
    lm = r.get("LastModified")
    return {"exists": True,
            "last_modified": lm.isoformat() if lm else None,
            "size": r.get("ContentLength")}


def rule_state(name):
    """Classic EventBridge rule state. Never raises."""
    try:
        r = ev.describe_rule(Name=name)
        return {"found": True, "state": r.get("State"),
                "expression": r.get("ScheduleExpression")}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"found": False}
        return {"found": False, "error": str(e)[:80]}
    except Exception:  # noqa: BLE001
        return {"found": False}


def scheduler_state(name):
    """Scheduler schedule state. Never raises."""
    try:
        r = sched.get_schedule(Name=name)
        return {"found": True, "state": r.get("State"),
                "expression": r.get("ScheduleExpression")}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"found": False}
        return {"found": False, "error": str(e)[:80]}
    except Exception:  # noqa: BLE001
        return {"found": False}


# (upgrade label, lambda, trigger check, artifacts, notes)
CHECKS = [
    ("1/10 FRED macro regime", "justhodl-fred-macro-regime",
     ("rule", "justhodl-fred-macro-regime-daily"),
     ["data/fred-macro-regime.json"], ""),
    ("2/10 SEC 8-K enrichment", "justhodl-sec-8k-enrich",
     ("rule", "justhodl-sec-8k-enrich-schedule"),
     ["data/8k-filings-enriched.json", "data/8k-by-ticker.json"],
     "schedule created by 1063"),
    ("3/10 FINRA short-interest", "justhodl-short-interest",
     ("rule", "justhodl-short-interest-daily"),
     ["data/short-interest.json", "data/short-interest-tickers.json"],
     "request_path regex fixed; redeployed"),
    ("4/10 SEC XBRL", "justhodl-xbrl-fundamentals",
     ("rule", "justhodl-xbrl-fundamentals-schedule"),
     ["data/xbrl-fundamentals-index.json"],
     "schedule created by 1063"),
    ("5/10 CBOE options", "justhodl-cboe-options-chain",
     ("rule", "justhodl-cboe-options-chain-schedule"),
     ["data/cboe-options-chain.json"],
     "schedule created by 1063"),
    ("6/10 FINRA TRACE (bond)", "justhodl-bond-trace",
     ("rule", "justhodl-bond-trace-daily"),
     ["data/bond-trace.json"],
     "finra_trace rewired to corporateMarketBreadth; 1067 PASS"),
    ("7/10 Corporate actions", "justhodl-corporate-actions",
     ("rule", "justhodl-corporate-actions-schedule"),
     ["data/corporate-actions-index.json"],
     "schedule created by 1063"),
    ("8/10 Price redundancy", "justhodl-price-redundancy",
     ("rule", "justhodl-price-redundancy-15min"),
     ["data/quote-health.json"],
     "redeployed; rule pre-existed+enabled"),
    ("9/10 Symbology/OpenFIGI", "justhodl-symbology-master",
     ("rule", "justhodl-symbology-master-daily"),
     ["data/symbology-master.json"], ""),
    ("10/10 ETF issuer holdings", "justhodl-etf-issuer-holdings",
     ("rule", "justhodl-etf-issuer-holdings-daily"),
     ["data/etf-issuer-holdings.json"],
     "percent parser fixed; rule created by 1060; redeployed"),
]

RENGINE = [
    "justhodl-peer-comparison-daily", "justhodl-screen-builder-universe-daily",
    "justhodl-fedwatch-rate-probability-daily", "fedwatch-rate-probability-sched",
    "justhodl-sec-filing-diff-daily", "justhodl-supply-chain-linkage-daily",
    "justhodl-factor-decomposition-weekly", "justhodl-fx-decomposition-daily",
    "justhodl-cftc-deep-view-fri", "justhodl-transcript-query-weekly",
]


def classify(u):
    """PASS / DEGRADED / FAIL for one upgrade unit. Never raises."""
    trig = u["trigger"]
    trig_ok = trig.get("found") and trig.get("state") in ("ENABLED", "ACTIVE")
    invoked = (u["activity"].get("invocations") or 0) > 0
    errors = u["activity"].get("errors") or 0
    arts = [a for a in u["artifacts"].values() if a.get("exists")]
    if trig_ok and invoked and errors == 0 and arts:
        return "PASS"
    if trig_ok and (invoked or arts):
        return "DEGRADED"
    return "FAIL"


def main():
    """Run the sweep, write the report, print the classification."""
    report = {"script": "1068_final_validation",
              "read_at": NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
              "upgrades": {}, "r_engine": {}}
    for label, fn, (kind, tname), arts, notes in CHECKS:
        trig = rule_state(tname) if kind == "rule" else scheduler_state(tname)
        u = {"lambda": fn, "code": lambda_state(fn), "trigger": trig,
             "trigger_name": tname, "activity": invocations(fn),
             "artifacts": {a: artifact(a) for a in arts}, "notes": notes}
        u["verdict"] = classify(u)
        report["upgrades"][label] = u
    for sname in RENGINE:
        st = scheduler_state(sname)
        if not st.get("found"):
            st = rule_state(sname)
        report["r_engine"][sname] = st
    counts = {}
    for u in report["upgrades"].values():
        counts[u["verdict"]] = counts.get(u["verdict"], 0) + 1
    report["summary"] = counts
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps({"summary": counts,
                      "verdicts": {k: v["verdict"]
                                   for k, v in report["upgrades"].items()}},
                     indent=2))


if __name__ == "__main__":
    main()
