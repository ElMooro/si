"""
Ops 1070: discover triggers for the 10 upgrade lambdas (read-only).

Scans classic EventBridge rules and Scheduler schedules for bindings that
target each lambda, instead of guessing rule names. Also re-checks code
freshness, 24h activity, and artifacts.

Writes: aws/ops/reports/1070_triggers.json (committed back by run-ops).
Read-only. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1070_triggers.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
ACCT = "857687956942"

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

LAMBDAS = {
    "1/10 FRED macro regime": "justhodl-macro-regime",
    "2/10 SEC 8-K enrichment": "justhodl-sec-8k-enrich",
    "3/10 FINRA short-interest": "justhodl-short-interest",
    "4/10 SEC XBRL": "justhodl-xbrl-fundamentals",
    "5/10 CBOE options": "justhodl-cboe-options-chain",
    "6/10 FINRA TRACE (bond)": "justhodl-bond-trace",
    "7/10 Corporate actions": "justhodl-corporate-actions",
    "8/10 Price redundancy": "justhodl-price-redundancy",
    "9/10 Symbology/OpenFIGI": "justhodl-symbology-master",
    "10/10 ETF issuer holdings": "justhodl-etf-issuer-holdings",
}

ARTIFACTS = {
    "1/10 FRED macro regime": ["data/macro-regime.json"],
    "2/10 SEC 8-K enrichment": ["data/8k-filings-enriched.json"],
    "3/10 FINRA short-interest": ["data/short-interest.json",
                                  "data/short-interest-tickers.json"],
    "4/10 SEC XBRL": ["data/xbrl-fundamentals.json"],
    "5/10 CBOE options": ["data/options-chain.json"],
    "6/10 FINRA TRACE (bond)": ["data/bond-trace.json"],
    "7/10 Corporate actions": ["data/corporate-actions.json"],
    "8/10 Price redundancy": ["data/quote-health.json"],
    "9/10 Symbology/OpenFIGI": ["data/symbology.json"],
    "10/10 ETF issuer holdings": ["data/etf-issuer-holdings.json"],
}


def _safe(fn, *a, **k):
    try:
        return fn(*a, **k)
    except Exception:  # noqa: BLE001
        return None


def discover_triggers(fn):
    """Find classic rules + scheduler schedules targeting fn. Never raises."""
    arn_frag = f":function:{fn}"
    found = []
    try:
        pag = ev.get_paginator("list_rules")
        for page in pag.paginate(NamePrefix="justhodl-"):
            for r in page.get("Rules", []):
                rn = r.get("Name")
                tg = _safe(ev.list_targets_by_rule, Rule=rn) or {}
                for t in tg.get("Targets", []):
                    if arn_frag in t.get("Arn", ""):
                        found.append({"type": "rule", "name": rn,
                                      "state": r.get("State"),
                                      "expression": r.get("ScheduleExpression")})
                        break
    except Exception:  # noqa: BLE001
        pass
    try:
        pag = sched.get_paginator("list_schedules")
        for page in pag.paginate():
            for s in page.get("Schedules", []):
                sn = s.get("Name")
                det = _safe(sched.get_schedule, Name=sn) or {}
                if arn_frag in (det.get("Target") or {}).get("Arn", ""):
                    found.append({"type": "scheduler", "name": sn,
                                  "state": det.get("State"),
                                  "expression": det.get("ScheduleExpression")})
    except Exception:  # noqa: BLE001
        pass
    return found


def activity(fn):
    """24h invocations/errors. Never raises."""
    out = {}
    for metric in ("Invocations", "Errors"):
        r = _safe(cw.get_metric_statistics, Namespace="AWS/Lambda",
                  MetricName=metric,
                  Dimensions=[{"Name": "FunctionName", "Value": fn}],
                  StartTime=NOW - timedelta(hours=24), EndTime=NOW,
                  Period=86400, Statistics=["Sum"])
        out[metric.lower()] = (int(sum(d.get("Sum", 0)
                                       for d in (r.get("Datapoints", [])
                                                 if r else [])))
                               if r else None)
    return out


def main():
    """Discover triggers + state for each upgrade lambda, write report."""
    report = {"script": "1070_triggers",
              "read_at": NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
              "upgrades": {}}
    for label, fn in LAMBDAS.items():
        r = _safe(lam.get_function, FunctionName=fn) or {}
        cfg = r.get("Configuration", {})
        arts = {}
        for a in ARTIFACTS.get(label, []):
            h = _safe(s3.head_object, Bucket=BUCKET, Key=a)
            arts[a] = {"exists": bool(h),
                       "size": h.get("ContentLength") if h else None}
        trig = discover_triggers(fn)
        enabled = [t for t in trig if t.get("state") in ("ENABLED", "ACTIVE")]
        act = activity(fn)
        inv = act.get("invocations") or 0
        err = act.get("errors") or 0
        has_art = any(a.get("exists") for a in arts.values())
        if enabled and inv > 0 and err == 0 and has_art:
            verdict = "PASS"
        elif enabled and (inv > 0 or has_art):
            verdict = "DEGRADED"
        elif not enabled and (inv > 0 or has_art):
            verdict = "DEGRADED (no enabled trigger)"
        else:
            verdict = "FAIL"
        report["upgrades"][label] = {
            "lambda": fn,
            "last_modified": cfg.get("LastModified"),
            "timeout": cfg.get("Timeout"),
            "triggers": trig,
            "activity_24h": act,
            "artifacts": arts,
            "verdict": verdict,
        }
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps({k: v["verdict"]
                      for k, v in report["upgrades"].items()}, indent=2))


if __name__ == "__main__":
    main()
