#!/usr/bin/env python3
"""529 — Sweep audit BUILDs 10-15 Lambdas + sidecars + schedules."""
import json, os, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/529_buildlist_sweep.json"
lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

# Candidates to check (Lambda name → likely sidecar key)
CANDIDATES = {
    "justhodl-retail-sentiment": ("data/retail-sentiment.json", "BUILD 9 (Reddit/StockTwits)"),
    "google-trends-agent": ("data/google-trends.json", "BUILD 10 (Google Trends)"),
    "justhodl-cb-stance": ("data/cb-stance.json", "BUILD 11 (Central Bank Hawkish/Dovish)"),
    "justhodl-fed-speak": ("data/fed-speak.json", "BUILD 11 alt (Fed Speak)"),
    "justhodl-uspto-patents": ("data/uspto-patents.json", "BUILD 12 (USPTO Patents)"),
    "justhodl-0dte-flow": ("data/0dte-flow.json", "BUILD 13 (0DTE Expansion)"),
    "justhodl-international-flows": ("data/international.json", "BUILD 14 (International)"),
    "justhodl-commodity-curves": ("data/commodity-curves.json", "BUILD 15 (Commodity)"),
}


def audit_lambda(name):
    info = {"name": name}
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        info["exists"] = True
        info["last_modified"] = cfg.get("LastModified")
        info["memory"] = cfg.get("MemorySize")
        info["timeout"] = cfg.get("Timeout")
        info["env_keys"] = sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys())
        info["state"] = cfg.get("State")
        rules = []
        for r in eb.list_rules()["Rules"]:
            try:
                ts = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
                if any(name in t.get("Arn", "") for t in ts):
                    rules.append({"name": r["Name"], "schedule": r.get("ScheduleExpression"),
                                    "state": r.get("State")})
            except: pass
        info["rules"] = rules
    except lam.exceptions.ResourceNotFoundException:
        info["exists"] = False
    except Exception as e:
        info["err"] = str(e)[:200]
    return info


def audit_sidecar(key):
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        body = obj["Body"].read()
        try:
            p = json.loads(body)
        except: p = None
        info = {"size_kb": round(len(body) / 1024, 1),
                  "modified": obj["LastModified"].isoformat()[:19]}
        if isinstance(p, dict):
            info["top_keys"] = list(p.keys())[:25]
            info["version"] = p.get("version")
            info["generated_at"] = p.get("generated_at")
            # Surface common regime/signal fields
            for k in ("composite_regime", "regime", "market_regime", "signal",
                        "composite_signal", "market_regime_signal"):
                if k in p: info[k] = p[k]
        return info
    except s3.exceptions.NoSuchKey:
        return {"exists": False}
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    results = {}
    for name, (key, desc) in CANDIDATES.items():
        results[name] = {
            "desc": desc,
            "lambda": audit_lambda(name),
            "sidecar_key": key,
            "sidecar": audit_sidecar(key),
        }
    out["candidates"] = results

    # Also check for /retail/ /trends/ /fed/ /uspto/ /0dte/ pages in homepage
    try:
        with open("/var/task/index.html") if os.path.exists("/var/task/index.html") else open(os.environ.get("HOME","/tmp") + "/work/si/index.html") as f:
            idx = f.read()
    except: idx = ""
    if not idx:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="index.html")
            idx = obj["Body"].read().decode("utf-8")
        except: idx = ""
    out["homepage_links"] = {
        "/retail/": '"/retail/"' in idx,
        "/trends/": '"/trends/"' in idx,
        "/fed/": '"/fed/"' in idx or '"/cb/"' in idx,
        "/uspto/": '"/uspto/"' in idx or '"/patents/"' in idx,
        "/0dte/": '"/0dte/"' in idx,
        "/intl/": '"/intl/"' in idx or '"/international/"' in idx,
        "/commodity/": '"/commodity/"' in idx,
    }

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
