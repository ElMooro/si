#!/usr/bin/env python3
"""Step 317 — FULL system audit.

Inventories the entire JustHodl.AI platform:
  1. All Lambdas:     name, runtime, schedule, last invocation, code size,
                      S3 outputs (parsed from env vars), purpose (from docstring)
  2. All S3 data:     every key under data/ + portfolio/ + backtest/, with
                      size, age, top-level keys (schema hint)
  3. All EB rules:    which target which Lambda
  4. All DDB tables:  size, item count
  5. Frontend pages:  list of .html files in repo + their data dependencies
                      (which S3 keys they fetch)

Cross-references reveal:
  - ORPHAN LAMBDAS:    produce S3 data nobody reads
  - ORPHAN PAGES:      reference S3 data nobody produces
  - DEAD PAGES:        no recent S3 update behind them
  - GHOST DATA:        S3 files >7d old still being fetched
"""
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
ACCOUNT_ID = "857687956942"
REPORT = "aws/ops/reports/317_full_audit.json"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────────
# LAMBDAS
# ─────────────────────────────────────────────────────────────────────────────
def list_all_lambdas():
    out = {}
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page.get("Functions", []):
            name = fn["FunctionName"]
            if not name.startswith("justhodl") and \
               not any(p in name for p in ("dex-scanner", "cftc", "nyfed", "bloomberg", "openbb")):
                continue
            out[name] = {
                "runtime": fn.get("Runtime"),
                "memory_mb": fn.get("MemorySize"),
                "timeout_s": fn.get("Timeout"),
                "code_size_kb": round(fn.get("CodeSize", 0) / 1024, 1),
                "last_modified": fn.get("LastModified"),
                "description": (fn.get("Description") or "")[:200],
                "env_vars": list((fn.get("Environment", {}) or {}).get("Variables", {}).keys()),
                "s3_keys": [
                    v for k, v in (fn.get("Environment", {}) or {}).get("Variables", {}).items()
                    if "KEY" in k.upper() and "/" in str(v) and ".json" in str(v).lower()
                ],
            }
    return out


def get_lambda_invocations(name, hours=24):
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=hours)
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=start, EndTime=end,
            Period=hours * 3600,
            Statistics=["Sum"],
        )
        return int(sum(p["Sum"] for p in resp.get("Datapoints", [])))
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# EB RULES
# ─────────────────────────────────────────────────────────────────────────────
def list_eb_rules_for_all_lambdas(lambda_names):
    out = {}
    for name in lambda_names:
        arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{name}"
        try:
            rules = events.list_rule_names_by_target(TargetArn=arn).get("RuleNames", [])
            out[name] = []
            for rn in rules:
                r = events.describe_rule(Name=rn)
                out[name].append({
                    "rule": rn,
                    "schedule": r.get("ScheduleExpression"),
                    "state": r.get("State"),
                })
        except Exception as e:
            out[name] = [{"err": str(e)[:100]}]
    return out


# ─────────────────────────────────────────────────────────────────────────────
# S3 DATA INVENTORY
# ─────────────────────────────────────────────────────────────────────────────
def list_all_s3():
    """Walk all keys in dashboard bucket. Group by prefix."""
    out = []
    paginator = s3.get_paginator("list_objects_v2")
    for prefix in ("data/", "backtest/", "portfolio/", "screener/",
                    "calls/", "learning/", "themes/", "intel/"):
        try:
            for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []) or []:
                    key = obj["Key"]
                    if not key.endswith(".json"):
                        continue
                    age_h = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600
                    out.append({
                        "key": key,
                        "size_kb": round(obj["Size"] / 1024, 1),
                        "age_hours": round(age_h, 2),
                        "last_modified": obj["LastModified"].isoformat(),
                    })
        except Exception:
            continue
    return out


def sample_s3_top_keys(key, max_keys=12):
    """Get the top-level keys of a JSON file (cheap schema sniff)."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key, Range="bytes=0-2048")
        body = obj["Body"].read().decode("utf-8", errors="replace")
        # If truncated JSON, find the top keys via regex
        keys = re.findall(r'"([a-z_][a-z0-9_]*)"\s*:', body, re.IGNORECASE)
        return list(dict.fromkeys(keys))[:max_keys]
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# DDB TABLES
# ─────────────────────────────────────────────────────────────────────────────
def list_ddb_tables():
    out = {}
    try:
        tables = ddb.list_tables().get("TableNames", [])
        for t in tables:
            if not t.startswith("justhodl"):
                continue
            try:
                desc = ddb.describe_table(TableName=t)["Table"]
                out[t] = {
                    "item_count": desc.get("ItemCount"),
                    "size_bytes": desc.get("TableSizeBytes"),
                    "status": desc.get("TableStatus"),
                    "billing_mode": desc.get("BillingModeSummary", {}).get("BillingMode"),
                }
            except Exception as e:
                out[t] = {"err": str(e)[:100]}
    except Exception as e:
        out["_err"] = str(e)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# FRONTEND PAGES (read from local repo — sandbox limitation: skip if not run from repo)
# ─────────────────────────────────────────────────────────────────────────────
def scan_html_pages():
    """If running in repo, scan HTML files for data/* fetch patterns."""
    out = {}
    if not os.path.isdir("."):
        return out
    for fn in os.listdir("."):
        if not fn.endswith(".html"):
            continue
        try:
            with open(fn, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
            # Find S3 paths referenced
            patterns = re.findall(r'(data/[a-z0-9_\-/]+\.json|backtest/[a-z0-9_\-]+\.json|'
                                  r'portfolio/[a-z0-9_\-/]+\.json|screener/[a-z0-9_\-]+\.json)',
                                  content)
            out[fn] = {
                "size_kb": round(len(content) / 1024, 1),
                "data_refs": list(dict.fromkeys(patterns)),
            }
        except Exception:
            continue
    return out


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    started = time.time()
    print("[317] Listing Lambdas…")
    lambdas = list_all_lambdas()

    print(f"[317] Found {len(lambdas)} Lambdas. Getting invocations…")
    for name in lambdas:
        lambdas[name]["invocations_24h"] = get_lambda_invocations(name)

    print(f"[317] Listing EB rules…")
    rules = list_eb_rules_for_all_lambdas(list(lambdas.keys()))
    for name, rl in rules.items():
        if name in lambdas:
            lambdas[name]["eb_rules"] = rl

    print(f"[317] Listing S3 data…")
    s3_data = list_all_s3()

    print(f"[317] Sampling top keys for first 30 S3 files…")
    s3_data.sort(key=lambda x: x["age_hours"])
    for entry in s3_data[:30]:
        entry["top_keys"] = sample_s3_top_keys(entry["key"])

    print(f"[317] Listing DDB tables…")
    tables = list_ddb_tables()

    print(f"[317] Scanning HTML pages…")
    html = scan_html_pages()

    # Build cross-reference: orphans
    s3_keys_set = {e["key"] for e in s3_data}
    consumed_keys = set()
    for fn, info in html.items():
        for ref in info.get("data_refs", []):
            consumed_keys.add(ref.lstrip("/"))

    # Orphan S3: produced but never read by a page
    orphan_s3 = sorted(s3_keys_set - consumed_keys)

    # Orphan HTML refs: pages reference S3 keys that don't exist
    all_html_refs = set()
    for info in html.values():
        for ref in info.get("data_refs", []):
            all_html_refs.add(ref.lstrip("/"))
    missing_data_refs = sorted(all_html_refs - s3_keys_set)

    # Stale data: >24h old + still being referenced by pages
    stale = [e for e in s3_data if e["age_hours"] > 24 and e["key"] in consumed_keys]

    out = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 1),
        "lambdas": lambdas,
        "n_lambdas": len(lambdas),
        "s3_data": s3_data,
        "n_s3_data_keys": len(s3_data),
        "tables": tables,
        "html_pages": html,
        "n_html_pages": len(html),
        "cross_ref": {
            "orphan_s3_keys": orphan_s3[:60],
            "n_orphan_s3": len(orphan_s3),
            "missing_data_refs": missing_data_refs[:30],
            "n_missing_data_refs": len(missing_data_refs),
            "stale_consumed_data": [
                {"key": e["key"], "age_hours": e["age_hours"]}
                for e in stale[:30]
            ],
            "n_stale_consumed_data": len(stale),
        },
    }

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)

    # Pretty summary
    print()
    print("═" * 80)
    print(f"  PLATFORM AUDIT — {len(lambdas)} Lambdas · "
          f"{len(s3_data)} S3 datasets · {len(tables)} DDB tables · "
          f"{len(html)} HTML pages")
    print("═" * 80)

    # Lambdas with no schedule (orphan or on-demand)
    no_schedule = [n for n, d in lambdas.items()
                    if not d.get("eb_rules") or not any(r.get("schedule") for r in d.get("eb_rules", []))]
    print(f"\n  Lambdas without EB schedule: {len(no_schedule)}")
    for n in no_schedule[:15]:
        d = lambdas[n]
        print(f"    {n:<45s} inv24h={d.get('invocations_24h','?'):<5s}".replace("None","?"))

    # Orphan S3 datasets (data with no consumer)
    print(f"\n  ORPHAN S3 keys (no HTML page consumes them): {len(orphan_s3)}")
    for k in orphan_s3[:30]:
        print(f"    {k}")

    # Pages referencing missing data
    if missing_data_refs:
        print(f"\n  ⚠️  Pages reference {len(missing_data_refs)} S3 keys that DON'T EXIST:")
        for r in missing_data_refs[:15]:
            print(f"    {r}")

    # Stale data still being referenced
    if stale:
        print(f"\n  ⚠️  {len(stale)} S3 keys are >24h old but still consumed by pages:")
        for e in stale[:15]:
            print(f"    {e['key']:<50s} age={e['age_hours']:.1f}h")

    print(f"\n  Duration: {out['duration_s']}s")


if __name__ == "__main__":
    main()
