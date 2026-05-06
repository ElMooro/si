#!/usr/bin/env python3
"""Step 284 — Investigate the 2 console-created Lambdas not in repo.

Per parallel session step 261 audit: deployed_not_in_repo = [
  "justhodl-cdn-diag-temp",   # name suggests temporary CDN diagnostic
  "justhodl-ka-metrics",      # KA = Khalid Index? unclear
]

For each:
  1. Lambda metadata (handler, env, last_modified, code_size)
  2. 30d invocation pattern
  3. Any EB rule (full scan)
  4. Function URL?
  5. Download the actual source code so it can be either re-imported
     to git or trashed without losing the implementation
  6. Recent log messages (clue to purpose)

Output drives the decision: re-import to git (keep + maintain),
delete (truly disposable), or escalate to Khalid (unclear purpose).
"""
import base64
import io
import json
import os
import time
import urllib.request
import zipfile
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
REPORT_PATH = "aws/ops/reports/284_console_lambdas_investigation.json"
SOURCE_DUMP_DIR = "aws/ops/reports/284_console_lambdas_source"

LAMBDAS = ["justhodl-cdn-diag-temp", "justhodl-ka-metrics"]

lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def daily_invocations_30d(name):
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=start, EndTime=end,
            Period=86400, Statistics=["Sum"],
        )
        days = sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"])
        return [{"date": d["Timestamp"].strftime("%Y-%m-%d"),
                 "invocations": int(d["Sum"])}
                for d in days if d["Sum"] > 0]
    except Exception as e:
        return [{"err": str(e)[:200]}]


def has_eb_rule_full_scan(name):
    arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{name}"
    matches = []
    paginator = eb.get_paginator("list_rules")
    for page in paginator.paginate():
        for rule in page.get("Rules", []):
            try:
                tgts = eb.list_targets_by_rule(Rule=rule["Name"])
                for t in tgts.get("Targets", []):
                    if t.get("Arn") == arn:
                        matches.append({
                            "rule": rule["Name"],
                            "schedule": rule.get("ScheduleExpression"),
                            "state": rule.get("State"),
                        })
            except Exception:
                pass
    return matches


def get_function_url(name):
    try:
        return lam.get_function_url_config(FunctionName=name).get("FunctionUrl")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return None
        return f"err:{e}"


def download_source(name):
    """Download the deployment package + extract handler source."""
    try:
        cfg = lam.get_function(FunctionName=name)
        code_url = cfg["Code"]["Location"]

        # Download zip
        req = urllib.request.Request(code_url)
        with urllib.request.urlopen(req, timeout=30) as r:
            zip_bytes = r.read()

        # Extract handler file(s) — typically one .py file
        os.makedirs(SOURCE_DUMP_DIR, exist_ok=True)
        out_dir = os.path.join(SOURCE_DUMP_DIR, name)
        os.makedirs(out_dir, exist_ok=True)

        files_extracted = []
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for info in zf.namelist():
                if info.endswith("/"):
                    continue
                # Skip binary deps and large files
                if "/" in info and not info.startswith(("__pycache__", ".")):
                    # nested file — likely a dependency
                    continue
                try:
                    content = zf.read(info)
                    out_path = os.path.join(out_dir, info)
                    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
                    with open(out_path, "wb") as f:
                        f.write(content)
                    files_extracted.append({
                        "filename": info,
                        "size_bytes": len(content),
                    })
                except Exception:
                    pass

        return {
            "extracted_to": out_dir,
            "n_files": len(files_extracted),
            "files": files_extracted[:10],
        }
    except Exception as e:
        return {"err": str(e)[:300]}


def get_recent_logs(name, max_events=8):
    log_group = f"/aws/lambda/{name}"
    try:
        streams = logs.describe_log_streams(
            logGroupName=log_group, orderBy="LastEventTime",
            descending=True, limit=2,
        ).get("logStreams", [])
        events = []
        for s in streams:
            ev = logs.get_log_events(
                logGroupName=log_group, logStreamName=s["logStreamName"],
                limit=max_events,
            ).get("events", [])
            for e in ev:
                events.append({
                    "ts": datetime.fromtimestamp(e["timestamp"]/1000,
                                                  tz=timezone.utc).isoformat(),
                    "msg": e["message"][:400].strip(),
                })
        return events[:max_events]
    except Exception as e:
        return [{"err": str(e)[:200]}]


def get_meta(name):
    try:
        cfg = lam.get_function(FunctionName=name)
        return {
            "arn": cfg["Configuration"]["FunctionArn"],
            "runtime": cfg["Configuration"].get("Runtime"),
            "handler": cfg["Configuration"].get("Handler"),
            "memory_mb": cfg["Configuration"].get("MemorySize"),
            "timeout_s": cfg["Configuration"].get("Timeout"),
            "code_size": cfg["Configuration"].get("CodeSize"),
            "last_modified": cfg["Configuration"].get("LastModified"),
            "description": cfg["Configuration"].get("Description"),
            "env_vars": list((cfg["Configuration"].get("Environment") or {}).get("Variables", {}).keys()),
        }
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat(), "results": []}
    for name in LAMBDAS:
        print(f"[284] investigating {name}…")
        result = {"name": name}
        result["meta"] = get_meta(name)
        result["invocations_30d"] = daily_invocations_30d(name)
        result["total_invocations_30d"] = sum(
            d.get("invocations", 0) for d in result["invocations_30d"]
            if isinstance(d, dict) and "invocations" in d
        )
        result["eb_rules"] = has_eb_rule_full_scan(name)
        result["function_url"] = get_function_url(name)
        result["source_download"] = download_source(name)
        result["recent_logs"] = get_recent_logs(name)
        out["results"].append(result)

    out["duration_s"] = round(time.time() - started, 1)

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
