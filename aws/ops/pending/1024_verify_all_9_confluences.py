"""
ops 1024 - Verify all 9 unique cross-engine confluence Lambdas deployed +
their S3 outputs landed (post engines #8 + #9 ship).

Engines verified:
  #1 justhodl-sequence-alpha-detector    -> data/sequence-alpha.json
  #2 justhodl-quality-on-sale            -> data/quality-on-sale.json
  #3 justhodl-forced-selling-bounce      -> data/forced-selling-bounce.json
  #4 justhodl-regime-conditional-router  -> data/regime-conditional-router.json
  #5 justhodl-ma-target-predictor        -> data/ma-target-predictor.json
  #6 justhodl-consensus-bottom           -> data/consensus-bottom.json
  #7 justhodl-corr-break-trade-router    -> data/corr-break-trade-router.json
  #8 justhodl-powell-pivot               -> data/powell-pivot.json
  #9 justhodl-earnings-tone-velocity     -> data/earnings-tone-velocity.json

For each:
  - Lambda deployed? (get_function — get name + state + last_modified +
    code_size + schedule existence)
  - INVOKE the Lambda fresh (forces real S3 write) — gets fresh JSON output
  - S3 output present? size? age?
  - Output parseable JSON? schema_version present? expected top-level keys?

Writes aws/ops/reports/1024.json with full scorecard.
"""
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=120, connect_timeout=15)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)

ENGINES = [
    {"num": 1, "lambda": "justhodl-sequence-alpha-detector",
     "s3": "data/sequence-alpha.json",
     "expected_top_keys": ["schema_version", "as_of"]},
    {"num": 2, "lambda": "justhodl-quality-on-sale",
     "s3": "data/quality-on-sale.json",
     "expected_top_keys": ["schema_version", "as_of"]},
    {"num": 3, "lambda": "justhodl-forced-selling-bounce",
     "s3": "data/forced-selling-bounce.json",
     "expected_top_keys": ["schema_version", "as_of"]},
    {"num": 4, "lambda": "justhodl-regime-conditional-router",
     "s3": "data/regime-conditional-router.json",
     "expected_top_keys": ["schema_version", "as_of"]},
    {"num": 5, "lambda": "justhodl-ma-target-predictor",
     "s3": "data/ma-target-predictor.json",
     "expected_top_keys": ["schema_version", "as_of"]},
    {"num": 6, "lambda": "justhodl-consensus-bottom",
     "s3": "data/consensus-bottom.json",
     "expected_top_keys": ["schema_version", "as_of"]},
    {"num": 7, "lambda": "justhodl-corr-break-trade-router",
     "s3": "data/corr-break-trade-router.json",
     "expected_top_keys": ["schema_version", "as_of"]},
    {"num": 8, "lambda": "justhodl-powell-pivot",
     "s3": "data/powell-pivot.json",
     "expected_top_keys": ["schema_version", "as_of", "current_state",
                            "score_0_100", "factor_rotation_recommendation"]},
    {"num": 9, "lambda": "justhodl-earnings-tone-velocity",
     "s3": "data/earnings-tone-velocity.json",
     "expected_top_keys": ["schema_version", "as_of", "summary",
                            "top_positive_velocity",
                            "top_negative_velocity"]},
]


def lambda_status(name):
    """Check Lambda exists + return summary."""
    try:
        c = lam.get_function(FunctionName=name)["Configuration"]
        return {
            "exists": True,
            "state": c.get("State"),
            "last_modified": c.get("LastModified"),
            "code_size": c.get("CodeSize"),
            "runtime": c.get("Runtime"),
            "memory_mb": c.get("MemorySize"),
            "timeout_s": c.get("Timeout"),
        }
    except lam.exceptions.ResourceNotFoundException:
        return {"exists": False}
    except Exception as e:
        return {"exists": "unknown", "error": str(e)[:200]}


def lambda_has_schedule(name):
    """Check if Lambda has any EventBridge schedule (Rules OR Scheduler)."""
    found = []
    try:
        paginator = sch.get_paginator("list_schedules")
        for page in paginator.paginate():
            for s in page.get("Schedules", []):
                try:
                    full = sch.get_schedule(
                        GroupName=s.get("GroupName", "default"),
                        Name=s["Name"])
                    arn = (full.get("Target") or {}).get("Arn", "")
                    if name in arn:
                        found.append({
                            "api": "scheduler", "name": s.get("Name"),
                            "expr": s.get("ScheduleExpression"),
                            "state": s.get("State")})
                except Exception:
                    pass
    except Exception:
        pass
    try:
        paginator = events.get_paginator("list_rules")
        for page in paginator.paginate():
            for r in page.get("Rules", []):
                if not r.get("ScheduleExpression"):
                    continue
                try:
                    targets = events.list_targets_by_rule(
                        Rule=r["Name"]).get("Targets", [])
                    for t in targets:
                        if name in t.get("Arn", ""):
                            found.append({
                                "api": "events", "name": r["Name"],
                                "expr": r.get("ScheduleExpression"),
                                "state": r.get("State")})
                            break
                except Exception:
                    pass
    except Exception:
        pass
    return found


def invoke_lambda(name):
    """Invoke fresh + return result."""
    try:
        r = lam.invoke(FunctionName=name,
                       InvocationType="RequestResponse",
                       Payload=b"{}")
        return {
            "ok": r.get("StatusCode", 0) < 300,
            "status": r.get("StatusCode"),
            "function_error": r.get("FunctionError"),
            "duration_ms": None,  # not exposed in API
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def s3_check(key):
    """HEAD on S3 key + return basic metadata + age."""
    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key=key)
        now = datetime.now(timezone.utc)
        lm = h["LastModified"]
        return {
            "present": True,
            "size_bytes": h.get("ContentLength"),
            "last_modified": lm.isoformat(),
            "age_seconds": int((now - lm).total_seconds()),
            "etag": h.get("ETag"),
        }
    except s3.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey"):
            return {"present": False}
        return {"present": "unknown", "error": str(e)[:200]}
    except Exception as e:
        return {"present": False, "error": str(e)[:200]}


def s3_parse(key):
    """Fetch JSON content + return top-level structure."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = obj["Body"].read()
        j = json.loads(body.decode("utf-8"))
        return {
            "parsed": True,
            "type": type(j).__name__,
            "top_keys": (list(j.keys())[:20] if isinstance(j, dict)
                          else None),
            "n_items": (len(j) if isinstance(j, (list, dict)) else None),
            "schema_version": (j.get("schema_version")
                                if isinstance(j, dict) else None),
            "as_of": j.get("as_of") if isinstance(j, dict) else None,
            "has_error_field": (bool(j.get("error"))
                                  if isinstance(j, dict) else False),
            "error_msg": j.get("error") if isinstance(j, dict) else None,
        }
    except Exception as e:
        return {"parsed": False, "error": str(e)[:200]}


def main():
    started = time.time()
    report = {"started_at": datetime.now(timezone.utc).isoformat(),
              "engines": [],
              "scorecard": {}}

    pass_lambda_exists = 0
    pass_lambda_invoke_ok = 0
    pass_lambda_scheduled = 0
    pass_s3_present = 0
    pass_s3_fresh_5min = 0
    pass_s3_parseable = 0
    pass_expected_keys = 0

    for eng in ENGINES:
        print(f"[ops 1024] verifying #{eng['num']} {eng['lambda']}")
        entry = {"engine_num": eng["num"], "lambda": eng["lambda"],
                  "s3_key": eng["s3"]}

        # Lambda state
        entry["lambda_status"] = lambda_status(eng["lambda"])
        if entry["lambda_status"].get("exists") is True:
            pass_lambda_exists += 1

        # Schedule
        entry["schedules"] = lambda_has_schedule(eng["lambda"])
        if entry["schedules"]:
            pass_lambda_scheduled += 1

        # Force fresh invoke if Lambda exists (proves end-to-end)
        if entry["lambda_status"].get("exists") is True:
            entry["fresh_invoke"] = invoke_lambda(eng["lambda"])
            if entry["fresh_invoke"].get("ok") and not entry["fresh_invoke"].get(
                    "function_error"):
                pass_lambda_invoke_ok += 1
            # Brief sleep before S3 check to let write settle
            time.sleep(2)

        # S3 check
        entry["s3_check"] = s3_check(eng["s3"])
        if entry["s3_check"].get("present") is True:
            pass_s3_present += 1
            if entry["s3_check"].get("age_seconds", 99999) < 300:
                pass_s3_fresh_5min += 1

        # Parse + validate schema
        if entry["s3_check"].get("present") is True:
            entry["s3_parse"] = s3_parse(eng["s3"])
            if entry["s3_parse"].get("parsed") is True:
                pass_s3_parseable += 1
                missing_keys = [k for k in eng["expected_top_keys"]
                                  if k not in (entry["s3_parse"].get(
                                      "top_keys") or [])]
                entry["missing_expected_keys"] = missing_keys
                if not missing_keys:
                    pass_expected_keys += 1
            else:
                entry["missing_expected_keys"] = "parse failed"

        report["engines"].append(entry)

    n = len(ENGINES)
    report["scorecard"] = {
        "total_engines": n,
        "lambda_deployed":      f"{pass_lambda_exists}/{n}",
        "lambda_scheduled":      f"{pass_lambda_scheduled}/{n}",
        "fresh_invoke_ok":       f"{pass_lambda_invoke_ok}/{n}",
        "s3_output_present":     f"{pass_s3_present}/{n}",
        "s3_fresh_under_5min":   f"{pass_s3_fresh_5min}/{n}",
        "s3_parseable_json":     f"{pass_s3_parseable}/{n}",
        "schema_keys_correct":   f"{pass_expected_keys}/{n}",
        "ALL_PASS": (pass_lambda_exists == n
                     and pass_lambda_invoke_ok == n
                     and pass_s3_present == n
                     and pass_s3_parseable == n
                     and pass_expected_keys == n),
    }
    report["duration_seconds"] = round(time.time() - started, 1)
    report["ended_at"] = datetime.now(timezone.utc).isoformat()

    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1024.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1024] report written {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps(report["scorecard"], indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
