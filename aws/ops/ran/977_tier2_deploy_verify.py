"""
ops 977 - Tier-2 Retail-Edges 8-Engine Force-Deploy + Verify
==============================================================

Idempotent end-to-end deploy + verify for the 8 Tier-2 retail-edge engines:
  1. precatalyst-vol-expansion  (long-vol pre-event)
  2. cef-discount               (CEF NAV mean-rev)
  3. reit-nav-discount          (REIT NAV mean-rev)
  4. divcut-warning             (avoidance)
  5. rating-change-cluster      (smart-money clustering)
  6. multi-tf-convergence       (3-TF trend alignment)
  7. 52wk-quality-breakout      (quality-gated breakouts)
  8. spac-floor-warrant         (asymmetric trust-value floor)

For each engine:
  - Create or update Lambda (poll LastUpdateStatus between code+config -- ops 976 fix)
  - Donor env injection (FMP/FRED/Polygon/Telegram/etc from buyback-scanner)
  - EventBridge schedule create or update
  - Scheduler invoke permission (idempotent add)
  - Sync invoke and capture status
  - S3 verify: file exists with engine + state + summary keys

Then redeploys signal-board so 36-engine FEEDS list goes live, invokes it,
verifies n_engines == 36.

Finally fetches retail-edges.html, confirms 15 engine markers present.

Patterns inherited from ops 976 (which passed 7/7 PASS):
  - REPO_ROOT = Path(__file__).resolve().parents[3]
  - wait_for_settled() between every Lambda update operation
  - try/finally guarantees report write even on partial failure
  - Final stdout JSON dump for workflow-log visibility
"""
import json
import os
import sys
import time
import traceback
import urllib.request
import zipfile
from io import BytesIO
from pathlib import Path

import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
SCHEDULER_ROLE = f"arn:aws:iam::{ACCOUNT}:role/justhodl-scheduler-role"
S3_BUCKET = "justhodl-dashboard-live"
DONOR_FN = "justhodl-buyback-scanner"

ENGINES = [
    {"name": "justhodl-precatalyst-vol-expansion",
     "s3_key": "data/precatalyst-vol-expansion.json",
     "schedule": "cron(30 21 * * ? *)",
     "memory": 1024, "timeout": 540,
     "schedule_name": "justhodl-precatalyst-vol-expansion-daily"},
    {"name": "justhodl-cef-discount",
     "s3_key": "data/cef-discount.json",
     "schedule": "cron(30 22 * * ? *)",
     "memory": 512, "timeout": 240,
     "schedule_name": "justhodl-cef-discount-daily"},
    {"name": "justhodl-reit-nav-discount",
     "s3_key": "data/reit-nav-discount.json",
     "schedule": "cron(0 22 ? * MON *)",
     "memory": 1024, "timeout": 540,
     "schedule_name": "justhodl-reit-nav-discount-weekly"},
    {"name": "justhodl-divcut-warning",
     "s3_key": "data/divcut-warning.json",
     "schedule": "cron(0 22 ? * SUN *)",
     "memory": 768, "timeout": 540,
     "schedule_name": "justhodl-divcut-warning-weekly"},
    {"name": "justhodl-rating-change-cluster",
     "s3_key": "data/rating-change-cluster.json",
     "schedule": "cron(30 23 * * ? *)",
     "memory": 512, "timeout": 300,
     "schedule_name": "justhodl-rating-change-cluster-daily"},
    {"name": "justhodl-multi-tf-convergence",
     "s3_key": "data/multi-tf-convergence.json",
     "schedule": "cron(0 0 * * ? *)",
     "memory": 1024, "timeout": 600,
     "schedule_name": "justhodl-multi-tf-convergence-daily"},
    {"name": "justhodl-52wk-quality-breakout",
     "s3_key": "data/52wk-quality-breakout.json",
     "schedule": "cron(0 22 * * ? *)",
     "memory": 1024, "timeout": 600,
     "schedule_name": "justhodl-52wk-quality-breakout-daily"},
    {"name": "justhodl-spac-floor-warrant",
     "s3_key": "data/spac-floor-warrant.json",
     "schedule": "cron(0 22 ? * MON *)",
     "memory": 512, "timeout": 180,
     "schedule_name": "justhodl-spac-floor-warrant-weekly"},
]

REPO_ROOT = Path(__file__).resolve().parents[3]


def wait_for_settled(lambda_c, name, max_wait=120):
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            r = lambda_c.get_function_configuration(FunctionName=name)
            state = r.get("State")
            lus = r.get("LastUpdateStatus")
            if state == "Active" and lus in ("Successful", None):
                return ("Active", lus or "None")
            if lus == "Failed":
                return (state, lus)
        except Exception as e:
            return ("Unknown", f"err:{str(e)[:60]}")
        time.sleep(2)
    return ("Timeout", "?")


def make_zip(fn_name):
    src = REPO_ROOT / "aws" / "lambdas" / fn_name / "source" / "lambda_function.py"
    if not src.exists():
        return None, f"source missing: {src}"
    code = src.read_text(encoding="utf-8")
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", code)
    return buf.getvalue(), None


def donor_env(lambda_c):
    try:
        r = lambda_c.get_function_configuration(FunctionName=DONOR_FN)
        env = r.get("Environment", {}).get("Variables", {})
        wanted = ["FMP_KEY", "FRED_KEY", "POLYGON_KEY", "ALPHA_VANTAGE_KEY",
                  "CMC_KEY", "ANTHROPIC_API_KEY", "TELEGRAM_BOT_TOKEN",
                  "TELEGRAM_CHAT_ID", "NEWSAPI_KEY", "BLS_KEY", "BEA_KEY",
                  "CENSUS_KEY", "S3_BUCKET"]
        return {k: env[k] for k in wanted if k in env}
    except Exception as e:
        print(f"donor_env err: {e}")
        return {}


def force_deploy(lambda_c, eng, env_vars):
    name = eng["name"]
    out = {"name": name}
    zip_bytes, err = make_zip(name)
    if err:
        return {**out, "ok": False, "stage": "zip", "error": err}
    try:
        lambda_c.get_function(FunctionName=name)
        exists = True
    except lambda_c.exceptions.ResourceNotFoundException:
        exists = False
    out["exists_before"] = exists
    if exists:
        state_b, lus_b = wait_for_settled(lambda_c, name, max_wait=60)
        out["pre_state"] = f"{state_b}/{lus_b}"
    desc = f"tier2-retail-edges/{name.replace('justhodl-', '')}"[:255]
    try:
        if exists:
            lambda_c.update_function_code(
                FunctionName=name, ZipFile=zip_bytes, Publish=True)
            state_c, lus_c = wait_for_settled(lambda_c, name, max_wait=120)
            out["post_code_state"] = f"{state_c}/{lus_c}"
            if lus_c == "Failed":
                return {**out, "ok": False, "stage": "code_update", "error": "code update failed"}
            lambda_c.update_function_configuration(
                FunctionName=name, Runtime="python3.12",
                Handler="lambda_function.lambda_handler", Role=ROLE,
                Timeout=eng["timeout"], MemorySize=eng["memory"],
                Environment={"Variables": env_vars}, Description=desc)
            state_f, lus_f = wait_for_settled(lambda_c, name, max_wait=120)
            out["post_cfg_state"] = f"{state_f}/{lus_f}"
            mode = "updated"
        else:
            lambda_c.create_function(
                FunctionName=name, Runtime="python3.12", Role=ROLE,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zip_bytes},
                Timeout=eng["timeout"], MemorySize=eng["memory"],
                Environment={"Variables": env_vars}, Description=desc)
            state_f, lus_f = wait_for_settled(lambda_c, name, max_wait=120)
            out["post_create_state"] = f"{state_f}/{lus_f}"
            mode = "created"
        return {**out, "ok": True, "mode": mode}
    except Exception as e:
        return {**out, "ok": False, "stage": "ensure", "error": str(e)[:300],
                "trace": traceback.format_exc()[:400]}


def ensure_schedule(sched_c, eng):
    name = eng["schedule_name"]
    target_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{eng['name']}"
    params = {
        "Name": name, "ScheduleExpression": eng["schedule"],
        "ScheduleExpressionTimezone": "UTC",
        "Target": {"Arn": target_arn, "RoleArn": SCHEDULER_ROLE,
                    "Input": json.dumps({"source": "ops977"})},
        "FlexibleTimeWindow": {"Mode": "OFF"}, "State": "ENABLED",
        "Description": f"tier2-retail-edges {eng['name']}"[:512],
    }
    try:
        sched_c.get_schedule(Name=name)
        sched_c.update_schedule(**params)
        return {"ok": True, "mode": "updated"}
    except sched_c.exceptions.ResourceNotFoundException:
        try:
            sched_c.create_schedule(**params)
            return {"ok": True, "mode": "created"}
        except Exception as e:
            return {"ok": False, "error": str(e)[:200]}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def grant_invoke(lambda_c, eng):
    sid = f"AllowScheduler-{eng['schedule_name']}"[:100]
    try:
        lambda_c.add_permission(
            FunctionName=eng["name"], StatementId=sid,
            Action="lambda:InvokeFunction", Principal="scheduler.amazonaws.com",
            SourceArn=f"arn:aws:scheduler:{REGION}:{ACCOUNT}:schedule/default/{eng['schedule_name']}")
        return {"ok": True, "mode": "added"}
    except lambda_c.exceptions.ResourceConflictException:
        return {"ok": True, "mode": "exists"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def invoke_engine(lambda_c, name):
    try:
        r = lambda_c.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
        sc = r["StatusCode"]
        body = r["Payload"].read().decode("utf-8", errors="ignore")
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {"raw": body[:500]}
        return {"ok": sc == 200 and not r.get("FunctionError"),
                "status_code": sc, "function_error": r.get("FunctionError"),
                "body_preview": str(parsed)[:800]}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def verify_s3(s3, key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        return {"ok": True, "size_bytes": obj["ContentLength"],
                "last_modified": obj["LastModified"].isoformat(),
                "engine": data.get("engine"), "state": data.get("state"),
                "as_of": data.get("as_of"),
                "schema_keys": sorted(list(data.keys()))[:25]}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def verify_signal_board(s3):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
        data = json.loads(obj["Body"].read())
        return {"ok": True, "n_engines": data.get("n_engines"),
                "n_live": data.get("n_live"), "n_stale": data.get("n_stale"),
                "composite_posture": data.get("composite_posture"),
                "composite_signal": data.get("composite_signal"),
                "expects_36": data.get("n_engines") == 36}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def verify_page(url="https://justhodl.ai/retail-edges.html"):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ops977/1.0",
                                                     "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="ignore")
        all_markers = [
            # Tier-1 (7)
            "earnings-iv-crush", "stealth-accumulation", "failed-pattern-reversal",
            "squeeze-pretrigger", "catalyst-skew-premove", "crypto-etf-arb",
            "lockup-expiration",
            # Tier-2 (8)
            "precatalyst-vol-expansion", "cef-discount", "reit-nav-discount",
            "divcut-warning", "rating-change-cluster", "multi-tf-convergence",
            "52wk-quality-breakout", "spac-floor-warrant",
        ]
        markers = {m: m in html for m in all_markers}
        return {"ok": True, "status": r.status, "size": len(html),
                "markers": markers, "markers_found": sum(markers.values()),
                "total_markers_expected": len(all_markers)}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def main():
    print("=" * 70)
    print("ops 977 -- Tier-2 Retail-Edges 8-Engine Force-Deploy + Verify")
    print(f"REPO_ROOT={REPO_ROOT}")
    print("=" * 70)
    lambda_c = boto3.client("lambda", region_name=REGION)
    sched_c = boto3.client("scheduler", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    env_vars = donor_env(lambda_c)
    env_vars["S3_BUCKET"] = S3_BUCKET
    print(f"donor env keys: {sorted(env_vars.keys())}")

    report = {
        "ops": 977,
        "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "repo_root": str(REPO_ROOT),
        "engines": {},
    }

    try:
        for eng in ENGINES:
            print(f"\n--- {eng['name']} ---")
            engr = {}
            engr["deploy"] = force_deploy(lambda_c, eng, env_vars)
            print(f"  deploy: ok={engr['deploy'].get('ok')} mode={engr['deploy'].get('mode')} "
                  f"pre={engr['deploy'].get('pre_state','-')} "
                  f"post_code={engr['deploy'].get('post_code_state','-')} "
                  f"post_cfg={engr['deploy'].get('post_cfg_state','-')}")
            if not engr["deploy"].get("ok"):
                print(f"  deploy ERROR: {engr['deploy'].get('error','')[:200]}")
            engr["schedule"] = ensure_schedule(sched_c, eng)
            print(f"  schedule: {engr['schedule']}")
            engr["permission"] = grant_invoke(lambda_c, eng)
            print(f"  permission: {engr['permission']}")
            print("  invoking...")
            engr["invoke"] = invoke_engine(lambda_c, eng["name"])
            print(f"  invoke: ok={engr['invoke'].get('ok')} status={engr['invoke'].get('status_code')} "
                  f"err={engr['invoke'].get('function_error')}")
            if not engr["invoke"].get("ok"):
                print(f"  invoke body: {engr['invoke'].get('body_preview','')[:300]}")
            time.sleep(3)
            engr["s3"] = verify_s3(s3, eng["s3_key"])
            print(f"  s3: ok={engr['s3'].get('ok')} state={engr['s3'].get('state')} "
                  f"size={engr['s3'].get('size_bytes')}")
            if not engr["s3"].get("ok"):
                print(f"  s3 error: {engr['s3'].get('error','')[:200]}")
            report["engines"][eng["name"]] = engr

        # Signal-board redeploy + verify
        print(f"\n--- justhodl-signal-board (redeploy + verify 36 engines) ---")
        try:
            sb_zip, sb_err = make_zip("justhodl-signal-board")
            if not sb_err:
                wait_for_settled(lambda_c, "justhodl-signal-board", max_wait=60)
                lambda_c.update_function_code(
                    FunctionName="justhodl-signal-board", ZipFile=sb_zip, Publish=True)
                wait_for_settled(lambda_c, "justhodl-signal-board", max_wait=120)
                print("  signal-board code updated, invoking...")
                sb_invoke = invoke_engine(lambda_c, "justhodl-signal-board")
                print(f"  invoke: {sb_invoke.get('status_code')}")
                time.sleep(4)
                sb_check = verify_signal_board(s3)
                report["signal_board"] = {
                    "deploy_ok": True, "invoke": sb_invoke, "verify": sb_check}
                print(f"  signal-board: n_engines={sb_check.get('n_engines')} "
                      f"expects36={sb_check.get('expects_36')} "
                      f"n_live={sb_check.get('n_live')}")
            else:
                report["signal_board"] = {"deploy_ok": False, "error": sb_err}
        except Exception as e:
            report["signal_board"] = {"deploy_ok": False, "error": str(e)[:300],
                                       "trace": traceback.format_exc()[:600]}

        print(f"\n--- retail-edges.html page (15 markers expected) ---")
        report["page"] = verify_page()
        print(f"  page: ok={report['page'].get('ok')} "
              f"markers={report['page'].get('markers_found')}/{report['page'].get('total_markers_expected')}")

        # Scorecard
        n_deploy = sum(1 for e in report["engines"].values() if e.get("deploy", {}).get("ok"))
        n_schedule = sum(1 for e in report["engines"].values() if e.get("schedule", {}).get("ok"))
        n_invoke = sum(1 for e in report["engines"].values() if e.get("invoke", {}).get("ok"))
        n_s3 = sum(1 for e in report["engines"].values() if e.get("s3", {}).get("ok"))
        page_ok = (report["page"].get("ok") and
                   report["page"].get("markers_found", 0) == 15)
        sb_ok = report.get("signal_board", {}).get("verify", {}).get("expects_36", False)

        report["scorecard"] = {
            "n_engines": len(ENGINES),
            "n_deploy_ok": n_deploy,
            "n_schedule_ok": n_schedule,
            "n_invoke_ok": n_invoke,
            "n_s3_ok": n_s3,
            "page_ok": page_ok,
            "signal_board_36_ok": sb_ok,
            "all_pass": (n_deploy == 8 and n_schedule == 8 and
                         n_invoke == 8 and n_s3 == 8 and page_ok and sb_ok),
        }
        report["ended_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        print("\n" + "=" * 70)
        print(f"SCORECARD: {json.dumps(report['scorecard'], indent=2)}")
        print("=" * 70)

    finally:
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "977.json"
        try:
            out_path.write_text(json.dumps(report, indent=2, default=str))
            print(f"\nReport written: {out_path.relative_to(REPO_ROOT)}")
        except Exception as wex:
            print(f"\nReport write FAILED: {wex}")
        print("\n=== FULL REPORT JSON ===")
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(f"FATAL: {e}\n{traceback.format_exc()}")
        sys.exit(1)
