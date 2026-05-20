"""
ops 975 - Retail-Edges 7-Engine Force-Deploy + Verify (BUG-FIXED from 974)
==========================================================================

Bug discovered in ops 974: `Path(__file__).resolve().parents[2]` resolves to
`/runner/work/si/si/aws/` instead of the repo root, so the JSON report was
written to a phantom path `aws/aws/ops/reports/974.json` and lost. Fix:
use `parents[3]` (or Path.cwd() — workflow runs from repo root anyway).

Also wraps the entire main() in try/finally so the partial report always
persists, and dumps the final report to stdout as JSON for workflow-log
visibility regardless of file-write success.

Operations (all idempotent):
  1. Force create-or-update 7 retail-edges Lambdas with donor env vars
  2. Create-or-update 7 EventBridge schedules
  3. Add scheduler→Lambda invoke permissions
  4. Synchronously invoke each Lambda, parse response
  5. Verify S3 output: schema, state, last-modified
  6. Redeploy signal-board so its 28-engine FEEDS list goes live
  7. Verify retail-edges.html page is live with all 7 engine markers

Output:
  - aws/ops/reports/975.json (full structured report)
  - stdout (same report as JSON at end, for log inspection)
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
    {"name": "justhodl-earnings-iv-crush",
     "s3_key": "data/earnings-iv-crush.json",
     "schedule": "cron(0 22 * * ? *)",
     "memory": 1024, "timeout": 600,
     "schedule_name": "justhodl-earnings-iv-crush-daily"},
    {"name": "justhodl-stealth-accumulation",
     "s3_key": "data/stealth-accumulation.json",
     "schedule": "cron(0 23 * * ? *)",
     "memory": 256, "timeout": 60,
     "schedule_name": "justhodl-stealth-accumulation-daily"},
    {"name": "justhodl-failed-pattern-reversal",
     "s3_key": "data/failed-pattern-reversal.json",
     "schedule": "cron(30 22 * * ? *)",
     "memory": 1024, "timeout": 600,
     "schedule_name": "justhodl-failed-pattern-reversal-daily"},
    {"name": "justhodl-squeeze-pretrigger",
     "s3_key": "data/squeeze-pretrigger.json",
     "schedule": "cron(30 23 * * ? *)",
     "memory": 768, "timeout": 540,
     "schedule_name": "justhodl-squeeze-pretrigger-daily"},
    {"name": "justhodl-catalyst-skew-premove",
     "s3_key": "data/catalyst-skew-premove.json",
     "schedule": "cron(30 0 * * ? *)",
     "memory": 256, "timeout": 60,
     "schedule_name": "justhodl-catalyst-skew-premove-daily"},
    {"name": "justhodl-crypto-etf-arb",
     "s3_key": "data/crypto-etf-arb.json",
     "schedule": "cron(0,30 13-21 ? * MON-FRI *)",
     "memory": 256, "timeout": 60,
     "schedule_name": "justhodl-crypto-etf-arb-30min"},
    {"name": "justhodl-lockup-expiration",
     "s3_key": "data/lockup-expiration.json",
     "schedule": "cron(0 22 * * ? *)",
     "memory": 512, "timeout": 180,
     "schedule_name": "justhodl-lockup-expiration-daily"},
]

# FIX: parents[3] is the actual repo root (not parents[2] which is /repo/aws)
REPO_ROOT = Path(__file__).resolve().parents[3]


def make_zip(fn_name):
    src = REPO_ROOT / "aws" / "lambdas" / fn_name / "source" / "lambda_function.py"
    if not src.exists():
        return None, f"source missing: {src}"
    code = src.read_text(encoding="utf-8")
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", code)
    return buf.getvalue(), None


def donor_env(lambda_client):
    try:
        r = lambda_client.get_function_configuration(FunctionName=DONOR_FN)
        env = r.get("Environment", {}).get("Variables", {})
        wanted = ["FMP_KEY", "FRED_KEY", "POLYGON_KEY", "ALPHA_VANTAGE_KEY",
                  "CMC_KEY", "ANTHROPIC_API_KEY", "TELEGRAM_BOT_TOKEN",
                  "TELEGRAM_CHAT_ID", "NEWSAPI_KEY", "BLS_KEY", "BEA_KEY",
                  "CENSUS_KEY", "S3_BUCKET"]
        return {k: env[k] for k in wanted if k in env}
    except Exception as e:
        print(f"donor_env err: {e}")
        return {}


def ensure_lambda(lambda_client, eng, env_vars):
    name = eng["name"]
    zip_bytes, err = make_zip(name)
    if err:
        return {"ok": False, "error": err, "stage": "zip"}
    try:
        lambda_client.get_function(FunctionName=name)
        exists = True
    except lambda_client.exceptions.ResourceNotFoundException:
        exists = False
    desc = f"retail-edges/{name.replace('justhodl-', '')}"[:255]
    try:
        if exists:
            lambda_client.update_function_code(
                FunctionName=name, ZipFile=zip_bytes, Publish=True)
            time.sleep(3)
            lambda_client.update_function_configuration(
                FunctionName=name,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Role=ROLE,
                Timeout=eng["timeout"],
                MemorySize=eng["memory"],
                Environment={"Variables": env_vars},
                Description=desc,
            )
            mode = "updated"
        else:
            lambda_client.create_function(
                FunctionName=name,
                Runtime="python3.12",
                Role=ROLE,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zip_bytes},
                Timeout=eng["timeout"],
                MemorySize=eng["memory"],
                Environment={"Variables": env_vars},
                Description=desc,
            )
            mode = "created"
        for _ in range(20):
            r = lambda_client.get_function_configuration(FunctionName=name)
            if r.get("State") == "Active" and r.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        return {"ok": True, "mode": mode}
    except Exception as e:
        return {"ok": False, "error": str(e), "stage": "ensure_lambda",
                "trace": traceback.format_exc()[:400]}


def ensure_schedule(sched_client, eng):
    name = eng["schedule_name"]
    target_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{eng['name']}"
    params = {
        "Name": name,
        "ScheduleExpression": eng["schedule"],
        "ScheduleExpressionTimezone": "UTC",
        "Target": {
            "Arn": target_arn,
            "RoleArn": SCHEDULER_ROLE,
            "Input": json.dumps({"source": "ops975", "manual": False}),
        },
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "State": "ENABLED",
        "Description": f"retail-edges {eng['name']}"[:512],
    }
    try:
        sched_client.get_schedule(Name=name)
        sched_client.update_schedule(**params)
        return {"ok": True, "mode": "updated"}
    except sched_client.exceptions.ResourceNotFoundException:
        try:
            sched_client.create_schedule(**params)
            return {"ok": True, "mode": "created"}
        except Exception as e:
            return {"ok": False, "error": str(e), "stage": "create_schedule"}
    except Exception as e:
        return {"ok": False, "error": str(e), "stage": "ensure_schedule"}


def grant_invoke(lambda_client, eng):
    name = eng["name"]
    sid = f"AllowScheduler-{eng['schedule_name']}"[:100]
    try:
        lambda_client.add_permission(
            FunctionName=name,
            StatementId=sid,
            Action="lambda:InvokeFunction",
            Principal="scheduler.amazonaws.com",
            SourceArn=f"arn:aws:scheduler:{REGION}:{ACCOUNT}:schedule/default/{eng['schedule_name']}",
        )
        return {"ok": True, "mode": "added"}
    except lambda_client.exceptions.ResourceConflictException:
        return {"ok": True, "mode": "exists"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def invoke_engine(lambda_client, name):
    try:
        r = lambda_client.invoke(FunctionName=name, InvocationType="RequestResponse",
                                   Payload=b"{}")
        sc = r["StatusCode"]
        body = r["Payload"].read().decode("utf-8", errors="ignore")
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {"raw": body[:500]}
        return {"ok": sc == 200, "status_code": sc,
                "function_error": r.get("FunctionError"),
                "body_preview": (str(parsed)[:600] if isinstance(parsed, (dict, list)) else str(parsed)[:600])}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def verify_s3(s3, key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        return {
            "ok": True,
            "size_bytes": obj["ContentLength"],
            "last_modified": obj["LastModified"].isoformat(),
            "engine": data.get("engine"),
            "state": data.get("state"),
            "as_of": data.get("as_of") or data.get("generated_at") or data.get("timestamp"),
            "schema_keys": sorted(list(data.keys()))[:20],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def verify_signal_board(s3):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
        data = json.loads(obj["Body"].read())
        return {
            "ok": True,
            "n_engines": data.get("n_engines"),
            "n_live": data.get("n_live"),
            "composite_posture": data.get("composite_posture"),
            "composite_signal": data.get("composite_signal"),
            "expects_28": data.get("n_engines") == 28,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def verify_page(url="https://justhodl.ai/retail-edges.html"):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ops975/1.0",
                                                     "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="ignore")
        markers = {
            "earnings-iv-crush": "earnings-iv-crush" in html,
            "stealth-accumulation": "stealth-accumulation" in html,
            "failed-pattern-reversal": "failed-pattern-reversal" in html,
            "squeeze-pretrigger": "squeeze-pretrigger" in html,
            "catalyst-skew-premove": "catalyst-skew-premove" in html,
            "crypto-etf-arb": "crypto-etf-arb" in html,
            "lockup-expiration": "lockup-expiration" in html,
        }
        return {
            "ok": True,
            "status": r.status,
            "size": len(html),
            "has_best_trades": "Today's Best Retail Trades" in html or "best-trades" in html.lower(),
            "markers_found": sum(1 for v in markers.values() if v),
            "markers": markers,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    print("=" * 70)
    print("ops 975 -- Retail-Edges 7-Engine Force-Deploy + Verify (bug-fixed)")
    print(f"REPO_ROOT={REPO_ROOT}")
    print("=" * 70)
    lambda_c = boto3.client("lambda", region_name=REGION)
    sched_c = boto3.client("scheduler", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    env_vars = donor_env(lambda_c)
    env_vars["S3_BUCKET"] = S3_BUCKET
    print(f"donor env keys: {sorted(env_vars.keys())}")

    report = {
        "ops": 975,
        "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "repo_root": str(REPO_ROOT),
        "engines": {},
    }

    try:
        for eng in ENGINES:
            print(f"\n--- {eng['name']} ---")
            engr = {}
            engr["deploy"] = ensure_lambda(lambda_c, eng, env_vars)
            print(f"  deploy: {engr['deploy'].get('mode') or engr['deploy'].get('error', '?')[:80]}")
            engr["schedule"] = ensure_schedule(sched_c, eng)
            print(f"  schedule: {engr['schedule'].get('mode') or engr['schedule'].get('error', '?')[:80]}")
            engr["permission"] = grant_invoke(lambda_c, eng)
            print(f"  permission: {engr['permission'].get('mode') or engr['permission'].get('error', '?')[:80]}")
            if engr["deploy"].get("ok"):
                print("  invoking...")
                engr["invoke"] = invoke_engine(lambda_c, eng["name"])
                print(f"  invoke: status={engr['invoke'].get('status_code')} "
                      f"err={engr['invoke'].get('function_error')}")
                time.sleep(2)
                engr["s3"] = verify_s3(s3, eng["s3_key"])
                print(f"  s3: ok={engr['s3'].get('ok')} state={engr['s3'].get('state')} "
                      f"size={engr['s3'].get('size_bytes')}")
            report["engines"][eng["name"]] = engr

        # Signal-board redeploy
        print(f"\n--- justhodl-signal-board (redeploy) ---")
        try:
            sb_zip, sb_err = make_zip("justhodl-signal-board")
            if not sb_err:
                lambda_c.update_function_code(
                    FunctionName="justhodl-signal-board", ZipFile=sb_zip, Publish=True)
                for _ in range(20):
                    r = lambda_c.get_function_configuration(FunctionName="justhodl-signal-board")
                    if r.get("LastUpdateStatus") == "Successful":
                        break
                    time.sleep(2)
                print("  signal-board code updated, invoking...")
                sb_invoke = invoke_engine(lambda_c, "justhodl-signal-board")
                print(f"  invoke: {sb_invoke.get('status_code')}")
                time.sleep(3)
                sb_check = verify_signal_board(s3)
                report["signal_board"] = {
                    "deploy_ok": True,
                    "invoke": sb_invoke,
                    "verify": sb_check,
                }
                print(f"  signal-board: n_engines={sb_check.get('n_engines')} "
                      f"expects28={sb_check.get('expects_28')}")
            else:
                report["signal_board"] = {"deploy_ok": False, "error": sb_err}
        except Exception as e:
            report["signal_board"] = {"deploy_ok": False, "error": str(e),
                                        "trace": traceback.format_exc()[:600]}

        # Page verify
        print(f"\n--- retail-edges.html page ---")
        report["page"] = verify_page()
        print(f"  page: ok={report['page'].get('ok')} size={report['page'].get('size')} "
              f"markers_found={report['page'].get('markers_found')}/7")

        # Scorecard
        n_deploy_ok = sum(1 for e in report["engines"].values() if e.get("deploy", {}).get("ok"))
        n_schedule_ok = sum(1 for e in report["engines"].values() if e.get("schedule", {}).get("ok"))
        n_invoke_ok = sum(1 for e in report["engines"].values()
                          if e.get("invoke", {}).get("ok") and not e.get("invoke", {}).get("function_error"))
        n_s3_ok = sum(1 for e in report["engines"].values() if e.get("s3", {}).get("ok"))
        page_ok = report["page"].get("ok") and report["page"].get("markers_found", 0) == 7
        sb_ok = report.get("signal_board", {}).get("verify", {}).get("expects_28", False)

        report["scorecard"] = {
            "n_engines": len(ENGINES),
            "n_deploy_ok": n_deploy_ok,
            "n_schedule_ok": n_schedule_ok,
            "n_invoke_ok": n_invoke_ok,
            "n_s3_ok": n_s3_ok,
            "page_ok": page_ok,
            "signal_board_ok": sb_ok,
            "all_pass": (n_deploy_ok == 7 and n_schedule_ok == 7 and
                         n_invoke_ok == 7 and n_s3_ok == 7 and page_ok and sb_ok),
        }
        report["ended_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        print("\n" + "=" * 70)
        print(f"SCORECARD: {json.dumps(report['scorecard'], indent=2)}")
        print("=" * 70)

    finally:
        # Always persist the report, even if main raised
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "975.json"
        try:
            report_path.write_text(json.dumps(report, indent=2, default=str))
            print(f"\nReport written: {report_path.relative_to(REPO_ROOT)}")
        except Exception as wex:
            print(f"\nReport file write FAILED: {wex}")
        # Also dump to stdout for log-visibility
        print("\n" + "=" * 70 + "\nFULL REPORT JSON:\n" + "=" * 70)
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(f"FATAL: {e}\n{traceback.format_exc()}")
        sys.exit(1)
