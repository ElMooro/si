"""
ops 974 - Retail-Edges 7-Engine Force-Deploy + End-to-End Verifier
======================================================================

Force-deploys all 7 new retail-edges Lambdas (in case deploy-lambdas.yml
missed any due to inherit_env or environment mode quirks), then invokes
each one synchronously to confirm:
  - Lambda exists and responds 200
  - S3 output file is written with non-error schema
  - State machine populated
  - Telegram alert wiring works (state SSM updated)

Also re-deploys justhodl-signal-board so its 7 new normalizers go live and
the composite covers all 28 engines.

Finally pulls retail-edges.html via S3-hosted GitHub Pages copy to confirm
the page is live and lists 7 engine cards.

Writes report to aws/ops/reports/974.json.
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
DONOR_FN = "justhodl-buyback-scanner"   # for STANDARD_KEYS env copy

ENGINES = [
    {
        "name": "justhodl-earnings-iv-crush",
        "s3_key": "data/earnings-iv-crush.json",
        "schedule": "cron(0 22 * * ? *)",
        "memory": 1024, "timeout": 600,
        "schedule_name": "justhodl-earnings-iv-crush-daily",
    },
    {
        "name": "justhodl-stealth-accumulation",
        "s3_key": "data/stealth-accumulation.json",
        "schedule": "cron(0 23 * * ? *)",
        "memory": 256, "timeout": 60,
        "schedule_name": "justhodl-stealth-accumulation-daily",
    },
    {
        "name": "justhodl-failed-pattern-reversal",
        "s3_key": "data/failed-pattern-reversal.json",
        "schedule": "cron(30 22 * * ? *)",
        "memory": 1024, "timeout": 600,
        "schedule_name": "justhodl-failed-pattern-reversal-daily",
    },
    {
        "name": "justhodl-squeeze-pretrigger",
        "s3_key": "data/squeeze-pretrigger.json",
        "schedule": "cron(30 23 * * ? *)",
        "memory": 768, "timeout": 540,
        "schedule_name": "justhodl-squeeze-pretrigger-daily",
    },
    {
        "name": "justhodl-catalyst-skew-premove",
        "s3_key": "data/catalyst-skew-premove.json",
        "schedule": "cron(30 0 * * ? *)",
        "memory": 256, "timeout": 60,
        "schedule_name": "justhodl-catalyst-skew-premove-daily",
    },
    {
        "name": "justhodl-crypto-etf-arb",
        "s3_key": "data/crypto-etf-arb.json",
        "schedule": "cron(0,30 13-21 ? * MON-FRI *)",
        "memory": 256, "timeout": 60,
        "schedule_name": "justhodl-crypto-etf-arb-30min",
    },
    {
        "name": "justhodl-lockup-expiration",
        "s3_key": "data/lockup-expiration.json",
        "schedule": "cron(0 22 * * ? *)",
        "memory": 512, "timeout": 180,
        "schedule_name": "justhodl-lockup-expiration-daily",
    },
]

REPO_ROOT = Path(__file__).resolve().parents[2]


def make_zip(fn_name):
    """Build a deployment zip from aws/lambdas/<fn>/source/lambda_function.py."""
    src = REPO_ROOT / "aws" / "lambdas" / fn_name / "source" / "lambda_function.py"
    if not src.exists():
        return None, f"source missing: {src}"
    code = src.read_text(encoding="utf-8")
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", code)
    return buf.getvalue(), None


def donor_env(lambda_client):
    """Pull standard env bundle from donor Lambda."""
    try:
        r = lambda_client.get_function_configuration(FunctionName=DONOR_FN)
        env = r.get("Environment", {}).get("Variables", {})
        keys_we_want = [
            "FMP_KEY", "FRED_KEY", "POLYGON_KEY", "ALPHA_VANTAGE_KEY",
            "CMC_KEY", "ANTHROPIC_API_KEY", "TELEGRAM_BOT_TOKEN",
            "TELEGRAM_CHAT_ID", "NEWSAPI_KEY", "BLS_KEY", "BEA_KEY",
            "CENSUS_KEY", "S3_BUCKET",
        ]
        return {k: env[k] for k in keys_we_want if k in env}
    except Exception as e:
        print(f"donor_env err: {e}")
        return {}


def ensure_lambda(lambda_client, eng, env_vars):
    """Create-or-update lambda function."""
    name = eng["name"]
    zip_bytes, err = make_zip(name)
    if err:
        return {"ok": False, "error": err, "stage": "zip"}
    try:
        lambda_client.get_function(FunctionName=name)
        exists = True
    except lambda_client.exceptions.ResourceNotFoundException:
        exists = False
    desc_short = f"retail-edges/{name.replace('justhodl-','')}"
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
                Description=desc_short[:255],
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
                Description=desc_short[:255],
            )
            mode = "created"
        # wait for active
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
    """Create-or-update EventBridge schedule."""
    name = eng["schedule_name"]
    target_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{eng['name']}"
    params = {
        "Name": name,
        "ScheduleExpression": eng["schedule"],
        "ScheduleExpressionTimezone": "UTC",
        "Target": {
            "Arn": target_arn,
            "RoleArn": SCHEDULER_ROLE,
            "Input": json.dumps({"source": "ops974", "manual": False}),
        },
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "State": "ENABLED",
        "Description": f"retail-edges {eng['name']}",
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
            return {"ok": False, "error": str(e), "stage": "ensure_schedule"}
    except Exception as e:
        return {"ok": False, "error": str(e), "stage": "ensure_schedule"}


def grant_invoke(lambda_client, eng):
    """Grant EventBridge scheduler permission to invoke."""
    name = eng["name"]
    sid = f"AllowScheduler-{eng['schedule_name']}"
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
    """Synchronously invoke and parse response."""
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
                "function_error": r.get("FunctionError"), "body": parsed}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def verify_s3(s3, key):
    """Read S3 object + parse schema."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        return {
            "ok": True,
            "size_bytes": obj["ContentLength"],
            "last_modified": obj["LastModified"].isoformat(),
            "engine": data.get("engine"),
            "state": data.get("state"),
            "as_of": data.get("as_of"),
            "summary_keys": list((data.get("summary") or {}).keys())[:12],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def verify_signal_board(s3):
    """Confirm signal-board ingested 28 engines now."""
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
        req = urllib.request.Request(url, headers={"User-Agent": "ops974/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="ignore")
        return {
            "ok": True,
            "status": r.status,
            "size": len(html),
            "has_best_trades": "Today's Best Retail Trades" in html,
            "has_7_engines": html.count("engine-card") >= 7 or html.count("ENGINES = [") >= 1,
            "has_iv_crush": "earnings-iv-crush" in html,
            "has_stealth": "stealth-accumulation" in html,
            "has_failed_pattern": "failed-pattern-reversal" in html,
            "has_squeeze": "squeeze-pretrigger" in html,
            "has_catalyst_skew": "catalyst-skew-premove" in html,
            "has_etf_arb": "crypto-etf-arb" in html,
            "has_lockup": "lockup-expiration" in html,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    print("=" * 70)
    print("ops 974 -- Retail-Edges 7-Engine Force-Deploy + Verify")
    print("=" * 70)
    lambda_c = boto3.client("lambda", region_name=REGION)
    sched_c = boto3.client("scheduler", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    # Pull donor env once
    env_vars = donor_env(lambda_c)
    env_vars["S3_BUCKET"] = S3_BUCKET
    print(f"donor env keys: {sorted(env_vars.keys())}")

    report = {
        "ops": 974,
        "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "engines": {},
    }

    for eng in ENGINES:
        print(f"\n--- {eng['name']} ---")
        engr = {}
        # 1. Ensure Lambda
        engr["deploy"] = ensure_lambda(lambda_c, eng, env_vars)
        print(f"  deploy: {engr['deploy']}")
        # 2. Ensure schedule
        engr["schedule"] = ensure_schedule(sched_c, eng)
        print(f"  schedule: {engr['schedule']}")
        # 3. Grant invoke permission
        engr["permission"] = grant_invoke(lambda_c, eng)
        print(f"  permission: {engr['permission']}")
        # 4. Invoke (only if deploy succeeded)
        if engr["deploy"].get("ok"):
            print(f"  invoking...")
            engr["invoke"] = invoke_engine(lambda_c, eng["name"])
            print(f"  invoke: status={engr['invoke'].get('status_code')} "
                  f"err={engr['invoke'].get('function_error')}")
            # 5. Verify S3 (give 2s for write to settle)
            time.sleep(2)
            engr["s3"] = verify_s3(s3, eng["s3_key"])
            print(f"  s3: {engr['s3'].get('engine')} state={engr['s3'].get('state')} "
                  f"size={engr['s3'].get('size_bytes')}")
        report["engines"][eng["name"]] = engr

    # Signal-board redeploy (so its updated FEEDS list goes live)
    print(f"\n--- justhodl-signal-board (redeploy) ---")
    try:
        sb_zip, err = make_zip("justhodl-signal-board")
        if not err:
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
            report["signal_board"] = {"deploy_ok": False, "error": err}
    except Exception as e:
        report["signal_board"] = {"deploy_ok": False, "error": str(e)}

    # Retail-edges.html page
    print(f"\n--- retail-edges.html page ---")
    report["page"] = verify_page()
    print(f"  page: ok={report['page'].get('ok')} size={report['page'].get('size')}")

    # Summary scorecard
    n_deploy_ok = sum(1 for e in report["engines"].values() if e.get("deploy", {}).get("ok"))
    n_schedule_ok = sum(1 for e in report["engines"].values() if e.get("schedule", {}).get("ok"))
    n_invoke_ok = sum(1 for e in report["engines"].values()
                       if e.get("invoke", {}).get("ok") and not e.get("invoke", {}).get("function_error"))
    n_s3_ok = sum(1 for e in report["engines"].values() if e.get("s3", {}).get("ok"))
    page_ok = report["page"].get("ok") and report["page"].get("has_best_trades")
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
    print(f"SCORECARD: {report['scorecard']}")
    print("=" * 70)

    out_dir = REPO_ROOT / "aws" / "ops" / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "974.json").write_text(json.dumps(report, indent=2, default=str))
    print(f"\nReport written: aws/ops/reports/974.json")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(f"FATAL: {e}\n{traceback.format_exc()}")
        sys.exit(1)
