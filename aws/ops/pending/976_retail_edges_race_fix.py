"""
ops 976 - Retail-Edges 7-Engine Force-Deploy v3 (race-condition fix)
====================================================================

Bug discovered in ops 975: between `update_function_code` and
`update_function_configuration`, the Lambda is in `InProgress` state on
AWS's side. A 3-second sleep was not enough — every config update hit
`ResourceConflictException: An update is in progress`.

The Lambdas DO exist with current code (update_function_code succeeded).
Schedules + permissions also succeeded. What we lost was the env-var
update + invoke + S3 verify.

Fix: poll `LastUpdateStatus` until `Successful` (or `Failed`) before each
config call. Then re-invoke + verify S3.

This also handles the case where Lambdas already have the right env from
a prior deploy-lambdas.yml run — we just need to fire them and verify.
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
     "memory": 1024, "timeout": 600},
    {"name": "justhodl-stealth-accumulation",
     "s3_key": "data/stealth-accumulation.json",
     "memory": 256, "timeout": 60},
    {"name": "justhodl-failed-pattern-reversal",
     "s3_key": "data/failed-pattern-reversal.json",
     "memory": 1024, "timeout": 600},
    {"name": "justhodl-squeeze-pretrigger",
     "s3_key": "data/squeeze-pretrigger.json",
     "memory": 768, "timeout": 540},
    {"name": "justhodl-catalyst-skew-premove",
     "s3_key": "data/catalyst-skew-premove.json",
     "memory": 256, "timeout": 60},
    {"name": "justhodl-crypto-etf-arb",
     "s3_key": "data/crypto-etf-arb.json",
     "memory": 256, "timeout": 60},
    {"name": "justhodl-lockup-expiration",
     "s3_key": "data/lockup-expiration.json",
     "memory": 512, "timeout": 180},
]

REPO_ROOT = Path(__file__).resolve().parents[3]


def wait_for_settled(lambda_c, name, max_wait=120):
    """Poll until LastUpdateStatus is Successful or Failed; return (state, last_status)."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            r = lambda_c.get_function_configuration(FunctionName=name)
            state = r.get("State")
            lus = r.get("LastUpdateStatus")
            if state == "Active" and lus in ("Successful", None):
                return ("Active", lus or "None")
            if lus in ("Failed",):
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
    """Deploy code + config, polling between each step. Returns dict with full state."""
    name = eng["name"]
    out = {"name": name}
    # Build zip
    zip_bytes, err = make_zip(name)
    if err:
        return {**out, "ok": False, "stage": "zip", "error": err}
    # Check existence
    try:
        cur = lambda_c.get_function(FunctionName=name)
        exists = True
        cur_cfg = cur.get("Configuration", {})
    except lambda_c.exceptions.ResourceNotFoundException:
        exists = False
        cur_cfg = {}
    out["exists_before"] = exists
    # Wait for any in-progress update to settle BEFORE we start
    if exists:
        state_b, lus_b = wait_for_settled(lambda_c, name, max_wait=60)
        out["pre_state"] = f"{state_b}/{lus_b}"
    desc = f"retail-edges/{name.replace('justhodl-', '')}"[:255]
    try:
        if exists:
            # 1) update_function_code
            lambda_c.update_function_code(
                FunctionName=name, ZipFile=zip_bytes, Publish=True)
            # 2) WAIT for code update to settle (NEW)
            state_c, lus_c = wait_for_settled(lambda_c, name, max_wait=120)
            out["post_code_state"] = f"{state_c}/{lus_c}"
            if lus_c == "Failed":
                return {**out, "ok": False, "stage": "code_update", "error": "code update failed"}
            # 3) update_function_configuration
            lambda_c.update_function_configuration(
                FunctionName=name,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Role=ROLE,
                Timeout=eng["timeout"],
                MemorySize=eng["memory"],
                Environment={"Variables": env_vars},
                Description=desc,
            )
            # 4) WAIT for config update to settle
            state_f, lus_f = wait_for_settled(lambda_c, name, max_wait=120)
            out["post_cfg_state"] = f"{state_f}/{lus_f}"
            mode = "updated"
        else:
            lambda_c.create_function(
                FunctionName=name, Runtime="python3.12", Role=ROLE,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zip_bytes},
                Timeout=eng["timeout"], MemorySize=eng["memory"],
                Environment={"Variables": env_vars},
                Description=desc,
            )
            state_f, lus_f = wait_for_settled(lambda_c, name, max_wait=120)
            out["post_create_state"] = f"{state_f}/{lus_f}"
            mode = "created"
        return {**out, "ok": True, "mode": mode}
    except Exception as e:
        return {**out, "ok": False, "stage": "ensure", "error": str(e)[:300],
                "trace": traceback.format_exc()[:400]}


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
            "as_of": data.get("as_of") or data.get("generated_at"),
            "schema_keys": sorted(list(data.keys()))[:25],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def verify_signal_board(s3):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
        data = json.loads(obj["Body"].read())
        # Inspect engines list for stale ones
        engines = data.get("engines", [])
        stale = [e.get("name") for e in engines if e.get("status") == "stale" or e.get("stale") is True]
        live = [e.get("name") for e in engines if e.get("status") != "stale" and e.get("stale") is not True]
        return {
            "ok": True,
            "n_engines": data.get("n_engines"),
            "n_live": data.get("n_live"),
            "n_stale": data.get("n_stale"),
            "composite_posture": data.get("composite_posture"),
            "composite_signal": data.get("composite_signal"),
            "stale_engines_listed": stale[:15],
            "expects_28": data.get("n_engines") == 28,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def verify_page(url="https://justhodl.ai/retail-edges.html"):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ops976/1.0",
                                                     "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="ignore")
        markers = {n: n in html for n in [
            "earnings-iv-crush", "stealth-accumulation", "failed-pattern-reversal",
            "squeeze-pretrigger", "catalyst-skew-premove", "crypto-etf-arb", "lockup-expiration"]}
        return {"ok": True, "status": r.status, "size": len(html),
                "markers": markers, "markers_found": sum(markers.values())}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    print("=" * 70)
    print("ops 976 -- Retail-Edges 7-Engine Force-Deploy v3 (race-fix)")
    print(f"REPO_ROOT={REPO_ROOT}")
    print("=" * 70)
    lambda_c = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    env_vars = donor_env(lambda_c)
    env_vars["S3_BUCKET"] = S3_BUCKET
    print(f"donor env keys: {sorted(env_vars.keys())}")

    report = {
        "ops": 976,
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
            # Always try to invoke (even if config update collided, the code is fresh
            # and the Lambda was previously deployed with valid env via deploy-lambdas.yml)
            print("  invoking...")
            engr["invoke"] = invoke_engine(lambda_c, eng["name"])
            print(f"  invoke: ok={engr['invoke'].get('ok')} status={engr['invoke'].get('status_code')} "
                  f"err={engr['invoke'].get('function_error')}")
            if not engr["invoke"].get("ok") and engr["invoke"].get("body_preview"):
                print(f"  invoke body: {engr['invoke']['body_preview'][:300]}")
            time.sleep(3)
            engr["s3"] = verify_s3(s3, eng["s3_key"])
            print(f"  s3: ok={engr['s3'].get('ok')} state={engr['s3'].get('state')} "
                  f"size={engr['s3'].get('size_bytes')} "
                  f"as_of={engr['s3'].get('as_of')}")
            if not engr["s3"].get("ok"):
                print(f"  s3 error: {engr['s3'].get('error','')[:200]}")
            report["engines"][eng["name"]] = engr

        # Signal-board verify (no redeploy needed — already at 28)
        print(f"\n--- signal-board verify ---")
        report["signal_board"] = verify_signal_board(s3)
        print(f"  n_engines={report['signal_board'].get('n_engines')} "
              f"n_live={report['signal_board'].get('n_live')} "
              f"stale={report['signal_board'].get('stale_engines_listed',[])[:8]}")

        print(f"\n--- retail-edges.html page ---")
        report["page"] = verify_page()
        print(f"  page: ok={report['page'].get('ok')} "
              f"markers={report['page'].get('markers_found')}/7")

        # Scorecard
        n_deploy = sum(1 for e in report["engines"].values() if e.get("deploy", {}).get("ok"))
        n_invoke = sum(1 for e in report["engines"].values() if e.get("invoke", {}).get("ok"))
        n_s3 = sum(1 for e in report["engines"].values() if e.get("s3", {}).get("ok"))
        page_ok = report["page"].get("ok") and report["page"].get("markers_found", 0) == 7
        sb_ok = report.get("signal_board", {}).get("expects_28", False)

        report["scorecard"] = {
            "n_engines": len(ENGINES),
            "n_deploy_ok": n_deploy,
            "n_invoke_ok": n_invoke,
            "n_s3_ok": n_s3,
            "page_ok": page_ok,
            "signal_board_ok": sb_ok,
            "all_pass": n_deploy == 7 and n_invoke == 7 and n_s3 == 7 and page_ok and sb_ok,
        }
        report["ended_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        print("\n" + "=" * 70)
        print(f"SCORECARD: {json.dumps(report['scorecard'], indent=2)}")
        print("=" * 70)

    finally:
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "976.json"
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
