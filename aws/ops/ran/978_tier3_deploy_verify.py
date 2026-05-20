"""
ops 978 - Tier-3 Retail-Edges 6-Engine Deploy + Signal-Board + Page Verify
==========================================================================

Builds on the proven ops 976 template:
  - REPO_ROOT = parents[3]
  - wait_for_settled() between update_function_code and update_function_configuration
  - try/finally with stdout dump for log-visibility
  - donor env from justhodl-buyback-scanner

Adds:
  - Redeploy of justhodl-signal-board to pick up Tier-3 FEEDS entries
    (signal-board source was edited in this push; CI is fine but we
    redeploy here so the verifier can confirm n_engines == 42)
  - expects_42 check (was 28 for Tier-1, 36 for Tier-2, 42 for Tier-3)
  - Page verifier checks for 21 engine markers (15 prior + 6 new)
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
    {"name": "justhodl-vvix-vov-regime",
     "s3_key": "data/vvix-vov-regime.json",
     "memory": 512, "timeout": 180},
    {"name": "justhodl-sympathetic-momentum",
     "s3_key": "data/sympathetic-momentum.json",
     "memory": 1024, "timeout": 540},
    {"name": "justhodl-insider-buyback-confluence",
     "s3_key": "data/insider-buyback-confluence.json",
     "memory": 768, "timeout": 300},
    {"name": "justhodl-gap-fill-confirm",
     "s3_key": "data/gap-fill-confirm.json",
     "memory": 1024, "timeout": 600},
    {"name": "justhodl-13f-price-divergence",
     "s3_key": "data/13f-price-divergence.json",
     "memory": 1024, "timeout": 540},
    {"name": "justhodl-credit-equity-divergence",
     "s3_key": "data/credit-equity-divergence.json",
     "memory": 512, "timeout": 180},
]

SIGNAL_BOARD_FN = "justhodl-signal-board"

# Expected markers in retail-edges.html (21 total: 7 Tier-1 + 8 Tier-2 + 6 Tier-3)
PAGE_MARKERS = [
    # Tier-1
    "earnings-iv-crush", "stealth-accumulation", "failed-pattern-reversal",
    "squeeze-pretrigger", "catalyst-skew-premove", "crypto-etf-arb", "lockup-expiration",
    # Tier-2
    "precatalyst-vol-expansion", "cef-discount", "reit-nav-discount", "divcut-warning",
    "rating-change-cluster", "multi-tf-convergence", "52wk-quality-breakout",
    "spac-floor-warrant",
    # Tier-3
    "vvix-vov-regime", "sympathetic-momentum", "insider-buyback-confluence",
    "gap-fill-confirm", "13f-price-divergence", "credit-equity-divergence",
]

REPO_ROOT = Path(__file__).resolve().parents[3]


def wait_for_settled(lambda_c, name, max_wait=120):
    """Poll until LastUpdateStatus is Successful or Failed."""
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
    name = eng["name"]
    out = {"name": name}
    zip_bytes, err = make_zip(name)
    if err:
        return {**out, "ok": False, "stage": "zip", "error": err}
    try:
        cur = lambda_c.get_function(FunctionName=name)
        exists = True
    except lambda_c.exceptions.ResourceNotFoundException:
        exists = False
    out["exists_before"] = exists
    if exists:
        state_b, lus_b = wait_for_settled(lambda_c, name, max_wait=60)
        out["pre_state"] = f"{state_b}/{lus_b}"
    desc = f"retail-edges-tier3/{name.replace('justhodl-', '')}"[:255]
    try:
        if exists:
            lambda_c.update_function_code(
                FunctionName=name, ZipFile=zip_bytes, Publish=True)
            state_c, lus_c = wait_for_settled(lambda_c, name, max_wait=120)
            out["post_code_state"] = f"{state_c}/{lus_c}"
            if lus_c == "Failed":
                return {**out, "ok": False, "stage": "code_update", "error": "code update failed"}
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


def ensure_schedule(scheduler_c, eng):
    """Ensure EventBridge schedule exists with the right cron."""
    cfg_path = REPO_ROOT / "aws" / "lambdas" / eng["name"] / "config.json"
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as e:
        return {"ok": False, "error": f"cfg read: {str(e)[:200]}"}
    sched = cfg.get("eventbridge_scheduler") or {}
    sched_name = sched.get("schedule_name")
    cron = sched.get("cron")
    if not sched_name or not cron:
        return {"ok": False, "error": "no schedule in config"}
    target_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{eng['name']}"
    payload = {
        "Name": sched_name,
        "ScheduleExpression": cron,
        "ScheduleExpressionTimezone": sched.get("timezone", "UTC"),
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "State": "ENABLED",
        "Target": {
            "Arn": target_arn,
            "RoleArn": SCHEDULER_ROLE,
            "Input": "{}",
            "RetryPolicy": {"MaximumEventAgeInSeconds": 86400, "MaximumRetryAttempts": 2},
        },
        "Description": sched.get("description", "")[:512],
    }
    try:
        try:
            scheduler_c.update_schedule(**payload)
            return {"ok": True, "mode": "updated", "name": sched_name, "cron": cron}
        except scheduler_c.exceptions.ResourceNotFoundException:
            scheduler_c.create_schedule(**payload)
            return {"ok": True, "mode": "created", "name": sched_name, "cron": cron}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


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
            "n_setups": data.get("n_setups") or data.get("n_confluences")
                       or data.get("n_divergences") or data.get("n_tickets"),
            "schema_keys": sorted(list(data.keys()))[:25],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def deploy_signal_board(lambda_c, env_vars):
    """Redeploy signal-board so the new FEEDS entries take effect on invoke."""
    out = {"name": SIGNAL_BOARD_FN}
    zip_bytes, err = make_zip(SIGNAL_BOARD_FN)
    if err:
        return {**out, "ok": False, "error": err}
    try:
        cur = lambda_c.get_function_configuration(FunctionName=SIGNAL_BOARD_FN)
        cur_mem = cur.get("MemorySize", 1024)
        cur_to = cur.get("Timeout", 540)
    except Exception:
        cur_mem, cur_to = 1024, 540
    state_b, _ = wait_for_settled(lambda_c, SIGNAL_BOARD_FN, max_wait=60)
    out["pre_state"] = state_b
    try:
        lambda_c.update_function_code(FunctionName=SIGNAL_BOARD_FN, ZipFile=zip_bytes, Publish=True)
        state_c, lus_c = wait_for_settled(lambda_c, SIGNAL_BOARD_FN, max_wait=120)
        out["post_code"] = f"{state_c}/{lus_c}"
        if lus_c == "Failed":
            return {**out, "ok": False, "error": "code update failed"}
        # Merge env vars: keep existing, ensure S3_BUCKET set
        try:
            cur_env = cur.get("Environment", {}).get("Variables", {}) if isinstance(cur, dict) else {}
        except Exception:
            cur_env = {}
        merged = dict(env_vars)
        merged.update(cur_env)  # cur env wins to avoid stomping prior tuning
        merged["S3_BUCKET"] = S3_BUCKET
        lambda_c.update_function_configuration(
            FunctionName=SIGNAL_BOARD_FN,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE,
            Timeout=cur_to,
            MemorySize=cur_mem,
            Environment={"Variables": merged},
            Description="Unified signal board across 42 engines (Tier-3 integrated)"[:255],
        )
        state_f, lus_f = wait_for_settled(lambda_c, SIGNAL_BOARD_FN, max_wait=120)
        out["post_cfg"] = f"{state_f}/{lus_f}"
        return {**out, "ok": True, "mode": "updated"}
    except Exception as e:
        return {**out, "ok": False, "error": str(e)[:300]}


def verify_signal_board(s3):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
        data = json.loads(obj["Body"].read())
        engines = data.get("engines", [])
        stale = [e.get("name") for e in engines if e.get("status") == "stale" or e.get("stale") is True]
        return {
            "ok": True,
            "n_engines": data.get("n_engines"),
            "n_live": data.get("n_live"),
            "n_stale": data.get("n_stale"),
            "composite_posture": data.get("composite_posture"),
            "composite_signal": data.get("composite_signal"),
            "stale_engines_listed": stale[:15],
            "expects_42": data.get("n_engines") == 42,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def verify_page(url="https://justhodl.ai/retail-edges.html"):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ops978/1.0",
                                                    "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="ignore")
        markers = {n: n in html for n in PAGE_MARKERS}
        return {"ok": True, "status": r.status, "size": len(html),
                "markers": markers, "markers_found": sum(markers.values()),
                "expected_markers": len(PAGE_MARKERS)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    print("=" * 70)
    print("ops 978 -- Tier-3 Retail-Edges 6-Engine Deploy + Signal-Board")
    print(f"REPO_ROOT={REPO_ROOT}")
    print("=" * 70)
    lambda_c = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    scheduler_c = boto3.client("scheduler", region_name=REGION)

    env_vars = donor_env(lambda_c)
    env_vars["S3_BUCKET"] = S3_BUCKET
    print(f"donor env keys: {sorted(env_vars.keys())}")

    report = {
        "ops": 978,
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
            engr["schedule"] = ensure_schedule(scheduler_c, eng)
            print(f"  schedule: ok={engr['schedule'].get('ok')} mode={engr['schedule'].get('mode')} "
                  f"cron={engr['schedule'].get('cron','-')}")
            print("  invoking...")
            engr["invoke"] = invoke_engine(lambda_c, eng["name"])
            print(f"  invoke: ok={engr['invoke'].get('ok')} status={engr['invoke'].get('status_code')} "
                  f"err={engr['invoke'].get('function_error')}")
            if engr["invoke"].get("body_preview"):
                print(f"  invoke body: {engr['invoke']['body_preview'][:250]}")
            time.sleep(3)
            engr["s3"] = verify_s3(s3, eng["s3_key"])
            print(f"  s3: ok={engr['s3'].get('ok')} state={engr['s3'].get('state')} "
                  f"size={engr['s3'].get('size_bytes')} "
                  f"as_of={engr['s3'].get('as_of')}")
            if not engr["s3"].get("ok"):
                print(f"  s3 error: {engr['s3'].get('error','')[:200]}")
            report["engines"][eng["name"]] = engr

        # Redeploy signal-board (it's edited in this push)
        print(f"\n--- signal-board redeploy ---")
        report["signal_board_deploy"] = deploy_signal_board(lambda_c, env_vars)
        print(f"  ok={report['signal_board_deploy'].get('ok')} "
              f"post_cfg={report['signal_board_deploy'].get('post_cfg','-')}")
        # Invoke signal-board to write fresh data with 42 engines
        time.sleep(3)
        print("  invoking signal-board to refresh data/signal-board.json...")
        sb_inv = invoke_engine(lambda_c, SIGNAL_BOARD_FN)
        report["signal_board_invoke"] = sb_inv
        print(f"  sb invoke: ok={sb_inv.get('ok')} status={sb_inv.get('status_code')}")
        if sb_inv.get("body_preview"):
            print(f"  sb body: {sb_inv['body_preview'][:300]}")
        time.sleep(2)
        print(f"\n--- signal-board verify ---")
        report["signal_board"] = verify_signal_board(s3)
        print(f"  n_engines={report['signal_board'].get('n_engines')} "
              f"n_live={report['signal_board'].get('n_live')} "
              f"posture={report['signal_board'].get('composite_posture')} "
              f"signal={report['signal_board'].get('composite_signal')} "
              f"expects_42={report['signal_board'].get('expects_42')}")

        print(f"\n--- retail-edges.html page (21 markers expected) ---")
        report["page"] = verify_page()
        print(f"  page: ok={report['page'].get('ok')} "
              f"markers={report['page'].get('markers_found')}/{report['page'].get('expected_markers')} "
              f"size={report['page'].get('size')}")

        n_deploy = sum(1 for e in report["engines"].values() if e.get("deploy", {}).get("ok"))
        n_sched = sum(1 for e in report["engines"].values() if e.get("schedule", {}).get("ok"))
        n_invoke = sum(1 for e in report["engines"].values() if e.get("invoke", {}).get("ok"))
        n_s3 = sum(1 for e in report["engines"].values() if e.get("s3", {}).get("ok"))
        page_ok = (report["page"].get("ok") and
                   report["page"].get("markers_found") == len(PAGE_MARKERS))
        sb_ok = report.get("signal_board", {}).get("expects_42", False)

        report["scorecard"] = {
            "n_engines": len(ENGINES),
            "n_deploy_ok": n_deploy,
            "n_schedule_ok": n_sched,
            "n_invoke_ok": n_invoke,
            "n_s3_ok": n_s3,
            "page_ok": page_ok,
            "signal_board_42_ok": sb_ok,
            "all_pass": (n_deploy == 6 and n_sched == 6 and n_invoke == 6
                          and n_s3 == 6 and page_ok and sb_ok),
        }
        report["ended_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        print("\n" + "=" * 70)
        print(f"SCORECARD: {json.dumps(report['scorecard'], indent=2)}")
        print("=" * 70)

    finally:
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "978.json"
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
