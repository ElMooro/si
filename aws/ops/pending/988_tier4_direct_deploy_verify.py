"""
ops 988 - Tier-4 Direct Deploy + Verify (bypass failed CI)
===========================================================

ROOT CAUSE recap of ops 986/987 failure:
  1. All 6 Tier-4 descriptions in config.json exceeded Lambda's
     256-char hard limit -> aws lambda create-function returned
     InvalidParameterValueException -> CI deploy loop exited.
  2. Even if CI had succeeded, configs' inherit_env=true pulls from
     justhodl-buyback-scanner which only has CMC_KEY (per Tier-3
     saga ops 982-984), so Lambdas would launch without FMP_KEY and
     immediately fail like credit-equity-divergence did.

This script:
  - Reads each Tier-4 config (now with trimmed descriptions <=250)
  - For each engine:
      * Zips aws/lambdas/<fn>/source/ in memory
      * If Lambda exists -> update_function_code + update env (merge)
      * If Lambda missing -> create_function with full BASELINE_ENV
      * wait_for_settled (Active + LastUpdateStatus=Successful)
      * Patch description + memory + timeout + env from config + baseline
      * Wait
      * Create/update EventBridge Scheduler schedule
      * Invoke with extended boto3 timeout (read_timeout=900s)
      * Sleep 3s, read S3 output, verify state populated, no payload error
  - Re-invoke signal-board so it picks up fresh Tier-4 outputs
  - Verify signal-board has n_engines==48 AND n_live==48
  - Fetch retail-edges.html via HTTP (urlopen), confirm 27 markers
  - Send Telegram summary with full scorecard

Hardcoded BASELINE_ENV is the proven gold-standard pattern from ops 984.
Keys come from userMemories doctrine; same set used across all engines.
"""
import io
import json
import os
import sys
import time
import traceback
import urllib.request
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
S3_BUCKET = "justhodl-dashboard-live"
REPO_ROOT = Path(__file__).resolve().parents[3]
EXEC_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
SCHEDULER_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/justhodl-scheduler-role"

# Hardcoded baseline env -- all 11 API keys + S3_BUCKET + Telegram bundle.
# Proven pattern from ops 984. Direct injection bypasses the broken
# inherit_env=true (which would pull from buyback-scanner=CMC_KEY only).
BASELINE_ENV = {
    "S3_BUCKET": S3_BUCKET,
    "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
    "FRED_KEY": "2f057499936072679d8843d7fce99989",
    "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
    "ALPHA_VANTAGE_KEY": "EOLGKSGAYZUXKPUL",
    "CMC_KEY": "17ba8e87-53f0-46f4-abe5-014d9cd99597",
    "NEWSAPI_KEY": "17d36cdd13c44e139853b3a6876cf940",
    "BEA_KEY": "997E5691-4F0E-4774-8B4E-CAE836D4AC47",
    "BLS_KEY": "a759447531f04f1f861f29a381aab863",
    "CENSUS_KEY": "8423ffa543d0e95cdba580f2e381649b6772f515",
    "TELEGRAM_BOT_TOKEN": "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs",
    "TELEGRAM_CHAT_ID": "8678089260",
    "TELEGRAM_TOKEN": "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs",
}

ENGINES = [
    ("justhodl-post-earnings-mean-rev", "data/post-earnings-mean-rev.json"),
    ("justhodl-insider-sell-cluster",   "data/insider-sell-cluster.json"),
    ("justhodl-vix9d-vix-inversion",    "data/vix9d-vix-inversion.json"),
    ("justhodl-breadth-divergence",     "data/breadth-divergence.json"),
    ("justhodl-skew-tail-hedging",      "data/skew-tail-hedging.json"),
    ("justhodl-dxy-equity-divergence",  "data/dxy-equity-divergence.json"),
]
SIGNAL_BOARD_FN = "justhodl-signal-board"

PAGE_MARKERS = [
    "earnings-iv-crush", "stealth-accumulation", "failed-pattern-reversal",
    "squeeze-pretrigger", "catalyst-skew-premove", "crypto-etf-arb", "lockup-expiration",
    "precatalyst-vol-expansion", "cef-discount", "reit-nav-discount", "divcut-warning",
    "rating-change-cluster", "multi-tf-convergence", "52wk-quality-breakout", "spac-floor-warrant",
    "vvix-vov-regime", "sympathetic-momentum", "insider-buyback-confluence",
    "gap-fill-confirm", "13f-price-divergence", "credit-equity-divergence",
    "post-earnings-mean-rev", "insider-sell-cluster", "vix9d-vix-inversion",
    "breadth-divergence", "skew-tail-hedging", "dxy-equity-divergence",
]


def long_lambda_client():
    cfg = Config(connect_timeout=10, read_timeout=900,
                 retries={"max_attempts": 0})
    return boto3.client("lambda", region_name=REGION, config=cfg)


def zip_source_dir(src_dir):
    """Build in-memory deploy zip of the Lambda source directory."""
    buf = io.BytesIO()
    src_path = Path(src_dir)
    if not src_path.is_dir():
        return None, f"source dir not found: {src_dir}"
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fp in src_path.rglob("*"):
            if fp.is_file() and "__pycache__" not in fp.parts:
                zf.write(fp, fp.relative_to(src_path))
    buf.seek(0)
    return buf.getvalue(), None


def wait_for_settled(lambda_c, name, max_wait=180):
    deadline = time.time() + max_wait
    last = {}
    while time.time() < deadline:
        try:
            r = lambda_c.get_function_configuration(FunctionName=name)
            state = r.get("State")
            last_status = r.get("LastUpdateStatus")
            last = {"state": state, "last_status": last_status}
            if state == "Active" and last_status in ("Successful", None):
                return {"ok": True, **last,
                        "waited_s": round(max_wait - (deadline - time.time()), 1)}
            if state == "Failed" or last_status == "Failed":
                return {"ok": False, **last, "fatal": True}
        except Exception as e:
            last = {"err": str(e)[:200]}
        time.sleep(3)
    return {"ok": False, **last, "timeout": True}


def deploy_engine(lambda_c, fn_name, repo_root):
    """Create or update Lambda. Returns dict with deploy state."""
    out = {"fn": fn_name}
    cfg_path = repo_root / "aws" / "lambdas" / fn_name / "config.json"
    src_dir = repo_root / "aws" / "lambdas" / fn_name / "source"
    if not cfg_path.is_file():
        return {**out, "ok": False, "error": "no config.json"}
    cfg = json.loads(cfg_path.read_text())
    runtime = cfg.get("runtime", "python3.12")
    memory = int(cfg.get("memory", 512))
    timeout = int(cfg.get("timeout", 300))
    handler = cfg.get("handler", "lambda_function.lambda_handler")
    desc = cfg.get("description", f"JustHodl.AI {fn_name}")
    if len(desc) > 250:
        desc = desc[:247] + "..."
        out["desc_truncated"] = True

    # Build zip in memory
    zip_bytes, err = zip_source_dir(str(src_dir))
    if err:
        return {**out, "ok": False, "error": err}
    out["zip_bytes"] = len(zip_bytes)

    # Build merged env: config.env (e.g. S3_BUCKET) + BASELINE_ENV
    cfg_env = (cfg.get("env") or cfg.get("environment") or {})
    merged_env = {**BASELINE_ENV, **cfg_env}

    # Check if exists
    exists = True
    try:
        lambda_c.get_function(FunctionName=fn_name)
    except lambda_c.exceptions.ResourceNotFoundException:
        exists = False
    except Exception as e:
        return {**out, "ok": False, "error": f"get_function err: {str(e)[:200]}"}
    out["existed_before"] = exists

    try:
        if not exists:
            # CREATE path
            r = lambda_c.create_function(
                FunctionName=fn_name,
                Runtime=runtime,
                Role=EXEC_ROLE_ARN,
                Handler=handler,
                Code={"ZipFile": zip_bytes},
                Description=desc,
                Timeout=timeout,
                MemorySize=memory,
                Environment={"Variables": merged_env},
                Publish=False,
            )
            out["create_ok"] = True
            out["create_resp"] = {"arn": r.get("FunctionArn"),
                                  "state": r.get("State")}
            settled = wait_for_settled(lambda_c, fn_name, max_wait=180)
            out["wait_active"] = settled
            if not settled.get("ok"):
                return {**out, "ok": False, "error": "did not reach Active"}
        else:
            # UPDATE path: code first, then config
            lambda_c.update_function_code(
                FunctionName=fn_name, ZipFile=zip_bytes)
            settled1 = wait_for_settled(lambda_c, fn_name, max_wait=120)
            out["wait_after_code"] = settled1
            if not settled1.get("ok"):
                return {**out, "ok": False, "error": "code update did not settle"}
            # Now update config with merged env + description + memory + timeout
            existing_env = lambda_c.get_function_configuration(
                FunctionName=fn_name).get("Environment", {}).get("Variables", {})
            # Existing baseline-patched values take precedence over config defaults
            # (to keep any ops-patched secret rotation), but ensure all baseline
            # keys are present.
            final_env = {**merged_env, **existing_env}
            for k, v in BASELINE_ENV.items():
                if not final_env.get(k):
                    final_env[k] = v
            lambda_c.update_function_configuration(
                FunctionName=fn_name,
                Description=desc,
                Timeout=timeout,
                MemorySize=memory,
                Environment={"Variables": final_env},
            )
            settled2 = wait_for_settled(lambda_c, fn_name, max_wait=120)
            out["wait_after_config"] = settled2
            if not settled2.get("ok"):
                return {**out, "ok": False, "error": "config update did not settle"}
        out["ok"] = True
        return out
    except Exception as e:
        return {**out, "ok": False, "error": f"deploy err: {str(e)[:300]}",
                "tb": traceback.format_exc()[-800:]}


def setup_scheduler(scheduler_c, lambda_c, fn_name, repo_root):
    """Create or update EventBridge Scheduler schedule for the function."""
    cfg_path = repo_root / "aws" / "lambdas" / fn_name / "config.json"
    cfg = json.loads(cfg_path.read_text())
    sched = cfg.get("eventbridge_scheduler")
    if not sched:
        return {"ok": False, "skipped": True, "reason": "no eventbridge_scheduler"}
    name = sched["schedule_name"]
    cron = sched["cron"]
    tz = sched.get("timezone", "UTC")
    role = sched.get("role_arn", SCHEDULER_ROLE_ARN)
    desc = sched.get("description", "Scheduled run")[:512]
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{fn_name}"
    target = {
        "Arn": fn_arn, "RoleArn": role, "Input": "{}",
        "RetryPolicy": {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600},
    }
    try:
        # Check exists
        try:
            scheduler_c.get_schedule(Name=name)
            scheduler_c.update_schedule(
                Name=name,
                ScheduleExpression=cron,
                ScheduleExpressionTimezone=tz,
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Description=desc,
                Target=target,
            )
            mode = "updated"
        except scheduler_c.exceptions.ResourceNotFoundException:
            scheduler_c.create_schedule(
                Name=name,
                ScheduleExpression=cron,
                ScheduleExpressionTimezone=tz,
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Description=desc,
                Target=target,
            )
            mode = "created"
        # Add invoke permission (idempotent)
        statement_id = f"Scheduler-{name}"[:100]
        try:
            lambda_c.add_permission(
                FunctionName=fn_name,
                StatementId=statement_id,
                Action="lambda:InvokeFunction",
                Principal="scheduler.amazonaws.com",
                SourceArn=f"arn:aws:scheduler:{REGION}:{ACCOUNT_ID}:schedule/default/{name}",
            )
            perm_added = True
        except lambda_c.exceptions.ResourceConflictException:
            perm_added = False  # already exists
        except Exception:
            perm_added = False
        return {"ok": True, "mode": mode, "name": name, "cron": cron,
                "tz": tz, "perm_added": perm_added}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def invoke_long(fn_name):
    lc = long_lambda_client()
    try:
        r = lc.invoke(FunctionName=fn_name, InvocationType="RequestResponse",
                      Payload=b"{}")
        outer = r["StatusCode"]
        fn_err = r.get("FunctionError")
        body = r["Payload"].read().decode("utf-8", errors="ignore")
        inner = None
        parsed = None
        try:
            parsed = json.loads(body)
            inner = parsed.get("statusCode")
        except Exception:
            parsed = {"raw": body[:500]}
        return {
            "ok": outer == 200 and not fn_err
                  and (inner is None or inner < 400),
            "outer_status": outer, "inner_status": inner,
            "function_error": fn_err,
            "body_preview": str(parsed)[:500],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:400]}


def read_s3(s3, key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        is_err = "error" in data and not data.get("state")
        return {
            "ok": not is_err,
            "size": obj["ContentLength"],
            "last_modified": obj["LastModified"].isoformat(),
            "engine": data.get("engine"),
            "state": data.get("state"),
            "signal_strength": data.get("signal_strength"),
            "as_of": data.get("as_of"),
            "n_picks": (data.get("n_setups") or data.get("n_tickets")
                        or data.get("n_clusters") or data.get("n_signals")
                        or 0),
            "error_in_payload": data.get("error") if is_err else None,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def fetch_page(url="https://justhodl.ai/retail-edges.html"):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "ops988/1.0",
                          "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="ignore")
        markers = {n: n in html for n in PAGE_MARKERS}
        return {
            "ok": True, "status": r.status, "size": len(html),
            "markers_found": sum(markers.values()),
            "expected": len(PAGE_MARKERS),
            "missing": [n for n, v in markers.items() if not v],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def verify_signal_board(s3):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
        data = json.loads(obj["Body"].read())
        return {
            "ok": True,
            "n_engines": data.get("n_engines"),
            "n_live": data.get("n_live"),
            "n_stale": data.get("n_stale"),
            "composite_posture": data.get("composite_posture"),
            "composite_signal": data.get("composite_signal"),
            "expects_48": data.get("n_engines") == 48,
            "all_48_live": data.get("n_live") == 48,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def telegram_alert(text):
    try:
        url = (f"https://api.telegram.org/bot{BASELINE_ENV['TELEGRAM_BOT_TOKEN']}"
               "/sendMessage")
        data = json.dumps({
            "chat_id": BASELINE_ENV["TELEGRAM_CHAT_ID"],
            "text": text, "parse_mode": "Markdown",
        }).encode("utf-8")
        req = urllib.request.Request(url, data=data,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception:
        return False


def main():
    print("=" * 70)
    print("ops 988 -- Tier-4 direct deploy + verify (bypass failed CI)")
    print(f"REPO_ROOT={REPO_ROOT}")
    print("=" * 70)
    lambda_c = boto3.client("lambda", region_name=REGION)
    scheduler_c = boto3.client("scheduler", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    report = {
        "ops": 988,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "engines": {},
        "baseline_env_keys": sorted(BASELINE_ENV.keys()),
    }
    try:
        for name, key in ENGINES:
            print(f"\n=== {name} ===")
            engine_report = {}
            # 1. Deploy (create or update)
            t0 = time.time()
            dep = deploy_engine(lambda_c, name, REPO_ROOT)
            print(f"  deploy: ok={dep.get('ok')} "
                  f"existed_before={dep.get('existed_before')} "
                  f"zip={dep.get('zip_bytes', 0)}B "
                  f"took={round(time.time()-t0, 1)}s")
            if not dep.get("ok"):
                print(f"  ERROR: {dep.get('error')}")
            engine_report["deploy"] = dep
            if not dep.get("ok"):
                report["engines"][name] = engine_report
                continue
            # 2. Schedule
            sch = setup_scheduler(scheduler_c, lambda_c, name, REPO_ROOT)
            print(f"  schedule: ok={sch.get('ok')} mode={sch.get('mode')} "
                  f"cron={sch.get('cron')}")
            engine_report["schedule"] = sch
            # 3. Invoke
            time.sleep(2)
            inv = invoke_long(name)
            print(f"  invoke: ok={inv.get('ok')} outer={inv.get('outer_status')} "
                  f"inner={inv.get('inner_status')} "
                  f"err={inv.get('function_error')}")
            if inv.get("body_preview"):
                print(f"  body: {inv['body_preview'][:240]}")
            engine_report["invoke"] = inv
            # 4. S3 check
            time.sleep(3)
            s3v = read_s3(s3, key)
            print(f"  s3: ok={s3v.get('ok')} state={s3v.get('state')} "
                  f"strength={s3v.get('signal_strength')} "
                  f"picks={s3v.get('n_picks')} size={s3v.get('size')}")
            if s3v.get("error_in_payload"):
                print(f"  PAYLOAD ERR: {s3v['error_in_payload'][:200]}")
            engine_report["s3"] = s3v
            report["engines"][name] = engine_report

        # Signal-board re-invoke + verify
        print("\n=== signal-board re-invoke ===")
        sb_inv = invoke_long(SIGNAL_BOARD_FN)
        print(f"  invoke ok={sb_inv.get('ok')} "
              f"inner={sb_inv.get('inner_status')}")
        if sb_inv.get("body_preview"):
            print(f"  body: {sb_inv['body_preview'][:400]}")
        report["signal_board_invoke"] = sb_inv
        time.sleep(4)
        sb_v = verify_signal_board(s3)
        print(f"  n_engines={sb_v.get('n_engines')} "
              f"n_live={sb_v.get('n_live')} n_stale={sb_v.get('n_stale')} "
              f"posture={sb_v.get('composite_posture')} "
              f"signal={sb_v.get('composite_signal')}")
        report["signal_board"] = sb_v

        # Page check
        print("\n=== retail-edges.html ===")
        page = fetch_page()
        print(f"  ok={page.get('ok')} markers={page.get('markers_found')}/"
              f"{page.get('expected')}")
        if page.get("missing"):
            print(f"  MISSING: {page['missing']}")
        report["page"] = page

        # Scorecard
        engines = report["engines"]
        n_deploy = sum(1 for e in engines.values()
                       if e.get("deploy", {}).get("ok"))
        n_sched = sum(1 for e in engines.values()
                      if e.get("schedule", {}).get("ok"))
        n_inv = sum(1 for e in engines.values()
                    if e.get("invoke", {}).get("ok"))
        n_s3 = sum(1 for e in engines.values()
                   if e.get("s3", {}).get("ok"))
        n_real = sum(1 for e in engines.values()
                     if e.get("s3", {}).get("state")
                     and e["s3"]["state"] != "ERROR")
        sb_ok = sb_v.get("expects_48") and sb_v.get("all_48_live")
        page_ok = page.get("ok") and page.get("markers_found") == 27
        scorecard = {
            "n_engines_total": len(ENGINES),
            "n_deploy_ok": n_deploy,
            "n_schedule_ok": n_sched,
            "n_invoke_ok": n_inv,
            "n_s3_ok": n_s3,
            "n_real_state": n_real,
            "signal_board_48_48_ok": sb_ok,
            "page_27_markers_ok": page_ok,
            "all_pass": (n_deploy == 6 and n_sched == 6 and n_inv == 6
                         and n_s3 == 6 and n_real == 6 and sb_ok and page_ok),
        }
        report["scorecard"] = scorecard
        report["ended_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        print("\n" + "=" * 70)
        print(f"SCORECARD: {json.dumps(scorecard, indent=2)}")
        print("=" * 70)

        # Telegram alert
        emoji = "✅" if scorecard["all_pass"] else "⚠️"
        rows = []
        for name, e in engines.items():
            short = name.replace("justhodl-", "")
            state = e.get("s3", {}).get("state") or "—"
            strength = e.get("s3", {}).get("signal_strength")
            rows.append(f"`{short}` {state} ({strength})")
        msg = (
            f"{emoji} *Tier-4 retail edges DEPLOY+VERIFY*\n"
            f"deploy {n_deploy}/6 | sched {n_sched}/6 | invoke {n_inv}/6 | "
            f"s3 {n_s3}/6 | real {n_real}/6\n"
            f"signal-board: {sb_v.get('n_engines')}/{sb_v.get('n_live')} live\n"
            f"page: {page.get('markers_found')}/27 markers\n\n"
            + "\n".join(rows) +
            f"\n\nall_pass: *{scorecard['all_pass']}*"
        )
        sent = telegram_alert(msg)
        report["telegram_sent"] = sent
        print(f"\nTelegram: {sent}")
    finally:
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "988.json"
        try:
            out_path.write_text(json.dumps(report, indent=2, default=str))
            print(f"\nReport: {out_path.relative_to(REPO_ROOT)}")
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
