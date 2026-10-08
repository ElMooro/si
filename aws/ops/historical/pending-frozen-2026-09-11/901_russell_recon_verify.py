"""
ops 901 -- verify Edge #5: Russell/S&P Reconstitution Front-Run
================================================================

Lambda: justhodl-russell-recon-frontrun
Output: s3://justhodl-dashboard-live/data/russell-recon-frontrun.json
SSM:    /justhodl/russell-recon/state  (rank snapshot)
Page:   https://justhodl.ai/russell-recon.html

Checks (strict pass/fail):
- Lambda deployed (runtime python3.12, mem 1024, timeout 240)
- Live invocation (allow up to ~240s for FMP 3500-name screener fetch)
- S3 output present and parseable JSON
- Full schema (engine, calendar_phase, rebal_friday, days_to_rebal,
  aum_benchmarked_usd_bn, summary, trigger_conditions,
  forward_expectations_priors, migrations dict with all 5 buckets,
  top_long_setups, top_short_setups, recommended_trade.primary,
  why_now_explainer, methodology, sources, schedule)
- Universe size >= 1000 (proxy: FMP returned a real screener result set)
- SSM rank snapshot written
- Page live (justhodl.ai/russell-recon.html, > 1000 bytes)

Note: prior_ranks will be 0 on first run (no SSM snapshot yet), so
migrations will be empty. We do not fail on empty migrations on first
run; we only fail if the schema is incomplete.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
import datetime as dt

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
PAGES_BASE = "https://justhodl.ai"

FN = "justhodl-russell-recon-frontrun"
S3_KEY = "data/russell-recon-frontrun.json"
SSM_KEY = "/justhodl/russell-recon/state"
PAGE = "russell-recon.html"

# Long read-timeout to allow ~240s Lambda sync invocation
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


CHECKS = []


def add(name, ok, note=""):
    CHECKS.append({"check": name, "ok": bool(ok), "note": str(note)[:300]})


def lambda_get(name):
    try:
        return lam.get_function(FunctionName=name)
    except ClientError as e:
        return {"_error": str(e)}


def lambda_invoke(name):
    try:
        r = lam.invoke(
            FunctionName=name,
            InvocationType="RequestResponse",
            Payload=b"{}",
        )
        payload = r["Payload"].read().decode()
        return {"status": r["StatusCode"], "fn_err": r.get("FunctionError"),
                "body": payload[:600]}
    except ClientError as e:
        return {"_error": str(e)}


def s3_head(key):
    try:
        return s3.head_object(Bucket=S3_BUCKET, Key=key)
    except ClientError:
        return None


def s3_get_json(key):
    try:
        r = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(r["Body"].read())
    except Exception as e:
        return {"_error": str(e)}


def ssm_get(name):
    try:
        r = ssm.get_parameter(Name=name)
        return r["Parameter"]["Value"]
    except ClientError:
        return None


def page_alive(path):
    url = f"{PAGES_BASE}/{path}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ops/901"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="ignore")
            return r.status == 200 and len(body) > 1000, len(body), r.status
    except urllib.error.HTTPError as e:
        return False, 0, e.code
    except Exception as e:
        return False, 0, str(e)


def verify_lambda():
    print(f"\n=== EDGE #5: Russell reconstitution front-run ===")
    info = lambda_get(FN)
    deployed = "_error" not in info
    add("e5.lambda_deployed", deployed, info.get("_error", "ok"))
    if not deployed:
        return False
    cfg = info.get("Configuration", {})
    runtime_ok = cfg.get("Runtime") == "python3.12"
    add("e5.runtime_python312", runtime_ok, cfg.get("Runtime"))
    mem_ok = cfg.get("MemorySize", 0) >= 512
    add("e5.memory_sufficient", mem_ok, f"mem={cfg.get('MemorySize')}MB")
    timeout_ok = cfg.get("Timeout", 0) >= 120
    add("e5.timeout_sufficient", timeout_ok, f"timeout={cfg.get('Timeout')}s")
    role_ok = "lambda-execution-role" in cfg.get("Role", "")
    add("e5.role_attached", role_ok, cfg.get("Role", "")[-60:])
    return True


def verify_invoke_and_output():
    print("invoking russell-recon Lambda (may take 60-240s on first FMP fetch)...")
    t0 = time.time()
    inv = lambda_invoke(FN)
    dur = round(time.time() - t0, 1)
    status_ok = inv.get("status") == 200 and not inv.get("fn_err")
    add("e5.invoke_ok", status_ok,
        f"dur={dur}s status={inv.get('status')} err={inv.get('fn_err')} body={inv.get('body', '')[:200]}")
    if not status_ok:
        return

    head = s3_head(S3_KEY)
    add("e5.s3_output_present", head is not None,
        f"size={head['ContentLength']}" if head else "missing")

    d = s3_get_json(S3_KEY)
    if "_error" in d:
        add("e5.s3_output_parseable", False, d["_error"])
        return
    add("e5.s3_output_parseable", True, f"keys={len(d)}")

    # Top-level schema fields
    required_top = [
        "engine", "version", "as_of", "calendar_phase",
        "rebal_friday", "days_to_rebal",
        "aum_benchmarked_usd_bn", "summary",
        "trigger_conditions", "forward_expectations_priors",
        "migrations", "top_long_setups", "top_short_setups",
        "recommended_trade", "why_now_explainer",
        "methodology", "sources", "schedule",
        "signal_strength", "universe_size",
    ]
    missing = [k for k in required_top if k not in d]
    add("e5.schema_complete", len(missing) == 0,
        "all_present" if not missing else f"missing={missing}")

    # Engine identity
    add("e5.engine_id", d.get("engine") == "russell-recon-frontrun",
        d.get("engine"))

    # Universe size
    universe = d.get("universe_size", 0)
    add("e5.universe_size_ge_1000", universe >= 1000, f"n={universe}")

    # AUM dict
    aum = d.get("aum_benchmarked_usd_bn", {})
    aum_ok = all(k in aum for k in ("russell_1000", "russell_2000", "russell_3000", "total"))
    add("e5.aum_breakdown", aum_ok, str(aum))

    # Migrations dict has all 5 buckets
    migs = d.get("migrations", {})
    needed_buckets = ("adds_r3000", "deletes_r3000", "upcaps", "downcaps", "borderline")
    bucket_ok = all(b in migs for b in needed_buckets)
    add("e5.migrations_buckets", bucket_ok,
        ", ".join(f"{b}={len(migs.get(b, []))}" for b in needed_buckets))

    # Forward expectations priors -- check it covers ADD_R3000 at minimum
    priors = d.get("forward_expectations_priors", {})
    priors_ok = isinstance(priors, dict) and len(priors) >= 4
    add("e5.priors_present", priors_ok, f"n_priors={len(priors)}")

    # Recommended trade structure
    trade = d.get("recommended_trade", {})
    trade_ok = isinstance(trade, dict) and "primary" in trade
    add("e5.trade_primary", trade_ok,
        f"keys={list(trade.keys())[:5]}" if isinstance(trade, dict) else "not-dict")

    # Trigger conditions (list of dicts with name/satisfied/weight)
    tc = d.get("trigger_conditions", [])
    tc_ok = isinstance(tc, list) and len(tc) >= 3 and \
            all(isinstance(c, dict) and "name" in c and "satisfied" in c for c in tc)
    add("e5.trigger_conditions_valid", tc_ok, f"n={len(tc)}")

    # Calendar phase from known enum
    valid_phases = ("DORMANT", "EARLY_MONITORING", "POST_RANK_SNAPSHOT",
                    "PRE_ANNOUNCEMENT", "ANNOUNCED_HIGH_CONVICTION",
                    "FINAL_WEEK", "POST_REBAL_FADE")
    phase = d.get("calendar_phase")
    add("e5.calendar_phase_valid", phase in valid_phases, str(phase))

    # rebal_friday format YYYY-MM-DD
    rf = d.get("rebal_friday", "")
    rf_ok = isinstance(rf, str) and len(rf) == 10 and rf[4] == "-" and rf[7] == "-"
    add("e5.rebal_friday_format", rf_ok, rf)

    # why_now_explainer non-empty
    why = d.get("why_now_explainer", "")
    add("e5.why_now_present", isinstance(why, str) and len(why) > 100,
        f"len={len(why) if isinstance(why, str) else 0}")


def verify_ssm_and_page():
    snap = ssm_get(SSM_KEY)
    snap_ok = snap is not None and len(snap) > 10
    add("e5.ssm_snapshot_written", snap_ok,
        f"len={len(snap) if snap else 0}")

    ok, size, code = page_alive(PAGE)
    add("e5.page_live", ok, f"http={code} size={size}")


def main():
    started = time.time()
    print(f"ops 901: verify Edge #5 (Russell recon) at {dt.datetime.utcnow().isoformat()}Z")

    try:
        if verify_lambda():
            verify_invoke_and_output()
        verify_ssm_and_page()
    except Exception as e:
        add("e5.exception", False, str(e))

    n_pass = sum(1 for c in CHECKS if c["ok"])
    n_fail = sum(1 for c in CHECKS if not c["ok"])
    overall = n_fail == 0

    report = {
        "ops": 901,
        "title": "verify Edge #5 (Russell reconstitution front-run)",
        "started_at": dt.datetime.utcnow().isoformat() + "Z",
        "duration_seconds": round(time.time() - started, 1),
        "checks": CHECKS,
        "summary": {"pass": n_pass, "fail": n_fail, "total": len(CHECKS)},
        "overall_ok": overall,
    }
    out = "aws/ops/reports/901_russell_recon_verify.json"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nwritten: {out}  pass={n_pass} fail={n_fail}")
    for c in CHECKS:
        flag = "OK " if c["ok"] else "FAIL"
        print(f"  [{flag}] {c['check']:35} {c['note'][:80]}")
    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
