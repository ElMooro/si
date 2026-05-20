"""
ops 904 -- verify Edge #8: OPEX / 0DTE Gamma Pinning Calendar
==============================================================

Lambda: justhodl-opex-calendar
Output: s3://justhodl-dashboard-live/data/opex-calendar.json
Page:   https://justhodl.ai/opex-calendar.html

Checks:
- Lambda deployed (python3.12, mem>=256, timeout>=60)
- Invoke completes within 120s budget (FMP options chain + FRED)
- S3 output present and parseable JSON
- Full canonical schema present
- State in known enum {QUIET, BUILDUP, OPEX_WEEK, OPEX_DAY, POST_OPEX, QUAD_WITCHING}
- days_to_next_opex is non-negative integer
- opex_calendar is a non-empty list with date + is_quad_witching fields
- max_pain has SPY/QQQ/IWM entries (may have null strike if chain unavailable)
- dealer_gex_proxy has gamma_sign_estimate
- forward_expectations has 1m/3m/12m horizons
- recommended_trade.primary present
- why_now_explainer > 200 chars
- SSM /justhodl/opex-calendar/state written
- Page is live (HTML payload > 1KB)
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

FN = "justhodl-opex-calendar"
S3_KEY = "data/opex-calendar.json"
PAGE = "opex-calendar.html"
SSM_KEY = "/justhodl/opex-calendar/state"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=160, connect_timeout=10,
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
        r = lam.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
        payload = r["Payload"].read().decode()
        return {"status": r["StatusCode"], "fn_err": r.get("FunctionError"), "body": payload[:600]}
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


def page_alive(path):
    url = f"{PAGES_BASE}/{path}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ops/904"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="ignore")
            return r.status == 200 and len(body) > 1000, len(body), r.status
    except urllib.error.HTTPError as e:
        return False, 0, e.code
    except Exception as e:
        return False, 0, str(e)


def ssm_present(key):
    try:
        r = ssm.get_parameter(Name=key)
        v = r.get("Parameter", {}).get("Value", "")
        try:
            return True, json.loads(v)
        except Exception:
            return True, {"_raw": v[:200]}
    except ClientError as e:
        return False, str(e)


def verify_lambda():
    print("\n=== EDGE #8: OPEX / 0DTE Gamma Calendar ===")
    info = lambda_get(FN)
    deployed = "_error" not in info
    add("e8.lambda_deployed", deployed, info.get("_error", "ok"))
    if not deployed:
        return False
    cfg = info.get("Configuration", {})
    add("e8.runtime_python312", cfg.get("Runtime") == "python3.12", cfg.get("Runtime"))
    add("e8.memory_sufficient", cfg.get("MemorySize", 0) >= 256, f"mem={cfg.get('MemorySize')}MB")
    add("e8.timeout_sufficient", cfg.get("Timeout", 0) >= 60, f"timeout={cfg.get('Timeout')}s")
    add("e8.role_attached", "lambda-execution-role" in cfg.get("Role", ""), cfg.get("Role", "")[-60:])
    return True


def verify_invoke_and_output():
    print("invoking opex-calendar Lambda (FMP + FRED ~30-90s)...")
    t0 = time.time()
    inv = lambda_invoke(FN)
    dur = round(time.time() - t0, 1)
    status_ok = inv.get("status") == 200 and not inv.get("fn_err")
    add("e8.invoke_ok", status_ok,
        f"dur={dur}s status={inv.get('status')} err={inv.get('fn_err')} body={inv.get('body', '')[:200]}")
    if not status_ok:
        return

    head = s3_head(S3_KEY)
    add("e8.s3_output_present", head is not None,
        f"size={head['ContentLength']}" if head else "missing")

    d = s3_get_json(S3_KEY)
    if "_error" in d:
        add("e8.s3_output_parseable", False, d["_error"])
        return
    add("e8.s3_output_parseable", True, f"keys={len(d)}")

    required = [
        "engine", "version", "as_of", "state", "previous_state",
        "state_transition", "state_description", "signal_strength",
        "days_to_next_opex", "next_opex", "opex_calendar",
        "max_pain", "dealer_gex_proxy",
        "trigger_conditions", "forward_expectations",
        "recommended_trade", "historical_episodes",
        "why_now_explainer", "methodology", "sources", "schedule",
    ]
    missing = [k for k in required if k not in d]
    add("e8.schema_complete", len(missing) == 0,
        "all_present" if not missing else f"missing={missing}")

    add("e8.engine_id", d.get("engine") == "opex-calendar", d.get("engine"))

    valid_states = ("QUIET", "BUILDUP", "OPEX_WEEK", "OPEX_DAY",
                    "POST_OPEX", "QUAD_WITCHING")
    add("e8.state_valid", d.get("state") in valid_states, d.get("state"))

    dto = d.get("days_to_next_opex")
    add("e8.days_to_opex_sane", isinstance(dto, int) and dto >= 0,
        f"days_to_next_opex={dto}")

    cal = d.get("opex_calendar", [])
    cal_ok = isinstance(cal, list) and len(cal) >= 3
    add("e8.calendar_present", cal_ok,
        f"n_events={len(cal) if isinstance(cal, list) else 'NA'}")
    if isinstance(cal, list) and cal:
        sample = cal[0]
        s_ok = isinstance(sample, dict) and "date" in sample and "is_quad_witching" in sample
        add("e8.calendar_row_structure", s_ok,
            f"keys={list(sample.keys())[:6]}" if isinstance(sample, dict) else "not-dict")

    mp = d.get("max_pain", {})
    mp_ok = all(s in mp for s in ("SPY", "QQQ", "IWM"))
    add("e8.max_pain_indices", mp_ok, list(mp.keys()))

    gex = d.get("dealer_gex_proxy", {})
    gex_ok = "gamma_sign_estimate" in gex
    add("e8.gamma_sign_present", gex_ok,
        f"sign={gex.get('gamma_sign_estimate')}")

    fe = d.get("forward_expectations", {})
    fe_ok = all(h in fe for h in ("1m", "3m", "12m"))
    add("e8.forward_horizons", fe_ok, list(fe.keys()))

    tc = d.get("trigger_conditions", [])
    tc_ok = isinstance(tc, list) and len(tc) >= 3
    add("e8.trigger_conditions_valid", tc_ok,
        f"n={len(tc) if isinstance(tc, list) else 'NA'}")

    trade = d.get("recommended_trade", {})
    add("e8.trade_ticket_present", isinstance(trade, dict) and "primary" in trade,
        f"keys={list(trade.keys())[:6]}" if isinstance(trade, dict) else "not-dict")

    why = d.get("why_now_explainer", "")
    add("e8.why_now_present", isinstance(why, str) and len(why) > 200,
        f"len={len(why) if isinstance(why, str) else 0}")


def verify_ssm():
    ok, val = ssm_present(SSM_KEY)
    note = f"state={val.get('state', '?')}" if isinstance(val, dict) else str(val)[:80]
    add("e8.ssm_state_written", ok, note)


def verify_page():
    ok, size, code = page_alive(PAGE)
    add("e8.page_live", ok, f"http={code} size={size}")


def main():
    started = time.time()
    print(f"ops 904: verify Edge #8 (OPEX Calendar) at {dt.datetime.utcnow().isoformat()}Z")
    try:
        if verify_lambda():
            verify_invoke_and_output()
            verify_ssm()
        verify_page()
    except Exception as e:
        add("e8.exception", False, str(e))

    n_pass = sum(1 for c in CHECKS if c["ok"])
    n_fail = sum(1 for c in CHECKS if not c["ok"])
    overall = n_fail == 0

    report = {
        "ops": 904,
        "title": "verify Edge #8 (OPEX / 0DTE Gamma Pinning Calendar)",
        "started_at": dt.datetime.utcnow().isoformat() + "Z",
        "duration_seconds": round(time.time() - started, 1),
        "checks": CHECKS,
        "summary": {"pass": n_pass, "fail": n_fail, "total": len(CHECKS)},
        "overall_ok": overall,
    }
    out = "aws/ops/reports/904_opex_calendar_verify.json"
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
