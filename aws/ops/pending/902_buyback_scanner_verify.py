"""
ops 902 -- verify Edge #6: Buyback Authorization + Drift Scanner
=================================================================

Lambda: justhodl-buyback-scanner
Output: s3://justhodl-dashboard-live/data/buyback-scanner.json
Page:   https://justhodl.ai/buyback-scanner.html

Checks:
- Lambda deployed (python3.12, mem>=512, timeout>=240)
- Invoke completes within 240s budget (EDGAR + FMP enrichment is heavy)
- S3 output present and parseable JSON
- Full schema (engine, state, signal_strength, trigger_conditions,
  forward_expectations, tranche_priors_drift_90d_pct,
  cross_confirmation_multipliers, top_opportunities, why_now_explainer)
- top_opportunities is a list; if non-empty, each has ticker + trade_ticket
- State is in known enum
- Page is live
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

FN = "justhodl-buyback-scanner"
S3_KEY = "data/buyback-scanner.json"
PAGE = "buyback-scanner.html"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=320, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

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
        req = urllib.request.Request(url, headers={"User-Agent": "ops/902"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="ignore")
            return r.status == 200 and len(body) > 1000, len(body), r.status
    except urllib.error.HTTPError as e:
        return False, 0, e.code
    except Exception as e:
        return False, 0, str(e)


def verify_lambda():
    print("\n=== EDGE #6: Buyback Authorization Scanner ===")
    info = lambda_get(FN)
    deployed = "_error" not in info
    add("e6.lambda_deployed", deployed, info.get("_error", "ok"))
    if not deployed:
        return False
    cfg = info.get("Configuration", {})
    add("e6.runtime_python312", cfg.get("Runtime") == "python3.12", cfg.get("Runtime"))
    add("e6.memory_sufficient", cfg.get("MemorySize", 0) >= 512, f"mem={cfg.get('MemorySize')}MB")
    add("e6.timeout_sufficient", cfg.get("Timeout", 0) >= 240, f"timeout={cfg.get('Timeout')}s")
    add("e6.role_attached", "lambda-execution-role" in cfg.get("Role", ""), cfg.get("Role", "")[-60:])
    return True


def verify_invoke_and_output():
    print("invoking buyback-scanner Lambda (may take 60-240s for EDGAR+FMP)...")
    t0 = time.time()
    inv = lambda_invoke(FN)
    dur = round(time.time() - t0, 1)
    status_ok = inv.get("status") == 200 and not inv.get("fn_err")
    add("e6.invoke_ok", status_ok,
        f"dur={dur}s status={inv.get('status')} err={inv.get('fn_err')} body={inv.get('body', '')[:200]}")
    if not status_ok:
        return

    head = s3_head(S3_KEY)
    add("e6.s3_output_present", head is not None,
        f"size={head['ContentLength']}" if head else "missing")

    d = s3_get_json(S3_KEY)
    if "_error" in d:
        add("e6.s3_output_parseable", False, d["_error"])
        return
    add("e6.s3_output_parseable", True, f"keys={len(d)}")

    required = [
        "engine", "version", "as_of", "state", "state_description",
        "signal_strength", "n_total_candidates_8k", "n_unique_tickers",
        "n_enriched_opportunities", "n_fresh_last_7d",
        "tranche_priors_drift_90d_pct", "cross_confirmation_multipliers",
        "trigger_conditions", "forward_expectations",
        "top_opportunities", "why_now_explainer",
        "methodology", "sources", "schedule",
    ]
    missing = [k for k in required if k not in d]
    add("e6.schema_complete", len(missing) == 0,
        "all_present" if not missing else f"missing={missing}")

    add("e6.engine_id", d.get("engine") == "buyback-scanner", d.get("engine"))

    valid_states = ("VERY_QUIET", "NORMAL", "HIGH_ACTIVITY", "CRISIS_OVERSHOOT")
    add("e6.state_valid", d.get("state") in valid_states, d.get("state"))

    candidates = d.get("n_total_candidates_8k", 0)
    add("e6.edgar_returned_results", candidates >= 1, f"n_8k={candidates}")

    tp = d.get("tranche_priors_drift_90d_pct", {})
    tp_ok = all(k in tp for k in ("SMALL_lt_2pct", "MEDIUM_2_to_5pct",
                                  "LARGE_5_to_10pct", "MEGA_gt_10pct"))
    add("e6.tranche_priors", tp_ok, list(tp.keys()))

    fe = d.get("forward_expectations", {})
    fe_ok = all(h in fe for h in ("1m", "3m", "12m"))
    add("e6.forward_horizons", fe_ok, list(fe.keys()))

    top = d.get("top_opportunities", [])
    add("e6.top_opps_list", isinstance(top, list), f"n={len(top) if isinstance(top, list) else 'NA'}")
    if isinstance(top, list) and top:
        sample = top[0]
        sample_ok = isinstance(sample, dict) and "ticker" in sample and "trade_ticket" in sample
        add("e6.top_opp_structure", sample_ok,
            f"keys={list(sample.keys())[:6]}" if isinstance(sample, dict) else "not-dict")

    tc = d.get("trigger_conditions", [])
    tc_ok = isinstance(tc, list) and len(tc) >= 3
    add("e6.trigger_conditions_valid", tc_ok, f"n={len(tc) if isinstance(tc, list) else 'NA'}")

    why = d.get("why_now_explainer", "")
    add("e6.why_now_present", isinstance(why, str) and len(why) > 200,
        f"len={len(why) if isinstance(why, str) else 0}")


def verify_page():
    ok, size, code = page_alive(PAGE)
    add("e6.page_live", ok, f"http={code} size={size}")


def main():
    started = time.time()
    print(f"ops 902: verify Edge #6 (Buyback Scanner) at {dt.datetime.utcnow().isoformat()}Z")

    try:
        if verify_lambda():
            verify_invoke_and_output()
        verify_page()
    except Exception as e:
        add("e6.exception", False, str(e))

    n_pass = sum(1 for c in CHECKS if c["ok"])
    n_fail = sum(1 for c in CHECKS if not c["ok"])
    overall = n_fail == 0

    report = {
        "ops": 902,
        "title": "verify Edge #6 (Buyback Authorization + Drift Scanner)",
        "started_at": dt.datetime.utcnow().isoformat() + "Z",
        "duration_seconds": round(time.time() - started, 1),
        "checks": CHECKS,
        "summary": {"pass": n_pass, "fail": n_fail, "total": len(CHECKS)},
        "overall_ok": overall,
    }
    out = "aws/ops/reports/902_buyback_scanner_verify.json"
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
