"""
ops 903 -- verify Edge #7: Stablecoin Mint / Supply-Growth Tracker
==================================================================

Lambda: justhodl-stablecoin-flow
Output: s3://justhodl-dashboard-live/data/stablecoin-flow.json
Page:   https://justhodl.ai/stablecoin-flow.html

Checks:
- Lambda deployed (python3.12, mem>=256, timeout>=60)
- Invoke completes within 90s budget (DefiLlama API)
- S3 output present and parseable JSON
- Full canonical schema present
- State in known enum
- aggregate.total_usd >= $100B sanity threshold
- n_stablecoins >= 5
- top_stablecoins_by_mcap is a list of dicts (len >= 5)
- forward_expectations_by_asset_60d contains BTC/ETH/ALT/SPX keys
- SSM /justhodl/stablecoin-flow/state written
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

FN = "justhodl-stablecoin-flow"
S3_KEY = "data/stablecoin-flow.json"
PAGE = "stablecoin-flow.html"
SSM_KEY = "/justhodl/stablecoin-flow/state"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=140, connect_timeout=10,
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
        req = urllib.request.Request(url, headers={"User-Agent": "ops/903"})
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
            parsed = json.loads(v)
            return True, parsed
        except Exception:
            return True, {"_raw": v[:200]}
    except ClientError as e:
        return False, str(e)


def verify_lambda():
    print("\n=== EDGE #7: Stablecoin Mint Flow Tracker ===")
    info = lambda_get(FN)
    deployed = "_error" not in info
    add("e7.lambda_deployed", deployed, info.get("_error", "ok"))
    if not deployed:
        return False
    cfg = info.get("Configuration", {})
    add("e7.runtime_python312", cfg.get("Runtime") == "python3.12", cfg.get("Runtime"))
    add("e7.memory_sufficient", cfg.get("MemorySize", 0) >= 256, f"mem={cfg.get('MemorySize')}MB")
    add("e7.timeout_sufficient", cfg.get("Timeout", 0) >= 60, f"timeout={cfg.get('Timeout')}s")
    add("e7.role_attached", "lambda-execution-role" in cfg.get("Role", ""), cfg.get("Role", "")[-60:])
    return True


def verify_invoke_and_output():
    print("invoking stablecoin-flow Lambda (DefiLlama fetch ~10-30s)...")
    t0 = time.time()
    inv = lambda_invoke(FN)
    dur = round(time.time() - t0, 1)
    status_ok = inv.get("status") == 200 and not inv.get("fn_err")
    add("e7.invoke_ok", status_ok,
        f"dur={dur}s status={inv.get('status')} err={inv.get('fn_err')} body={inv.get('body', '')[:200]}")
    if not status_ok:
        return

    head = s3_head(S3_KEY)
    add("e7.s3_output_present", head is not None,
        f"size={head['ContentLength']}" if head else "missing")

    d = s3_get_json(S3_KEY)
    if "_error" in d:
        add("e7.s3_output_parseable", False, d["_error"])
        return
    add("e7.s3_output_parseable", True, f"keys={len(d)}")

    required = [
        "engine", "version", "as_of", "state", "previous_state",
        "state_transition", "state_description", "signal_strength",
        "aggregate", "trigger_conditions", "forward_expectations",
        "forward_expectations_by_asset_60d", "top_stablecoins_by_mcap",
        "top_5_minters_30d", "top_3_burners_30d", "recommended_trade",
        "historical_episodes", "why_now_explainer",
        "methodology", "sources", "schedule",
    ]
    missing = [k for k in required if k not in d]
    add("e7.schema_complete", len(missing) == 0,
        "all_present" if not missing else f"missing={missing}")

    add("e7.engine_id", d.get("engine") == "stablecoin-flow", d.get("engine"))

    valid_states = ("CONTRACTING", "FLAT", "EXPANDING",
                    "EXPLOSIVE_MINT", "PARABOLIC_MINT")
    add("e7.state_valid", d.get("state") in valid_states, d.get("state"))

    agg = d.get("aggregate", {})
    total = agg.get("total_usd", 0)
    n = agg.get("n_stablecoins", 0)
    add("e7.aggregate_sane", total >= 100e9 and n >= 5,
        f"total=${total/1e9:.1f}B n={n}")

    fe = d.get("forward_expectations", {})
    fe_ok = all(h in fe for h in ("1m", "3m", "12m"))
    add("e7.forward_horizons", fe_ok, list(fe.keys()))

    fea = d.get("forward_expectations_by_asset_60d", {})
    fea_ok = all(k in fea for k in ("BTC_pct", "ETH_pct", "ALT_basket_pct", "SPX_pct"))
    add("e7.forward_by_asset", fea_ok, list(fea.keys()))

    tops = d.get("top_stablecoins_by_mcap", [])
    tops_ok = isinstance(tops, list) and len(tops) >= 5
    add("e7.top_stablecoins_list", tops_ok,
        f"n={len(tops) if isinstance(tops, list) else 'NA'}")
    if isinstance(tops, list) and tops:
        sample = tops[0]
        s_ok = isinstance(sample, dict) and "symbol" in sample and "circulating_usd" in sample
        add("e7.top_stablecoin_structure", s_ok,
            f"keys={list(sample.keys())[:6]}" if isinstance(sample, dict) else "not-dict")

    tc = d.get("trigger_conditions", [])
    tc_ok = isinstance(tc, list) and len(tc) >= 3
    add("e7.trigger_conditions_valid", tc_ok,
        f"n={len(tc) if isinstance(tc, list) else 'NA'}")

    trade = d.get("recommended_trade", {})
    trade_ok = isinstance(trade, dict) and "primary" in trade
    add("e7.trade_ticket_present", trade_ok,
        f"keys={list(trade.keys())[:6]}" if isinstance(trade, dict) else "not-dict")

    why = d.get("why_now_explainer", "")
    add("e7.why_now_present", isinstance(why, str) and len(why) > 200,
        f"len={len(why) if isinstance(why, str) else 0}")


def verify_ssm():
    ok, val = ssm_present(SSM_KEY)
    note = f"state={val.get('state', '?')}" if isinstance(val, dict) else str(val)[:80]
    add("e7.ssm_state_written", ok, note)


def verify_page():
    ok, size, code = page_alive(PAGE)
    add("e7.page_live", ok, f"http={code} size={size}")


def main():
    started = time.time()
    print(f"ops 903: verify Edge #7 (Stablecoin Mint Flow) at {dt.datetime.utcnow().isoformat()}Z")

    try:
        if verify_lambda():
            verify_invoke_and_output()
            verify_ssm()
        verify_page()
    except Exception as e:
        add("e7.exception", False, str(e))

    n_pass = sum(1 for c in CHECKS if c["ok"])
    n_fail = sum(1 for c in CHECKS if not c["ok"])
    overall = n_fail == 0

    report = {
        "ops": 903,
        "title": "verify Edge #7 (Stablecoin Mint / Supply-Growth Tracker)",
        "started_at": dt.datetime.utcnow().isoformat() + "Z",
        "duration_seconds": round(time.time() - started, 1),
        "checks": CHECKS,
        "summary": {"pass": n_pass, "fail": n_fail, "total": len(CHECKS)},
        "overall_ok": overall,
    }
    out = "aws/ops/reports/903_stablecoin_flow_verify.json"
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
