"""
ops 953 -- post-fix force-invoke + re-verify of 4 Lambdas patched in this push:

  Edge #5  justhodl-russell-recon-frontrun   (fixed: per-exchange paginated FMP screener)
  Edge #6  justhodl-buyback-scanner          (fixed: added recommended_trade.primary)
  Edge #8  justhodl-opex-calendar            (fixed: top-level days_to_next_opex/max_pain/dealer_gex_proxy)
  Edge #9  justhodl-activist-13d             (fixed: all_setups key + extended QUIET why_now >200 chars)

For each: synchronous invoke -> wait -> read S3 -> check the previously-failing
condition. Writes single consolidated JSON report.
"""

import json
import os
import time
import boto3
import datetime as dt

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
REPORT_PATH = "aws/ops/reports/953_post_fix_invoke_and_verify.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def invoke(fn, timeout_s=620):
    cfg = boto3.session.Config(
        region_name=REGION,
        read_timeout=timeout_s,
        connect_timeout=20,
        retries={"max_attempts": 0},
    )
    c = boto3.client("lambda", config=cfg)
    t0 = time.time()
    try:
        resp = c.invoke(FunctionName=fn, InvocationType="RequestResponse",
                        Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8", errors="replace")
        return {
            "ok": True, "dur": round(time.time() - t0, 2),
            "status": resp.get("StatusCode"),
            "err": resp.get("FunctionError"),
            "body": body[:400],
        }
    except Exception as e:
        return {"ok": False, "err": str(e)[:300], "dur": round(time.time() - t0, 2)}


def read_s3(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = obj["Body"].read()
        return json.loads(data), len(data), None
    except Exception as e:
        return None, 0, str(e)[:200]


CHECKS = []


def add(name, passed, detail):
    CHECKS.append({"name": name, "passed": bool(passed), "detail": str(detail)[:300]})


# -- Edge #5: russell-recon-frontrun -----------------------------------------
print("== Edge #5: russell-recon-frontrun ==")
r = invoke("justhodl-russell-recon-frontrun", timeout_s=240)
add("e5.invoke_ok", r["ok"] and r.get("status") == 200 and not r.get("err"),
    f"dur={r.get('dur')}s status={r.get('status')} err={r.get('err')} "
    f"body={r.get('body','')[:200]}")
# Allow up to 30s for S3 propagation
time.sleep(3)
data, sz, err = read_s3("data/russell-recon-frontrun.json")
add("e5.s3_output_present", data is not None and sz > 1000,
    f"size={sz}B err={err}")
if data:
    add("e5.universe_nonempty",
        (data.get("summary", {}).get("n_universe", 0) or
         data.get("n_universe", 0) or
         len(data.get("predicted_adds", []) or []) +
         len(data.get("predicted_deletes", []) or [])) > 100,
        f"n_universe={data.get('summary',{}).get('n_universe', data.get('n_universe','?'))}")

# -- Edge #6: buyback-scanner ------------------------------------------------
print("== Edge #6: buyback-scanner ==")
r = invoke("justhodl-buyback-scanner", timeout_s=600)
add("e6.invoke_ok", r["ok"] and r.get("status") == 200 and not r.get("err"),
    f"dur={r.get('dur')}s status={r.get('status')} err={r.get('err')} "
    f"body={r.get('body','')[:160]}")
time.sleep(3)
data, sz, err = read_s3("data/buyback-scanner.json")
add("e6.s3_output_present", data is not None and sz > 1000,
    f"size={sz}B err={err}")
if data:
    rt = data.get("recommended_trade") or {}
    primary = rt.get("primary") or {}
    add("e6.recommended_trade_present", bool(rt),
        f"keys={list(rt.keys())[:5]}")
    add("e6.trade_ticket_primary_nonempty",
        bool(primary.get("instrument") and primary.get("thesis")),
        f"instrument={primary.get('instrument','?')[:30]} "
        f"thesis_len={len(primary.get('thesis',''))}")

# -- Edge #8: opex-calendar --------------------------------------------------
print("== Edge #8: opex-calendar ==")
r = invoke("justhodl-opex-calendar", timeout_s=120)
add("e8.invoke_ok", r["ok"] and r.get("status") == 200 and not r.get("err"),
    f"dur={r.get('dur')}s status={r.get('status')} err={r.get('err')} "
    f"body={r.get('body','')[:160]}")
time.sleep(3)
data, sz, err = read_s3("data/opex-calendar.json")
add("e8.s3_output_present", data is not None and sz > 500,
    f"size={sz}B err={err}")
if data:
    missing = [k for k in ("days_to_next_opex", "max_pain", "dealer_gex_proxy")
               if k not in data]
    add("e8.top_level_keys_present", not missing,
        f"missing={missing} dgex={data.get('dealer_gex_proxy','?')} "
        f"dtno={data.get('days_to_next_opex','?')} mp={data.get('max_pain','?')}")
    add("e8.dealer_gex_proxy_valid",
        data.get("dealer_gex_proxy") in ("LONG_GAMMA", "SHORT_GAMMA", "NEUTRAL"),
        f"got={data.get('dealer_gex_proxy')}")

# -- Edge #9: activist-13d ---------------------------------------------------
print("== Edge #9: activist-13d ==")
r = invoke("justhodl-activist-13d", timeout_s=600)
add("e9.invoke_ok", r["ok"] and r.get("status") == 200 and not r.get("err"),
    f"dur={r.get('dur')}s status={r.get('status')} err={r.get('err')} "
    f"body={r.get('body','')[:160]}")
time.sleep(3)
data, sz, err = read_s3("data/activist-13d.json")
add("e9.s3_output_present", data is not None and sz > 1000,
    f"size={sz}B err={err}")
if data:
    add("e9.all_setups_key_present", "all_setups" in data,
        f"keys_subset={[k for k in data.keys() if 'setup' in k.lower()]}")
    wn = data.get("why_now_explainer", "")
    add("e9.why_now_substantive", len(wn) >= 200,
        f"len={len(wn)} preview={wn[:80]!r}")

# -- Summary -----------------------------------------------------------------
report = {
    "ops": 953,
    "title": "post-fix invoke + verify of 4 Lambdas (Edges #5/#6/#8/#9)",
    "run_at": dt.datetime.utcnow().isoformat() + "Z",
    "checks": CHECKS,
    "summary": {
        "total": len(CHECKS),
        "passed": sum(1 for c in CHECKS if c["passed"]),
        "failed": sum(1 for c in CHECKS if not c["passed"]),
    },
    "overall_ok": all(c["passed"] for c in CHECKS),
}

print("\n=== SUMMARY ===")
print(json.dumps(report["summary"], indent=2))
print("OK:", report["overall_ok"])

os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
with open(REPORT_PATH, "w") as f:
    json.dump(report, f, indent=2)

print(f"report written to {REPORT_PATH}")
