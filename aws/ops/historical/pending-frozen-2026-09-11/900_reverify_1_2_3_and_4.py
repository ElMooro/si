"""
ops 900 — re-verify edges #1, #2, #3 (post-deploy) and verify NEW edge #4
=========================================================================

Re-runs:
- Edge #1 (VIX backwardation trigger) — ops 897 ran too early
- Edge #2 (Insider open-market BUY enriched) — ops 898 ran too early
- Edge #3 (Breadth thrust + Whaley + Coppock) — ops 899 ran too early

New verification:
- Edge #4 (Vol-target unwind trigger) — full e2e

Pattern: live-state queries against AWS + S3 + page CDN. Strict pass/fail
per check, JSON report committed.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
import datetime as dt

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
PAGES_BASE = "https://justhodl.ai"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
events = boto3.client("events", region_name=REGION)


def s3_head(key):
    try:
        return s3.head_object(Bucket=S3_BUCKET, Key=key)
    except ClientError as e:
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
        req = urllib.request.Request(url, headers={"User-Agent": "ops/900"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="ignore")
            return r.status == 200 and len(body) > 1000, len(body), r.status
    except urllib.error.HTTPError as e:
        return False, 0, e.code
    except Exception as e:
        return False, 0, str(e)


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
        return {"status": r["StatusCode"], "body": payload[:500]}
    except ClientError as e:
        return {"_error": str(e)}


def schedule_exists(group_or_rule, prefer="rule"):
    """Check either EventBridge Rule or Scheduler schedule by name."""
    try:
        r = events.describe_rule(Name=group_or_rule)
        return {"type": "rule", "schedule": r.get("ScheduleExpression"), "state": r.get("State")}
    except ClientError:
        pass
    try:
        r = sch.get_schedule(Name=group_or_rule)
        return {"type": "schedule", "schedule": r.get("ScheduleExpression"), "state": r.get("State")}
    except ClientError:
        pass
    return None


def ssm_get(name):
    try:
        r = ssm.get_parameter(Name=name)
        return r["Parameter"]["Value"]
    except ClientError as e:
        return None


CHECKS = []


def add(name, ok, note=""):
    CHECKS.append({"check": name, "ok": bool(ok), "note": str(note)[:300]})


# =====================================================================
# EDGE #1 — VIX backwardation trigger
# =====================================================================
def verify_edge1():
    print("\n=== EDGE #1: VIX backwardation trigger ===")
    fn = "justhodl-vix-backwardation-trigger"
    info = lambda_get(fn)
    add("e1.lambda_deployed", "_error" not in info, info.get("_error", "ok"))

    inv = lambda_invoke(fn)
    add("e1.invoke_ok", inv.get("status") == 200, str(inv)[:200])

    time.sleep(8)
    head = s3_head("data/vix-backwardation-trigger.json")
    add("e1.s3_output_present", head is not None,
        f"size={head['ContentLength']}" if head else "missing")

    d = s3_get_json("data/vix-backwardation-trigger.json")
    if "_error" in d:
        add("e1.schema_complete", False, d["_error"])
    else:
        req = ["engine", "state", "current_readings", "trigger_conditions",
               "forward_expectations", "recommended_trade", "why_now_explainer",
               "historical_episodes"]
        missing = [k for k in req if k not in d]
        add("e1.schema_complete", not missing, f"missing={missing}")

        fwd = d.get("forward_expectations") or {}
        n_ok = sum(1 for h in ["1m", "3m", "12m"]
                   if isinstance(fwd.get(h), dict) and (fwd[h].get("n") or 0) >= 5)
        add("e1.forward_horizons", n_ok >= 2, f"horizons_with_n_ge_5={n_ok}/3")

        cr = d.get("current_readings") or {}
        readings = sum(1 for k in ["vix9d", "vix", "vix3m", "vvix"] if cr.get(k))
        add("e1.live_readings", readings >= 3, f"readings_with_value={readings}/4")

        eps = d.get("historical_episodes") or []
        with_fwd = sum(1 for e in eps if e.get("fwd_3m_pct") is not None)
        add("e1.historical_episodes", with_fwd >= 5, f"episodes_with_fwd_3m={with_fwd}")

        trade = d.get("recommended_trade") or {}
        add("e1.trade_present", bool(trade.get("primary")),
            "primary_set" if trade.get("primary") else "missing")

    ssm_state = ssm_get("/justhodl/vix-backwardation/state")
    add("e1.ssm_state", ssm_state is not None, "set" if ssm_state else "missing")

    ok, size, code = page_alive("vix-capitulation.html")
    add("e1.page_live", ok, f"http={code} size={size}")


# =====================================================================
# EDGE #2 — Insider open-market BUY enrichment
# =====================================================================
def verify_edge2():
    print("\n=== EDGE #2: insider buys enriched ===")
    fn = "justhodl-insider-buys-enriched"
    info = lambda_get(fn)
    add("e2.lambda_deployed", "_error" not in info, info.get("_error", "ok"))

    src = s3_head("data/insider-clusters.json")
    add("e2.source_clusters_present", src is not None,
        f"size={src['ContentLength']}" if src else "missing")

    inv = lambda_invoke(fn)
    add("e2.invoke_ok", inv.get("status") == 200, str(inv)[:200])

    time.sleep(8)
    head = s3_head("data/insider-buys-enriched.json")
    add("e2.s3_output_present", head is not None,
        f"size={head['ContentLength']}" if head else "missing")

    d = s3_get_json("data/insider-buys-enriched.json")
    if "_error" in d:
        add("e2.schema_complete", False, d["_error"])
    else:
        req = ["engine", "summary", "top_setups", "methodology", "sources"]
        missing = [k for k in req if k not in d]
        add("e2.schema_complete", not missing, f"missing={missing}")

        setups = d.get("top_setups") or []
        add("e2.has_setups", len(setups) >= 1, f"n_setups={len(setups)}")

        if setups:
            first = setups[0]
            has_returns = bool(first.get("expected_returns"))
            has_trade = bool(first.get("recommended_trade"))
            has_why = bool(first.get("why_now_explainer"))
            has_boosts = bool(first.get("quality_boosts_applied"))
            add("e2.setup_complete",
                has_returns and has_trade and has_why and has_boosts,
                f"returns={has_returns} trade={has_trade} why={has_why} boosts={has_boosts}")

    ok, size, code = page_alive("insider-buys.html")
    add("e2.page_live", ok, f"http={code} size={size}")


# =====================================================================
# EDGE #3 — Breadth thrust + Whaley + Coppock
# =====================================================================
def verify_edge3():
    print("\n=== EDGE #3: breadth thrust ===")
    fn = "justhodl-breadth-thrust"
    info = lambda_get(fn)
    add("e3.lambda_deployed", "_error" not in info, info.get("_error", "ok"))

    inv = lambda_invoke(fn)
    add("e3.invoke_ok", inv.get("status") == 200, str(inv)[:200])

    time.sleep(10)
    head = s3_head("data/breadth-thrust.json")
    add("e3.s3_output_present", head is not None,
        f"size={head['ContentLength']}" if head else "missing")

    d = s3_get_json("data/breadth-thrust.json")
    if "_error" in d:
        add("e3.schema_complete", False, d["_error"])
    else:
        req = ["engine", "state", "current_readings", "trigger_conditions",
               "forward_expectations", "supporting_signals", "recommended_trade",
               "historical_episodes"]
        missing = [k for k in req if k not in d]
        add("e3.schema_complete", not missing, f"missing={missing}")

        fwd = d.get("forward_expectations") or {}
        n_ok = sum(1 for h in ["1m", "3m", "6m", "12m"]
                   if isinstance(fwd.get(h), dict) and (fwd[h].get("n") or 0) >= 3)
        add("e3.forward_horizons", n_ok >= 2, f"horizons_with_n_ge_3={n_ok}/4")

        ss = d.get("supporting_signals") or {}
        add("e3.whaley_coppock", bool(ss.get("whaley")) and bool(ss.get("coppock")),
            f"whaley={bool(ss.get('whaley'))} coppock={bool(ss.get('coppock'))}")

        eps = d.get("historical_episodes") or []
        add("e3.historical_episodes", len(eps) >= 5,
            f"n_episodes={len(eps)}")

        trade = d.get("recommended_trade") or {}
        add("e3.trade_present", bool(trade.get("primary")),
            "primary_set" if trade.get("primary") else "missing")

    ssm_state = ssm_get("/justhodl/breadth-thrust/state")
    add("e3.ssm_state", ssm_state is not None, "set" if ssm_state else "missing")

    ok, size, code = page_alive("breadth-thrust.html")
    add("e3.page_live", ok, f"http={code} size={size}")


# =====================================================================
# EDGE #4 — Vol-target unwind trigger
# =====================================================================
def verify_edge4():
    print("\n=== EDGE #4: vol-target unwind ===")
    fn = "justhodl-vol-target-unwind"
    info = lambda_get(fn)
    add("e4.lambda_deployed", "_error" not in info, info.get("_error", "ok"))

    inv = lambda_invoke(fn)
    add("e4.invoke_ok", inv.get("status") == 200, str(inv)[:200])

    time.sleep(10)
    head = s3_head("data/vol-target-unwind.json")
    add("e4.s3_output_present", head is not None,
        f"size={head['ContentLength']}" if head else "missing")

    d = s3_get_json("data/vol-target-unwind.json")
    if "_error" in d:
        add("e4.schema_complete", False, d["_error"])
    else:
        req = ["engine", "state", "current_readings", "trigger_conditions",
               "thresholds", "aum_at_risk_usd_bn", "forward_expectations",
               "recommended_trade", "why_now_explainer",
               "historical_episodes_up", "historical_episodes_down"]
        missing = [k for k in req if k not in d]
        add("e4.schema_complete", not missing, f"missing={missing}")

        fwd = d.get("forward_expectations") or {}
        n_ok = sum(1 for h in ["1w", "1m", "3m", "12m"]
                   if isinstance(fwd.get(h), dict) and (fwd[h].get("n") or 0) >= 3)
        add("e4.forward_horizons", n_ok >= 3, f"horizons_with_n_ge_3={n_ok}/4")

        cr = d.get("current_readings") or {}
        readings = sum(1 for k in ["spy_close", "spy_realized_vol_21d_pct",
                                    "spy_realized_vol_5d_pct"]
                       if cr.get(k))
        add("e4.live_readings", readings >= 3, f"readings_present={readings}/3")

        eps_up = d.get("historical_episodes_up") or []
        with_fwd = sum(1 for e in eps_up if e.get("fwd_1m_pct") is not None)
        add("e4.historical_up_episodes", with_fwd >= 5,
            f"up_episodes_with_fwd={with_fwd}")

        aum = d.get("aum_at_risk_usd_bn")
        add("e4.aum_estimated", aum is not None and aum > 0,
            f"aum_bn=${aum}")

        trade = d.get("recommended_trade") or {}
        add("e4.trade_present", bool(trade.get("primary")),
            "primary_set" if trade.get("primary") else "missing")

    ssm_state = ssm_get("/justhodl/vol-target-unwind/state")
    add("e4.ssm_state", ssm_state is not None, "set" if ssm_state else "missing")

    ok, size, code = page_alive("vol-target-unwind.html")
    add("e4.page_live", ok, f"http={code} size={size}")


# =====================================================================
# Main
# =====================================================================
def main():
    started = time.time()
    print(f"ops 900: combined re-verify edges #1-#3 + new #4 at {dt.datetime.utcnow().isoformat()}Z")

    try:
        verify_edge1()
    except Exception as e:
        add("e1.exception", False, str(e))

    try:
        verify_edge2()
    except Exception as e:
        add("e2.exception", False, str(e))

    try:
        verify_edge3()
    except Exception as e:
        add("e3.exception", False, str(e))

    try:
        verify_edge4()
    except Exception as e:
        add("e4.exception", False, str(e))

    n_pass = sum(1 for c in CHECKS if c["ok"])
    n_fail = sum(1 for c in CHECKS if not c["ok"])
    overall = n_fail == 0

    report = {
        "ops": 900,
        "title": "re-verify edges 1-3 post-deploy + verify new edge 4",
        "started_at": dt.datetime.utcnow().isoformat() + "Z",
        "duration_seconds": round(time.time() - started, 1),
        "checks": CHECKS,
        "summary": {"pass": n_pass, "fail": n_fail, "total": len(CHECKS)},
        "overall_ok": overall,
    }
    out = "aws/ops/reports/900_reverify_1_2_3_and_4.json"
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
