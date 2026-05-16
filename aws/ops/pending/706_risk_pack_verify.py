"""ops/706 — verify the 4 new risk/liquidity/opportunity engines.

Invocation order matters: crisis-composite first (writes the sidecar that
capitulation reads), then capitulation, then the two FRED engines.
"""
import json, os, time, base64
import boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def cfg(fn):
    try:
        c = lam.get_function_configuration(FunctionName=fn)
        env = (c.get("Environment") or {}).get("Variables", {}) or {}
        return {"state": c.get("State"), "env_keys": sorted(env.keys()),
                "timeout": c.get("Timeout")}
    except Exception as e:
        return {"err": str(e)[:200]}


def invoke(fn, payload=b"{}"):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                        Payload=payload, LogType="Tail")
        body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
        log = base64.b64decode(r.get("LogResult", b"")).decode("utf-8", "replace") if r.get("LogResult") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response": body[:600], "log_tail": log[-1300:]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:300]}


def sidecar(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {str(e)[:160]}"}


def main():
    report = {"started": datetime.now(timezone.utc).isoformat(), "engines": {}}

    # ── 1. crisis-composite (first — capitulation depends on its sidecar) ──
    print("crisis-composite...")
    cc = {"config": cfg("justhodl-crisis-composite"),
          "invoke": invoke("justhodl-crisis-composite")}
    sc = sidecar("data/crisis-composite.json")
    if "_error" not in sc:
        cc["sidecar"] = {
            "generated_at": sc.get("generated_at"),
            "master_crisis_score": sc.get("master_crisis_score"),
            "defcon_level": sc.get("defcon_level"),
            "defcon_name": sc.get("defcon_name"),
            "trend": sc.get("trend"),
            "components_available": sc.get("components_available"),
            "primary_drivers": sc.get("primary_drivers"),
            "components": [{"src": c.get("source"), "contrib": c.get("crisis_contribution"),
                            "avail": c.get("available")}
                           for c in (sc.get("components") or [])],
        }
    else:
        cc["sidecar"] = sc
    report["engines"]["crisis-composite"] = cc

    # ── 2. capitulation (reads crisis-composite) ──
    print("capitulation...")
    cap = {"config": cfg("justhodl-capitulation"),
           "invoke": invoke("justhodl-capitulation")}
    sc = sidecar("data/capitulation.json")
    if "_error" not in sc:
        cap["sidecar"] = {
            "generated_at": sc.get("generated_at"),
            "capitulation_score": sc.get("capitulation_score"),
            "signal": sc.get("signal"),
            "stabilising": sc.get("stabilising"),
            "smart_money_confirm": sc.get("smart_money_confirm"),
            "washout_components": [{"l": w.get("label"), "i": w.get("intensity")}
                                   for w in (sc.get("washout_components") or [])],
            "shopping_list_n": len(sc.get("shopping_list") or []),
        }
    else:
        cap["sidecar"] = sc
    report["engines"]["capitulation"] = cap

    # ── 3. china-liquidity ──
    print("china-liquidity...")
    ch = {"config": cfg("justhodl-china-liquidity"),
          "invoke": invoke("justhodl-china-liquidity")}
    sc = sidecar("data/china-liquidity.json")
    if "_error" not in sc:
        ch["sidecar"] = {
            "generated_at": sc.get("generated_at"),
            "fred_failed": sc.get("fred_failed"),
            "series_resolved": sc.get("series_resolved"),
            "regime": sc.get("regime"),
            "money": sc.get("money"),
            "credit_impulse": sc.get("credit_impulse"),
            "dr_copper": sc.get("dr_copper"),
        }
    else:
        ch["sidecar"] = sc
    report["engines"]["china-liquidity"] = ch

    # ── 4. bank-stress ──
    print("bank-stress...")
    bs = {"config": cfg("justhodl-bank-stress"),
          "invoke": invoke("justhodl-bank-stress")}
    sc = sidecar("data/bank-stress.json")
    if "_error" not in sc:
        bs["sidecar"] = {
            "generated_at": sc.get("generated_at"),
            "fred_failed": sc.get("fred_failed"),
            "series_resolved": sc.get("series_resolved"),
            "bank_stress_score": sc.get("bank_stress_score"),
            "regime": sc.get("regime"),
            "emergency_draw": sc.get("emergency_draw"),
            "emergency_liquidity": sc.get("emergency_liquidity"),
            "reserve_adequacy": sc.get("reserve_adequacy"),
        }
    else:
        bs["sidecar"] = sc
    report["engines"]["bank-stress"] = bs

    summ = {}
    for name, blk in report["engines"].items():
        sc = blk.get("sidecar", {})
        ok = ("_error" not in sc and blk.get("invoke", {}).get("fn_error") is None
              and blk.get("invoke", {}).get("status") == 200)
        summ[name] = "OK" if ok else "CHECK"
    report["summary"] = summ
    report["finished"] = datetime.now(timezone.utc).isoformat()

    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/706_risk_pack_verify.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 706_risk_pack_verify.json :: " + json.dumps(summ))


if __name__ == "__main__":
    main()
