"""ops/704 — verify the 4 new bagger-pack Lambdas deployed + working.

  justhodl-coffee-can        — multibagger holding tracker
  justhodl-hiring-velocity   — headcount-inflection detector
  justhodl-global-liquidity  — Global Liquidity Index
  justhodl-insider-aggregate — market-wide insider buy/sell

For each: confirm env inherited, invoke, inspect sidecar for real output.
Hiring-velocity gets a limited invoke (limit=120) so the ops invoke returns
in time; the weekly cron does the full universe.
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
        return {"state": c.get("State"), "last_modified": c.get("LastModified"),
                "timeout": c.get("Timeout"), "env_keys": sorted(env.keys())}
    except Exception as e:
        return {"err": str(e)[:200]}


def invoke(fn, payload=b"{}"):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                        Payload=payload, LogType="Tail")
        log = base64.b64decode(r.get("LogResult", b"")).decode("utf-8", "replace") if r.get("LogResult") else ""
        body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response": body[:500], "log_tail": log[-1400:]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:300]}


def sidecar(key):
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        return d
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {str(e)[:160]}"}


def main():
    report = {"started": datetime.now(timezone.utc).isoformat(), "lambdas": {}}

    # ── coffee-can ──
    print("coffee-can...")
    cc = {"config": cfg("justhodl-coffee-can")}
    cc["invoke"] = invoke("justhodl-coffee-can")
    sc = sidecar("data/coffee-can.json")
    if "_error" not in sc:
        cc["sidecar"] = {"generated_at": sc.get("generated_at"),
                         "n_holdings": sc.get("n_holdings"),
                         "portfolio": sc.get("portfolio"),
                         "last_action": sc.get("last_action")}
    else:
        cc["sidecar"] = sc
    report["lambdas"]["coffee-can"] = cc

    # ── hiring-velocity (limited) ──
    print("hiring-velocity (limit=120)...")
    hv = {"config": cfg("justhodl-hiring-velocity")}
    hv["invoke"] = invoke("justhodl-hiring-velocity", json.dumps({"limit": 120}).encode())
    sc = sidecar("data/hiring-velocity.json")
    if "_error" not in sc:
        hv["sidecar"] = {
            "generated_at": sc.get("generated_at"),
            "n_scanned": sc.get("n_scanned"), "n_scored": sc.get("n_scored"),
            "n_errors": sc.get("n_errors"), "counts": sc.get("counts"),
            "top_5": [{"symbol": r.get("symbol"), "score": r.get("expansion_score"),
                       "state": r.get("state"), "hc_yoy": r.get("headcount_yoy_pct")}
                      for r in (sc.get("top_50") or [])[:5]],
        }
    else:
        hv["sidecar"] = sc
    report["lambdas"]["hiring-velocity"] = hv

    # ── global-liquidity ──
    print("global-liquidity...")
    gl = {"config": cfg("justhodl-global-liquidity")}
    gl["invoke"] = invoke("justhodl-global-liquidity")
    sc = sidecar("data/global-liquidity.json")
    if "_error" not in sc:
        gl["sidecar"] = {
            "generated_at": sc.get("generated_at"),
            "fred_failed": sc.get("fred_failed"),
            "regime": sc.get("regime"),
            "global_impulse_13w_pct": sc.get("global_impulse_13w_pct"),
            "gli": sc.get("global_liquidity_index"),
            "fed_net_liquidity": sc.get("fed_net_liquidity"),
            "broad_money": sc.get("broad_money"),
        }
    else:
        gl["sidecar"] = sc
    report["lambdas"]["global-liquidity"] = gl

    # ── insider-aggregate ──
    print("insider-aggregate...")
    ia = {"config": cfg("justhodl-insider-aggregate")}
    ia["invoke"] = invoke("justhodl-insider-aggregate")
    sc = sidecar("data/insider-aggregate.json")
    if "_error" not in sc:
        w30 = (sc.get("windows") or {}).get("last_30d", {})
        ia["sidecar"] = {
            "generated_at": sc.get("generated_at"),
            "n_transactions": sc.get("n_transactions"),
            "regime": sc.get("regime"),
            "headline_ratio_30d_dollar": sc.get("headline_ratio_30d_dollar"),
            "w30_buy_count": w30.get("buy_count"), "w30_sell_count": w30.get("sell_count"),
            "w30_buy_usd": w30.get("buy_usd"), "w30_sell_usd": w30.get("sell_usd"),
            "n_cluster_buys": len(sc.get("notable_cluster_buys") or []),
            "top_clusters": (sc.get("notable_cluster_buys") or [])[:5],
            "err": sc.get("err"),
        }
    else:
        ia["sidecar"] = sc
    report["lambdas"]["insider-aggregate"] = ia

    # summary
    summ = {}
    for name, blk in report["lambdas"].items():
        sc = blk.get("sidecar", {})
        ok = ("_error" not in sc and not sc.get("err")
              and blk.get("invoke", {}).get("fn_error") is None)
        summ[name] = "OK" if ok else "CHECK"
    report["summary"] = summ
    report["finished"] = datetime.now(timezone.utc).isoformat()

    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/704_bagger_pack_verify.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 704_bagger_pack_verify.json :: " + json.dumps(summ))


if __name__ == "__main__":
    main()
