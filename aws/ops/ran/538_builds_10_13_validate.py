#!/usr/bin/env python3
"""538 — Validate BUILDs 10 (news-velocity v1.1.0) + 13 (dealer-gex v1.3.0) by force-invoke.
Also audit unknown Lambdas: justhodl-options-flow-scanner, justhodl-google-trends."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/538_builds_10_13_validate.json"
lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def audit_lambda(name):
    info = {"name": name}
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        info["exists"] = True
        info["last_modified"] = cfg.get("LastModified")
        info["memory"] = cfg.get("MemorySize")
        info["timeout"] = cfg.get("Timeout")
        info["env_keys"] = sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys())
        info["description"] = cfg.get("Description", "")[:200]
    except lam.exceptions.ResourceNotFoundException:
        info["exists"] = False
        return info

    rules = []
    for r in eb.list_rules()["Rules"]:
        try:
            ts = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
            if any(name in t.get("Arn", "") for t in ts):
                rules.append({"name": r["Name"], "schedule": r.get("ScheduleExpression"),
                                "state": r.get("State")})
        except: pass
    info["rules"] = rules
    return info


def invoke_and_capture(name, payload=b"{}"):
    try:
        resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                           LogType="Tail", Payload=payload)
        out = {"status": resp.get("StatusCode"), "fn_error": resp.get("FunctionError")}
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if p.get("body") else p
        except: out["raw"] = body[:1500]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-3500:]
        return out
    except Exception as e:
        return {"err": str(e)[:400]}


def check_sidecar(key, want_keys=None):
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        body = obj["Body"].read()
        p = json.loads(body)
        out = {"size_kb": round(len(body)/1024, 1),
                "modified": obj["LastModified"].isoformat()[:19]}
        if want_keys:
            for k in want_keys: out[k] = p.get(k)
        else:
            out["top_keys"] = list(p.keys())[:15]
        out["_full"] = p
        return out
    except s3.exceptions.NoSuchKey: return {"exists": False}
    except Exception as e: return {"err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ── BUILD 13: force-invoke dealer-gex v1.3.0 + verify sidecar has new 0DTE fields ──
    out["build_13_dealer_gex"] = {"audit": audit_lambda("justhodl-dealer-gex")}
    out["build_13_dealer_gex"]["invoke"] = invoke_and_capture("justhodl-dealer-gex")

    _time.sleep(3)
    sc = check_sidecar("data/dealer-gex.json")
    if "_full" in sc:
        full = sc.pop("_full")
        sc["version"] = full.get("version")
        sc["generated_at"] = full.get("generated_at")
        sc["composite_regime"] = (full.get("market_composite") or {}).get("composite_regime")
        sc["composite_signal"] = (full.get("market_composite") or {}).get("composite_signal", "")[:200]
        # Inspect 0DTE schema in 3 underlyings
        ulying = full.get("underlyings") or {}
        zero_samples = {}
        for sym in ("SPY", "QQQ", "IWM"):
            u = ulying.get(sym) or {}
            z = u.get("zero_dte") or {}
            zero_samples[sym] = {
                "spot": u.get("spot"),
                "total_gex_b": u.get("total_dealer_gex_billions"),
                "0dte_keys": sorted(list(z.keys())),
                "vol_pct": z.get("vol_pct"),
                "oi_pct": z.get("oi_pct"),
                "call_oi": z.get("call_oi"),
                "put_oi": z.get("put_oi"),
                "call_vol": z.get("call_vol"),
                "put_vol": z.get("put_vol"),
                "pcr_oi": z.get("pcr_oi"),
                "pcr_vol": z.get("pcr_vol"),
                "gex_billions": z.get("gex_billions"),
                "call_walls_oi": (z.get("call_walls_oi") or [])[:2],
                "put_walls_oi": (z.get("put_walls_oi") or [])[:2],
                "pin_strike": z.get("pin_strike"),
                "pct_to_pin": z.get("pct_to_pin"),
            }
        sc["zero_dte_samples"] = zero_samples
        # Check whether the NEW v1.3.0 fields are present
        spy_z = zero_samples.get("SPY", {})
        has_new_fields = all(k in spy_z["0dte_keys"] for k in
                              ["call_oi", "put_oi", "call_vol", "put_vol",
                                "pcr_oi", "pcr_vol", "gex_billions",
                                "call_walls_oi", "put_walls_oi", "pin_strike"])
        sc["v130_fields_present"] = has_new_fields
    out["build_13_dealer_gex"]["sidecar"] = sc

    # ── BUILD 10: force-invoke news-velocity v1.1.0 + read fresh sidecar ──
    out["build_10_news_velocity"] = {"audit": audit_lambda("justhodl-news-velocity")}
    out["build_10_news_velocity"]["invoke"] = invoke_and_capture("justhodl-news-velocity")

    _time.sleep(3)
    sc2 = check_sidecar("data/news-velocity.json")
    if "_full" in sc2:
        full = sc2.pop("_full")
        sc2["version"] = full.get("version")
        sc2["generated_at"] = full.get("generated_at")
        sc2["n_tickers"] = full.get("n_tickers")
        sc2["n_with_data"] = full.get("n_with_data")
        sc2["n_with_err"] = full.get("n_with_err")
        sc2["composite_regime"] = full.get("composite_regime")
        sc2["composite_signal"] = full.get("composite_signal", "")[:200]
        sc2["ranked_top_velocity"] = ((full.get("ranked") or {}).get("top_5_velocity") or [])[:5]
        sc2["ranked_bottom_subdued"] = ((full.get("ranked") or {}).get("bottom_5_subdued") or [])[:5]
        # Count entries with from_prior_cache flag
        by = full.get("by_ticker") or {}
        n_from_cache = sum(1 for v in by.values() if (v or {}).get("from_prior_cache"))
        sc2["n_from_prior_cache"] = n_from_cache
    out["build_10_news_velocity"]["sidecar"] = sc2

    # ── Audit unknown Lambdas autonomous session created ──
    out["audit_options_flow_scanner"] = audit_lambda("justhodl-options-flow-scanner")
    out["audit_google_trends"] = audit_lambda("justhodl-google-trends")
    out["audit_commodity_curves"] = audit_lambda("justhodl-commodity-curves")
    out["audit_global_markets"] = audit_lambda("justhodl-global-markets")

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
