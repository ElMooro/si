"""ops/718 — verify the 5-item enhancement roadmap end-to-end.

#1 Risk Pack -> master-ranker + allocator
#2 PM Decision Layer
#3 Cross-Asset RV engine
#4 Signal Scorecard
#5 consolidation (already confirmed by 717)
"""
import json, os, time, urllib.request
import boto3
from datetime import datetime, timezone

BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def invoke(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "body": body[:400]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:200]}


def sidecar(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"_error": str(e)[:150]}


def main():
    rep = {"started": datetime.now(timezone.utc).isoformat()}

    # ── #1 — refresh master-ranker + allocator, confirm Risk Pack wiring ──
    rep["item1_master_ranker"] = {"invoke": invoke("justhodl-master-ranker")}
    time.sleep(3)
    rep["item1_allocator"] = {"invoke": invoke("justhodl-allocator")}
    time.sleep(3)
    mr = sidecar("data/master-ranker.json")
    if "_error" not in mr:
        rc = mr.get("regime_context") or {}
        rep["item1_master_ranker"]["risk_pack_in_regime_context"] = {
            "defcon_level": rc.get("defcon_level"),
            "leading_markets_signal": rc.get("leading_markets_signal"),
            "capitulation_signal": rc.get("capitulation_signal"),
            "risk_posture": rc.get("risk_posture"),
            "WIRED": rc.get("risk_posture") is not None,
        }
        rep["item1_master_ranker"]["risk_pack_macro_signals"] = [
            s.get("type") for s in (mr.get("top_macro") or [])
            if s.get("type") in ("crisis_composite", "capitulation", "leading_markets")]
    al = sidecar("data/allocator.json")
    if "_error" not in al:
        rr = al.get("rule_results") or {}
        newr = ["crisis_composite", "capitulation", "leading_markets", "global_liquidity"]
        rep["item1_allocator"] = {
            "new_rules_present": {k: rr.get(k, {}).get("applied") for k in newr},
            "n_rules_total": al.get("n_rules_total"),
            "WIRED": all(k in rr for k in newr),
        }

    # ── #2 — PM Decision Layer ──
    rep["item2_pm_decision"] = {"invoke": invoke("justhodl-pm-decision")}
    pm = sidecar("data/pm-decision.json")
    if "_error" not in pm:
        rep["item2_pm_decision"]["check"] = {
            "posture_word": pm.get("posture_word"),
            "headline": pm.get("headline"),
            "n_trim": len((pm.get("actions") or {}).get("trim", [])),
            "n_add": len((pm.get("actions") or {}).get("add", [])),
            "n_hedge": len((pm.get("actions") or {}).get("hedge", [])),
            "n_triggers": len(pm.get("triggers") or []),
            "macro_frame": pm.get("macro_frame"),
            "inputs_used": pm.get("inputs_used"),
        }
    else:
        rep["item2_pm_decision"]["check"] = pm

    # ── #3 — Cross-Asset RV ──
    rep["item3_cross_asset_rv"] = {"invoke": invoke("justhodl-cross-asset-rv")}
    rv = sidecar("data/cross-asset-rv.json")
    if "_error" not in rv:
        rep["item3_cross_asset_rv"]["check"] = {
            "rv_state": rv.get("rv_state"),
            "n_dislocated": rv.get("n_dislocated"),
            "n_stretched": rv.get("n_stretched"),
            "data_failed": rv.get("data_failed"),
            "relationships": [{"key": r.get("key"), "z": r.get("residual_z"),
                               "state": r.get("state"), "r2": r.get("fit_r2")}
                              for r in (rv.get("relationships") or [])],
        }
    else:
        rep["item3_cross_asset_rv"]["check"] = rv

    # ── #4 — Signal Scorecard ──
    rep["item4_signal_scorecard"] = {"invoke": invoke("justhodl-signal-scorecard")}
    sc = sidecar("data/signal-scorecard.json")
    if "_error" not in sc:
        rep["item4_signal_scorecard"]["check"] = {
            "n_signals_tracked": sc.get("n_signals_tracked"),
            "n_outcomes_scanned": sc.get("n_outcomes_scanned"),
            "n_promoted": sc.get("n_promoted"),
            "n_deprecated": sc.get("n_deprecated"),
            "n_probation": sc.get("n_probation"),
            "avg_graded_wilson_lb": sc.get("avg_graded_wilson_lb"),
            "top5": [{"signal": r.get("signal_type"), "grade": r.get("grade"),
                      "status": r.get("status"), "n": r.get("n"),
                      "wilson_lb": r.get("wilson_lb")}
                     for r in (sc.get("scorecard") or [])[:5]],
        }
    else:
        rep["item4_signal_scorecard"]["check"] = sc

    # live page
    try:
        ts = int(time.time())
        req = urllib.request.Request(f"https://justhodl.ai/signal-scorecard.html?cb={ts}",
                                      headers={"User-Agent": "JH-Verify/1.0",
                                               "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=20) as r:
            code = r.getcode()
            html = r.read().decode("utf-8", "replace")
        rep["item4_signal_scorecard"]["page_live"] = (code == 200 and "Signal Scorecard" in html)
    except Exception as e:
        rep["item4_signal_scorecard"]["page_live"] = f"err:{str(e)[:90]}"

    # ── summary ──
    rep["summary"] = {
        "item1_risk_pack_wired": (
            rep.get("item1_master_ranker", {}).get("risk_pack_in_regime_context", {}).get("WIRED")
            and rep.get("item1_allocator", {}).get("WIRED")),
        "item2_pm_decision_ok": bool(rep.get("item2_pm_decision", {}).get("check", {}).get("posture_word")),
        "item3_cross_asset_rv_ok": bool(rv.get("rv_state")) if "_error" not in rv else False,
        "item4_signal_scorecard_ok": bool(sc.get("n_signals_tracked") is not None) if "_error" not in sc else False,
        "item5_consolidation": "see 717 — 14 deleted, 262 Lambdas",
    }
    rep["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/718_enhancements_verify.json", "w") as f:
        json.dump(rep, f, indent=2, default=str)
    print("DONE -> 718_enhancements_verify.json")
    print(json.dumps(rep["summary"], indent=2))


if __name__ == "__main__":
    main()
