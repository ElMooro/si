"""ops/718 — end-to-end verification of the 5-item enhancement roadmap.

#1 Risk Pack -> master-ranker + allocator
#2 PM Decision Layer (justhodl-pm-decision)
#3 Cross-Asset Relative Value (justhodl-cross-asset-rv)
#4 Signal Scorecard (justhodl-signal-scorecard)
#5 Consolidation (already done in 717 — 14 dead Lambdas deleted)

Invokes each engine, reads its S3 output, confirms the contract, and checks
the EventBridge schedule is attached.
"""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

BUCKET = "justhodl-dashboard-live"
cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1")
ev = boto3.client("events", region_name="us-east-1")
ACCT = "857687956942"


def invoke(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response": body[:400]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:250]}


def sidecar(key):
    try:
        o = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"].isoformat()
    except Exception as e:
        return {"_error": str(e)[:160]}, None


def has_schedule(fn):
    try:
        arn = f"arn:aws:lambda:us-east-1:{ACCT}:function:{fn}"
        rules = ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames", [])
        return rules
    except Exception as e:
        return [f"err:{str(e)[:80]}"]


def main():
    rep = {"started": datetime.now(timezone.utc).isoformat()}

    # ── #1: Risk Pack into master-ranker + allocator ──
    rep["item1_master_ranker"] = {"invoke": invoke("justhodl-master-ranker")}
    time.sleep(2)
    rep["item1_allocator"] = {"invoke": invoke("justhodl-allocator")}
    time.sleep(3)
    mr, mr_ts = sidecar("data/master-ranker.json")
    if "_error" not in mr:
        rc = mr.get("regime_context", {})
        rep["item1_master_ranker"]["check"] = {
            "s3_age_min": None if not mr_ts else round(
                (datetime.now(timezone.utc) - datetime.fromisoformat(mr_ts)).total_seconds() / 60, 1),
            "regime_context_keys": sorted(rc.keys()),
            "risk_posture": rc.get("risk_posture"),
            "defcon_level": rc.get("defcon_level"),
            "leading_markets_signal": rc.get("leading_markets_signal"),
            "capitulation_signal": rc.get("capitulation_signal"),
            "n_top_macro": len(mr.get("top_macro") or []),
            "risk_pack_wired": all(rc.get(k) is not None for k in
                                   ("risk_posture", "defcon_level", "leading_markets_signal")),
        }
    else:
        rep["item1_master_ranker"]["check"] = mr
    al, al_ts = sidecar("data/allocator.json")
    if "_error" not in al:
        ev_txt = json.dumps(al.get("evidence", al)).lower()
        rep["item1_allocator"]["check"] = {
            "s3_age_min": None if not al_ts else round(
                (datetime.now(timezone.utc) - datetime.fromisoformat(al_ts)).total_seconds() / 60, 1),
            "n_assets_scored": len(al.get("scores") or {}),
            "crisis_rule_fired": "crisis" in ev_txt or "defcon" in ev_txt,
            "capitulation_rule_fired": "capitulation" in ev_txt or "washout" in ev_txt,
            "leading_rule_fired": "leading" in ev_txt or "canary" in ev_txt,
            "liquidity_rule_fired": "global liquidity" in ev_txt or "gli" in ev_txt,
        }
    else:
        rep["item1_allocator"]["check"] = al

    # ── #2: PM Decision Layer ──
    time.sleep(3)
    rep["item2_pm_decision"] = {"invoke": invoke("justhodl-pm-decision"),
                                 "schedule": has_schedule("justhodl-pm-decision")}
    pm, pm_ts = sidecar("data/pm-decision.json")
    if "_error" not in pm:
        acts = pm.get("actions", {})
        rep["item2_pm_decision"]["check"] = {
            "top_keys": sorted(pm.keys()),
            "posture": pm.get("posture"),
            "headline": (pm.get("headline") or "")[:200],
            "n_trim": len(acts.get("trim", [])),
            "n_add": len(acts.get("add", [])),
            "n_hedge": len(acts.get("hedge", [])),
            "has_triggers": bool(pm.get("triggers")),
        }
    else:
        rep["item2_pm_decision"]["check"] = pm

    # ── #3: Cross-Asset Relative Value ──
    rep["item3_cross_asset_rv"] = {"invoke": invoke("justhodl-cross-asset-rv"),
                                    "schedule": has_schedule("justhodl-cross-asset-rv")}
    rv, rv_ts = sidecar("data/cross-asset-rv.json")
    if "_error" not in rv:
        pairs = (rv.get("pairs") or rv.get("relationships") or rv.get("dislocations")
                 or rv.get("signals") or [])
        rep["item3_cross_asset_rv"]["check"] = {
            "top_keys": sorted(rv.keys()),
            "n_pairs": len(pairs),
            "sample_pairs": [{"k": p.get("key") or p.get("label"),
                              "verdict": p.get("verdict") or p.get("signal")
                              or p.get("state"), "z": p.get("z") or p.get("zscore")}
                             for p in pairs[:6]],
        }
    else:
        rep["item3_cross_asset_rv"]["check"] = rv

    # ── #4: Signal Scorecard ──
    rep["item4_signal_scorecard"] = {"invoke": invoke("justhodl-signal-scorecard"),
                                      "schedule": has_schedule("justhodl-signal-scorecard")}
    sc, sc_ts = sidecar("data/signal-scorecard.json")
    if "_error" not in sc:
        rep["item4_signal_scorecard"]["check"] = {
            "top_keys": sorted(sc.keys()),
            "n_signals_tracked": sc.get("n_signals_tracked"),
            "n_outcomes_scanned": sc.get("n_outcomes_scanned"),
            "n_promoted": sc.get("n_promoted"),
            "n_deprecated": sc.get("n_deprecated"),
            "n_probation": sc.get("n_probation"),
            "avg_graded_wilson_lb": sc.get("avg_graded_wilson_lb"),
        }
    else:
        rep["item4_signal_scorecard"]["check"] = sc

    # ── #5: consolidation already verified in 717 ──
    try:
        c717 = json.load(open("aws/ops/reports/717_cruft_delete.json"))
        rep["item5_consolidation"] = {"n_deleted": c717.get("n_deleted"),
                                       "lambda_count_after": c717.get("lambda_count_after"),
                                       "status": "done in ops/717"}
    except Exception as e:
        rep["item5_consolidation"] = {"_error": str(e)[:120]}

    # ── summary ──
    def ok(d):
        c = d.get("check", {})
        return c if isinstance(c, dict) and "_error" not in c else None
    mrc = ok(rep["item1_master_ranker"]) or {}
    alc = ok(rep["item1_allocator"]) or {}
    pmc = ok(rep["item2_pm_decision"]) or {}
    rvc = ok(rep["item3_cross_asset_rv"]) or {}
    scc = ok(rep["item4_signal_scorecard"]) or {}
    rep["summary"] = {
        "item1_master_ranker_ok": bool(mrc.get("risk_pack_wired")),
        "item1_allocator_ok": any([alc.get("crisis_rule_fired"),
                                   alc.get("capitulation_rule_fired"),
                                   alc.get("leading_rule_fired")]),
        "item2_pm_decision_ok": bool(pmc.get("posture")) and bool(rep["item2_pm_decision"]["schedule"]),
        "item3_cross_asset_rv_ok": rvc.get("n_pairs", 0) >= 3,
        "item4_signal_scorecard_ok": scc.get("n_signals_tracked") is not None,
        "item5_consolidation_ok": rep["item5_consolidation"].get("n_deleted") == 14,
    }
    rep["all_five_verified"] = all(rep["summary"].values())
    rep["finished"] = datetime.now(timezone.utc).isoformat()

    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/718_roadmap_verify.json", "w") as f:
        json.dump(rep, f, indent=2, default=str)
    print("DONE -> 718_roadmap_verify.json")
    print(json.dumps(rep["summary"], indent=2))
    print("ALL FIVE VERIFIED:", rep["all_five_verified"])


if __name__ == "__main__":
    main()
