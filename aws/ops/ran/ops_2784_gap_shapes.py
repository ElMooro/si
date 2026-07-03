"""ops 2784 — dump exact shapes of the data on the 4 detail pages not yet on the hub."""
import os, json
from datetime import datetime, timezone
import boto3
s3 = boto3.client("s3", region_name="us-east-1"); B = "justhodl-dashboard-live"
def rd(k): return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
R = {"ops": 2784, "ts": datetime.now(timezone.utc).isoformat(), "d": {}}
# options-flow (scanner)
try:
    f = rd("data/options-flow.json"); aq = f.get("all_qualifying") or []
    R["d"]["flow"] = {"top": list(f), "summary": f.get("summary"), "n_all": len(aq),
                      "item_keys": list(aq[0]) if aq else [], "sample": aq[0] if aq else None,
                      "tiers": sorted(set(x.get("tier") for x in aq)), "metrics_keys": list((aq[0].get("metrics") or {})) if aq else []}
    print("flow: n=%d tiers=%s" % (len(aq), R["d"]["flow"]["tiers"]))
    print("  item keys:", R["d"]["flow"]["item_keys"]); print("  metrics keys:", R["d"]["flow"]["metrics_keys"])
    print("  summary:", json.dumps(f.get("summary"))[:200])
    print("  sample flags:", (aq[0].get("flags") if aq else None))
except Exception as e: R["d"]["flow"] = {"err": str(e)[:100]}; print("flow err", str(e)[:80])
# options-confluence by_posture + counts
try:
    c = rd("data/options-confluence.json")
    bp = c.get("by_posture") or {}
    R["d"]["confluence"] = {"counts": c.get("counts"), "by_posture_keys": list(bp),
                            "by_posture_sizes": {k: len(v) for k, v in bp.items()},
                            "sample_item": (list(bp.values())[0][0] if bp and list(bp.values())[0] else None),
                            "thesis": (c.get("thesis") or "")[:120], "alpha_gate": (c.get("alpha_gate") or "")[:120]}
    print("confluence counts:", json.dumps(c.get("counts"))); print("  by_posture:", R["d"]["confluence"]["by_posture_sizes"])
except Exception as e: R["d"]["confluence"] = {"err": str(e)[:100]}; print("conf err", str(e)[:80])
# options-analytics squeeze_setups + most_unusual + counts
try:
    a = rd("data/options-analytics.json")
    ss = a.get("squeeze_setups") or []; mu = a.get("most_unusual") or []
    R["d"]["analytics"] = {"top": list(a), "n_board": len(a.get("board") or []),
                           "squeeze_setups_n": len(ss), "squeeze_item": ss[0] if ss else None,
                           "most_unusual_n": len(mu), "unusual_item": mu[0] if mu else None,
                           "caveats": a.get("caveats"), "data_source": a.get("data_source"),
                           "counts": {k: a.get(k) for k in ("n_short_gamma", "n_squeeze", "n_unusual_names") if k in a}}
    print("analytics: squeeze_setups=%d most_unusual=%d" % (len(ss), len(mu)))
    print("  squeeze item:", json.dumps(ss[0])[:220] if ss else "none")
    print("  unusual item:", json.dumps(mu[0])[:200] if mu else "none")
except Exception as e: R["d"]["analytics"] = {"err": str(e)[:100]}; print("analytics err", str(e)[:80])
# dealer-gex squeeze_candidates + OI
try:
    dg = rd("data/dealer-gex.json"); spy = (dg.get("underlyings") or {}).get("SPY") or {}
    sc = dg.get("squeeze_candidates") or []
    R["d"]["gex"] = {"squeeze_candidates_n": len(sc), "squeeze_item": sc[0] if sc else None,
                     "spy_oi": {"total_call_oi": spy.get("total_call_oi"), "total_put_oi": spy.get("total_put_oi"),
                                "n_contracts_modeled": spy.get("n_contracts_modeled"),
                                "top_gamma_strikes": spy.get("top_gamma_strikes")}}
    print("gex squeeze_candidates:", len(sc), "| SPY OI call/put:", spy.get("total_call_oi"), spy.get("total_put_oi"), "| contracts:", spy.get("n_contracts_modeled"))
    print("  top_gamma_strikes:", json.dumps(spy.get("top_gamma_strikes"))[:200])
except Exception as e: R["d"]["gex"] = {"err": str(e)[:100]}; print("gex err", str(e)[:80])
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2784_gap_shapes.json", "w"), indent=1, default=str)
print("OPS 2784 COMPLETE")
