# ops 1529 — fusion recon: schemas for apex-fusion + global-tide inputs; resurrect cascade-validator
import json, time, boto3
from botocore.config import Config
cfg = Config(read_timeout=240, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
dd = boto3.client("dynamodb", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1529}

def rd(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception as e:
        return {"_err": str(e)[:60]}

def sample(obj, n=2):
    if isinstance(obj, list): return obj[:n]
    return obj

# 1) input schemas for apex-fusion
pp = rd("data/pump-positioning.json")
out["pump_positioning"] = {"n_candidates": pp.get("n_candidates"), "macro_regime": pp.get("macro_regime"),
                           "candidate_sample": sample(pp.get("candidates"), 2),
                           "aggressive_sample": sample(pp.get("aggressive_basket"), 2)}
ml = rd("data/momentum-leaders.json")
out["momentum_leaders"] = {"keys": list(ml.keys())[:10], "row": sample(ml.get("all_scored") or ml.get("leaders") or [], 1)}
sq = rd("data/microcap-float-squeeze.json")
out["squeeze"] = {"stats": sq.get("stats"), "row": sample(sq.get("all_qualifying"), 1)}
of = rd("data/options-flow.json")
out["options_flow"] = {"stats": of.get("stats"), "row": sample(of.get("all_qualifying"), 1)}
ic = rd("data/insider-clusters.json")
out["insider_clusters"] = {"stats": ic.get("stats"), "row": sample(ic.get("clusters"), 1)}
bo = rd("data/52wk-quality-breakout.json")
out["breakout52"] = {"keys": list(bo.keys())[:10], "row": sample(bo.get("all_qualifying") or bo.get("qualifying") or bo.get("breakouts") or [], 1)}
sc = rd("data/signal-scorecard.json")
out["scorecard"] = {"keys": list(sc.keys())[:12]}
for k in ("engines", "per_engine", "scorecard", "rows", "by_engine"):
    if isinstance(sc.get(k), (dict, list)):
        v = sc[k]
        out["scorecard"]["store_key"] = k
        out["scorecard"]["sample"] = sample(list(v.items())[:3] if isinstance(v, dict) else v, 3)
        break
bs = rd("data/best-setups.json")
out["best_setups"] = {"keys": list(bs.keys())[:10], "row": sample(bs.get("setups") or bs.get("best") or [], 1)}

# 2) global-tide inputs
gl = rd("data/global-liquidity.json")
snaps = gl.get("snapshots")
out["global_liquidity"] = {"updated_at": gl.get("updated_at"),
                           "snap_sample": (snaps[-1] if isinstance(snaps, list) and snaps else snaps if isinstance(snaps, dict) else None)}
out["china_liquidity"] = {"keys": list(rd("data/china-liquidity.json").keys())[:14]}
cl = rd("data/china-liquidity.json")
out["china_detail"] = {k: cl.get(k) for k in list(cl.keys())[:6]}
bj = rd("data/boj-detail.json")
out["boj_detail"] = {k: (bj.get(k) if not isinstance(bj.get(k), (list, dict)) else str(type(bj.get(k)))) for k in list(bj.keys())[:14]}
vx = rd("data/vix-curve.json")
out["vix_curve"] = {"keys": list(vx.keys())[:12]}
ed = rd("data/eurodollar-intel.json")
if ed.get("_err"):
    ed = rd("data/ecb-detail.json")
    out["ed_file"] = "data/ecb-detail.json"
else:
    out["ed_file"] = "data/eurodollar-intel.json"
out["ed_keys"] = list(ed.keys())[:16]
out["ed_balance_sheet"] = ed.get("balance_sheet")

# 3) signal-logger DDB schema
try:
    t = dd.describe_table(TableName="justhodl-signals")["Table"]
    out["ddb_signals"] = {"keys": t["KeySchema"], "n_items_approx": t.get("ItemCount"), "size_mb": round(t.get("TableSizeBytes", 0)/1e6, 1)}
except Exception as e:
    out["ddb_signals"] = {"_err": str(e)[:80]}
# peek one signal-logger prediction item shape via scan limit 1
try:
    sc1 = dd.scan(TableName="justhodl-signals", Limit=2)
    out["ddb_item_sample_keys"] = [sorted(i.keys()) for i in sc1.get("Items", [])][:2]
except Exception as e:
    out["ddb_item_sample_keys"] = str(e)[:80]

# 4) cascade-validator: rule state → enable + invoke
try:
    rules = ev.list_rules(NamePrefix="justhodl-cascade")["Rules"]
    out["cascade_rules"] = [{"name": r["Name"], "state": r["State"], "sched": r.get("ScheduleExpression")} for r in rules]
    for r in rules:
        if "valid" in r["Name"] and r["State"] == "DISABLED":
            ev.enable_rule(Name=r["Name"]); out.setdefault("enabled", []).append(r["Name"])
except Exception as e:
    out["cascade_rules_err"] = str(e)[:100]
try:
    rr = lam.invoke(FunctionName="justhodl-cascade-validator", InvocationType="RequestResponse", Payload=b"{}")
    out["validator_invoke"] = {"err": rr.get("FunctionError", "NONE"), "resp": rr["Payload"].read().decode()[:200]}
    time.sleep(2)
    vl = rd("data/cascade-validation-log.json")
    out["validation_now"] = {"generated_at": vl.get("generated_at"), "n": vl.get("n_predictions_validated"),
                             "by_tier_stats": vl.get("by_tier_stats"), "outcomes": vl.get("outcome_counts")}
except Exception as e:
    out["validator_invoke"] = str(e)[:120]

# 5) also re-check opportunity-calibrator freshness driver
try:
    rr = lam.invoke(FunctionName="justhodl-opportunity-calibrator", InvocationType="Event", Payload=b"{}")
    out["opp_calibrator_kicked"] = rr["StatusCode"]
except Exception as e:
    out["opp_calibrator_kicked"] = str(e)[:80]

open("aws/ops/reports/1529_recon.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"validator": out.get("validator_invoke"), "cascade_rules": out.get("cascade_rules"), "scorecard_key": out["scorecard"].get("store_key")}, default=str))
