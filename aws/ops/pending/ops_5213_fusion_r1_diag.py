"""ops_5213 -- read-only diagnostics after the Release-1 launch (ops 5212 GREEN).

Questions the first live bridge run raised:
  * global_business_cycle and etf_flows returned OK but 0 signals -- what does the artifact actually look like?
  * fortress and catalyst fell back to s3 LastModified -- which key carries their timestamp?
  * regime_composite (23h old) and tail_risk (19h old) were STALE -- are the engines still scheduled/running?
Prints the top-level keys, timestamp-like keys and a sample row per artifact, the bridge's own per-engine
diagnostics from data/jhsignal/bridge-run.json, and the schedule + last-log-event for the four engines.
No writes. sys.exit(1) only on a hard read failure of the bridge run report.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=60)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
sch = boto3.client("scheduler", region_name="us-east-1", config=CFG)
ev = boto3.client("events", region_name="us-east-1", config=CFG)
logs = boto3.client("logs", region_name="us-east-1", config=CFG)
BUCKET = "justhodl-dashboard-live"
ARTIFACTS = {
    "global_business_cycle": "data/global-business-cycle.json", "etf_flows": "data/etf-flows.json", "fortress": "data/fortress.json",
    "catalyst": "data/catalyst.json", "regime_composite": "data/regime-composite.json", "tail_risk": "data/tail-risk.json",
    "liquidity_credit_engine": "data/liquidity-credit-engine.json", "insider_radar": "data/insider-radar.json",
}
ENGINES = ["justhodl-regime-composite", "justhodl-tail-risk", "justhodl-etf-flows", "justhodl-global-business-cycle", "justhodl-fortress", "justhodl-catalyst"]
TS_HINT = ("generated", "as_of", "asof", "updated", "timestamp", "date", "session", "run")


def get_json(key):
    r = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(r["Body"].read()), r["LastModified"]


def sample(v, n=900):
    return json.dumps(v, default=str)[:n]


with report("ops_5213_fusion_r1_diag") as R:
    R.heading("ops 5213 -- fusion R1 diagnostics (read-only)")
    R.section("bridge run report")
    try:
        run, _ = get_json("data/jhsignal/bridge-run.json")
        R.log("   run %s: %s signals / %s entities, freshness %s, bus %s" % (run.get("run_id"), run.get("n_signals"), run.get("n_entities"), run.get("freshness_counts"), run.get("bus")))
        for e in run.get("engines") or []:
            R.kv(engine=e.get("engine_id"), status=e.get("source_status"), n=e.get("n_signals"), asof=e.get("data_asof"), basis=e.get("asof_basis"))
    except Exception as exc:
        R.fail("bridge-run.json unreadable: %s" % str(exc)[:200]); sys.exit(1)
    R.section("artifact shapes")
    for eid, key in ARTIFACTS.items():
        try:
            doc, lm = get_json(key)
        except Exception as exc:
            R.warn("   %s %s: %s" % (eid, key, str(exc)[:120])); continue
        keys = list(doc.keys()) if isinstance(doc, dict) else ["<%s>" % type(doc).__name__]
        tsk = {k: doc[k] for k in keys if any(h in k.lower() for h in TS_HINT) and not isinstance(doc[k], (dict, list))}
        R.log("   %s (%s, S3 %s): %d keys %s" % (eid, key, lm.isoformat()[:19], len(keys), keys[:40]))
        R.log("      timestamp-like: %s" % sample(tsk, 400))
        if eid == "global_business_cycle":
            agg = doc.get("aggregate") or {}
            R.log("      aggregate keys %s | global_phase=%s cli=%s" % (list(agg.keys())[:30] if isinstance(agg, dict) else type(agg), agg.get("global_phase") if isinstance(agg, dict) else None, agg.get("global_avg_cli") if isinstance(agg, dict) else None))
            R.log("      downturn_probability_6m=%s composite=%s n_countries=%s fresh_count=%s" % (doc.get("downturn_probability_6m"), sample(doc.get("composite"), 300), doc.get("n_countries"), doc.get("fresh_count")))
            cs = doc.get("countries") or doc.get("by_country")
            R.log("      countries: %s -> %s" % (type(cs).__name__, (len(cs) if hasattr(cs, "__len__") else cs)))
        if eid == "etf_flows":
            be = doc.get("by_etf") or {}
            R.log("      by_etf: %s entries; first: %s" % (len(be), sample(next(iter(be.values())) if be else None, 700)))
            R.log("      heavy_inflow=%s rotation_in=%s n_etfs_analyzed=%s" % (len(doc.get("heavy_inflow") or []), len(doc.get("rotation_in") or []), doc.get("n_etfs_analyzed")))
        if eid == "fortress":
            b = doc.get("board") or []
            R.log("      board %d rows; first row keys %s" % (len(b), list(b[0].keys())[:45] if b else None))
            R.log("      first row: %s" % sample(b[0] if b else None, 700))
        if eid == "catalyst":
            bt = doc.get("by_ticker") or {}
            R.log("      by_ticker %d; first: %s" % (len(bt), sample(next(iter(bt.items())) if bt else None, 500)))
        if eid == "regime_composite":
            R.log("      composite_score=%s meta_regime=%s n_modules_with_data=%s/%s" % (doc.get("composite_score"), doc.get("meta_regime"), doc.get("n_modules_with_data"), doc.get("n_modules_total")))
        if eid == "tail_risk":
            R.log("      indices: %s" % sample(doc.get("indices"), 600))
    R.section("engine cadence (schedules + last log event)")
    now = time.time()
    for fn in ENGINES:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            arn = cfg["FunctionArn"]
        except Exception as exc:
            R.warn("   %s: %s" % (fn, str(exc)[:120])); continue
        scheds = []
        try:
            for s in sch.list_schedules(NamePrefix=fn).get("Schedules") or []:
                d = sch.get_schedule(Name=s["Name"], GroupName=s.get("GroupName") or "default")
                scheds.append("%s %s %s" % (s["Name"], d.get("ScheduleExpression"), d.get("State")))
        except Exception as exc:
            scheds.append("scheduler err %s" % str(exc)[:80])
        rules = []
        try:
            for rn in ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames") or []:
                rd = ev.describe_rule(Name=rn)
                rules.append("%s %s %s" % (rn, rd.get("ScheduleExpression") or rd.get("EventPattern", "")[:60], rd.get("State")))
        except Exception as exc:
            rules.append("rules err %s" % str(exc)[:80])
        last = None
        try:
            st = logs.describe_log_streams(logGroupName="/aws/lambda/" + fn, orderBy="LastEventTime", descending=True, limit=1).get("logStreams") or []
            if st and st[0].get("lastEventTimestamp"):
                last = datetime.fromtimestamp(st[0]["lastEventTimestamp"] / 1000, tz=timezone.utc).isoformat()[:19]
        except Exception as exc:
            last = "logs err %s" % str(exc)[:60]
        R.log("   %s: modified %s | schedules %s | rules %s | last log event %s" % (fn, cfg.get("LastModified", "")[:19], scheds or "-", rules or "-", last))
    R.ok("diagnostics complete")
    sys.exit(0)
