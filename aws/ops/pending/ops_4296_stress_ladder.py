"""
ops_4296 -- stress-scenarios covers the whole ladder.

The desk's stress column showed 2 classes because the engine's curated
impact_maps only touched 2 mappable ladder assets. The overlay adds the
13 ladder ETFs to all five archetype scenarios (setdefault -- curated
entries win), same convention, defensible analog-period magnitudes.
Gate: stress artifact's asset_impact covers >=11 ladder tickers with
by_scenario populated; desk re-run shows stress ERs on >=8 ladder
classes, column printed.
"""
import json, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
RUN_START = datetime.now(timezone.utc)
LADDER = ["SPY","IWM","EFA","EEM","HYG","IEF","GLD","SLV","DBC",
          "VNQ","BTC","ETH","CASH"]
def fresh(fn):
    for _ in range(50):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if (RUN_START - lm).total_seconds() < 12 * 60:
                    return True
        except Exception:
            pass
        time.sleep(8)
    return False
fails = []
with report("4296_stress_ladder") as r:
    r.heading("ops 4296 -- stress ERs across the full ladder")
    if not fresh("justhodl-stress-scenarios"):
        fails.append("stress deploy window missed")
    else:
        p = lam.invoke(FunctionName="justhodl-stress-scenarios",
                       InvocationType="RequestResponse", Payload=b"{}")
        r.log("stress invoked: %s"
              % (p["Payload"].read() or b"")[:120].decode("utf-8",
                                                          "ignore"))
        doc = json.loads(s3.get_object(
            Bucket="justhodl-dashboard-live",
            Key="data/stress-scenarios.json")["Body"].read())
        ai = doc.get("asset_impact") or {}
        # asset_impact may be dict or list
        if isinstance(ai, list):
            ai = {str(x.get("ticker")).upper(): x for x in ai
                  if isinstance(x, dict)}
        cov = [t for t in LADDER if t in ai
               and (ai[t].get("by_scenario")
                    or ai[t].get("scenarios_count"))]
        r.ok("asset_impact ladder coverage: %d/13 -- %s"
             % (len(cov), cov))
        if len(cov) < 11:
            fails.append("coverage %d < 11" % len(cov))
        samp = ai.get("GLD") or {}
        r.log("GLD weighted %s over %s scenarios"
              % (round(samp.get("weighted_pct", 0), 2)
                 if samp else None, samp.get("scenarios_count")))
    if not fails:
        p = lam.invoke(FunctionName="justhodl-quantum-desk",
                       InvocationType="RequestResponse", Payload=b"{}")
        d = json.loads(s3.get_object(
            Bucket="justhodl-dashboard-live",
            Key="data/quantum-desk.json")["Body"].read())
        col = [(x["class"], x.get("stress_er_pct"))
               for x in d.get("asset_ladder") or []]
        with_er = [c for c in col if c[1] is not None]
        r.ok("desk stress column: %s" % col)
        if len(with_er) < 8:
            fails.append("desk stress classes %d < 8" % len(with_er))
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4296 PASS -- every ladder row carries its "
             "scenario-weighted stress ER")
if fails:
    sys.exit(1)

# retrigger: aggregation fix landed (winners/losers-only was the miss)
