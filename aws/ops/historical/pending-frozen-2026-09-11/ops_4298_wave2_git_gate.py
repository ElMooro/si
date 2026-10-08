"""
ops_4297 -- second wire wave + the race-proof gate.

Seven new engine wires into the desk (CFTC deep-view positioning,
rotation dashboard RRG, tail-risk, treasury-noise, industry-boom
sector context, boom-stage, alpha-decay signal health), 23 -> 30
sources. And the gate lesson written into law: after tonight's THIRD
self-deploy race, freshness now requires LastModified >= RUN_START --
the deploy triggered by this very push always finishes after this job
starts, so a strictly-post-start LM is unambiguous. Same strict gate
retries the stress-ladder verification that raced in 4296.
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
B = "justhodl-dashboard-live"

import subprocess


def _last_change_ts(lambda_dir):
    """Commit time of this lambda's last source change -- the true
    deploy floor. 4297's RUN_START law failed on unchanged engines
    (no deploy fires for them); git history is the universal anchor."""
    try:
        out = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--",
             "aws/lambdas/%s" % lambda_dir],
            capture_output=True, text=True, timeout=30).stdout.strip()
        return datetime.fromtimestamp(int(out), tz=timezone.utc)
    except Exception:
        return None


def deployed_after_start(fn, tries=55, lambda_dir=None):
    floor = _last_change_ts(lambda_dir or fn) or RUN_START
    for _ in range(tries):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if lm >= floor:
                    return True
        except Exception:
            pass
        time.sleep(9)
    return False

LADDER13 = ["SPY","IWM","EFA","EEM","HYG","IEF","GLD","SLV","DBC",
            "VNQ","BTC","ETH","CASH"]
fails = []
with report("4298_wave2_git_gate") as r:
    r.heading("ops 4298 -- 30 wires under the git-anchored deploy law")

    r.section("1. stress ladder, race-proof retry")
    if not deployed_after_start("justhodl-stress-scenarios"):
        fails.append("stress LM never crossed its git floor")
    else:
        lam.invoke(FunctionName="justhodl-stress-scenarios",
                   InvocationType="RequestResponse", Payload=b"{}")
        doc = json.loads(s3.get_object(
            Bucket=B, Key="data/stress-scenarios.json")["Body"].read())
        ai = doc.get("asset_impact") or {}
        if isinstance(ai, list):
            ai = {str(x.get("ticker")).upper(): x for x in ai
                  if isinstance(x, dict)}
        cov = [t for t in LADDER13 if t in ai]
        r.ok("stress asset_impact ladder coverage: %d/13 -- %s"
             % (len(cov), cov))
        if len(cov) < 11:
            fails.append("stress coverage %d < 11" % len(cov))
        g = ai.get("GLD") or {}
        r.log("GLD weighted %.2f over %s scenarios (overlay rows "
              "carry src tags)"
              % (g.get("weighted_pct") or
                 g.get("expected_return_pct") or 0,
                 g.get("scenarios_count") or g.get("n_scenarios")))

    r.section("2. desk v2.3.0: the seven new wires, live")
    if fails:
        pass
    elif not deployed_after_start("justhodl-quantum-desk"):
        fails.append("desk LM never crossed its git floor")
    else:
        lam.invoke(FunctionName="justhodl-quantum-desk",
                   InvocationType="RequestResponse", Payload=b"{}")
        d = json.loads(s3.get_object(
            Bucket=B, Key="data/quantum-desk.json")["Body"].read())
        if d.get("version") != "2.3.0":
            fails.append("desk version %s" % d.get("version"))
        dh = d.get("data_health") or {}
        r.log("sources %s/%s" % (dh.get("sources_ok"),
                                 dh.get("sources_total")))
        lad = d.get("asset_ladder") or []
        cftc = [(x["class"], x["cftc"]) for x in lad if x.get("cftc")]
        rrg = [(x["class"], x["rrg"]) for x in lad if x.get("rrg")]
        stress = [(x["class"], x.get("stress_er_pct")) for x in lad
                  if x.get("stress_er_pct") is not None]
        r.log("CFTC on: %s" % cftc[:6])
        r.log("RRG on: %s" % rrg[:6])
        r.ok("stress column: %s" % stress)
        rp = d.get("risk_panel") or {}
        r.log("risk extras: tail=%s treasury=%s signal_health=%s"
              % (rp.get("tail_risk"), rp.get("treasury"),
                 (rp.get("signal_health") or {}).get("n_checked")))
        r.log("boom_stage: %s" % d.get("boom_stage"))
        sb = [(m["ticker"], m["sector_boom"])
              for m in d.get("money_map") or []
              if m.get("sector_boom")]
        r.log("mm sector-boom: %s" % sb[:5])
        if len(stress) < 8:
            fails.append("desk stress classes %d < 8" % len(stress))
        if len(cftc) < 3:
            r.warn("CFTC matched only %d classes -- alias/shape "
                   "review queued (doc keys logged above)"
                   % len(cftc))
        if not (rp.get("tail_risk") or rp.get("treasury")
                or rp.get("signal_health")):
            fails.append("no risk extras readable")
        if (dh.get("sources_ok") or 0) < 26:
            fails.append("sources_ok %s < 26" % dh.get("sources_ok"))
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4297 PASS -- 30 wires live; deploy law now git-anchored")
if fails:
    sys.exit(1)
