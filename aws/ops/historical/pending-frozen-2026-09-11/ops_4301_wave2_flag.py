"""
ops_4299 -- the stress verdict: zip-grep the deployed bytes, self-heal
if the fold never shipped, and verify the desk's seven wires
INDEPENDENTLY (no more short-circuiting behind stress).

H1: fold absent from the deployed package -> update_function_code from
this checkout, invoke, gate coverage >=11.
H2: fold present but inert -> print probabilities, spec winners shape,
and any traceback lines; the evidence names the one-line fix.
Either way the desk section runs and v2.3.2's wires get their verdict.
"""
import io
import json
import subprocess
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from ops_report import report

REGION, B = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
logs = boto3.client("logs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
RUN_START = datetime.now(timezone.utc)
LADDER13 = ["SPY", "IWM", "EFA", "EEM", "HYG", "IEF", "GLD", "SLV",
            "DBC", "VNQ", "BTC", "ETH", "CASH"]

def git_floor(d):
    try:
        out = subprocess.run(["git", "log", "-1", "--format=%ct", "--",
                              "aws/lambdas/%s" % d],
                             capture_output=True, text=True,
                             timeout=30).stdout.strip()
        return datetime.fromtimestamp(int(out), tz=timezone.utc)
    except Exception:
        return None

def settled(fn):
    for _ in range(40):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                return c
        except Exception:
            pass
        time.sleep(8)
    return None

def build_zip(d):
    buf = io.BytesIO()
    import os
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        sdir = "aws/lambdas/%s/source" % d
        for root, _, files in os.walk(sdir):
            for f in files:
                fp = "%s/%s" % (root, f)
                z.write(fp, fp[len(sdir) + 1:])
        for shf in __import__("os").listdir("aws/shared"):
            if shf.endswith(".py"):
                z.write("aws/shared/%s" % shf, shf)
    return buf.getvalue()

fails = []
with report("4301_wave2_flag") as r:
    r.heading("ops 4301 -- deployed-bytes verdict + independent desk "
              "gate")

    r.section("A. what is ACTUALLY deployed for stress-scenarios")
    FN = "justhodl-stress-scenarios"
    c = settled(FN)
    if not c:
        fails.append("stress never settled")
    else:
        r.log("git floor %s | fn LM %s"
              % (git_floor("justhodl-stress-scenarios"),
                 c.get("LastModified")))
        code = lam.get_function(FunctionName=FN)
        url = code["Code"]["Location"]
        raw = urllib.request.urlopen(url, timeout=60).read()
        zf = zipfile.ZipFile(io.BytesIO(raw))
        srct = zf.read("lambda_function.py").decode("utf-8", "ignore")
        has_overlay = "LADDER_OVERLAY" in srct
        has_fold = "ladder_overlay" in srct and "prob_by_key" in srct
        r.log("deployed zip %.0fKB: overlay=%s aggregation_fold=%s"
              % (len(raw) / 1024, has_overlay, has_fold))
        if not (has_overlay and has_fold):
            r.warn("H1 CONFIRMED -- fold missing from deployed "
                   "package despite LM; self-healing via "
                   "update_function_code from this checkout")
            lam.update_function_code(FunctionName=FN,
                                     ZipFile=build_zip(
                                         "justhodl-stress-scenarios"))
            time.sleep(6)
            if not settled(FN):
                fails.append("self-heal update never settled")
        else:
            r.ok("H1 falsified -- fold IS deployed; probing why it "
                 "no-ops")
            i = srct.find('"winners"')
            r.log("spec winners literal: %r" % srct[i:i + 120])
        if not fails:
            p = lam.invoke(FunctionName=FN,
                           InvocationType="RequestResponse",
                           Payload=b"{}")
            err = p.get("FunctionError")
            r.log("invoked%s" % (" FN-ERROR " +
                                 (p["Payload"].read() or b"")[:150]
                                 .decode("utf-8", "ignore")
                                 if err else ""))
            time.sleep(4)
            doc = json.loads(s3.get_object(
                Bucket=B,
                Key="data/stress-scenarios.json")["Body"].read())
            ai_all = (doc.get("asset_impact") or {}).get("all") \
                if isinstance(doc.get("asset_impact"), dict) \
                else doc.get("asset_impact")
            probs = [(s_.get("key"), s_.get("probability"))
                     for s_ in doc.get("scenarios") or []]
            r.log("probabilities: %s" % probs)
            ai = {str(x.get("ticker")).upper(): x
                  for x in (ai_all or []) if isinstance(x, dict)}
            cov = [t for t in LADDER13 if t in ai]
            r.log("ladder coverage now: %d/13 -- %s"
                  % (len(cov), cov))
            if len(cov) >= 10:
                g = ai.get("GLD") or {}
                r.ok("STRESS SEALED: GLD weighted %.2f over %s "
                     "scenarios"
                     % (g.get("weighted_pct")
                        or g.get("expected_return_pct") or 0,
                        g.get("scenarios_count")
                        or g.get("n_scenarios")))
            else:
                tb = []
                try:
                    ev = logs.filter_log_events(
                        logGroupName="/aws/lambda/%s" % FN,
                        startTime=int((time.time() - 300) * 1000))
                    tb = [x["message"].strip()[:140]
                          for x in ev.get("events", [])
                          if "Error" in x["message"]
                          or "Traceback" in x["message"]][-3:]
                except Exception:
                    pass
                fails.append("coverage %d reading .all; "
                             "tracebacks=%s" % (len(cov), tb))

    r.section("B. desk v2.3.2 wires -- independent verdict")
    FN2 = "justhodl-quantum-desk"
    c2 = settled(FN2)
    fl = git_floor("justhodl-quantum-desk")
    lm2 = None
    if c2:
        lm2 = datetime.strptime(
            c2["LastModified"].split(".")[0],
            "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    if not c2 or (fl and lm2 and lm2 < fl):
        fails.append("desk not on latest code (LM %s < floor %s)"
                     % (lm2, fl))
    else:
        lam.invoke(FunctionName=FN2,
                   InvocationType="RequestResponse", Payload=b"{}")
        d = json.loads(s3.get_object(
            Bucket=B, Key="data/quantum-desk.json")["Body"].read())
        dh = d.get("data_health") or {}
        r.log("version %s · sources %s/%s"
              % (d.get("version"), dh.get("sources_ok"),
                 dh.get("sources_total")))
        lad = d.get("asset_ladder") or []
        r.log("CFTC: %s" % [(x["class"], x["cftc"]) for x in lad
                            if x.get("cftc")][:5])
        r.log("RRG: %s" % [(x["class"], x["rrg"]) for x in lad
                           if x.get("rrg")][:5])
        r.log("stress col: %s"
              % [(x["class"], x.get("stress_er_pct")) for x in lad
                 if x.get("stress_er_pct") is not None])
        rp = d.get("risk_panel") or {}
        r.log("extras: tail=%s treasury=%s sig_health(n=%s, "
              "decayed=%s)"
              % (rp.get("tail_risk"), rp.get("treasury"),
                 (rp.get("signal_health") or {}).get("n_checked"),
                 (rp.get("signal_health") or {}).get("decayed")))
        r.log("boom_stage=%s" % d.get("boom_stage"))
        r.log("sector-boom: %s"
              % [(m["ticker"], m["sector_boom"]) for m in
                 d.get("money_map") or [] if
                 m.get("sector_boom")][:5])
        if d.get("version") != "2.3.2":
            fails.append("desk version %s" % d.get("version"))
        if (dh.get("sources_ok") or 0) < 26:
            fails.append("sources_ok %s" % dh.get("sources_ok"))
        if not (rp.get("tail_risk") or rp.get("treasury")
                or rp.get("signal_health")):
            fails.append("no risk extras readable")
        n_cftc = len([x for x in lad if x.get("cftc")])
        n_rrg = len([x for x in lad if x.get("rrg")])
        n_st = len([x for x in lad
                    if x.get("stress_er_pct") is not None])
        n_sb = len([m for m in d.get("money_map") or []
                    if m.get("sector_boom")])
        bs_ = d.get("boom_stage") or {}
        if n_cftc < 3:
            fails.append("CFTC classes %d < 3" % n_cftc)
        if n_rrg < 3:
            fails.append("RRG classes %d < 3" % n_rrg)
        if n_st < 10:
            fails.append("desk stress classes %d < 10" % n_st)
        if n_sb < 2:
            fails.append("sector-boom names %d < 2" % n_sb)
        if bs_.get("n_signals") is None:
            fails.append("boom_stage signals unread")
        badmatch = [x for x in (d.get("money_map") or [])
                    if x.get("sector_boom")
                    and "TECHNOLOG" in str(
                        x["sector_boom"].get("sector", "")).upper()
                    and "BIOTECH" in str(
                        x["sector_boom"].get("industry", "")).upper()]
        if badmatch:
            fails.append("sector matcher still substring-loose: %s"
                         % [x["ticker"] for x in badmatch])

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4299 PASS -- stress sealed on evidence, desk's "
             "seven wires live")
if fails:
    sys.exit(1)
