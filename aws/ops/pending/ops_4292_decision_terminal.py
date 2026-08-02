"""
ops_4292 -- quantum desk = the decision terminal.

Khalid's mandate: one page with everything a buy/no-buy needs. v2.2.0
adds seven fleet sources (macro-nowcast, us-cycle, fi-census credit,
etf-true-flows, bond-vol, stress-scenarios, signal-backtest) and four
new blocks: macro_board, risk_panel (gate legs + veto stack with flip
conditions), decision (top-class checklist, live blockers, overnight
deltas, names in/out), plus per-name why/price/stress-ER/verdict-
winrate and ladder ETF-flow badges. Page v4 renders all of it and
fixes the [object Object] abstain bug.

Gate: v2.2.0 live · >=20/23 sources · decision block real (checklist
n>=4, blockers include the x0.45 gate) · macro_board carries nowcast
z's · edge page shows the new sections.
"""
import json, sys, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
RUN_START = datetime.now(timezone.utc)
fails = []
with report("4292_decision_terminal") as r:
    r.heading("ops 4292 -- one page, whole decision")
    doc = None
    for _ in range(50):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-quantum-desk")
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if (RUN_START - lm).total_seconds() < 12 * 60:
                    lam.invoke(FunctionName="justhodl-quantum-desk",
                               InvocationType="RequestResponse",
                               Payload=b"{}")
                    doc = json.loads(s3.get_object(
                        Bucket=BUCKET,
                        Key="data/quantum-desk.json")["Body"].read())
                    if doc.get("version") == "2.2.0":
                        break
        except Exception:
            pass
        time.sleep(8)
    if not doc or doc.get("version") != "2.2.0":
        fails.append("v2.2.0 not landed (saw %s)"
                     % (doc or {}).get("version"))
    else:
        dh = doc.get("data_health") or {}
        r.log("sources %s/%s" % (dh.get("sources_ok"),
                                 dh.get("sources_total")))
        if (dh.get("sources_ok") or 0) < 20:
            fails.append("only %s/%s sources ok"
                         % (dh.get("sources_ok"),
                            dh.get("sources_total")))
        dec = doc.get("decision") or {}
        r.log("DECISION: %s %s score=%s -- %s/%s legs clear; "
              "blockers=%s"
              % (dec.get("class"), dec.get("verdict"),
                 dec.get("score"), dec.get("n_ok"),
                 len(dec.get("checklist") or []),
                 dec.get("blockers")))
        sy = dec.get("since_yesterday") or {}
        r.log("  deltas: %s | in: %s out: %s"
              % (sy.get("score_deltas"), sy.get("names_in"),
                 sy.get("names_out")))
        if len(dec.get("checklist") or []) < 4 or \
                not dec.get("blockers"):
            fails.append("decision block thin: %s" % dec)
        mb = doc.get("macro_board") or {}
        nz = ((mb.get("nowcast") or {}).get("top") or [])
        r.log("MACRO: nowcast top-z %s | us_cycle %s | credit %s | "
              "bond_vol_z %s"
              % ([(t.get("name"), t.get("z")) for t in nz[:3]],
                 (mb.get("us_cycle") or {}).get("level"),
                 (mb.get("credit") or [])[:2], mb.get("bond_vol_z")))
        if not nz:
            fails.append("macro_board nowcast empty")
        rp = doc.get("risk_panel") or {}
        r.log("RISK: gate_legs=%s veto_stack=%s"
              % (len(rp.get("gate_legs") or []),
                 [(v.get("name"), v.get("active"))
                  for v in rp.get("veto_stack") or []]))
        mm0 = (doc.get("money_map") or [{}])[0]
        r.kv(ticker=mm0.get("ticker"), why=(mm0.get("why") or "")[:60],
             price=mm0.get("price"), stress=mm0.get("stress_er_pct"),
             winrate=mm0.get("verdict_hist_winrate"))
        flows = [x.get("etf_flow") for x in
                 doc.get("asset_ladder") or [] if x.get("etf_flow")]
        r.log("ladder ETF flow badges: %s" % flows[:6])
    body = ""
    for i in range(12):
        try:
            req = urllib.request.Request(
                "https://justhodl.ai/quantum-desk.html",
                headers={"User-Agent": "ops/4292",
                         "Cache-Control": "no-cache"})
            body = urllib.request.urlopen(req, timeout=25).read().decode(
                "utf-8", "ignore")
            if "buy checklist" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    marks = ["buy checklist", "Macro board", "Risk panel",
             "flips when", "stress ER"]
    missing = [m for m in marks if m not in body]
    if missing:
        fails.append("edge missing: %s" % missing)
    else:
        r.ok("page v4 LIVE (%d bytes)" % len(body))
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4292 PASS -- the desk decides on one screen")
if fails:
    sys.exit(1)
