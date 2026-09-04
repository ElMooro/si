"""ops_5183 -- auction desk v1.0.2: announced rows out of the bank (today = the latest
auction WITH results), AI-note failures surfaced. Wait for the deploy, run, print
today's verdict + operations + the AI note (or its error)."""
import json
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-auction-desk"
CFG = Config(retries={"max_attempts": 4, "mode": "adaptive"}, read_timeout=330)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
FAILS = []

with report("ops_5183_auction_desk_v102") as R:
    R.heading("ops 5183 -- auction desk v1.0.2 verify")
    marker_ok = False
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=FN)
        lm = c.get("LastModified", "")
        if lm >= "2026-09-04T02:50" and c.get("LastUpdateStatus") in (None, "Successful"):
            marker_ok = True
            break
        time.sleep(15)
    R.log("   deployed at %s (fresh=%s)" % (c.get("LastModified"), marker_ok))
    if not marker_ok:
        R.warn("   deploy not observed yet -- running whatever is live")
    t0 = time.time()
    out = json.loads(lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")["Payload"].read() or b"{}")
    R.log("   run (%.0fs) -> %s" % (time.time() - t0, json.dumps(out)[:500]))
    D = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-desk.json")["Body"].read())
    T = D["today"]; v = T["verdict"]
    R.log("   version %s TODAY %s: %s" % (D.get("version"), T["date"], v["headline"]))
    R.log("   tags=%s liquidity=%s rates=%s risk=%s" % (v["tags"], v["liquidity"], v["rates"], v["risk_assets"]))
    for b in v["bullets"]:
        R.log("     - %s" % b[:240])
    for a in T["auctions"]:
        R.log("   auction %s %s grade=%s btc=%s z=%s tail=%s ind=%s pd=%s" % (a["term"], a["type"], a["grade"], a.get("btc"), {k: a["z"].get(k) for k in ("btc", "indirect", "pd", "tail")}, a.get("tail_bp"), a.get("indirect_pct"), a.get("pd_pct")))
    for b in T["buybacks"]:
        R.log("   buyback %s accepted=%s fill=%s cover=%s tags=%s" % (b["operation_date"], b.get("accepted"), b.get("fill_pct"), b.get("coverage"), b.get("tags")))
    R.log("   AI note: %s" % json.dumps(T.get("ai_note"))[:600])
    R.log("   notes: %s" % D.get("notes"))
    if T["date"] != "2026-09-03" and T["date"] > "2026-09-04":
        FAILS.append("today is %s (future-dated row still leaking)" % T["date"])
    if not any("RISK-ASSET BULLISH" in t for t in v["tags"]):
        FAILS.append("day verdict lacks RISK-ASSET BULLISH despite the max-fill buyback")
    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        sys.exit(1)
    R.ok("   GREEN")
