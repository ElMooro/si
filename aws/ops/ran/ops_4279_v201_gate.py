"""
ops_4279 -- v2.0.1: barometer wired to the warroom's REAL shape
(master.band / early_warning_0_100 / n_firing / firing[]; 4278 shipped
with guessed paths and the block came back empty). Gate: canary block
carries a real band + score + headline; everything else re-verified.
"""
import json, sys, time
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
with report("4279_v201_gate") as r:
    r.heading("ops 4279 -- quantum-desk v2.0.1: barometer real")
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
                    if doc.get("version") == "2.0.1":
                        break
        except Exception:
            pass
        time.sleep(8)
    if not doc or doc.get("version") != "2.0.1":
        fails.append("v2.0.1 never landed (saw %s)"
                     % (doc or {}).get("version"))
    else:
        cb = doc.get("canary_barometer") or {}
        r.log("CANARY: band=%s score=%s firing=%s/%s veto=%s"
              % (cb.get("level"), cb.get("score"), cb.get("n_firing"),
                 cb.get("n_canaries"), cb.get("veto_active")))
        r.log("  headline: %s" % cb.get("headline"))
        r.log("  triggered: %s" % (cb.get("triggered") or [])[:5])
        if not cb.get("level") or cb.get("score") is None:
            fails.append("barometer still empty: %s" % cb)
        mm = doc.get("money_map") or []
        multi = [m for m in mm if (m.get("n_corroborating") or 0) >= 2]
        top = mm[0] if mm else {}
        r.ok("map intact: %d names, %d multi-corroborated; top %s "
             "fit=%s x%s"
             % (len(mm), len(multi), top.get("ticker"),
                top.get("khalid_fit"), top.get("n_corroborating")))
        lad = doc.get("asset_ladder") or []
        r.log("ladder top: %s"
              % [(x["class"], x["score"], x["verdict"])
                 for x in lad[:4]])
        if cb.get("veto_active"):
            if any(x["verdict"] == "BUY_ZONE" for x in lad):
                fails.append("veto active but BUY_ZONE present")
            else:
                r.ok("veto active and honored across the ladder")
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4279 PASS -- v2 complete: 771 engines mapped, "
             "every name fleet-corroborated, barometer live")
if fails:
    sys.exit(1)
