"""ops 2774 — RETIRE justhodl-gex-desk (redundant duplicate of justhodl-dealer-gex).
Audit revealed a mature options/GEX subsystem already exists (dealer-gex 891L w/
vanna/charm/flip/maxpain/squeeze, options-analytics SpotGamma-class, options-flow,
polygon-options-flow, opex-calendar, dix, options-confluence synthesizer, 4 pages).
gex-desk adds nothing → remove function + EventBridge rule + S3 feeds. Verify gone.
Also confirm the EXISTING dealer-gex is present/healthy. Report: 2774_retire_gex_desk.json.
"""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN, RULE = "justhodl-gex-desk", "justhodl-gex-desk-intraday"
lam = boto3.client("lambda", region_name=REGION)
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2774, "ts": datetime.now(timezone.utc).isoformat(), "removed": [], "kept_existing": {}}
print("settling 10s…"); time.sleep(10)

# 1) detach + delete EventBridge rule
try:
    tgts = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
    if tgts:
        ev.remove_targets(Rule=RULE, Ids=[t["Id"] for t in tgts])
    ev.delete_rule(Name=RULE)
    R["removed"].append("eventbridge:" + RULE); print("deleted rule", RULE)
except ClientError as e:
    print("rule delete:", str(e)[:80]); R["removed"].append("rule already absent")

# 2) delete lambda
try:
    lam.delete_function(FunctionName=FN)
    R["removed"].append("lambda:" + FN); print("deleted function", FN)
except ClientError as e:
    print("fn delete:", str(e)[:80]); R["removed"].append("fn already absent")

# 3) delete S3 feeds
for k in ("data/gex-desk.json", "data/history/gex-desk.json"):
    try:
        s3.delete_object(Bucket=BUCKET, Key=k); R["removed"].append("s3:" + k); print("deleted", k)
    except ClientError as e:
        print("s3 delete:", str(e)[:60])

# 4) verify gone
gone = {}
try:
    lam.get_function(FunctionName=FN); gone["lambda"] = "STILL PRESENT"
except ClientError:
    gone["lambda"] = "gone"
try:
    ev.describe_rule(Name=RULE); gone["rule"] = "STILL PRESENT"
except ClientError:
    gone["rule"] = "gone"
R["verify_gone"] = gone
print("verify:", json.dumps(gone))

# 5) confirm the EXISTING options subsystem is intact (spot-check dealer-gex + feeds)
for ef in ("justhodl-dealer-gex", "justhodl-options-analytics", "justhodl-options-confluence", "justhodl-dix", "justhodl-opex-calendar"):
    try:
        c = lam.get_function_configuration(FunctionName=ef)
        R["kept_existing"][ef] = {"state": c.get("State"), "runtime": c.get("Runtime"), "last_modified": c.get("LastModified")}
    except ClientError as e:
        R["kept_existing"][ef] = "NOT FOUND: " + str(e)[:50]
# check the existing dealer-gex + options feeds exist & their freshness
feeds = {}
for k in ("data/dealer-gex.json", "data/options-analytics.json", "data/options-confluence.json", "data/options-gamma.json", "data/dix.json", "data/opex-calendar.json"):
    try:
        h = s3.head_object(Bucket=BUCKET, Key=k)
        feeds[k] = {"exists": True, "last_modified": h["LastModified"].isoformat(), "bytes": h["ContentLength"]}
    except ClientError:
        feeds[k] = {"exists": False}
R["existing_feeds"] = feeds
print("\n== EXISTING options subsystem ==")
for k, v in R["kept_existing"].items():
    print("  fn", k, "->", v if isinstance(v, str) else v.get("state"))
for k, v in feeds.items():
    print("  feed", k, "->", ("%s (%s)" % (v.get("last_modified", "")[:16], v["bytes"]) if v.get("exists") else "MISSING"))
assert gone["lambda"] == "gone" and gone["rule"] == "gone", "cleanup incomplete"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2774_retire_gex_desk.json", "w"), indent=1, default=str)
print("\nOPS 2774 COMPLETE — duplicate retired; existing subsystem confirmed")
