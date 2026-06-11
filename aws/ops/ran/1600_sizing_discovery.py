# ops 1600 — discovery for the Kelly/Sizing engine: signals grading schema,
# portfolio book schema, skill aggregator outputs. Read-only.
import json, boto3
from boto3.dynamodb.conditions import Attr
from botocore.config import Config
cfg = Config(read_timeout=600, retries={"max_attempts": 2})
ddb = boto3.resource("dynamodb", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1600}

# A) signals table — full scan, minimal projection, aggregate by type/status
t = ddb.Table("justhodl-signals")
items, lek = [], None
while True:
    kw = {"ProjectionExpression": "#s, signal_type, horizon_days_primary",
          "ExpressionAttributeNames": {"#s": "status"}}
    if lek:
        kw["ExclusiveStartKey"] = lek
    r = t.scan(**kw)
    items.extend(r.get("Items", []))
    lek = r.get("LastEvaluatedKey")
    if not lek:
        break
by_type, by_status = {}, {}
for it in items:
    st = it.get("status", "?"); ty = it.get("signal_type", "?")
    by_status[st] = by_status.get(st, 0) + 1
    d = by_type.setdefault(ty, {"n": 0, "resolved": 0, "pending": 0})
    d["n"] += 1
    if st == "pending":
        d["pending"] += 1
    elif st in ("resolved", "complete", "completed", "graded", "scored"):
        d["resolved"] += 1
out["signals_total"] = len(items)
out["by_status"] = by_status
out["by_type"] = dict(sorted(by_type.items(), key=lambda x: -x[1]["n"])[:40])

# sample one fully-resolved item (full attrs) + one pending
res_status = [s_ for s_ in by_status if s_ != "pending"]
samples = {}
for want in (res_status[:2] + ["pending"]):
    r = t.scan(FilterExpression=Attr("status").eq(want), Limit=60)
    its = r.get("Items", [])
    # prefer an item with non-empty outcomes
    pick = next((x for x in its if x.get("outcomes")), its[0] if its else None)
    if pick:
        samples[want] = json.loads(json.dumps(pick, default=str))
out["samples"] = {k: {kk: (vv if not isinstance(vv, (dict, list)) or kk in
                            ("outcomes", "accuracy_scores", "check_windows", "metadata")
                            else str(vv)[:80])
                       for kk, vv in v.items()} for k, v in samples.items()}

# B) portfolio book
try:
    pt = ddb.Table("justhodl-portfolio")
    pr = pt.scan(Limit=60)
    pits = json.loads(json.dumps(pr.get("Items", []), default=str))
    out["portfolio_count"] = pr.get("Count")
    out["portfolio_items"] = pits[:25]
except Exception as e:
    out["portfolio_err"] = str(e)[:140]

# C) skill aggregator outputs on S3
ks = []
for pref in ("data/skill", "data/_skill", "data/self-improvement", "data/calibration"):
    try:
        r = s3.list_objects_v2(Bucket=B, Prefix=pref)
        ks += [o["Key"] for o in r.get("Contents", [])]
    except Exception:
        pass
out["skill_keys"] = ks
out["skill_heads"] = {}
for k in ks[:6]:
    try:
        body = s3.get_object(Bucket=B, Key=k)["Body"].read()
        out["skill_heads"][k] = body[:700].decode(errors="replace")
    except Exception as e:
        out["skill_heads"][k] = "ERR " + str(e)[:80]

open("aws/ops/reports/1600_sizing_discovery.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"total": out["signals_total"], "statuses": by_status,
                   "types_n": len(by_type), "portfolio": out.get("portfolio_count"),
                   "skill_keys": len(ks)}, default=str))
