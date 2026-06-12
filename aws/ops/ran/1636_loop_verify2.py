# ops 1636 — loop integrity round 2: GetItem on known ids + fully paginated type counts
import json, os
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
ddb = boto3.resource("dynamodb", region_name="us-east-1", config=cfg)
T = ddb.Table("justhodl-signals")
out = {"ops": 1636}
known = ["ps_value_momentum#CNC#2026-06-12", "hp_score#SPNT#2026-06-12",
          "research_paper#LOB#2026-06-12", "research_paper#OPRA#2026-06-12"]
gets = {}
for sid in known:
    it = (T.get_item(Key={"signal_id": sid}).get("Item")) or None
    gets[sid] = ({"found": True, "status": it.get("status"),
                   "baseline": str(it.get("baseline_price")),
                   "conf": str(it.get("confidence")),
                   "windows": it.get("check_windows"),
                   "schema_ok": all(k in it for k in ("baseline_price", "check_windows",
                                      "measure_against", "horizon_days_primary"))}
                  if it else {"found": False})
out["get_items"] = gets
counts = {}
for st_ in ("ps_value_momentum", "hp_score", "research_paper", "insider_decline_cluster"):
    n, statuses, lek = 0, set(), None
    while True:
        kw = {"FilterExpression": Attr("signal_id").begins_with(st_ + "#"),
               "ProjectionExpression": "signal_id, #s",
               "ExpressionAttributeNames": {"#s": "status"}}
        if lek:
            kw["ExclusiveStartKey"] = lek
        r = T.scan(**kw)
        for i in r.get("Items") or []:
            n += 1
            statuses.add(i.get("status"))
        lek = r.get("LastEvaluatedKey")
        if not lek:
            break
    counts[st_] = {"count": n, "statuses": sorted(x for x in statuses if x)}
out["paginated_counts"] = counts
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1636_loop_verify2.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"found": {k.split("#")[0] + "#" + k.split("#")[1]: v.get("found")
                              for k, v in gets.items()},
                   "counts": {k: v["count"] for k, v in counts.items()}}, default=str))
