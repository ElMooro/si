"""ops 2777 — dump real JSON shapes of the 7 options feeds to build the hub page
against actual fields (no guessing). Read-only. Report: 2777_feed_shapes.json.
"""
import os, json
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
R = {"ops": 2777, "ts": datetime.now(timezone.utc).isoformat(), "feeds": {}}
FEEDS = ["dealer-gex", "options-gamma", "options-confluence", "dix", "opex-calendar",
         "polygon-options-flow", "options-flow"]
def shape(v, depth=0):
    if isinstance(v, dict):
        return {k: shape(v[k], depth + 1) for k in list(v)[:14]} if depth < 1 else ("dict[%d]:%s" % (len(v), list(v)[:8]))
    if isinstance(v, list):
        return ["list[%d]" % len(v)] + ([shape(v[0], depth + 1)] if v else [])
    if isinstance(v, str):
        return "str:" + v[:32]
    return type(v).__name__
for f in FEEDS:
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/%s.json" % f)["Body"].read())
        info = {"top_keys": list(d)[:16] if isinstance(d, dict) else "list[%d]" % len(d)}
        # find the primary array and show its first item's keys
        if isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    info["array_%s_item_keys" % k] = list(v[0])[:16]
                    info["array_%s_sample" % k] = {kk: (str(v[0][kk])[:24]) for kk in list(v[0])[:10]}
                    break
            for k, v in d.items():
                if isinstance(v, dict) and v and all(isinstance(x, dict) for x in list(v.values())[:2]):
                    sub = list(v.values())[0]
                    info["map_%s_item_keys" % k] = list(sub)[:14]
                    break
            info["scalars"] = {k: (str(v)[:40]) for k, v in d.items() if isinstance(v, (int, float, str, bool))}
        R["feeds"][f] = info
        print("── %s ── top:%s" % (f, info.get("top_keys")))
        for kk in info:
            if kk.startswith("array_") or kk.startswith("map_"):
                print("     %s: %s" % (kk, info[kk]))
    except ClientError:
        R["feeds"][f] = {"MISSING": True}; print("── %s MISSING" % f)
    except Exception as e:
        R["feeds"][f] = {"err": str(e)[:80]}; print("── %s err %s" % (f, str(e)[:60]))
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2777_feed_shapes.json", "w"), indent=1, default=str)
print("OPS 2777 COMPLETE")
