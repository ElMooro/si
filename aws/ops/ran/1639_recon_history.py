# ops 1639 — recon: rings depth/dates, SPX deep history, signals table stats
import json, gzip, os
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.config import Config
cfg = Config(read_timeout=600, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ddb = boto3.resource("dynamodb", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1639}
up = json.loads(gzip.decompress(s3.get_object(Bucket=B, Key="data/_upside/state.json.gz")["Body"].read()))
out["upside_keys"] = sorted(up.keys())[:15]
rings = up.get("rings") or {}
spy = rings.get("SPY") or []
out["rings"] = {"n_tickers": len(rings), "spy_len": len(spy),
                 "sample_lens": {t: len(rings[t]) for t in list(rings)[:5]},
                 "has_dates_key": "dates" in up or "ring_dates" in up}
try:
    sx = json.loads(s3.get_object(Bucket=B, Key="data/spx-history-deep.json")["Body"].read())
    out["spx_deep"] = {"type": type(sx).__name__,
                        "keys": (sorted(sx.keys())[:8] if isinstance(sx, dict) else None),
                        "n": (len(sx.get("closes") or sx.get("history") or sx)
                               if isinstance(sx, (dict, list)) else None)}
except Exception as e:
    out["spx_deep"] = {"err": str(e)[:60]}
T = ddb.Table("justhodl-signals")
n, types, lek = 0, {}, None
while True:
    kw = {"ProjectionExpression": "signal_id, #s", "ExpressionAttributeNames": {"#s": "status"}}
    if lek:
        kw["ExclusiveStartKey"] = lek
    r = T.scan(**kw)
    for i in r.get("Items") or []:
        n += 1
        ty = (i.get("signal_id") or "#").split("#")[0]
        types[ty] = types.get(ty, 0) + 1
    lek = r.get("LastEvaluatedKey")
    if not lek:
        break
out["signals"] = {"total": n, "n_types": len(types),
                   "top_types": dict(sorted(types.items(), key=lambda x: -x[1])[:14])}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1639_recon_history.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"rings": out["rings"]["n_tickers"], "spy_len": out["rings"]["spy_len"],
                   "signals": n, "types": len(types)}))
