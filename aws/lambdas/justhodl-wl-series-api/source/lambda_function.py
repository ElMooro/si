"""justhodl-wl-series-api v1.0 — ops 3273.

Serves any watchlist tile's weekly series straight from the fleet's
own cache (data/thesis-state-v2.json.gz) so chart-pro can chart EVERY
symbol in Khalid's TV watchlists — including derived/transform series
(FRED~A~div~FRED~B, INTERNALS) that no external charting source knows.

GET ?sym=<TV symbol or mapped id>  →  {sym, n, points:[[iso, val]...]}
CORS: * (page origin is justhodl.ai; this is a Function URL).
Container-caches the gz state; refreshes when older than 6h.
"""
import gzip
import json
import time

import boto3

BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name="us-east-1")
_STATE = {"weekly": None, "at": 0.0}
HDRS = {"Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "public, max-age=900"}


def weekly():
    if _STATE["weekly"] is None or time.time() - _STATE["at"] > 21600:
        raw = S3.get_object(Bucket=BUCKET,
                            Key="data/thesis-state-v2.json.gz")
        st = json.loads(gzip.decompress(raw["Body"].read()))
        _STATE["weekly"] = st.get("weekly") or {}
        _STATE["at"] = time.time()
        print(f"[api] cache loaded: {len(_STATE['weekly'])} series")
    return _STATE["weekly"]


def lambda_handler(event, context):
    qs = (event.get("queryStringParameters") or {})
    if qs.get("diag"):                      # ops 3276: client telemetry
        ua = ((event.get("headers") or {}).get("user-agent")
              or "")[:160]
        print(f"[diag] {json.dumps(qs, sort_keys=True)[:600]} "
              f"ua={ua}")
        return {"statusCode": 200, "headers": HDRS,
                "body": json.dumps({"ok": True})}
    sym = (qs.get("sym") or "").strip()
    if not sym:
        return {"statusCode": 400, "headers": HDRS,
                "body": json.dumps({"err": "sym required"})}
    w = weekly()
    ser = w.get(sym) or w.get(sym.upper()) or {}
    if not ser:
        return {"statusCode": 404, "headers": HDRS,
                "body": json.dumps({"err": "unknown sym", "sym": sym,
                                    "n_series": len(w)})}
    pts = sorted((k, ser[k]) for k in ser)
    return {"statusCode": 200, "headers": HDRS,
            "body": json.dumps({"sym": sym, "n": len(pts),
                                "points": pts})}
