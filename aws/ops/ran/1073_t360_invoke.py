"""
Ops 1073: invoke ticker-360 producer once (controlled).

The hub is verified (20 domains, 14 available). This performs one
controlled invoke of justhodl-ticker-360 to generate data/ticker-360.json
and reports universe size + indexed tickers.

Writes: aws/ops/reports/1073_t360_invoke.json. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1073_t360_invoke.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
FN = "justhodl-ticker-360"

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    """Invoke producer, check artifact, write report."""
    rep = {"script": "1073_t360_invoke",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps({}).encode())
        payload = r.get("Payload")
        body = json.loads(payload.read().decode()) if payload else {}
        rep["invoke"] = {"status_code": r.get("StatusCode"),
                         "function_error": r.get("FunctionError"),
                         "result": {k: body.get(k) for k in
                                    ("ok", "universe", "indexed", "bytes")}
                         if isinstance(body, dict) else str(body)[:200]}
    except Exception as e:  # noqa: BLE001
        rep["invoke"] = {"error": str(e)[:150]}
    time.sleep(3)
    try:
        h = s3.head_object(Bucket=BUCKET, Key="data/ticker-360.json")
        rep["artifact"] = {"exists": True, "size": h.get("ContentLength")}
        obj = s3.get_object(Bucket=BUCKET, Key="data/ticker-360.json")
        data = json.loads(obj["Body"].read().decode())
        rep["artifact"]["generated_at"] = data.get("generated_at")
        rep["artifact"]["universe_size"] = data.get("universe_size")
        rep["artifact"]["indexed_tickers"] = data.get("indexed_tickers")
        tk = data.get("tickers") or {}
        rep["sample_tickers"] = sorted(tk.keys())[:10]
    except ClientError:
        rep["artifact"] = {"exists": False}
    except Exception as e:  # noqa: BLE001
        rep["artifact"] = {"exists": True, "parse_error": str(e)[:100]}
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2)[:2500])


if __name__ == "__main__":
    main()
