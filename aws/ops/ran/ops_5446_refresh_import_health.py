"""ops_5446 -- invoke import-sentinel so dead-lanes reflects the 5445 write."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
KEY = "data/import-health.json"
FN = "justhodl-import-sentinel"


def main():
    with report("ops_5446_refresh_import_health") as R:
        R.heading("ops 5446 import-health refresh")
        lam = boto3.client("lambda", region_name="us-east-1")
        s3 = boto3.client("s3", region_name="us-east-1")
        inv = lam.invoke(
            FunctionName=FN,
            InvocationType="RequestResponse",
            Payload=json.dumps({"kicked_by": "ops_5446"}).encode("utf-8"),
        )
        body = json.loads(inv["Payload"].read() or b"{}")
        R.ok("sentinel invoke http=%s fn_error=%s" % (
            inv.get("StatusCode"), inv.get("FunctionError")))
        if inv.get("FunctionError"):
            R.warn(str(body)[:300])
        time.sleep(2)
        obj = s3.get_object(Bucket=B, Key=KEY)
        h = json.loads(obj["Body"].read())
        R.ok("overall=%s worst=%s generated=%s" % (
            h.get("overall"), h.get("worst"), h.get("generated_at")))
        for p in h.get("pipelines") or []:
            if p.get("name") in ("fred", "dead-lanes", "provider-catalog"):
                R.ok("%s status=%s detail=%s" % (p.get("name"), p.get("status"), (p.get("detail") or "")[:160]))


if __name__ == "__main__":
    main()
