"""ops_5566 — re-invoke futures curves after sort=ticker|date fallback."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120, retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
BANNED = {"CL", "ES", "SI", "HG", "NG", "GC", "NQ"}


def main():
    with report("ops_5566_futures_retry") as R:
        r = LAM.invoke(FunctionName="justhodl-polygon-futures-curves",
                       InvocationType="RequestResponse", Payload=b"{}")
        err = r.get("FunctionError")
        raw = (r.get("Payload").read() if r.get("Payload") else b"") or b"{}"
        R.ok("invoke FunctionError=%s body=%s" % (err, raw.decode("utf-8", "replace")[:300]))
        fut = json.loads(S3.get_object(Bucket=B, Key="data/polygon-futures-curves.json")["Body"].read())
        R.ok("status=%s identity_ok=%s n=%s version=%s" % (
            fut.get("status"), fut.get("identity_ok"), fut.get("n_products_with_data"), fut.get("version")))
        for product, series in (fut.get("product_data") or {}).items():
            for row in series or []:
                t = str(row.get("ticker") or "").upper()
                if t in BANNED:
                    R.fail("equity ticker %s still published as %s" % (t, product))
                    sys.exit(1)
                R.ok("%s %s px=%s venue=%s" % (product, t, row.get("latest_price"), row.get("trading_venue")))
        ident = fut.get("identity") or {}
        for p, rec in ident.items():
            R.ok("identity %s status=%s http=%s err=%s n_contracts=%s" % (
                p, rec.get("status"), rec.get("contracts_http"),
                str(rec.get("error") or "")[:80], len(rec.get("contracts") or [])))
        if err:
            R.fail("lambda FunctionError")
            sys.exit(1)
        R.ok("GREEN")


if __name__ == "__main__":
    main()
