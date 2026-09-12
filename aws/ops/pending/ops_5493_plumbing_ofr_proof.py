"""ops_5493 -- invoke plumbing after OFR compiler deploy."""
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
FN = "justhodl-brief-compiler"


def main():
    with report("ops_5493_plumbing_ofr_proof") as R:
        R.heading("ops 5493 plumbing OFR proof after 3212")
        lam = boto3.client("lambda", region_name="us-east-1")
        s3 = boto3.client("s3", region_name="us-east-1")
        resp = lam.invoke(
            FunctionName=FN,
            InvocationType="RequestResponse",
            Payload=json.dumps({"mode": "plumbing"}).encode("utf-8"),
        )
        R.ok("invoke status=%s" % resp.get("StatusCode"))
        time.sleep(2)
        brief = json.loads(s3.get_object(Bucket=B, Key="data/plumbing-brief.json")["Body"].read())
        fields = brief.get("fields") or {}
        inputs = brief.get("inputs") or {}
        R.ok("brief status=%s source=%s at=%s" % (brief.get("status"), brief.get("source"), brief.get("generated_at")))
        R.ok("why=%s" % brief.get("why"))
        if "data/ofr-funding.json" not in inputs:
            R.fail("ofr-funding still missing from inputs -- package not live")
            sys.exit(1)
        R.ok("ofr required=%s freshness=%s" % (
            inputs["data/ofr-funding.json"].get("required"),
            inputs["data/ofr-funding.json"].get("freshness")))
        if fields.get("ofr_sofr") is None:
            R.fail("ofr_sofr still empty")
            sys.exit(1)
        R.ok("ofr_sofr=%s triparty=%s dvp=%s gcf=%s" % (
            fields.get("ofr_sofr"), fields.get("ofr_triparty_rate"),
            fields.get("ofr_dvp_rate"), fields.get("ofr_gcf_rate")))
        if brief.get("status") != "LIVE":
            R.fail("brief not LIVE")
            sys.exit(1)


if __name__ == "__main__":
    main()
