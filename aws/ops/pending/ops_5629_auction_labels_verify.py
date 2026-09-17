"""Refresh auction detector only after its exact source release is proven on AWS."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

BUCKET = "justhodl-dashboard-live"
COMMIT = "9ea610bab154247e9a6941d73a1b460e2ca41f41"
TARGETS = (("justhodl-auction-crisis-detector", "data/auction-crisis.json"),)


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1",
                       config=Config(read_timeout=650, connect_timeout=10, retries={"max_attempts": 0}))
    with report("ops_5629_auction_labels_verify") as r:
        r.heading("Auction final labels source and publication proof")
        for FN, KEY in TARGETS:
            deadline = time.monotonic() + 12 * 60
            while True:
                try:
                    receipt = json.loads(s3.get_object(Bucket=BUCKET, Key=f"data/ops/releases/{FN}.json")["Body"].read())
                except Exception:
                    receipt = {}
                if receipt.get("commit") == COMMIT:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("Expected release receipt did not arrive; function was not invoked")
                time.sleep(15)
            config = lam.get_function_configuration(FunctionName=FN)
            if config.get("CodeSha256") != receipt.get("code_sha256"):
                raise RuntimeError("Live Lambda code hash differs from the expected release receipt")
            r.kv(function=FN, commit=COMMIT, code_sha256=receipt["code_sha256"])
            started = datetime.now(timezone.utc)
            response = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b'{"suppress_alerts":true}')
            body = json.loads(response["Payload"].read() or b"{}")
            if response.get("FunctionError") or body.get("statusCode") != 200:
                raise RuntimeError("Lambda refresh failed; inspect its CloudWatch log")
            obj = s3.get_object(Bucket=BUCKET, Key=KEY)
            feed = json.loads(obj["Body"].read())
            quality = feed.get("quality") or {}
            if obj["LastModified"] < started:
                raise RuntimeError("Refresh did not publish a new data object")
            assert feed.get("methodology_version") == "auction-labels.v2", "Old auction methodology"
            assert feed.get("recent_auctions"), "Auction records missing"
            assert feed.get("tail_risk") and all(row.get("probability") is None for row in feed["tail_risk"].values())
            assert feed.get("metric_definitions", {}).get("allocated_at_high_pct"), "Missing proration definition"
            r.kv(function=FN, methodology=feed["methodology_version"], generated_at=feed["generated_at"],
                 latest_auction_date=feed.get("freshness", {}).get("latest_auction_date"))
            r.ok("Exact source release and newly published measurement contract verified")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("TIC/auction refresh failed:", type(exc).__name__)
        sys.exit(1)
