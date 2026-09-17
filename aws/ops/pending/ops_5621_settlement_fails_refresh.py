"""Refresh settlement fails only after its exact source release is proven on AWS."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

FN = "justhodl-settlement-fails"
BUCKET = "justhodl-dashboard-live"
KEY = "data/settlement-fails.json"
COMMIT = "1961cf0d877ed130416b743d168aabc9318ff4c8"


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1",
                       config=Config(read_timeout=220, connect_timeout=10, retries={"max_attempts": 0}))
    with report("ops_5621_settlement_fails_refresh") as r:
        r.heading("Settlement fails source and publication proof")
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
        response = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        body = json.loads(response["Payload"].read() or b"{}")
        if response.get("FunctionError") or body.get("statusCode") != 200:
            raise RuntimeError("Lambda refresh failed; inspect its CloudWatch log")
        obj = s3.get_object(Bucket=BUCKET, Key=KEY)
        feed = json.loads(obj["Body"].read())
        quality = feed.get("quality") or {}
        if obj["LastModified"] < started:
            raise RuntimeError("Refresh did not publish a new data object")
        assert feed.get("version") == "1.1.0", "Unexpected engine version"
        assert quality.get("status") == "fresh", "Required Treasury inputs are not fresh"
        assert quality.get("missing") == [], "Required Treasury inputs are missing"
        assert feed["headline"]["scope"] == "ust_ex_tips"
        assert feed["treasury"]["scope_id"] == "treasury_incl_tips"
        assert feed["totals"]["scope"] == "all_asset"
        r.kv(version=feed["version"], as_of=feed["as_of"], generated_at=feed["generated_at"],
             quality_status=quality["status"], observation_date=quality["observation_date"],
             treasury_ex_tips_usd_bn=feed["headline"]["combined_bn"],
             treasury_including_tips_usd_bn=feed["treasury"]["gross_bn"])
        r.ok("Exact release and newly published Treasury scope/quality contract verified")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("Settlement refresh failed:", type(exc).__name__)
        sys.exit(1)
