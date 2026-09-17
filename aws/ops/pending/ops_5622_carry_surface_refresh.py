"""Refresh carry surface only after its exact source release is proven on AWS."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

FN = "justhodl-carry-surface"
BUCKET = "justhodl-dashboard-live"
KEY = "data/carry-surface.json"
COMMIT = "ca4096e33dad6a0847ee8cd6326e0150a6d4984e"


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1",
                       config=Config(read_timeout=650, connect_timeout=10, retries={"max_attempts": 0}))
    with report("ops_5622_carry_surface_refresh") as r:
        r.heading("Carry surface source and publication proof")
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
        assert feed.get("version") == "1.5.0", "Unexpected engine version"
        assert feed.get("public_history_review") == "20260910.v1", "Unreviewed public output"
        assert quality.get("status") in ("fresh", "incomplete"), "Refresh did not produce usable current observations"
        assert feed.get("n_assets", 0) > 0, "No observed carry assets were published"
        commodities = (feed.get("by_class") or {}).get("commodity", [])
        assert commodities and all(row.get("carry_pct") is None for row in commodities), "Invented roll yield"
        assert feed.get("methodology_version") == "carry-income.v2"
        r.kv(version=feed["version"], generated_at=feed["generated_at"], n_assets=feed["n_assets"],
             quality_status=quality["status"], observation_date=quality["observation_date"],
             missing_inputs=len(quality.get("missing", [])), financing_rate_pct=feed.get("financing_rate_pct"),
             source_bytes=receipt.get("source", {}).get("lambda_function.py", {}).get("bytes"))
        r.ok("Exact large-file release and reviewed current publication verified; unavailable curves remain null")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("Carry refresh failed:", type(exc).__name__)
        sys.exit(1)
