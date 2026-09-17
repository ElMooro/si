"""Refresh liquidity agent only after its exact source release is proven on AWS."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

FN = "justhodl-liquidity-agent"
BUCKET = "justhodl-dashboard-live"
KEY = "liquidity-data.json"
COMMIT = "de162e4186afc30e6f76541c1e836670bda006b3"


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1",
                       config=Config(read_timeout=180, connect_timeout=10, retries={"max_attempts": 0}))
    with report("ops_5625_liquidity_agent_refresh") as r:
        r.heading("Liquidity agent source and publication proof")
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
        assert feed.get("meta", {}).get("agent_version") == "2.1.1", "Unexpected engine version"
        assert quality.get("status") == "fresh", "Required core observations are not current"
        assert feed.get("spy_signal", {}).get("direction") is None, "Unvalidated directional forecast"
        assert feed.get("spy_signal", {}).get("confidence") is None, "Uncalibrated forecast confidence"
        core = feed.get("core") or {}
        assert core.get("net_liquidity", {}).get("value_bn") is not None, "Missing balance-sheet proxy"
        r.kv(version=feed["meta"]["agent_version"], generated_at=feed["meta"]["generated_at"],
             quality_status=quality["status"], observation_date=quality["observation_date"],
             input_dates=quality.get("input_dates"), net_proxy_usd_bn=core["net_liquidity"]["value_bn"],
             source_bytes=receipt.get("source", {}).get("lambda_function.py", {}).get("bytes"))
        r.ok("Exact source release and current core publication verified; unsupported forecasts expired")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("Liquidity refresh failed:", type(exc).__name__)
        sys.exit(1)
