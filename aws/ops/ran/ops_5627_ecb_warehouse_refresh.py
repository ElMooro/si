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

BUCKET = "justhodl-dashboard-live"
COMMIT = "3695c76e2fc9944a75890946c8e34bfce948fb06"
TARGETS = (("justhodl-ciss-stress", "data/ciss-stress.json"),
           ("justhodl-sovereign-stress", "data/sovereign-stress.json"),
           ("justhodl-euro-fragmentation", "data/euro-fragmentation.json"))


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1",
                       config=Config(read_timeout=650, connect_timeout=10, retries={"max_attempts": 0}))
    with report("ops_5627_ecb_warehouse_refresh") as r:
        r.heading("Canonical ECB warehouse and consumer proof")
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
            assert quality.get("status") in ("fresh", "incomplete"), "Current ECB contract unavailable"
            if FN == "justhodl-ciss-stress":
                assert feed.get("version") == "1.2.0"
                assert quality["status"] == "fresh" and feed.get("ea_composite") is not None
                warehouse = feed
                r.kv(function=FN, generated_at=feed["generated_at"], n_series=feed["n_series"],
                     observation_date=feed["ea_composite_date"], quality_status=quality["status"])
            else:
                assert feed.get("ciss_warehouse", {}).get("generated_at") == warehouse["generated_at"], "Consumer used another warehouse snapshot"
                if FN == "justhodl-sovereign-stress":
                    assert feed.get("version") == "2.5.0"
                    assert feed["most_stressed_sovereign"] == feed["europe_stress"]["worst_country"]
                    assert abs(feed["systemic_stress_ciss"]["euro_area"]["level"] - warehouse["ea_composite"]) < 0.0001
                    assert all(row.get("yoy_pct") is None for row in feed["systemic_stress_ciss"].values())
                    r.kv(function=FN, version=feed["version"], quality_status=quality["status"],
                         worst_country=feed["most_stressed_sovereign"], generated_at=feed["generated_at"])
                else:
                    for cc,row in feed["countries"].items():
                        candidates = [r for r in warehouse["series"] if r.get("area") == cc and ".SOV_CIN." in r.get("key", "")]
                        if candidates and row.get("sovciss") is not None:
                            latest = max(candidates, key=lambda r:r["latest_date"])
                            assert abs(row["sovciss"] - latest["latest"]) < 0.00001, "Divergent sovereign observations"
                    r.kv(function=FN, quality_status=quality["status"], generated_at=feed["generated_at"],
                         missing=quality.get("missing"), score=feed["fragmentation"]["score_0_100"])
            r.ok("Exact source release and newly published measurement contract verified")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("ECB warehouse refresh failed:", type(exc).__name__)
        sys.exit(1)
