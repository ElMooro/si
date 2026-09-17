#!/usr/bin/env python3
"""ops 5612 — invoke justhodl-sovereign-stress after 2.4.7 deploy (d60c3a82)."""
import json
import time
import boto3
from botocore.config import Config

FN = "justhodl-sovereign-stress"
BUCKET = "justhodl-dashboard-live"
KEY = "data/sovereign-stress.json"


def main():
    lam = boto3.client("lambda", config=Config(read_timeout=400, retries={"max_attempts": 0}))
    s3 = boto3.client("s3")
    t0 = time.time()
    raw = lam.invoke(
        FunctionName=FN,
        InvocationType="RequestResponse",
        Payload=b"{}",
    )
    body = json.loads(raw["Payload"].read() or b"{}")
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    feed = json.loads(obj["Body"].read())
    ciss = feed.get("systemic_stress_ciss") or {}
    tw = (ciss.get("united_kingdom") or {})
    tw_asia = ((feed.get("asia_sovereigns") or {}).get("taiwan") or {})
    print(json.dumps({
        "ops": 5612,
        "invoke_status": raw.get("StatusCode"),
        "fn_error": raw.get("FunctionError"),
        "body": body,
        "elapsed_s": round(time.time() - t0, 1),
        "version": feed.get("version"),
        "generated_at": feed.get("generated_at"),
        "quality": feed.get("quality"),
        "most_stressed_sovereign": feed.get("most_stressed_sovereign"),
        "worst_country": (feed.get("europe_stress") or {}).get("worst_country"),
        "uk_ciss_as_of": tw.get("as_of"),
        "uk_yoy_pct": tw.get("yoy_pct"),
        "ea_ciss_as_of": (ciss.get("euro_area") or {}).get("as_of"),
        "taiwan_cds_bp": tw_asia.get("cds_bp"),
        "taiwan_default_prob": tw_asia.get("cds_default_prob_pct"),
    }, indent=2, default=str))
    assert feed.get("version") == "2.4.7", feed.get("version")
    assert feed.get("quality"), "missing quality block"
    if tw_asia.get("cds_bp") is None:
        assert tw_asia.get("cds_default_prob_pct") is None, tw_asia


if __name__ == "__main__":
    main()
