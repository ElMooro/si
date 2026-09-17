#!/usr/bin/env python3
"""ops 5691 — publish risk-regime packet that already contains FTD/FTR in source."""
import json
import sys
import boto3


def main():
    try:
        lam = boto3.client("lambda", region_name="us-east-1")
        r = lam.invoke(FunctionName="justhodl-risk-regime", InvocationType="RequestResponse")
        raw = r["Payload"].read()
        print("invoke", raw[:500])
        s3 = boto3.client("s3", region_name="us-east-1")
        body = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/risk-regime.json")["Body"].read())
        print(json.dumps({
            "generated_at": body.get("generated_at"),
            "version": body.get("version"),
            "has_pd": "pd_settlement_fails" in body,
            "pd": body.get("pd_settlement_fails"),
            "regime": body.get("risk_regime") or body.get("regime"),
        }))
        return 0
    except Exception as e:
        print("FAIL", type(e).__name__, e)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main() or 0)
