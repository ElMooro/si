#!/usr/bin/env python3
"""ops 5641 — invoke liquidity-flow after attach lands."""
import json
import boto3


def main():
    lam = boto3.client("lambda", region_name="us-east-1")
    r = lam.invoke(FunctionName="justhodl-liquidity-flow", InvocationType="RequestResponse")
    payload = r["Payload"].read()
    print(payload[:2000])
    s3 = boto3.client("s3", region_name="us-east-1")
    body = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/liquidity-flow.json")["Body"].read())
    print(json.dumps({
        "generated_at": body.get("generated_at"),
        "has_pd_settlement_fails": "pd_settlement_fails" in body,
        "pd": body.get("pd_settlement_fails"),
        "net": (body.get("current") or {}).get("net_liquidity_b"),
    }))


if __name__ == "__main__":
    main()
