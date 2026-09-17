#!/usr/bin/env python3
import json, sys, boto3

def main():
    try:
        lam = boto3.client("lambda", region_name="us-east-1")
        r = lam.invoke(FunctionName="justhodl-market-extremes", InvocationType="RequestResponse")
        print(r["Payload"].read()[:800])
        s3 = boto3.client("s3", region_name="us-east-1")
        body = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/market-extremes.json")["Body"].read())
        print(json.dumps({"generated_at": body.get("generated_at"), "quality": body.get("quality"), "pd": body.get("pd_settlement_fails"), "call": body.get("call")}))
        return 0
    except Exception as e:
        print("FAIL", type(e).__name__, e)
        sys.exit(1)

if __name__ == "__main__":
    sys.exit(main() or 0)
