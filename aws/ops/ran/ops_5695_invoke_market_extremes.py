#!/usr/bin/env python3
import json, sys, time, boto3

def main():
    try:
        lam = boto3.client("lambda", region_name="us-east-1")
        r = lam.invoke(FunctionName="justhodl-market-extremes", InvocationType="RequestResponse")
        print(r["Payload"].read()[:600])
        time.sleep(2)
        s3 = boto3.client("s3", region_name="us-east-1")
        body = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/market-extremes.json")["Body"].read())
        print(json.dumps({
            "generated_at": body.get("generated_at"),
            "quality": body.get("quality"),
            "pd": body.get("pd_settlement_fails"),
            "posture": body.get("posture"),
        }))
        if not body.get("quality") and not body.get("pd_settlement_fails"):
            print("WARN live zip still pre-5693")
        return 0
    except Exception as e:
        print("FAIL", type(e).__name__, e)
        sys.exit(1)

if __name__ == "__main__":
    sys.exit(main() or 0)
