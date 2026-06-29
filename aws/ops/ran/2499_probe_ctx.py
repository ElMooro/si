import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k): return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
print("===== smart-beta.json =====")
sb=g("data/smart-beta.json")
print("keys:",list(sb.keys()))
for fld in ["factor_regime","leading_factor","lagging_factor","factors","by_factor"]:
    if fld in sb: print(f" .{fld}:",json.dumps(sb[fld])[:260])
print("\n===== liquidity-flow.json =====")
lf=g("data/liquidity-flow.json")
print("keys:",list(lf.keys()))
print(" regime:",json.dumps(lf.get("regime"))[:200])
print(" interpretation:",str(lf.get("interpretation"))[:220])
print(" current:",json.dumps(lf.get("current"))[:240])
print(" deltas:",json.dumps(lf.get("deltas"))[:240])
print("DONE 2499")
